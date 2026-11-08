package runners

// CANCEL MODE for the render types that run in a detached task host (pm/cancel.mdx §5–§8).
//
// When a job is cancelled (ACT3 production → render-server agent → POST /v1/queue/jobs/{id}/cancel → job_control →
// the scheduler cancels the job's context), the runner does not just drop the job. It puts the render into cancel
// mode and returns only once the render is VERIFIABLY stopped and cleaned up:
//
//  1. ask:      write runs/<job_id>/cancel. The task host sees it within CancelPoll, interrupts the step's process
//               group (SIGINT; on Windows the task script sees ACT3_CANCEL_FILE), and kills the group once the
//               render type's grace runs out. Task scripts we own unwind their finally blocks (stop a private
//               server, close an ffmpeg pipe, free CUDA memory) inside that grace.
//  2. wait:     for exit.json, at most CancelPoll + grace + StopMargin.
//  3. escalate: no exit.json → the task host itself is wedged: the controller kills the task host's own group and
//               every group in procs.json, again every second, until they are gone or StopHardCap passes.
//  4. verify:   every process group in procs.json is gone (signal 0).
//  5. GPU:      on an NVIDIA machine, nvidia-smi lists none of the tree's processes (VRAM released).
//  6. clean:    only a verified stop removes the run directory (inputs, segments, checkpoints, scratch, partial
//               output). An unverified one keeps it for the sweep and says so in error.err.
//  7. report:   a jobs.StopReport goes on the job record and into the final `cancelled` event.
//
// The slot (GPU, cores, RAM) stays held for the whole stop because the runner has not returned: the next job is
// never started on a GPU the cancelled render still occupies.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/procs"
	"github.com/ACT3ai/ai_processing_server/internal/taskhost"
)

// CancelProfile is how one render type is stopped (pm/cancel.mdx §8, one row per type).
type CancelProfile struct {
	Name string
	// Grace: how long the tree gets after the interrupt before the task host kills it.
	Grace time.Duration
	// GPU: the render holds GPU memory; on NVIDIA the stop waits until it is released.
	GPU bool
}

// The profiles (pm/cancel.mdx §8). The graces are measured, not guessed: a diffusion step on the 5090 is 1–9 s, so
// 20 s lets the step in flight end and the finally blocks run; a private ComfyUI takes up to ~25 s to shut down
// after its queue is interrupted; ffmpeg finalises in under a second.
var (
	ProfileSeedVR2      = CancelProfile{Name: "seedvr2", Grace: 20 * time.Second, GPU: true}
	ProfileFlashVSR     = CancelProfile{Name: "flashvsr", Grace: 15 * time.Second, GPU: true}
	ProfileWan          = CancelProfile{Name: "wan_2_2", Grace: 20 * time.Second, GPU: true}
	ProfileLTX          = CancelProfile{Name: "ltx_2_5", Grace: 20 * time.Second, GPU: true}
	ProfileKandinsky    = CancelProfile{Name: "kandinsky_6", Grace: 20 * time.Second, GPU: true}
	ProfileH3           = CancelProfile{Name: "minimax_h3", Grace: 45 * time.Second, GPU: true}
	ProfileH3Regenerate = CancelProfile{Name: "open_h3_regenerate", Grace: 45 * time.Second, GPU: true}
	ProfileFFmpeg       = CancelProfile{Name: "ffmpeg", Grace: 5 * time.Second}
	ProfileFake         = CancelProfile{Name: "fake", Grace: 3 * time.Second}
)

// Profiles lists every profile, for the spec's table test and `ai_processing_server cancel --profiles`.
func Profiles() []CancelProfile {
	return []CancelProfile{ProfileSeedVR2, ProfileFlashVSR, ProfileWan, ProfileLTX, ProfileKandinsky, ProfileH3,
		ProfileH3Regenerate, ProfileFFmpeg, ProfileFake}
}

func profileByName(name string) (CancelProfile, bool) {
	for _, p := range Profiles() {
		if p.Name == name {
			return p, true
		}
	}
	return CancelProfile{}, false
}

func (p CancelProfile) spec() *taskhost.CancelSpec {
	return &taskhost.CancelSpec{Profile: p.Name, GraceSeconds: p.Grace.Seconds()}
}

// Timing of the stop. Variables so tests can shorten them.
var (
	// StopMargin is added to CancelPoll + grace before the controller decides the task host did not stop.
	StopMargin = 15 * time.Second
	// StopHardCap bounds the escalation: after it, the stop is reported unverified and the run directory is kept.
	StopHardCap = 3 * time.Minute
	// TreeVerifyWait is how long a killed group may take to disappear (the kernel reaping it).
	TreeVerifyWait = 10 * time.Second
	// GPUReleaseWait is how long VRAM may take to be released after the processes are gone.
	GPUReleaseWait = 30 * time.Second
	// stopPoll is the escalation and verification poll.
	stopPoll = time.Second
	// gpuPIDs lists the processes holding NVIDIA GPU memory; ok=false when there is no nvidia-smi. Tests replace it.
	gpuPIDs = nvidiaComputePIDs
)

// StopRecorder is implemented by the scheduler's reporter: the stop report goes onto the job record.
type StopRecorder interface {
	Stopped(r jobs.StopReport)
}

// stopTask puts the render in dir into cancel mode and returns once it is stopped (or the hard cap passed).
func stopTask(dir string, rep Reporter) *jobs.StopReport {
	asked := time.Now().UTC()
	t, _ := taskhost.ReadTask(dir)
	prof := profileOf(t)
	r := &jobs.StopReport{Profile: prof.Name, Stage: string(jobs.StageRunning), AskedAt: asked, GraceSeconds: maxGrace(t, prof).Seconds()}
	rep.Log(fmt.Sprintf("cancel mode: asking the %s task host to stop (grace %.0f s)", prof.Name, r.GraceSeconds))
	if err := taskhost.Cancel(dir); err != nil {
		r.Problems = append(r.Problems, "could not write the cancel file: "+err.Error())
	}

	// 2. wait for the task host's own stop
	budget := taskhost.CancelPoll + maxGrace(t, prof) + StopMargin
	wctx, done := context.WithTimeout(context.Background(), budget)
	x, werr := taskhost.Watch(wctx, dir, nil)
	done()
	switch {
	case werr == nil && x.Stop != nil:
		r.SeenAt, r.Step, r.StepsDone, r.StepsTotal, r.Forced = x.Stop.SeenAt, x.Stop.Step, x.Stop.StepsDone, x.Stop.StepsTotal, x.Stop.Forced
		r.GraceSeconds, r.EscapedKilled = x.Stop.GraceSeconds, x.Stop.Escaped
		r.Graceful = !x.Stop.Forced
		r.How = map[bool]string{true: "forced", false: "graceful"}[x.Stop.Forced]
	case werr == nil:
		r.Graceful, r.How = true, "graceful" // it finished (or failed) on its own as the cancel arrived
	case errors.Is(werr, taskhost.ErrDead):
		r.How = "host_gone" // the task host was already gone (crash, reboot): only its leftovers are stopped below
	}

	// 3. escalate: the task host did not stop (wedged, or a group would not die)
	tr := treeOf(dir)
	if werr != nil && !errors.Is(werr, taskhost.ErrDead) {
		r.Escalated, r.How = true, "escalated"
		r.Problems = append(r.Problems, fmt.Sprintf("the task host did not stop within %.0f s; the controller killed its process tree", budget.Seconds()))
		elog.Warn("cancel mode: escalating to a controller kill", werr, "dir", dir, "profile", prof.Name)
		deadline := time.Now().Add(StopHardCap)
		for time.Now().Before(deadline) {
			tr = treeOf(dir)
			if len(tr.alive()) == 0 {
				break
			}
			tr.kill()
			time.Sleep(stopPoll)
		}
	}

	finishStop(r, dir, prof, rep, true)
	return r
}

// stopFromExit is the report for a task host that stopped on a cancel file the controller did not write in this
// run (the CLI, a previous controller, the sweep): it already stopped; the controller verifies and cleans.
func stopFromExit(dir string, x taskhost.Exit, rep Reporter) *jobs.StopReport {
	t, _ := taskhost.ReadTask(dir)
	prof := profileOf(t)
	r := &jobs.StopReport{Profile: prof.Name, Stage: string(jobs.StageRunning), GraceSeconds: maxGrace(t, prof).Seconds(),
		Graceful: true, How: "graceful"}
	if x.Stop != nil {
		r.AskedAt, r.SeenAt, r.Step, r.StepsDone, r.StepsTotal, r.Forced = x.Stop.AskedAt, x.Stop.SeenAt, x.Stop.Step, x.Stop.StepsDone, x.Stop.StepsTotal, x.Stop.Forced
		r.Graceful = !x.Stop.Forced
		if x.Stop.Forced {
			r.How = "forced"
		}
		r.StoppedAt = x.Stop.ExitedAt
	}
	if r.AskedAt.IsZero() {
		r.AskedAt = x.FinishedAt
	}
	finishStop(r, dir, prof, rep, false)
	return r
}

// StopRunDir is cancel mode for a run directory whose job is not running in this controller (a job cancelled while
// it waited for a slot after a restart, its task host alive, dead or finished): stop whatever is there, verify, clean,
// report. It never launches or relaunches anything (pm/cancel.mdx §5.6). logf may be nil.
func StopRunDir(dir string, logf func(string)) *jobs.StopReport {
	if logf == nil {
		logf = func(string) {}
	}
	return stopTask(dir, funcReporter(logf))
}

type funcReporter func(string)

func (f funcReporter) Stage(string)             {}
func (f funcReporter) Progress(float64, string) {}
func (f funcReporter) Log(l string)             { f(l) }

// finishStop does steps 4–7 for both entry points: verify the tree, the GPU, clean, report. removeDir=false leaves the
// directory to the caller.
func finishStop(r *jobs.StopReport, dir string, prof CancelProfile, rep Reporter, removeDir bool) {
	tr := treeOf(dir)
	// 4. verify the tree is gone (stragglers are killed while waiting)
	deadline := time.Now().Add(TreeVerifyWait)
	for {
		alive := tr.alive()
		if len(alive) == 0 {
			r.TreeVerified = true
			break
		}
		if time.Now().After(deadline) {
			r.Problems = append(r.Problems, "still alive after the kill: "+strings.Join(alive, ", "))
			break
		}
		tr.kill()
		time.Sleep(stopPoll / 4)
		tr = treeOf(dir)
	}
	if r.EscapedKilled > 0 {
		r.Problems = append(r.Problems, fmt.Sprintf("%d process(es) had left the render's process group and were killed separately", r.EscapedKilled))
	}

	// 5. GPU memory
	r.GPU = "not_used"
	if prof.GPU {
		r.GPU = waitGPUReleased(tr, GPUReleaseWait)
		if r.GPU == "held" {
			r.Problems = append(r.Problems, "nvidia-smi still lists a process of the cancelled render")
		}
	}

	// 6. clean up — only a verified stop removes the run directory
	if r.StoppedAt.IsZero() || r.StoppedAt.Before(r.AskedAt) {
		r.StoppedAt = time.Now().UTC()
	}
	if r.TreeVerified {
		r.StoppedAt = time.Now().UTC()
	}
	r.Seconds = r.StoppedAt.Sub(r.AskedAt).Seconds()
	if r.TreeVerified {
		r.ScratchBytesRemoved = dirBytes(dir)
		if removeDir {
			if err := os.RemoveAll(dir); err != nil {
				r.Problems = append(r.Problems, "could not remove the run directory: "+err.Error())
				r.RunDirKept = true
			}
		}
	} else {
		r.RunDirKept = true
	}
	if !r.Clean() {
		elog.Error("cancel mode: the stop needs attention", errors.New(r.Summary()), "dir", dir, "profile", prof.Name)
	}

	// 7. report
	rep.Log("cancel mode: " + r.Summary())
	if sr, ok := rep.(StopRecorder); ok {
		sr.Stopped(*r)
	}
}

// tree is everything a render's run directory may still have running, identified safely (pm/cancel.mdx §5.3–5.4):
// the task host (pid + start time), each step that has not ended (process group, identity-checked), each step's
// Windows Job Object, and any process carrying the run's ACT3_RUN_DIR marker that left its group.
type tree struct {
	dir                string
	host               int
	hostAt             time.Time
	hostKnown, hostOur bool
	steps              []taskhost.ProcStep
}

func treeOf(dir string) tree {
	t := tree{dir: dir}
	if pid, at, ok := taskhost.HostProcess(dir); ok {
		t.host, t.hostAt, t.hostKnown = pid, at, true
		t.hostOur = procs.SameProcess(pid, at)
	}
	if p, err := taskhost.ReadProcs(dir); err == nil {
		for _, g := range p.Groups {
			if g.EndedAt == nil && g.PID > 1 {
				t.steps = append(t.steps, g)
			}
		}
	}
	return t
}

// escaped lists processes of this render outside its recorded groups: carriers of its ACT3_RUN_DIR marker (Linux)
// and descendants of the task host and of its live steps by parentage (Linux, macOS), each by identity.
func (t tree) escaped() []procs.Proc {
	var out []procs.Proc
	seen := map[int]bool{t.host: true}
	add := func(p procs.Proc) {
		if !seen[p.PID] {
			seen[p.PID] = true
			out = append(out, p)
		}
	}
	for _, pid := range procs.FindByEnv(taskhost.EnvRunDir, t.dir) {
		if at, ok := procs.StartTime(pid); ok {
			add(procs.Proc{PID: pid, StartedAt: at})
		}
	}
	roots := []int{}
	if t.hostOur {
		roots = append(roots, t.host)
	}
	for _, g := range t.steps {
		if procs.SameProcess(g.PID, g.StartedAt) {
			roots = append(roots, g.PID)
		}
	}
	for _, r := range roots {
		for _, p := range procs.Descendants(r) {
			add(p)
		}
	}
	return out
}

// alive describes what is still running; empty means the whole tree is gone.
func (t tree) alive() []string {
	var out []string
	if t.hostOur && procs.SameProcess(t.host, t.hostAt) {
		out = append(out, fmt.Sprintf("task host %d", t.host))
	}
	for _, g := range t.steps {
		if procs.OurGroup(g.PID, g.StartedAt) {
			out = append(out, fmt.Sprintf("step %q group %d", g.Step, g.PID))
		}
		if g.JobName != "" {
			if pids, ok := procs.JobPIDs(g.JobName); ok && len(pids) > 0 {
				out = append(out, fmt.Sprintf("step %q job object (%d processes)", g.Step, len(pids)))
			}
		}
	}
	for _, p := range t.escaped() {
		if procs.SameProcess(p.PID, p.StartedAt) {
			out = append(out, fmt.Sprintf("process %d", p.PID))
		}
	}
	return out
}

// kill kills everything alive() would list, each identity-checked first.
func (t tree) kill() {
	if t.hostOur && procs.SameProcess(t.host, t.hostAt) {
		procs.KillGroup(t.host) // the task host is a session and group leader (setsid)
	}
	for _, g := range t.steps {
		if g.JobName != "" {
			procs.KillJob(g.JobName)
		}
		if procs.OurGroup(g.PID, g.StartedAt) {
			procs.KillGroup(g.PID)
		}
	}
	for _, p := range t.escaped() {
		if procs.SameProcess(p.PID, p.StartedAt) {
			procs.KillPID(p.PID)
		}
	}
}

// pids is every pid that may hold the GPU for this render, for the nvidia-smi check.
func (t tree) pids() (pids map[int]bool, groups map[int]bool) {
	pids, groups = map[int]bool{}, map[int]bool{}
	for _, g := range t.steps {
		groups[g.PID] = true
		pids[g.PID] = true
		if g.JobName != "" {
			if jp, ok := procs.JobPIDs(g.JobName); ok {
				for _, p := range jp {
					pids[p] = true
				}
			}
		}
	}
	for _, p := range t.escaped() {
		pids[p.PID] = true
	}
	return pids, groups
}

// profileOf is the task's profile from task.json; an older task.json has none, so it falls back to the default
// grace under the name "task".
func profileOf(t taskhost.Task) CancelProfile {
	if t.Cancel != nil {
		if p, ok := profileByName(t.Cancel.Profile); ok {
			if t.Cancel.GraceSeconds > 0 {
				p.Grace = time.Duration(t.Cancel.GraceSeconds * float64(time.Second))
			}
			return p
		}
		return CancelProfile{Name: t.Cancel.Profile, Grace: time.Duration(t.Cancel.GraceSeconds * float64(time.Second)), GPU: true}
	}
	return CancelProfile{Name: "task", Grace: procs.GraceBeforeKill, GPU: true}
}

// maxGrace is the longest grace any of the task's steps can take.
func maxGrace(t taskhost.Task, p CancelProfile) time.Duration {
	g := p.Grace
	if g <= 0 {
		g = procs.GraceBeforeKill
	}
	for _, st := range t.Steps {
		if d := time.Duration(st.GraceSeconds * float64(time.Second)); d > g {
			g = d
		}
	}
	return g
}

// waitGPUReleased: "released", "held" or "not_checked".
func waitGPUReleased(tr tree, wait time.Duration) string {
	deadline := time.Now().Add(wait)
	for {
		gpu, ok := gpuPIDs()
		if !ok {
			return "not_checked"
		}
		pids, groups := tr.pids()
		if !holdsGPU(gpu, pids, groups) {
			return "released"
		}
		if time.Now().After(deadline) {
			return "held"
		}
		time.Sleep(stopPoll)
	}
}

// holdsGPU: one of the GPU's processes belongs to the cancelled tree: one of its pids (step leaders, Job Object members,
// escaped marker carriers) or, on Unix, a member of one of its process groups. Another job's process is not ours.
func holdsGPU(gpu []int, pids, groups map[int]bool) bool {
	for _, pid := range gpu {
		if pids[pid] {
			return true
		}
		if g, ok := pgidOf(pid); ok && groups[g] {
			return true
		}
	}
	return false
}

// nvidiaComputePIDs runs nvidia-smi. Not an NVIDIA machine (the Mac Studio): ok=false.
func nvidiaComputePIDs() ([]int, bool) {
	if runtime.GOOS == "darwin" {
		return nil, false
	}
	bin, err := exec.LookPath("nvidia-smi")
	if err != nil {
		return nil, false
	}
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()
	out, err := exec.CommandContext(ctx, bin, "--query-compute-apps=pid", "--format=csv,noheader").Output()
	if err != nil {
		return nil, false
	}
	var pids []int
	for _, l := range strings.Split(string(out), "\n") {
		if n, err := strconv.Atoi(strings.TrimSpace(l)); err == nil && n > 0 {
			pids = append(pids, n)
		}
	}
	return pids, true
}

func dirBytes(dir string) int64 {
	var n int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if fi, e := d.Info(); e == nil {
				n += fi.Size()
			}
		}
		return nil
	})
	return n
}
