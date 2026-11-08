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
	if werr == nil && x.Stop != nil {
		r.SeenAt, r.Step, r.StepsDone, r.StepsTotal, r.Forced = x.Stop.SeenAt, x.Stop.Step, x.Stop.StepsDone, x.Stop.StepsTotal, x.Stop.Forced
		r.GraceSeconds = x.Stop.GraceSeconds
		r.Graceful = !x.Stop.Forced
	} else if werr == nil {
		r.Graceful = true // it finished (or failed) on its own as the cancel arrived
	}

	// 3. escalate: the task host did not stop (wedged, or a group would not die)
	groups := groupsOf(dir)
	if werr != nil && !errors.Is(werr, taskhost.ErrDead) {
		r.Escalated = true
		r.Problems = append(r.Problems, fmt.Sprintf("the task host did not stop within %.0f s; the controller killed its process groups", budget.Seconds()))
		elog.Warn("cancel mode: escalating to a controller kill", werr, "dir", dir, "profile", prof.Name)
		deadline := time.Now().Add(StopHardCap)
		for time.Now().Before(deadline) {
			if pid := taskhost.HostPID(dir); pid > 0 {
				procs.KillGroup(pid)
			}
			for _, g := range groupsOf(dir) {
				procs.KillGroup(g)
			}
			if taskhost.Alive(dir) != taskhost.Running && !anyAlive(groupsOf(dir)) {
				break
			}
			time.Sleep(stopPoll)
		}
		groups = groupsOf(dir)
	}

	// 4. verify the tree is gone
	r.TreeVerified = waitGone(groups, TreeVerifyWait)
	if !r.TreeVerified {
		r.Problems = append(r.Problems, "process group(s) still alive after the kill: "+joinInts(aliveOf(groups)))
	}

	// 5. GPU memory
	r.GPU = "not_used"
	if prof.GPU {
		r.GPU = waitGPUReleased(groups, GPUReleaseWait)
		if r.GPU == "held" {
			r.Problems = append(r.Problems, "nvidia-smi still lists a process of the cancelled render")
		}
	}

	// 6. clean up — only a verified stop removes the run directory
	r.StoppedAt = time.Now().UTC()
	r.Seconds = r.StoppedAt.Sub(asked).Seconds()
	if r.TreeVerified {
		r.ScratchBytesRemoved = dirBytes(dir)
		if err := os.RemoveAll(dir); err != nil {
			r.Problems = append(r.Problems, "could not remove the run directory: "+err.Error())
			r.RunDirKept = true
		}
	} else {
		r.RunDirKept = true
		elog.Error("cancel mode: the render could not be verified stopped; its run directory is kept for the sweep",
			errors.New(r.Summary()), "dir", dir, "profile", prof.Name)
	}

	// 7. report
	rep.Log("cancel mode: " + r.Summary())
	if sr, ok := rep.(StopRecorder); ok {
		sr.Stopped(*r)
	}
	return r
}

// stopFromExit is the report for a task host that stopped on a cancel file the controller did not write in this
// run (the CLI, a previous controller, the sweep): it already stopped; the controller verifies and cleans.
func stopFromExit(dir string, x taskhost.Exit, rep Reporter) *jobs.StopReport {
	t, _ := taskhost.ReadTask(dir)
	prof := profileOf(t)
	r := &jobs.StopReport{Profile: prof.Name, Stage: string(jobs.StageRunning), GraceSeconds: maxGrace(t, prof).Seconds(), Graceful: true}
	if x.Stop != nil {
		r.AskedAt, r.SeenAt, r.Step, r.StepsDone, r.StepsTotal, r.Forced = x.Stop.AskedAt, x.Stop.SeenAt, x.Stop.Step, x.Stop.StepsDone, x.Stop.StepsTotal, x.Stop.Forced
		r.Graceful = !x.Stop.Forced
		r.StoppedAt = x.Stop.ExitedAt
	}
	groups := groupsOf(dir)
	r.TreeVerified = waitGone(groups, TreeVerifyWait)
	r.GPU = "not_used"
	if prof.GPU {
		r.GPU = waitGPUReleased(groups, GPUReleaseWait)
	}
	if r.AskedAt.IsZero() {
		r.AskedAt = x.FinishedAt
	}
	if r.StoppedAt.IsZero() {
		r.StoppedAt = x.FinishedAt
	}
	r.Seconds = r.StoppedAt.Sub(r.AskedAt).Seconds()
	if r.TreeVerified {
		r.ScratchBytesRemoved = dirBytes(dir)
	} else {
		r.RunDirKept = true
	}
	rep.Log("cancel mode: " + r.Summary())
	if sr, ok := rep.(StopRecorder); ok {
		sr.Stopped(*r)
	}
	return r
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

func groupsOf(dir string) []int {
	p, err := taskhost.ReadProcs(dir)
	if err != nil {
		return nil
	}
	var out []int
	for _, g := range p.Groups {
		if g.PID > 1 {
			out = append(out, g.PID)
		}
	}
	return out
}

func anyAlive(groups []int) bool { return len(aliveOf(groups)) > 0 }

func aliveOf(groups []int) []int {
	var out []int
	for _, g := range groups {
		if procs.GroupAlive(g) {
			out = append(out, g)
		}
	}
	return out
}

func waitGone(groups []int, wait time.Duration) bool {
	deadline := time.Now().Add(wait)
	for {
		alive := aliveOf(groups)
		if len(alive) == 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		for _, g := range alive {
			procs.KillGroup(g) // a straggler of a group the task host already killed
		}
		time.Sleep(stopPoll / 4)
	}
}

// waitGPUReleased: "released", "held" or "not_checked".
func waitGPUReleased(groups []int, wait time.Duration) string {
	deadline := time.Now().Add(wait)
	for {
		pids, ok := gpuPIDs()
		if !ok {
			return "not_checked"
		}
		if !holdsGPU(pids, groups) {
			return "released"
		}
		if time.Now().After(deadline) {
			return "held"
		}
		time.Sleep(stopPoll)
	}
}

// holdsGPU: one of the GPU's processes belongs to the cancelled tree (its process group is one of ours; on Windows,
// which has no process groups, it is one of the step processes themselves).
func holdsGPU(gpu []int, groups []int) bool {
	ours := map[int]bool{}
	for _, g := range groups {
		ours[g] = true
	}
	for _, pid := range gpu {
		if ours[pid] {
			return true
		}
		if g, ok := pgidOf(pid); ok && ours[g] {
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

func joinInts(v []int) string {
	s := make([]string, len(v))
	for i, n := range v {
		s[i] = strconv.Itoa(n)
	}
	return strings.Join(s, ", ")
}
