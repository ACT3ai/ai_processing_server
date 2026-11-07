package runners

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/errlog"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
	"github.com/ACT3ai/ai_processing_server/internal/taskhost"
)

// Shared machinery for the open-source model runners (oss_video, oss_upscale): every render runs in a DETACHED task
// host started from a frozen code snapshot, so a git pull and a controller restart never stop a render that is
// underway (pm/task_queue.mdx §9, pm/task_authoring.mdx R1–R4, R6, R9–R12).
//
// Run is idempotent per job id (runner.go Reattacher). The run directory data/git_ignore/runs/<job_id>/ decides:
//
//	no task.json            → snapshot, copy the inputs in, launch the task host
//	task host alive         → re-attach (the controller restarted while it rendered)
//	exit.json written       → collect what it produced while the controller was down
//	task host gone          → resume from its last checkpoint when it has one (R8, a segmented upscale) on the
//	                          same snapshot; otherwise start over (machine reboot); the run directory is replaced
//
// When the job's context ends because the controller is stopping, Run returns WITHOUT touching the task host: the
// next controller re-attaches. Only a cancel or a timeout writes the cancel file.

var elog = errlog.For("runners")

// StateDir is data/git_ignore (main sets it). Empty: derived from the job's work dir (<state>/work/<job_id>).
var StateDir string

// CodeRoot is the repo checkout the snapshot is taken from (main sets it). Empty: the directory of the running
// binary, which `just build` puts at the repo root. $APS_CODE_ROOT overrides both.
var CodeRoot string

// HostCommand returns the argv prefix that starts a task host from a snapshot. Tests replace it.
var HostCommand = func(s snapshot.Snapshot) []string { return []string{s.Binary(), "taskhost"} }

// FakeModels runs code/scripts/fake/fake_render.py instead of the model ($APS_FAKE_MODELS=on). Never in production.
func FakeModels() bool {
	v := os.Getenv("APS_FAKE_MODELS")
	return v == "on" || v == "1" || v == "true"
}

// MaxTaskSeconds caps a task host's timeout. 24 h, the upscale engine's ceiling: a 30 s 4K 7B upscale on the Mac
// Studio takes ~13 h.
const MaxTaskSeconds = 24 * 3600

func stateDir(env Env) string {
	if StateDir != "" {
		return StateDir
	}
	return filepath.Dir(filepath.Dir(env.WorkDir))
}

func codeRoot() (string, error) {
	if v := os.Getenv("APS_CODE_ROOT"); v != "" {
		return v, nil
	}
	if CodeRoot != "" {
		return CodeRoot, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	exe, _ = filepath.EvalSymlinks(exe)
	return filepath.Dir(exe), nil
}

func runDirFor(env Env, jobID string) string {
	return filepath.Join(stateDir(env), "runs", safeID(jobID))
}

func safeID(id string) string {
	return regexp.MustCompile(`[^A-Za-z0-9._-]`).ReplaceAllString(id, "_")
}

// detachedTask is what a runner wants run: built against the snapshot, with the run directory's own input copies.
type detachedTask struct {
	Argv         []string
	Dir          string
	Env          []string
	Output       string           // inside the run directory
	Steps        []taskhost.Step  // instead of Argv: checkpointed steps, the last one writes Output (R8)
	ExpectFrames int              // the output must have about this many frames (0: any number above zero)
	Phases       []taskhost.Phase // one process's counted passes, weighted into one bar (SeedVR2)
}

// buildFunc builds the task. snap is the frozen code; inputs maps input name → its copy inside the run directory;
// out is where the task must write its video.
type buildFunc func(snap snapshot.Snapshot, inputs map[string]string, out string) (detachedTask, error)

// failureKind classifies a failed render from its output (R12): out of memory is infra (a bigger machine may take
// it), anything else internal.
var reOOM = regexp.MustCompile(`(?i)out of memory|OutOfMemory|Invalid buffer size|Killed: 9|larger than INT_MAX|without overflow`)

// reDiskFull: the machine ran out of disk mid-render. Infra, so the job is retried, on another machine if this
// one stays full (the scheduler stops taking work while the disk is low).
var reDiskFull = regexp.MustCompile(`(?i)no space left on device|ENOSPC|disk (is )?full|not enough space on the disk|quota exceeded`)

func failureKind(tail string) jobs.ErrorKind {
	// A task script's own verdict wins over the regexes (render_h3_regenerate.py ends a failure with ONE line
	// "ACT3_ERROR <kind>: <sentence>"): an echoed prompt that mentions "out of memory" never makes a bad job retried.
	if k, ok := scriptErrorKind(tail); ok {
		return k
	}
	if reOOM.MatchString(tail) || reDiskFull.MatchString(tail) {
		return jobs.KindInfra
	}
	return jobs.KindInternal
}

var reScriptError = regexp.MustCompile(`(?m)^ACT3_ERROR ([a-z_]+):`)

// scriptErrorKind maps the LAST "ACT3_ERROR <kind>:" line of a task's output to a job error kind. not_installed is
// infra (another server may have the model); license_not_cleared is invalid_input (no retry can fix it).
func scriptErrorKind(tail string) (jobs.ErrorKind, bool) {
	m := reScriptError.FindAllStringSubmatch(tail, -1)
	if len(m) == 0 {
		return "", false
	}
	switch m[len(m)-1][1] {
	case "invalid_input", "license_not_cleared":
		return jobs.KindInvalidInput, true
	case "infra", "not_installed":
		return jobs.KindInfra, true
	case "internal":
		return jobs.KindInternal, true
	}
	return "", false
}

// runDetached is Run for every task-host runner. outName is the output's name ("video").
func runDetached(ctx context.Context, job *jobs.Job, env Env, rep Reporter, outName string, build buildFunc) (*Result, error) {
	dir := runDirFor(env, job.ID)
	switch st := taskhost.Alive(dir); st {
	case taskhost.Running:
		rep.Log("re-attached to the task host already rendering this job (" + dir + ")")
	case taskhost.Finished:
		rep.Log("the task host finished this job while the controller was down; collecting it")
	case taskhost.Dead, taskhost.Missing:
		resumed := false
		var deaths []string
		if st == taskhost.Dead {
			// A task host that keeps dying (each time taking the machine down, so the job is re-adopted at boot under
			// the same attempt) would otherwise be started here forever. After MaxDeaths it fails as infra: the
			// queue's retry policy moves it on, to another machine or to DEAD.
			if n := taskhost.RecordDeath(dir); n >= taskhost.MaxDeaths {
				elog.Ignore("removing the run directory of a task host that keeps dying", os.RemoveAll(dir), "job_id", job.ID)
				return nil, jobs.Errf(jobs.KindInfra, "the task host for this job died %d times on this machine without finishing "+
					"(a reboot or crash each time); not starting it here again", n)
			}
			deaths = taskhost.Deaths(dir)
			resumed = resume(env, rep, dir)
			if !resumed {
				rep.Log("the previous task host for this job is gone (reboot or crash); starting it again")
			}
		}
		if !resumed {
			if err := launch(job, env, rep, dir, build); err != nil {
				return nil, err
			}
			elog.Check("carrying the task host death count into the new run directory", taskhost.CarryDeaths(dir, deaths), "job_id", job.ID)
		}
	}

	x, err := taskhost.Watch(ctx, dir, func(e taskhost.Event) {
		switch e.Type {
		case "stage":
			rep.Stage(e.Stage)
		case "progress":
			rep.Progress(e.Progress, e.Note)
		case "note", "started":
			rep.Log(e.Type + ": " + e.Note)
		}
	})
	if errors.Is(err, taskhost.ErrDead) {
		// Keep the checkpoints: when the retry lands on this machine again it resumes (the run-dir sweep removes
		// them once nothing here holds the job).
		taskhost.RecordDeath(dir)
		if done, _ := taskhost.Checkpoints(dir); done == 0 {
			elog.Ignore("removing a dead task host's run directory", os.RemoveAll(dir), "job_id", job.ID)
		}
		return nil, jobs.Errf(jobs.KindInfra, "%v", err)
	}
	if err != nil { // ctx ended
		cause := context.Cause(ctx)
		if !stopsTheTask(cause) {
			rep.Log("controller stopping: the task host keeps rendering and the next controller re-attaches")
			return nil, cause
		}
		elog.Check("cancelling a task host", taskhost.Cancel(dir), "job_id", job.ID)
		wctx, done := context.WithTimeout(context.Background(), 30*time.Second)
		if _, werr := taskhost.Watch(wctx, dir, nil); werr != nil && !errors.Is(werr, taskhost.ErrDead) {
			elog.Warn("waiting for a cancelled task host to stop", werr, "job_id", job.ID, "dir", dir)
		}
		done()
		elog.Ignore("removing a cancelled run directory", os.RemoveAll(dir), "job_id", job.ID) // R9: nothing is uploaded; R10: scratch removed
		return nil, cause
	}

	t, err := taskhost.ReadTask(dir)
	if err != nil {
		elog.Ignore("removing a run directory with an unreadable task.json", os.RemoveAll(dir), "job_id", job.ID)
		return nil, jobs.Wrap(jobs.KindInfra, err, "read task.json of the finished task host")
	}
	if x.Code != 0 || x.OutputBytes == 0 {
		elog.Ignore("removing a failed run directory", os.RemoveAll(dir), "job_id", job.ID) // a retry starts clean
		if x.Cancelled {
			return nil, jobs.Errf(jobs.KindCancelled, "cancelled")
		}
		if x.TimedOut {
			return nil, jobs.Errf(jobs.KindTimeout, "%s", x.Error)
		}
		why := x.Error
		if x.Code == 0 {
			why = "exit 0 but no output was written"
		}
		return nil, jobs.Errf(failureKind(x.Tail), "render failed (%s): %s", why, lastLine(x.Tail))
	}
	// Never mark a render done on exit code alone: a truncated or empty-stream file would be uploaded and billed.
	if err := validateVideo(enginePath(env, "ffprobe"), t.Output, t.ExpectFrames); err != nil {
		elog.Ignore("removing a run directory whose output failed validation", os.RemoveAll(dir), "job_id", job.ID)
		return nil, jobs.Errf(jobs.KindInternal, "the render finished but its output is not a valid video: %v", err)
	} else if enginePath(env, "ffprobe") == "" {
		rep.Log("ffprobe not detected: the output was not validated")
	}
	dst := filepath.Join(env.OutDir, outName+filepath.Ext(t.Output))
	if err := os.MkdirAll(env.OutDir, 0o700); err != nil {
		return nil, jobs.Wrap(jobs.KindInfra, err, "create out dir")
	}
	if err := moveFile(t.Output, dst); err != nil {
		return nil, jobs.Wrap(jobs.KindInfra, err, "collect the render")
	}
	elog.Ignore("removing a collected run directory", os.RemoveAll(dir), "job_id", job.ID)
	return &Result{
		Outputs: []jobs.Output{{Name: outName, Path: dst, ContentType: "video/mp4"}},
		Data:    map[string]any{"code_version": t.Snapshot, "bytes": x.OutputBytes},
	}, nil
}

// resume relaunches a dead task host from its checkpoints (R8). It needs at least one finished step and the task's
// own snapshot still on disk, so the resumed steps run exactly the code the finished ones ran. False: start over.
func resume(env Env, rep Reporter, dir string) bool {
	done, total := taskhost.Checkpoints(dir)
	if done == 0 || done >= total {
		return false
	}
	t, err := taskhost.ReadTask(dir)
	if err != nil {
		return false
	}
	snap := SnapshotFor(env, t.Snapshot)
	if _, err := os.Stat(snap.Binary()); err != nil {
		rep.Log("the task host died after " + fmt.Sprint(done) + " of " + fmt.Sprint(total) + " steps, but its snapshot " + t.Snapshot + " is gone; starting over")
		return false
	}
	if err := taskhost.Relaunch(HostCommand(snap), dir); err != nil {
		elog.Error("resuming a task host from its checkpoint", err, "job_id", t.JobID)
		return false
	}
	rep.Log(fmt.Sprintf("the previous task host died (reboot or crash) after %d of %d steps; resuming at step %d from the checkpoint, snapshot %s",
		done, total, done+1, t.Snapshot))
	return true
}

// SnapshotFor is the snapshot a run directory's task.json names.
func SnapshotFor(env Env, key string) snapshot.Snapshot {
	return snapshot.Snapshot{Key: key, Dir: filepath.Join(stateDir(env), "snapshots", key)}
}

func launch(job *jobs.Job, env Env, rep Reporter, dir string, build buildFunc) error {
	root, err := codeRoot()
	if err != nil {
		return jobs.Wrap(jobs.KindInternal, err, "find the code root")
	}
	bin, err := os.Executable()
	if err != nil {
		return jobs.Wrap(jobs.KindInternal, err, "find the binary")
	}
	snap, err := snapshot.Ensure(root, bin, filepath.Join(stateDir(env), "snapshots"))
	if err != nil {
		return jobs.Wrap(jobs.KindInfra, err, "build the code snapshot")
	}
	if err := os.RemoveAll(dir); err != nil {
		return jobs.Wrap(jobs.KindInfra, err, "clear the old run directory")
	}
	// Inputs are copied (hard-linked when possible) into the run directory: the work dir is deleted at every
	// controller boot, and the task host may still be reading them then.
	inputs := map[string]string{}
	for name, p := range env.Inputs {
		dst := filepath.Join(dir, "inputs", safeID(name)+filepath.Ext(p))
		if err := linkOrCopy(p, dst); err != nil {
			return jobs.Wrap(jobs.KindInfra, err, "copy input "+name)
		}
		inputs[name] = dst
	}
	t, err := build(snap, inputs, filepath.Join(dir, "out", "output.mp4"))
	if err != nil {
		return err
	}
	timeout := job.Timeout
	if timeout <= 0 || timeout > MaxTaskSeconds {
		timeout = MaxTaskSeconds
	}
	task := taskhost.Task{JobID: job.ID, Snapshot: snap.Key, Argv: t.Argv, Dir: t.Dir, Env: t.Env, Output: t.Output, Phases: t.Phases,
		GPU: env.Slot.GPU, TimeoutSeconds: timeout, Steps: t.Steps, ExpectFrames: t.ExpectFrames, MinFreeRAMGB: env.MinFreeRAMGB}
	if err := os.MkdirAll(filepath.Dir(t.Output), 0o700); err != nil {
		return jobs.Wrap(jobs.KindInfra, err, "create the output dir")
	}
	if err := taskhost.Launch(HostCommand(snap), dir, task); err != nil {
		return jobs.Wrap(jobs.KindInfra, err, "start the task host")
	}
	if len(t.Steps) > 0 {
		rep.Log(fmt.Sprintf("task host started from snapshot %s: %d checkpointed steps", snap.Key, len(t.Steps)))
	} else {
		rep.Log(fmt.Sprintf("task host started from snapshot %s: %s", snap.Key, strings.Join(t.Argv, " ")))
	}
	return nil
}

// stopsTheTask: a cancel or a timeout stops the render; anything else (the controller stopping, a reload) leaves it.
func stopsTheTask(cause error) bool {
	if cause == nil {
		return false
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return true
	}
	s := strings.ToLower(cause.Error())
	return strings.Contains(s, "cancel") || strings.Contains(s, "timeout") || strings.Contains(s, "exceeded")
}

// validateVideo checks a finished render with ffprobe: a readable video stream with frames, and about expect frames
// when the task knows how many (a segmented upscale: every segment made it into the join). No ffprobe: not checked.
func validateVideo(ffprobe, path string, expect int) error {
	if ffprobe == "" {
		return nil
	}
	vf, err := probeFrames(ffprobe, path)
	if err != nil {
		return err
	}
	if expect > 0 {
		if tol := max(2, expect/100); vf.Frames < expect-tol || vf.Frames > expect+tol {
			return fmt.Errorf("it has %d frames, expected %d", vf.Frames, expect)
		}
	}
	return nil
}

// reGrammar: our own stage, progress and note lines never explain a failure. stdout and stderr are read
// separately, so the error a script prints on stderr can land before its last progress line.
var reGrammar = regexp.MustCompile(`^ACT3_(STAGE|PROGRESS|NOTE)\b`)

// reErrorLine: a line that reports the failure. Verbose tools print a cleanup section and a banner after it (SeedVR2
// ends every run with its sponsor links), so the last line is often not the reason.
var reErrorLine = regexp.MustCompile(`(?i)\b\w*(error|exception)\b|out of memory|traceback|killed|no such file|not found|failed`)

// lastLine names the failure: the last line that reports an error, else the last line of output.
func lastLine(tail string) string {
	lines := strings.Split(strings.TrimSpace(tail), "\n")
	last := ""
	for i := len(lines) - 1; i >= 0; i-- {
		l := strings.TrimSpace(lines[i])
		if l == "" || reGrammar.MatchString(l) {
			continue
		}
		if reErrorLine.MatchString(l) && !strings.HasPrefix(l, "Traceback") {
			return truncate(l, 400)
		}
		if last == "" {
			last = l
		}
	}
	if last == "" {
		return "no output"
	}
	return truncate(last, 400)
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func linkOrCopy(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	if os.Link(src, dst) == nil {
		return nil
	}
	return copyAll(src, dst)
}

func moveFile(src, dst string) error {
	if os.Rename(src, dst) == nil {
		return nil
	}
	if err := copyAll(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

func copyAll(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// accelerator is the PyTorch device this machine renders on.
func accelerator() string {
	if runtime.GOOS == "darwin" {
		return "mps"
	}
	return "cuda"
}

// renderHome is a model's install home: $ACT3_RENDER_HOME_<ID>, else $ACT3_RENDER_HOME/<id>, else ~/act3_render/<id>
// (the same rule the internal web app's render server uses, so one install serves both).
func renderHome(id string) string {
	if v := os.Getenv("ACT3_RENDER_HOME_" + strings.ToUpper(id)); v != "" {
		return v
	}
	base := os.Getenv("ACT3_RENDER_HOME")
	if base == "" {
		home, _ := os.UserHomeDir()
		base = filepath.Join(home, "act3_render")
	}
	return filepath.Join(base, id)
}

func venvPython(dir string) string {
	if runtime.GOOS == "windows" {
		return filepath.Join(dir, ".venv", "Scripts", "python.exe")
	}
	return filepath.Join(dir, ".venv", "bin", "python")
}

func fileAtLeast(p string, n int64) bool {
	st, err := os.Stat(p)
	return err == nil && !st.IsDir() && st.Size() >= n
}

// fakeTask runs fake_render.py from the snapshot (APS_FAKE_MODELS=on).
func fakeTask(snap snapshot.Snapshot, out, input, prompt string, w, h int) (detachedTask, error) {
	py := "python3"
	if runtime.GOOS == "windows" {
		py = "python"
	}
	secs := os.Getenv("APS_FAKE_SECONDS")
	if secs == "" {
		secs = "4"
	}
	argv := []string{py, snap.Path("code/scripts/fake/fake_render.py"), "--out", out, "--seconds", secs,
		"--width", fmt.Sprint(w), "--height", fmt.Sprint(h), "--prompt", prompt}
	if input != "" {
		argv = append(argv, "--input", input)
	}
	return detachedTask{Argv: argv, Output: out}, nil
}
