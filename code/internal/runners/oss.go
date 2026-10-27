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
//	task host gone          → start over (machine reboot); the run directory is replaced
//
// When the job's context ends because the controller is stopping, Run returns WITHOUT touching the task host: the
// next controller re-attaches. Only a cancel or a timeout writes the cancel file.

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

// MaxTaskSeconds caps a task host when the job has no timeout of its own.
const MaxTaskSeconds = 12 * 3600

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
	Argv   []string
	Dir    string
	Env    []string
	Output string // inside the run directory
}

// buildFunc builds the task. snap is the frozen code; inputs maps input name → its copy inside the run directory;
// out is where the task must write its video.
type buildFunc func(snap snapshot.Snapshot, inputs map[string]string, out string) (detachedTask, error)

// failureKind classifies a failed render from its output (R12): out of memory is infra (a bigger machine may take
// it), anything else internal.
var reOOM = regexp.MustCompile(`(?i)out of memory|OutOfMemory|Invalid buffer size|Killed: 9|larger than INT_MAX|without overflow`)

func failureKind(tail string) jobs.ErrorKind {
	if reOOM.MatchString(tail) {
		return jobs.KindInfra
	}
	return jobs.KindInternal
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
		if st == taskhost.Dead {
			rep.Log("the previous task host for this job is gone (reboot or crash); starting it again")
		}
		if err := launch(job, env, rep, dir, build); err != nil {
			return nil, err
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
		os.RemoveAll(dir)
		return nil, jobs.Errf(jobs.KindInfra, "%v", err)
	}
	if err != nil { // ctx ended
		cause := context.Cause(ctx)
		if !stopsTheTask(cause) {
			rep.Log("controller stopping: the task host keeps rendering and the next controller re-attaches")
			return nil, cause
		}
		taskhost.Cancel(dir)
		wctx, done := context.WithTimeout(context.Background(), 30*time.Second)
		taskhost.Watch(wctx, dir, nil)
		done()
		os.RemoveAll(dir) // R9: nothing is uploaded; R10: scratch removed
		return nil, cause
	}

	t, _ := taskhost.ReadTask(dir)
	if x.Code != 0 || x.OutputBytes == 0 {
		os.RemoveAll(dir) // a retry starts clean
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
	dst := filepath.Join(env.OutDir, outName+filepath.Ext(t.Output))
	if err := os.MkdirAll(env.OutDir, 0o700); err != nil {
		return nil, jobs.Wrap(jobs.KindInfra, err, "create out dir")
	}
	if err := moveFile(t.Output, dst); err != nil {
		return nil, jobs.Wrap(jobs.KindInfra, err, "collect the render")
	}
	os.RemoveAll(dir)
	return &Result{
		Outputs: []jobs.Output{{Name: outName, Path: dst, ContentType: "video/mp4"}},
		Data:    map[string]any{"code_version": t.Snapshot, "bytes": x.OutputBytes},
	}, nil
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
	os.RemoveAll(dir)
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
	task := taskhost.Task{JobID: job.ID, Snapshot: snap.Key, Argv: t.Argv, Dir: t.Dir, Env: t.Env, Output: t.Output,
		GPU: env.Slot.GPU, TimeoutSeconds: timeout}
	os.MkdirAll(filepath.Dir(t.Output), 0o700)
	if err := taskhost.Launch(HostCommand(snap), dir, task); err != nil {
		return jobs.Wrap(jobs.KindInfra, err, "start the task host")
	}
	rep.Log(fmt.Sprintf("task host started from snapshot %s: %s", snap.Key, strings.Join(t.Argv, " ")))
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

func lastLine(tail string) string {
	lines := strings.Split(strings.TrimSpace(tail), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if l := strings.TrimSpace(lines[i]); l != "" {
			return truncate(l, 400)
		}
	}
	return "no output"
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
