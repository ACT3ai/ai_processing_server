// Package taskhost is the detached process that owns one running job (pm/task_queue.mdx §9.3). The controller
// starts it from the job's code snapshot with `ai_processing_server taskhost --run-dir <dir>`, in its own session
// (or, on Windows, its own detached process group), so stopping, killing or replacing the controller never stops the
// job. It talks to the controller only through files in the run directory (package runs) and needs no network to
// the broker.
//
// The task host runs the parent spec's lifecycle for one attempt: STAGING (inputs) → RUNNING (the runner) →
// fence check → UPLOADING (outputs), then writes result.json and exit.json.
package taskhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"sync"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/errlog"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/logging"
	"github.com/ACT3ai/ai_processing_server/internal/runners"
	"github.com/ACT3ai/ai_processing_server/internal/runs"
	"github.com/ACT3ai/ai_processing_server/internal/storage"
)

var elog = errlog.For("taskhost")

var (
	errTimeout    = errors.New("job exceeded its timeout")
	errRunDirGone = errors.New("the run directory was removed; stopping")
)

// Launch starts a task host for runDir from the snapshot binary, detached from the caller. extraArgs are passed
// through (--error-dir, --log-dir) so the task host writes where its controller does. It returns the pid; a
// goroutine reaps the process so it never lingers as a zombie while the controller lives.
func Launch(binary, runDir string, extraArgs []string) (int, error) {
	args := append([]string{"taskhost", "--run-dir", runDir}, extraArgs...)
	cmd := exec.Command(binary, args...)
	cmd.Dir = runDir
	logf, err := os.OpenFile(filepath.Join(runDir, runs.HostLog), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, err
	}
	defer logf.Close()
	cmd.Stdout, cmd.Stderr, cmd.Stdin = logf, logf, nil
	detach(cmd)
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start task host %s: %w", binary, err)
	}
	pid := cmd.Process.Pid
	elog.Go("reaping a task host", func() { _ = cmd.Wait() })
	return pid, nil
}

// Run is the body of `ai_processing_server taskhost --run-dir <dir>`. It always writes exit.json before returning,
// unless the run directory itself is gone.
func Run(runDir string, log *slog.Logger) (code int) {
	started := time.Now().UTC()
	var task runs.Task
	if err := runs.ReadJSON(runDir, runs.TaskFile, &task); err != nil {
		elog.Error("reading task.json", err, "run_dir", runDir)
		writeResult(runDir, runs.Result{Kind: jobs.KindInternal, Message: "task host could not read task.json: " + err.Error()})
		writeExit(runDir, 2)
		return 2
	}
	if task.RunVersion != runs.Version {
		writeResult(runDir, runs.Result{Kind: jobs.KindInfra, Message: fmt.Sprintf("run_version %d is not %d", task.RunVersion, runs.Version)})
		writeExit(runDir, 2)
		return 2
	}
	job := task.Job
	log = log.With("ctx", "taskhost", "job_id", job.ID, "service", job.Service)
	h := &host{dir: runDir, task: &task, log: log}
	h.openLog()
	defer h.closeLog()

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	timeout := time.Duration(job.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = time.Hour
	}
	ctx, stopTimer := context.WithTimeoutCause(ctx, timeout, errTimeout)
	defer stopTimer()

	elog.Go("writing the task host heartbeat", func() { h.heartbeat(ctx, started, cancel) })

	res := h.attempt(ctx, timeout)
	res.Seconds = time.Since(started).Seconds()
	if !task.KeepWorkDir {
		elog.Check("deleting the work dir after the job", os.RemoveAll(filepath.Join(task.WorkDir, job.ID)), "job_id", job.ID)
	}
	if errors.Is(context.Cause(ctx), errRunDirGone) {
		return 3 // nobody to report to
	}
	if res.OK {
		h.Log(fmt.Sprintf("done in %.1fs", res.Seconds))
	} else if res.Fenced {
		h.Log("another worker owns this job now; outputs discarded")
	} else {
		h.Log(fmt.Sprintf("failed in %s: %s: %s", res.Stage, res.Kind, res.Message))
	}
	writeResult(runDir, res)
	writeExit(runDir, 0)
	return 0
}

type host struct {
	dir  string
	task *runs.Task
	log  *slog.Logger
	mu   sync.Mutex
	logf *os.File
	ev   *os.File
}

// heartbeat rewrites heartbeat.json and watches for a cancel file or a removed run directory.
func (h *host) heartbeat(ctx context.Context, started time.Time, cancel context.CancelCauseFunc) {
	pid := os.Getpid()
	beat := func() {
		elog.Ignore("writing heartbeat.json", runs.WriteJSON(h.dir, runs.HeartbeatFile, runs.Heartbeat{PID: pid, StartedAt: started, At: time.Now().UTC()}))
	}
	beat()
	tBeat := time.NewTicker(runs.HeartbeatEvery)
	tWatch := time.NewTicker(200 * time.Millisecond)
	defer tBeat.Stop()
	defer tWatch.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tBeat.C:
			beat()
		case <-tWatch.C:
			if _, err := os.Stat(h.dir); errors.Is(err, os.ErrNotExist) {
				cancel(errRunDirGone)
				return
			}
			var c runs.Cancel
			if runs.ReadJSON(h.dir, runs.CancelFile, &c) == nil {
				h.Log("cancel requested: " + c.Reason)
				cancel(&jobs.Error{Kind: c.Kind, Message: c.Reason})
				return
			}
		}
	}
}

// attempt runs the lifecycle and classifies the outcome.
func (h *host) attempt(ctx context.Context, timeout time.Duration) (res runs.Result) {
	task, job := h.task, h.task.Job
	def := task.Service
	stage := string(jobs.Staging)
	defer func() {
		if r := recover(); r != nil {
			h.Log(fmt.Sprintf("panic: %v\n%s", r, debug.Stack()))
			errlog.For("runner."+def.Runner).Panic("running "+job.Service, r, "job_id", job.ID)
			res = runs.Result{Kind: jobs.KindInternal, Message: fmt.Sprintf("runner %s panicked: %v", def.Runner, r), Stage: stage}
		}
	}()
	fail := func(err error) runs.Result {
		kind, msg := classify(ctx, err)
		recordUnexpected(job, kind, stage, msg, err)
		return runs.Result{Kind: kind, Message: msg, Stage: stage}
	}

	runner := runners.All()[def.Runner]
	if runner == nil {
		return fail(jobs.Errf(jobs.KindInfra, "runner %q is not built into this binary", def.Runner))
	}
	workDir := filepath.Join(task.WorkDir, job.ID)
	outDir, inDir := filepath.Join(workDir, "out"), filepath.Join(workDir, "in")
	for _, d := range []string{outDir, inDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return fail(jobs.Wrap(jobs.KindInfra, err, "cannot create the work dir"))
		}
	}
	slot := jobs.Slot{}
	if job.Slot != nil {
		slot = *job.Slot
	}
	env := runners.Env{WorkDir: workDir, OutDir: outDir, Inputs: map[string]string{}, Slot: slot, Engines: task.Engines,
		Logger: h.log.With("ctx", "runner."+def.Runner)}
	h.Log(fmt.Sprintf("attempt %d of %s on %s (timeout %s, code %s, task host pid %d)", job.Attempt, job.Service, slot.ID, timeout, task.Snapshot, os.Getpid()))
	if err := runner.Check(ctx, job, env); err != nil {
		return fail(err)
	}

	// STAGING: inputs.
	h.line(runs.Line{Type: "stage", State: string(jobs.Staging)})
	stageCtx, stageCancel := context.WithTimeoutCause(ctx, def.Timeouts.StageInputs.Or(10*time.Minute), errTimeout)
	defer stageCancel()
	for i, in := range job.Envelope.Inputs {
		h.Log("staging input " + in.Name + " " + logging.RedactURL(in.URL) + in.Path)
		path, n, sum, err := storage.Fetch(stageCtx, in, inDir, task.AllowLocal)
		if err != nil {
			return fail(err)
		}
		env.Inputs[in.Name] = path
		h.Log(fmt.Sprintf("staged %s: %d bytes sha256 %s", in.Name, n, sum))
		h.Progress(0, fmt.Sprintf("staged %d/%d inputs", i+1, len(job.Envelope.Inputs)))
	}
	if ctx.Err() != nil {
		return fail(context.Cause(ctx))
	}

	// RUNNING.
	stage = string(jobs.Running)
	h.line(runs.Line{Type: "stage", State: stage})
	out, err := runner.Run(ctx, job, env, h)
	if err != nil {
		return fail(err)
	}
	if ctx.Err() != nil { // cancelled after the runner's last check: upload nothing
		return fail(context.Cause(ctx))
	}

	// Fence before anything leaves this machine (§16.3): the controller confirms we still own the claim.
	switch ok, err := h.fence(ctx); {
	case err != nil:
		return fail(err)
	case !ok:
		return runs.Result{Fenced: true, Stage: stage}
	}

	// UPLOADING.
	stage = string(jobs.Uploading)
	h.line(runs.Line{Type: "stage", State: stage})
	upCtx, upCancel := context.WithTimeoutCause(ctx, def.Timeouts.Upload.Or(20*time.Minute), errTimeout)
	defer upCancel()
	for i := range out.Outputs {
		if err := h.deliver(upCtx, job, &out.Outputs[i]); err != nil {
			return fail(err)
		}
	}
	if ctx.Err() != nil {
		return fail(context.Cause(ctx))
	}
	res = runs.Result{OK: true, Outputs: out.Outputs, Stage: stage}
	if out.Data != nil {
		if b, err := json.Marshal(out.Data); err == nil {
			res.Data = b
		}
	}
	return res
}

// fence asks the controller to compare-and-set job_state and waits for its answer. A controller restart in between
// is fine: the next controller re-adopts this job and answers the same request.
func (h *host) fence(ctx context.Context) (bool, error) {
	if err := os.WriteFile(filepath.Join(h.dir, runs.FenceRequest), []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0o600); err != nil {
		return false, jobs.Wrap(jobs.KindInfra, err, "write fence_request")
	}
	h.Log("waiting for the controller to confirm the claim before upload")
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		switch {
		case runs.Exists(h.dir, runs.FenceOK):
			return true, nil
		case runs.Exists(h.dir, runs.FenceLost):
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, context.Cause(ctx)
		case <-t.C:
		}
	}
}

// deliver hashes an output and either uploads it to the envelope's URL or keeps it under results/<job_id>/.
func (h *host) deliver(ctx context.Context, job *jobs.Job, o *jobs.Output) error {
	n, sum, err := storage.HashFile(o.Path)
	if err != nil {
		return jobs.Wrap(jobs.KindInternal, err, "output "+o.Name+" missing")
	}
	o.Bytes, o.SHA256, o.File = n, sum, filepath.Base(o.Path)
	for _, spec := range job.Envelope.Outputs {
		if spec.Name == o.Name && spec.Upload != nil {
			ct := o.ContentType
			if spec.ContentType != "" {
				ct = spec.ContentType
			}
			h.Log(fmt.Sprintf("uploading %s (%d bytes) to %s", o.Name, n, logging.RedactURL(spec.Upload.URL)))
			if err := storage.Put(ctx, o.Path, *spec.Upload, ct); err != nil {
				return err
			}
			o.UploadedTo = logging.RedactURL(spec.Upload.URL)
			if !job.Envelope.DataRules.RetainAfterUpload {
				return nil
			}
		}
	}
	dst := filepath.Join(h.task.ResultsDir, job.ID, o.File)
	if err := storage.MoveFile(o.Path, dst); err != nil {
		return jobs.Wrap(jobs.KindInfra, err, "keep output "+o.Name)
	}
	o.Path = dst
	o.Download = "/v1/jobs/" + job.ID + "/outputs/" + o.Name
	return nil
}

// ---- runners.Reporter: lines into events.jsonl and the per-job log ----

func (h *host) openLog() {
	var err error
	if h.logf, err = os.OpenFile(h.task.LogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600); err != nil {
		elog.Error("opening the per-job log", err, "job_id", h.task.Job.ID)
	}
	if h.ev, err = os.OpenFile(filepath.Join(h.dir, runs.EventsFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600); err != nil {
		elog.Error("opening events.jsonl", err, "job_id", h.task.Job.ID)
	}
}

func (h *host) closeLog() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, f := range []*os.File{h.logf, h.ev} {
		if f != nil {
			elog.Check("closing a task host file", f.Close(), "job_id", h.task.Job.ID)
		}
	}
	h.logf, h.ev = nil, nil
}

// line appends one event line. Each line is written with a single write so the controller never reads half of one.
func (h *host) line(l runs.Line) {
	l.At = time.Now().UTC()
	b, _ := json.Marshal(l)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ev != nil {
		if _, err := h.ev.Write(append(b, '\n')); err != nil {
			elog.Error("appending to events.jsonl", err, "job_id", h.task.Job.ID)
		}
	}
}

func (h *host) Stage(name string) {
	h.line(runs.Line{Type: "step", Step: name})
	h.Log("step " + name)
}

func (h *host) Progress(frac float64, note string) {
	h.line(runs.Line{Type: "progress", Progress: min(max(frac, 0), 1), Note: note})
	if note != "" {
		h.Log(fmt.Sprintf("progress %.3f %s", frac, note))
	}
}

func (h *host) Log(line string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.logf != nil {
		fmt.Fprintf(h.logf, "[%s] %s\n", time.Now().Format("2006-01-02T15:04:05.000Z07:00"), line)
	}
}

// ---- outcome ----

func writeResult(dir string, r runs.Result) {
	elog.Check("writing result.json", runs.WriteJSON(dir, runs.ResultFile, r), "run_dir", dir)
}

func writeExit(dir string, code int) {
	elog.Check("writing exit.json", runs.WriteJSON(dir, runs.ExitFile, runs.Exit{Code: code, At: time.Now().UTC()}), "run_dir", dir)
}

// classify turns any error into one kind and one sentence (spec §12.2), looking at why the context ended first.
func classify(ctx context.Context, err error) (jobs.ErrorKind, string) {
	var je *jobs.Error
	cause := context.Cause(ctx)
	switch {
	case errors.As(cause, &je) && ctx.Err() != nil: // a cancel file: its kind and reason
		return je.Kind, je.Message
	case errors.Is(cause, errRunDirGone):
		return jobs.KindInfra, errRunDirGone.Error()
	case errors.Is(cause, errTimeout) || errors.Is(err, errTimeout) || errors.Is(err, context.DeadlineExceeded):
		return jobs.KindTimeout, "job exceeded its timeout"
	}
	if errors.As(err, &je) {
		msg := je.Message
		if je.Err != nil {
			msg += ": " + je.Err.Error()
		}
		return je.Kind, msg
	}
	return jobs.KindInternal, err.Error()
}

// recordUnexpected sends the failures that point at our code or this machine to error.err. Expected failures
// (bad input, content policy, cancel, a timeout, a transient network error) stay in the job record and job log.
func recordUnexpected(job *jobs.Job, kind jobs.ErrorKind, stage, msg string, err error) {
	switch {
	case kind == jobs.KindInternal:
		elog.Error("running "+job.Service, err, "job_id", job.ID, "stage", stage, "kind", kind)
	case kind == jobs.KindInfra:
		elog.Warn("running "+job.Service+" (infra: this machine or an engine)", err, "job_id", job.ID, "stage", stage)
	}
}
