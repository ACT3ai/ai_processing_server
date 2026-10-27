package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/errlog"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/logging"
	"github.com/ACT3ai/ai_processing_server/internal/runners"
	"github.com/ACT3ai/ai_processing_server/internal/storage"
)

// execute runs one attempt of a job through STAGING → RUNNING → UPLOADING → REPORTING (spec §9.2).
// job is a private copy; every change to the shared record goes through s.update / s.finishLocked.
func (s *Server) execute(ctx context.Context, cancel context.CancelCauseFunc, job *jobs.Job, timeout time.Duration) {
	defer s.wg.Done()
	defer cancel(nil)
	defer elog.Recover("running the job lifecycle", "job_id", job.ID, "service", job.Type)
	ctx, stopTimer := context.WithTimeoutCause(ctx, timeout, errTimeout)
	defer stopTimer()

	s.mu.Lock()
	def := s.cat.Services[job.Type]
	runner := s.runners[def.Runner]
	engines := s.effective.Engines
	keep := s.cfg.Debug.KeepWorkDirs
	s.mu.Unlock()

	workDir := filepath.Join(s.workDir, job.ID)
	logPath := filepath.Join(s.jobLogDir, job.ID+".log")
	rep := newReporter(s, job.ID, logPath)
	defer rep.close()
	stage := jobs.StageStaging
	started := time.Now()

	var result *runners.Result
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				rep.Log(fmt.Sprintf("panic: %v\n%s", r, debug.Stack()))
				errlog.For("runner."+def.Runner).Panic("running "+job.Type, r, "job_id", job.ID)
				err = jobs.Errf(jobs.KindInternal, "runner %s panicked: %v", def.Runner, r)
			}
		}()
		if runner == nil {
			return jobs.Errf(jobs.KindInfra, "runner %q is not built into this binary", def.Runner)
		}
		outDir := filepath.Join(workDir, "out")
		inDir := filepath.Join(workDir, "in")
		for _, d := range []string{outDir, inDir} {
			if err := os.MkdirAll(d, 0o700); err != nil {
				return jobs.Wrap(jobs.KindInfra, err, "cannot create the work dir")
			}
		}
		env := runners.Env{WorkDir: workDir, OutDir: outDir, Inputs: map[string]string{}, Slot: *job.Slot,
			Engines: engines, Logger: s.log.With("ctx", "runner."+def.Runner, "job_id", job.ID, "service", job.Type)}
		rep.Log(fmt.Sprintf("attempt %d of %s on %s (timeout %s)", job.Retried+1, job.Type, job.Slot.ID, timeout))
		if err := runner.Check(ctx, job, env); err != nil {
			return err
		}

		// STAGING: inputs.
		stageCtx, stageCancel := context.WithTimeoutCause(ctx, def.Timeouts.StageInputs.Or(10*time.Minute), errTimeout)
		defer stageCancel()
		for i, in := range job.Payload.Inputs {
			rep.Log("staging input " + in.Name + " " + logging.RedactURL(in.URL) + in.Path)
			path, n, sum, err := storage.Fetch(stageCtx, in, inDir, job.Queue == "direct")
			if err != nil {
				return err
			}
			env.Inputs[in.Name] = path
			rep.Log(fmt.Sprintf("staged %s: %d bytes sha256 %s", in.Name, n, sum))
			rep.Progress(0, fmt.Sprintf("staged %d/%d inputs", i+1, len(job.Payload.Inputs)))
		}
		if err := context.Cause(ctx); ctx.Err() != nil {
			return err
		}

		// RUNNING.
		stage = jobs.StageRunning
		s.moveStage(job.ID, jobs.StageRunning, "")
		res, err := runner.Run(ctx, job, env, rep)
		if err != nil {
			return err
		}
		if ctx.Err() != nil { // cancelled after the runner's last check: upload nothing
			return context.Cause(ctx)
		}
		result = res

		// UPLOADING.
		stage = jobs.StageUploading
		s.moveStage(job.ID, jobs.StageUploading, "")
		upCtx, upCancel := context.WithTimeoutCause(ctx, def.Timeouts.Upload.Or(20*time.Minute), errTimeout)
		defer upCancel()
		for i := range result.Outputs {
			if err := s.deliver(upCtx, job, &result.Outputs[i], rep); err != nil {
				return err
			}
		}
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		stage = jobs.StageReporting
		s.moveStage(job.ID, jobs.StageReporting, "")
		return nil
	}()

	if !keep {
		// A work dir that cannot be deleted may hold customer data: always worth a line in error.err.
		elog.Check("deleting the work dir after the job", os.RemoveAll(workDir), "job_id", job.ID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[job.ID]
	if err == nil {
		j.Outputs = result.Outputs
		if result.Data != nil {
			if b, e := json.Marshal(result.Data); e == nil {
				j.Result = b
			}
		}
		j.Progress = 1
		s.finishLocked(j, jobs.Completed, "", jobs.StageDone, "done", nil)
		s.log.Info("job done", "job_id", j.ID, "service", j.Type, "platform", j.Queue, "slot", job.Slot.ID,
			"duration", time.Since(started).Round(time.Millisecond), "outputs", outputSummary(j.Outputs))
		return
	}
	kind, msg := classify(ctx, err)
	recordUnexpected(job, kind, stage, msg, err)
	rep.Log(fmt.Sprintf("failed in %s: %s: %s", stage, kind, msg))
	s.finishLocked(j, jobs.Archived, kind, stage, msg, &def.Retry.RetryOn)
}

// deliver hashes an output and either uploads it to the envelope's URL or keeps it under results/<job_id>/.
func (s *Server) deliver(ctx context.Context, job *jobs.Job, o *jobs.Output, rep *reporter) error {
	n, sum, err := storage.HashFile(o.Path)
	if err != nil {
		return jobs.Wrap(jobs.KindInternal, err, "output "+o.Name+" missing")
	}
	o.Bytes, o.SHA256, o.File = n, sum, filepath.Base(o.Path)
	for _, spec := range job.Payload.Outputs {
		if spec.Name == o.Name && spec.Upload != nil {
			ct := o.ContentType
			if spec.ContentType != "" {
				ct = spec.ContentType
			}
			rep.Log(fmt.Sprintf("uploading %s (%d bytes) to %s", o.Name, n, logging.RedactURL(spec.Upload.URL)))
			if err := storage.Put(ctx, o.Path, *spec.Upload, ct); err != nil {
				return err
			}
			o.UploadedTo = logging.RedactURL(spec.Upload.URL)
			if !job.Payload.DataRules.RetainAfterUpload {
				return nil
			}
		}
	}
	dst := filepath.Join(s.resultsDir, job.ID, o.File)
	if err := storage.MoveFile(o.Path, dst); err != nil {
		return jobs.Wrap(jobs.KindInfra, err, "keep output "+o.Name)
	}
	o.Path = dst
	o.Download = "/v1/jobs/" + job.ID + "/outputs/" + o.Name
	return nil
}

// finishLocked ends an attempt: release the slot, then complete, retry, cancel or archive (spec §12.1).
// retryOn is the service's retry policy; nil means "never retry" (cancel, deadline).
func (s *Server) finishLocked(j *jobs.Job, st jobs.State, kind jobs.ErrorKind, stage jobs.Stage, msg string, retryOn *[]string) {
	now := time.Now().UTC()
	wasActive := j.State == jobs.Active
	if j.Slot != nil {
		s.pool.Release(j.Slot.ID)
		j.Slot = nil
	}
	if wasActive {
		s.activeBy[j.Type]--
		s.active--
	}
	delete(s.cancels, j.ID)

	if kind == jobs.KindCancelled {
		st = jobs.Cancelled
	}
	switch st {
	case jobs.Completed:
		j.CompletedAt, j.FinishedAt = &now, &now
		j.ErrorKind, j.ErrorStage = "", ""
		j.Move(jobs.Completed, jobs.StageDone, msg)
	case jobs.Cancelled:
		j.FinishedAt = &now
		j.ErrorKind, j.ErrorStage, j.LastErr = jobs.KindCancelled, stage, msg
		j.Move(jobs.Cancelled, jobs.StageCancelled, msg)
		s.stats.Cancelled[j.Type]++
	default:
		j.ErrorKind, j.ErrorStage, j.LastErr, j.LastFailedAt = kind, stage, msg, &now
		retryable := retryOn != nil && slices.Contains(retryKinds(*retryOn), string(kind))
		if retryable && j.Retried < j.MaxRetry {
			j.Retried++
			next := now.Add(backoff(j.Retried))
			j.NextProcessAt = &next
			j.Move(jobs.Retry, jobs.StageLeased, fmt.Sprintf("%s: %s; retry %d/%d at %s", kind, msg, j.Retried, j.MaxRetry, next.Format(time.RFC3339)))
			s.stats.Retried[j.Type]++
			s.log.Warn("job failed; will retry", "job_id", j.ID, "service", j.Type, "kind", kind, "stage", stage, "message", msg, "retry", j.Retried)
			s.save(j, true)
			s.poke()
			return
		}
		j.FinishedAt = &now
		j.Move(jobs.Archived, jobs.StageFailed, fmt.Sprintf("%s: %s", kind, msg))
		s.stats.Failed[j.Type]++
		s.log.Error("job failed", "job_id", j.ID, "service", j.Type, "platform", j.Queue, "kind", kind, "stage", stage, "message", msg)
	}
	s.stats.Processed[j.Type]++
	s.save(j, true)
	delete(s.lastSave, j.ID)
	s.poke()
}

// recordUnexpected sends the failures that point at our code or this machine to error.err. Expected failures
// (bad input, content policy, cancel, a timeout, a transient network error the platform retries) stay in the job
// record and the job log only. A runner panic is already recorded with its stack where it was recovered.
func recordUnexpected(job *jobs.Job, kind jobs.ErrorKind, stage jobs.Stage, msg string, err error) {
	switch {
	case kind == jobs.KindInternal && !strings.Contains(msg, "panicked"):
		elog.Error("running "+job.Type, err, "job_id", job.ID, "stage", stage, "kind", kind)
	case kind == jobs.KindInfra && msg != "server shutting down":
		elog.Warn("running "+job.Type+" (infra: this machine or an engine)", err, "job_id", job.ID, "stage", stage)
	}
}

func retryKinds(on []string) []string {
	if len(on) == 0 {
		return []string{string(jobs.KindTransient), string(jobs.KindInfra), string(jobs.KindTimeout)}
	}
	return on
}

// backoff is 1 s, 2 s, 4 s ... capped at 30 s.
func backoff(retried int) time.Duration {
	d := time.Second << min(retried-1, 5)
	return min(d, 30*time.Second)
}

// classify turns any error into one kind and one sentence (spec §12.2), looking at why the context ended first.
func classify(ctx context.Context, err error) (jobs.ErrorKind, string) {
	cause := context.Cause(ctx)
	switch {
	case errors.Is(cause, errCancelled) || errors.Is(err, errCancelled):
		if errors.Is(err, errCancelled) {
			return jobs.KindCancelled, err.Error()
		}
		return jobs.KindCancelled, cause.Error()
	case errors.Is(cause, errShutdown) || errors.Is(err, errShutdown):
		return jobs.KindInfra, "server shutting down"
	case errors.Is(cause, errTimeout) || errors.Is(err, errTimeout) || errors.Is(err, context.DeadlineExceeded):
		return jobs.KindTimeout, "job exceeded its timeout"
	}
	var je *jobs.Error
	if errors.As(err, &je) {
		msg := je.Message
		if je.Err != nil {
			msg += ": " + je.Err.Error()
		}
		return je.Kind, msg
	}
	return jobs.KindInternal, err.Error()
}

func outputSummary(outs []jobs.Output) string {
	var parts []string
	for _, o := range outs {
		parts = append(parts, fmt.Sprintf("%s=%dB", o.Name, o.Bytes))
	}
	return strings.Join(parts, ",")
}

// moveStage records a lifecycle stage change on the shared record.
func (s *Server) moveStage(id string, sg jobs.Stage, note string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j, ok := s.jobs[id]; ok && j.State == jobs.Active {
		j.Move(jobs.Active, sg, note)
		s.save(j, true)
	}
}

// reporter implements runners.Reporter: progress into the job record, lines into the per-job log.
type reporter struct {
	s  *Server
	id string
	mu sync.Mutex
	f  *os.File
}

func newReporter(s *Server, id, path string) *reporter {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		s.log.Warn("cannot open job log", "job_id", id, "error", err)
		elog.Error("opening the per-job log", err, "job_id", id)
	}
	return &reporter{s: s, id: id, f: f}
}

func (r *reporter) Stage(name string) {
	r.s.mu.Lock()
	if j, ok := r.s.jobs[r.id]; ok && j.State == jobs.Active {
		j.Step = name
		r.s.save(j, false)
	}
	r.s.mu.Unlock()
	r.Log("step " + name)
}

func (r *reporter) Progress(frac float64, note string) {
	frac = min(max(frac, 0), 1)
	r.s.mu.Lock()
	if j, ok := r.s.jobs[r.id]; ok && j.State == jobs.Active {
		if frac > j.Progress {
			j.Progress = frac
		}
		if note != "" {
			j.Note = note
		}
		r.s.save(j, false)
	}
	r.s.mu.Unlock()
	if note != "" {
		r.Log(fmt.Sprintf("progress %.3f %s", frac, note))
	}
}

func (r *reporter) Log(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.f != nil {
		fmt.Fprintf(r.f, "[%s] %s\n", time.Now().Format("2006-01-02T15:04:05.000Z07:00"), line)
	}
}

func (r *reporter) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.f != nil {
		elog.Check("closing the per-job log", r.f.Close(), "job_id", r.id)
		r.f = nil
	}
}
