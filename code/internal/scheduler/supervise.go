package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
	"github.com/ACT3ai/ai_processing_server/internal/runs"
)

// held is one job running on this machine: its claim, its run directory and the controller's progress through
// the task host's files. Everything needed to rebuild it is in run.json (§9.4).
type held struct {
	dir        string
	rec        runs.Record
	del        queue.Delivery
	service    string
	caller     string
	lastBeat   time.Time // last InProgress sent
	lastPub    time.Time // last progress event published
	cancelSent bool
	final      *finalStep // set once finalize has decided; lets a failed publish or ack be retried
}

type finalStep struct {
	ev       *queue.Event
	state    jobs.State
	action   string // ack | requeue | dead | none
	delay    time.Duration
	reason   string
	casDone  bool
	pubDone  bool
	res      runs.Result
	fenced   bool
}

// progressEvery limits progress events on the bus (§13.5); the local record updates on every line.
const progressEvery = 5 * time.Second

// supervise moves every running job forward: task host lines become record changes and events, the claim is
// renewed, the fence is answered, and a finished or vanished task host is finalized.
func (s *Server) supervise(ctx context.Context) {
	s.mu.Lock()
	q := s.q
	list := make([]*held, 0, len(s.held))
	for _, h := range s.held {
		list = append(list, h)
	}
	beat := s.cfg.Continuity.Heartbeat.Duration
	s.mu.Unlock()
	if q == nil {
		return // task hosts keep running; we catch up when the broker is back
	}
	for _, h := range list {
		s.superviseOne(ctx, q, h, beat)
	}
}

func (s *Server) superviseOne(ctx context.Context, q queue.Queue, h *held, beat time.Duration) {
	id := h.rec.JobID
	if h.final == nil {
		lines, n, err := runs.ReadLines(h.dir, h.rec.EventsRead)
		if err != nil {
			elog.Error("reading events.jsonl", err, "job_id", id)
		}
		for _, l := range lines {
			s.applyLine(ctx, q, h, l)
		}
		if n != h.rec.EventsRead {
			h.rec.EventsRead = n
			elog.Check("writing run.json", runs.WriteJSON(h.dir, runs.RunFile, h.rec), "job_id", id)
		}
		if time.Since(h.lastBeat) >= beat {
			h.lastBeat = time.Now()
			s.brokerTrouble("renewing a claim", h.del.InProgress(ctx), "job_id", id)
		}
		if runs.Exists(h.dir, runs.FenceRequest) && !runs.Exists(h.dir, runs.FenceOK) && !runs.Exists(h.dir, runs.FenceLost) {
			s.answerFence(ctx, q, h)
		}
		switch {
		case runs.Exists(h.dir, runs.ExitFile):
		case runs.Alive(h.dir, &h.rec):
			return
		default:
			s.log.Error("task host gone without a result", "job_id", id, "pid", h.rec.TaskHost.PID)
			elog.Ignore("recording a vanished task host", runs.WriteJSON(h.dir, runs.ResultFile, runs.Result{Kind: jobs.KindInfra,
				Message: "the task host stopped without a result (killed, or the machine restarted)", Stage: string(s.stateOf(id))}))
		}
		// Apply any last lines written just before exit.
		lines, n, _ = runs.ReadLines(h.dir, h.rec.EventsRead)
		for _, l := range lines {
			s.applyLine(ctx, q, h, l)
		}
		h.rec.EventsRead = n
	}
	s.finalize(ctx, q, h)
}

func (s *Server) stateOf(id string) jobs.State {
	s.mu.Lock()
	defer s.mu.Unlock()
	if j := s.jobs[id]; j != nil {
		return j.State
	}
	return ""
}

// nextSeq bumps the job's event sequence and returns it. Caller holds mu.
func (h *held) nextSeq() int64 {
	h.rec.Seq++
	return h.rec.Seq
}

// cas writes job_state under the claim's fencing revision and keeps the new revision in run.json.
func (s *Server) cas(ctx context.Context, q queue.Queue, h *held, state string, mutate func(*queue.JobState)) error {
	st, _, err := q.State(ctx, h.rec.JobID)
	if err != nil {
		return err
	}
	st.State, st.Seq = state, h.rec.Seq
	if mutate != nil {
		mutate(&st)
	}
	rev, err := q.CASState(ctx, st, h.rec.Claim.FenceRev)
	if err != nil {
		return err
	}
	h.rec.Claim.FenceRev = rev
	elog.Check("writing run.json", runs.WriteJSON(h.dir, runs.RunFile, h.rec), "job_id", h.rec.JobID)
	return nil
}

// applyLine turns one events.jsonl line into a record change and, for stage changes and throttled progress, an event.
func (s *Server) applyLine(ctx context.Context, q queue.Queue, h *held, l runs.Line) {
	id := h.rec.JobID
	s.mu.Lock()
	j := s.jobs[id]
	if j == nil {
		s.mu.Unlock()
		return
	}
	var ev *queue.Event
	switch l.Type {
	case "stage":
		st := jobs.State(l.State)
		if st == j.State {
			break
		}
		j.Move(st, "")
		s.save(j, true)
		ev = &queue.Event{Type: queue.EvStage, State: l.State, Stage: l.State}
	case "step":
		j.Step = l.Step
		s.save(j, false)
	case "progress":
		if l.Progress > j.Progress {
			j.Progress = l.Progress
		}
		if l.Note != "" {
			j.Note = l.Note
		}
		s.save(j, false)
		if time.Since(h.lastPub) >= progressEvery {
			p := j.Progress
			ev = &queue.Event{Type: queue.EvProgress, State: string(j.State), Step: j.Step, Progress: &p, Note: j.Note}
		}
	}
	if ev == nil {
		s.mu.Unlock()
		return
	}
	ev.JobID, ev.Caller, ev.Attempt, ev.Server, ev.At = id, h.caller, h.rec.Claim.Attempt, j.Server, l.At
	ev.Seq = h.nextSeq()
	j.Seq = ev.Seq
	s.mu.Unlock()
	if ev.Type == queue.EvStage {
		if err := s.cas(ctx, q, h, ev.State, nil); errors.Is(err, queue.ErrRevision) {
			s.claimLost(h)
			return
		} else if err != nil {
			s.brokerTrouble("updating job_state", err, "job_id", id)
		}
	}
	if ev.Type == queue.EvProgress {
		h.lastPub = time.Now()
	}
	s.emit(ctx, q, ev)
}

// claimLost stops a job whose fencing revision moved: another worker holds a newer attempt (§16.3).
func (s *Server) claimLost(h *held) {
	if h.cancelSent {
		return
	}
	h.cancelSent = true
	s.log.Warn("claim lost: another worker owns this job now; stopping the local attempt", "job_id", h.rec.JobID)
	elog.Check("writing a cancel file", runs.WriteJSON(h.dir, runs.CancelFile, runs.Cancel{Reason: "claim lost to another worker", Kind: jobs.KindInfra}), "job_id", h.rec.JobID)
	elog.Ignore("answering the fence", runs.WriteJSON(h.dir, runs.FenceLost, map[string]string{"reason": "claim lost"}))
}

// answerFence confirms, by compare-and-set, that this worker still owns the job before the task host uploads.
func (s *Server) answerFence(ctx context.Context, q queue.Queue, h *held) {
	err := s.cas(ctx, q, h, queue.StateUploading, nil)
	switch {
	case err == nil:
		elog.Check("answering the fence", runs.WriteJSON(h.dir, runs.FenceOK, map[string]uint64{"fence_rev": h.rec.Claim.FenceRev}), "job_id", h.rec.JobID)
	case errors.Is(err, queue.ErrRevision):
		s.claimLost(h)
	default:
		s.brokerTrouble("checking the fence before upload", err, "job_id", h.rec.JobID) // retried next tick
	}
}

// retryable applies §16.2 to one failure.
func retryable(kind jobs.ErrorKind, def config.ServiceDef, attempt int) bool {
	on := def.Retry.RetryOn
	if len(on) == 0 {
		on = []string{string(jobs.KindTransient), string(jobs.KindInfra), string(jobs.KindTimeout)}
	}
	if !slices.Contains(on, string(kind)) {
		return false
	}
	return kind != jobs.KindTimeout || attempt < 2 // a timeout is requeued once, then fails
}

// finalize decides the outcome of an attempt, writes job_state under the fence, publishes the final (or requeued)
// event, and only then acks, requeues or dead-letters the delivery (§16.3: final event first, then Ack). Each step
// is remembered, so a broker failure part way is retried on the next tick without repeating earlier steps.
func (s *Server) finalize(ctx context.Context, q queue.Queue, h *held) {
	id := h.rec.JobID
	if h.final == nil {
		var res runs.Result
		if err := runs.ReadJSON(h.dir, runs.ResultFile, &res); err != nil {
			res = runs.Result{Kind: jobs.KindInfra, Message: "the task host left no readable result.json: " + err.Error()}
		}
		s.mu.Lock()
		j := s.jobs[id]
		def := s.cat.Services[h.service]
		key := s.cfg.Server.Key
		f := &finalStep{res: res, fenced: res.Fenced}
		attempt, maxAttempts := h.rec.Claim.Attempt, 1
		if j != nil {
			maxAttempts = j.MaxAttempts
		}
		ev := &queue.Event{JobID: id, Caller: h.caller, Attempt: attempt, Server: key, CodeVersion: h.rec.Snapshot}
		switch {
		case res.Fenced:
			f.action = "none"
		case res.OK:
			f.state, f.action = jobs.Succeeded, "ack"
			ev.Type = queue.EvSucceeded
			ev.Outputs, _ = json.Marshal(res.Outputs)
			ev.Result = res.Data
			ev.Timings = map[string]float64{"run_s": res.Seconds}
		case res.Kind == jobs.KindCancelled:
			f.state, f.action = jobs.Cancelled, "ack"
			ev.Type, ev.Kind, ev.Message, ev.By = queue.EvCancelled, string(res.Kind), res.Message, res.Message
		case retryable(res.Kind, def, attempt) && attempt < maxAttempts:
			f.state, f.action = jobs.Queued, "requeue"
			if res.Kind != jobs.KindInfra { // infra: at once, so another machine can take it (§16.2)
				f.delay = def.Queue.BackoffFor(attempt + 1)
			}
			next := time.Now().UTC().Add(f.delay)
			ev.Type, ev.Kind, ev.Message, ev.NextAttemptAfter = queue.EvRequeued, string(res.Kind), res.Message, &next
		default:
			f.state, f.action = jobs.Failed, "ack"
			msg := res.Message
			if retryable(res.Kind, def, attempt) && attempt > 1 {
				f.action, f.reason = "dead", fmt.Sprintf("gave up after %d attempts", attempt)
				msg = fmt.Sprintf("gave up after %d attempts: %s", attempt, res.Message)
			}
			ev.Type, ev.Kind, ev.Message, ev.Detail = queue.EvFailed, string(res.Kind), msg, res.Stage
		}
		if f.action != "none" {
			ev.State = string(f.state)
			ev.Seq = h.nextSeq()
		}
		f.ev = ev
		h.final = f
		s.mu.Unlock()
	}
	f := h.final

	if f.action != "none" && !f.casDone {
		err := s.cas(ctx, q, h, string(f.state), func(st *queue.JobState) {
			st.Reason = f.ev.Message
			if f.state == jobs.Queued {
				st.Attempt = h.rec.Claim.Attempt // the next delivery is attempt+1
			}
		})
		switch {
		case errors.Is(err, queue.ErrRevision):
			f.action, f.fenced = "none", true // another worker owns it: publish nothing, ack nothing
		case err != nil:
			s.brokerTrouble("writing the final job_state", err, "job_id", id)
			return
		default:
			f.casDone = true
		}
	}
	if f.action != "none" && !f.pubDone {
		if s.emit(ctx, q, f.ev) != nil {
			return
		}
		f.pubDone = true
	}
	switch f.action {
	case "ack":
		s.brokerTrouble("acking a finished job", h.del.Ack(ctx), "job_id", id)
	case "requeue":
		s.brokerTrouble("requeueing a job", h.del.Requeue(ctx, f.delay), "job_id", id)
	case "dead":
		s.brokerTrouble("dead-lettering a job", h.del.Dead(ctx, f.reason), "job_id", id)
	}
	s.finishRecord(h, f)
}

// finishRecord applies the outcome to the local record, frees the slot and marks the run directory handled.
func (s *Server) finishRecord(h *held, f *finalStep) {
	id := h.rec.JobID
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.held, id)
	s.pool.Release(h.rec.Slot.ID)
	s.activeBy[h.service] = max(s.activeBy[h.service]-1, 0)
	elog.Check("marking a run handled", runs.WriteJSON(h.dir, runs.HandledFile, map[string]any{"at": now, "action": f.action}), "job_id", id)
	j := s.jobs[id]
	if j == nil {
		return
	}
	j.Slot = nil
	if f.fenced {
		j.Note = "another worker owns this job now; this attempt's outputs were discarded"
		j.Move(jobs.Queued, j.Note)
		s.save(j, true)
		s.log.Warn("result fenced off", "job_id", id)
		return
	}
	j.Seq = h.rec.Seq
	res := f.res
	s.stats.Processed[j.Service]++
	switch f.state {
	case jobs.Succeeded:
		j.Outputs, j.Result, j.Progress = res.Outputs, res.Data, 1
		j.ErrorKind, j.ErrorMessage, j.ErrorStage = "", "", ""
		j.FinishedAt = &now
		j.Move(jobs.Succeeded, "done")
		s.log.Info("job succeeded", "job_id", id, "service", j.Service, "attempt", j.Attempt, "seconds", fmt.Sprintf("%.2f", res.Seconds),
			"outputs", outputSummary(j.Outputs), "code_version", h.rec.Snapshot)
	case jobs.Queued:
		j.ErrorKind, j.ErrorMessage, j.ErrorStage = res.Kind, res.Message, jobs.State(res.Stage)
		j.NextAttemptAfter = f.ev.NextAttemptAfter
		j.Move(jobs.Queued, fmt.Sprintf("requeued after %s: %s; attempt %d of %d", res.Kind, res.Message, j.Attempt+1, j.MaxAttempts))
		s.stats.Retried[j.Service]++
		s.log.Warn("job failed; requeued", "job_id", id, "service", j.Service, "kind", res.Kind, "message", res.Message, "attempt", j.Attempt, "delay", f.delay)
	case jobs.Cancelled:
		j.ErrorKind, j.ErrorMessage, j.ErrorStage = jobs.KindCancelled, res.Message, jobs.State(res.Stage)
		j.FinishedAt = &now
		j.Move(jobs.Cancelled, res.Message)
		s.stats.Cancelled[j.Service]++
		s.log.Info("job cancelled", "job_id", id, "service", j.Service, "stage", res.Stage)
	default:
		j.ErrorKind, j.ErrorMessage, j.ErrorStage = res.Kind, f.ev.Message, jobs.State(res.Stage)
		j.FinishedAt = &now
		j.Move(jobs.Failed, fmt.Sprintf("%s: %s", res.Kind, f.ev.Message))
		s.stats.Failed[j.Service]++
		s.log.Error("job failed", "job_id", id, "service", j.Service, "kind", res.Kind, "stage", res.Stage, "message", f.ev.Message)
	}
	s.save(j, true)
	delete(s.lastSave, id)
	s.poke()
}

func outputSummary(outs []jobs.Output) string {
	var b []byte
	for i, o := range outs {
		if i > 0 {
			b = append(b, ',')
		}
		b = fmt.Appendf(b, "%s=%dB", o.Name, o.Bytes)
	}
	return string(b)
}

// readopt runs once, after the broker is reachable and before any new pull (§9.4): every run directory not yet
// handled belongs to a job a previous controller started. Its slot is re-occupied, its claim renewed at once
// through the stored ack subject, and its events resume after the last one sent.
func (s *Server) readopt(ctx context.Context) {
	s.mu.Lock()
	q := s.q
	if q == nil || s.readopted {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	for _, id := range runs.List(s.runsDir) {
		dir := filepath.Join(s.runsDir, id)
		if runs.Exists(dir, runs.HandledFile) {
			continue
		}
		var rec runs.Record
		var task runs.Task
		if err := runs.ReadJSON(dir, runs.RunFile, &rec); err != nil || runs.ReadJSON(dir, runs.TaskFile, &task) != nil || task.Job == nil {
			elog.Warn("a run dir without a readable run.json/task.json; skipped (the broker redelivers its job)", err, "job_id", id)
			elog.Ignore("marking an unreadable run handled", runs.WriteJSON(dir, runs.HandledFile, map[string]string{"action": "unreadable"}))
			continue
		}
		if rec.RunVersion != runs.Version {
			elog.Warn("a run dir of another run_version; skipped", fmt.Errorf("run_version %d", rec.RunVersion), "job_id", id)
			continue
		}
		del, err := q.Resume(ctx, rec.Claim, &task.Job.Envelope)
		if err != nil {
			elog.Error("resuming a claim", err, "job_id", id)
			continue
		}
		alive := runs.Exists(dir, runs.ExitFile) || runs.Alive(dir, &rec)
		s.mu.Lock()
		if s.jobs[id] == nil {
			s.jobs[id] = task.Job
		}
		j := s.jobs[id]
		slot := rec.Slot
		j.Slot = &slot
		s.pool.Adopt(rec.Slot)
		s.activeBy[rec.Service]++
		h := &held{dir: dir, rec: rec, del: del, service: rec.Service, caller: rec.Caller, lastBeat: time.Now()}
		s.held[id] = h
		if alive {
			s.stats.Readopted++
		} else {
			s.stats.Lost++
		}
		s.mu.Unlock()
		s.brokerTrouble("renewing a re-adopted claim", del.InProgress(ctx), "job_id", id)
		if alive {
			s.log.Info("re-adopted running job", "job_id", id, "service", rec.Service, "attempt", rec.Claim.Attempt,
				"taskhost_pid", rec.TaskHost.PID, "code_version", rec.Snapshot, "events_already_sent", rec.EventsRead)
		} else {
			s.log.Error("readopt_lost: the task host of a running job is gone", "job_id", id, "pid", rec.TaskHost.PID)
			elog.Warn("readopt_lost: a task host was gone at re-adoption (expected only after a reboot)", errors.New("task host not running"), "job_id", id)
		}
	}
	s.mu.Lock()
	s.readopted = true
	s.mu.Unlock()
	s.poke()
}
