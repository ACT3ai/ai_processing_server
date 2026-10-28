package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/priority"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
	"github.com/ACT3ai/ai_processing_server/internal/runs"
	"github.com/ACT3ai/ai_processing_server/internal/taskhost"
)

// connect opens the broker when it is not open yet, at most every 2 seconds. Until it succeeds the server reports
// broker_missing and names the command that fixes it (§8.2); running task hosts are unaffected.
func (s *Server) connect(ctx context.Context) {
	s.mu.Lock()
	if s.q != nil || s.opt.OpenQueue == nil || time.Since(s.lastConn) < 2*time.Second {
		s.mu.Unlock()
		return
	}
	s.lastConn = time.Now()
	cfg, cat, prev := s.cfg, s.cat, s.brokerErr
	s.mu.Unlock()

	q, err := s.opt.OpenQueue(ctx, cfg, cat)
	if err != nil {
		s.mu.Lock()
		s.brokerErr = err.Error()
		s.mu.Unlock()
		if prev != err.Error() {
			s.log.Error("broker_missing: cannot reach the task queue broker; queued work waits, running jobs continue",
				"broker_url", cfg.Queue.BrokerURL, "error", err, "fix", s.opt.BrokerFix)
		}
		return
	}
	ch, werr := q.WatchControl(ctx)
	if werr != nil {
		elog.Error("watching job_control", werr)
	}
	s.mu.Lock()
	s.q, s.brokerErr, s.ctrl = q, "", ch
	s.mu.Unlock()
	info := q.Info()
	s.log.Info("broker connected", "kind", info.Kind, "url", info.URL, "version", info.Version)
	s.poke()
}

func (s *Server) brokerMessage() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.brokerErr == "" {
		return "broker not connected"
	}
	return s.brokerErr + "; fix: " + s.opt.BrokerFix
}

// brokerTrouble records an unexpected broker failure once per 30 s per action, so an outage is one line, not
// thousands.
var lastTrouble = map[string]time.Time{}

func (s *Server) brokerTrouble(attempting string, err error, kv ...any) {
	if err == nil {
		return
	}
	s.mu.Lock()
	t := lastTrouble[attempting]
	if time.Since(t) < 30*time.Second {
		s.mu.Unlock()
		return
	}
	lastTrouble[attempting] = time.Now()
	s.mu.Unlock()
	s.log.Warn("broker call failed", append([]any{"attempting", attempting, "error", err}, kv...)...)
	elog.Error(attempting, err, kv...)
}

// emit publishes an event and mirrors it into the local record. seq must already be the next number for the job.
func (s *Server) emit(ctx context.Context, q queue.Queue, ev *queue.Event) error {
	if ev.At.IsZero() {
		ev.At = time.Now().UTC()
	}
	err := q.Publish(ctx, ev)
	s.brokerTrouble("publishing a job event", err, "job_id", ev.JobID, "type", ev.Type)
	return err
}

// ---- admission (standalone role; the hub's job in phase Q3) ----

// admitSubmissions moves raw submissions from SUBMIT to WORK (§11.3). Q1 checks the envelope, the caller, the
// service and this machine's ability to run it; categories, models, quotas and signatures are phase Q2/Q3.
func (s *Server) admitSubmissions(ctx context.Context) {
	q := s.queueNow()
	if q == nil {
		return
	}
	for range 50 {
		sub, err := q.NextSubmission(ctx, 5*time.Millisecond)
		if errors.Is(err, queue.ErrNoWork) {
			return
		}
		if err != nil {
			s.brokerTrouble("reading SUBMIT", err)
			return
		}
		s.admitOne(ctx, q, sub)
	}
}

func (s *Server) admitOne(ctx context.Context, q queue.Queue, sub queue.Submission) {
	env, err := sub.Envelope()
	if err != nil {
		elog.Warn("a submission is not a task envelope; dropped", err)
		s.brokerTrouble("terminating a bad submission", sub.Term(ctx))
		return
	}
	st, rev, err := q.State(ctx, env.JobID)
	switch {
	case errors.Is(err, queue.ErrNotFound): // a caller that published to NATS directly
		st = queue.JobState{JobID: env.JobID, Caller: env.Caller, Service: env.Service, State: queue.StateSubmitted}
		if rev, err = q.CreateState(ctx, st); err != nil {
			s.brokerTrouble("creating job_state at admission", err, "job_id", env.JobID)
			return // the submission is redelivered after its ack wait
		}
	case err != nil:
		s.brokerTrouble("reading job_state at admission", err, "job_id", env.JobID)
		return
	}
	switch st.State {
	case queue.StateSubmitted:
	case queue.StateQueued: // admitted before a crash, not acked: make sure it is on WORK (idempotent) and ack
		elog.Ignore("re-enqueueing an admitted job", q.Enqueue(ctx, env, st.Band), "job_id", env.JobID)
		s.brokerTrouble("acking a submission", sub.Ack(ctx), "job_id", env.JobID)
		return
	default: // a duplicate of a job past admission, or cancelled before it
		s.brokerTrouble("acking a submission", sub.Ack(ctx), "job_id", env.JobID)
		return
	}

	if reason, kind := s.admissible(env); reason != "" {
		final := queue.StateRejected
		evType := queue.EvRejected
		if kind == jobs.KindExpired {
			final, evType = queue.StateExpired, queue.EvExpired
		}
		st.State, st.Reason, st.Seq = final, reason, st.Seq+1
		if _, err := q.CASState(ctx, st, rev); err != nil {
			return // changed under us (a cancel); the redelivery sorts it out
		}
		if s.emit(ctx, q, &queue.Event{JobID: env.JobID, Caller: env.Caller, Seq: st.Seq, Type: evType, State: final, Reason: reason, Kind: string(kind)}) != nil {
			return
		}
		s.brokerTrouble("acking a submission", sub.Ack(ctx), "job_id", env.JobID)
		s.mu.Lock()
		if j := s.jobs[env.JobID]; j != nil {
			now := time.Now().UTC()
			j.ErrorKind, j.ErrorMessage, j.ErrorStage, j.FinishedAt, j.Seq = kind, reason, j.State, &now, st.Seq
			j.Move(jobs.State(final), reason)
			s.save(j, true)
		}
		s.mu.Unlock()
		s.log.Warn("job rejected at admission", "job_id", env.JobID, "caller", env.Caller, "service", env.Service, "reason", reason)
		return
	}

	s.mu.Lock()
	bands := s.cat.Queue.Bands.Bands
	maxBand := ""
	if c := s.cat.Queue.Caller(env.Caller); c != nil {
		maxBand = c.MaxBand
	}
	s.mu.Unlock()
	band, capped := priority.CapBand(bands, priority.BandFor(bands, priority.Clamp(env.Priority)), maxBand)
	if err := q.Enqueue(ctx, env, band); err != nil {
		s.brokerTrouble("enqueueing an admitted job", err, "job_id", env.JobID)
		return
	}
	st.State, st.Band, st.Seq = queue.StateQueued, band, st.Seq+1
	if _, err := q.CASState(ctx, st, rev); err != nil {
		// Cancelled between our read and now: the WORK copy is dropped when a worker sees the final state.
		s.brokerTrouble("acking a submission", sub.Ack(ctx), "job_id", env.JobID)
		return
	}
	note := "admitted to band " + band
	if capped {
		note += " (lowered to the caller's ceiling)"
	}
	if s.emit(ctx, q, &queue.Event{JobID: env.JobID, Caller: env.Caller, Seq: st.Seq, Type: queue.EvAdmitted, State: queue.StateQueued, Band: band, Note: note}) != nil {
		return
	}
	s.brokerTrouble("acking a submission", sub.Ack(ctx), "job_id", env.JobID)
	s.mu.Lock()
	if j := s.jobs[env.JobID]; j != nil && j.State.Waiting() {
		j.Band, j.Seq = band, st.Seq
		j.Move(jobs.Queued, note)
		s.save(j, true)
	}
	s.mu.Unlock()
	s.log.Info("job admitted", "job_id", env.JobID, "caller", env.Caller, "service", env.Service, "band", band)
	s.poke()
}

// admissible returns a reason and kind when the job must not be queued.
func (s *Server) admissible(env *queue.Envelope) (string, jobs.ErrorKind) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := env.Validate(); err != nil {
		return err.Error(), jobs.KindInvalidInput
	}
	if time.Now().After(env.ExpiresAt) {
		return "the envelope expired at " + env.ExpiresAt.Format(time.RFC3339) + " before admission", jobs.KindExpired
	}
	if s.cat.Queue.Caller(env.Caller) == nil {
		return fmt.Sprintf("caller %q is not in data/queue/callers.yaml", env.Caller), jobs.KindNotAllowed
	}
	if c, ok := s.cfg.Callers[env.Caller]; !ok || !c.Enabled {
		return fmt.Sprintf("caller %q is not enabled in this server's this_server.yaml callers section", env.Caller), jobs.KindNotAllowed
	}
	if _, ok := s.cat.Services[env.Service]; !ok {
		return fmt.Sprintf("service %q is not in data/services", env.Service), jobs.KindInvalidInput
	}
	if o := s.offers[env.Service]; o.Parallel == 0 {
		return fmt.Sprintf("no server can run %s: %s", env.Service, o.Reason), jobs.KindNoCapable
	}
	return "", ""
}

// ---- pulling work ----

// callerOrder lists enabled callers in pull order: local first when callers.local.mode is first (§14.5).
func (s *Server) callerOrder() []string {
	var out []string
	local, hasLocal := s.cfg.Callers["local"]
	if hasLocal && local.Enabled && (local.Mode == "" || local.Mode == "first") {
		out = append(out, "local")
	}
	var rest []string
	for id, c := range s.cfg.Callers {
		if c.Enabled && !(id == "local" && len(out) > 0) {
			rest = append(rest, id)
		}
	}
	sort.Strings(rest)
	return append(out, rest...)
}

// pull takes jobs from the broker only while a slot is free for them (§14.4 "pull, not push").
func (s *Server) pull(ctx context.Context) {
	s.mu.Lock()
	q := s.q
	if q == nil || s.draining || !s.readopted || s.diskLowLocked() || len(s.held) >= s.cfg.Limits.MaxParallelJobs {
		s.mu.Unlock()
		return
	}
	var cands []priority.Candidate
	for id, o := range s.offers {
		if _, paused := s.pausedSvc[id]; o.Parallel > 0 && !paused && s.activeBy[id] < o.Parallel {
			cands = append(cands, priority.Candidate{Service: id, LocalBias: o.LocalBias})
		}
	}
	order := priority.PullOrder(s.cat.Queue.Bands.Bands, s.callerOrder(), cands)
	s.mu.Unlock()
	if len(order) == 0 {
		return
	}
	depth, err := q.Depth(ctx)
	if err != nil {
		s.brokerTrouble("reading work depth", err)
		return
	}
	for _, ref := range order {
		if depth[ref] == 0 {
			continue
		}
		s.mu.Lock()
		o := s.offers[ref.Service]
		if s.draining || len(s.held) >= s.cfg.Limits.MaxParallelJobs || s.activeBy[ref.Service] >= o.Parallel {
			s.mu.Unlock()
			continue
		}
		def := s.cat.Services[ref.Service]
		var allowed []int
		if set := s.cfg.Service(ref.Service); set != nil {
			allowed = set.GPUs
		}
		slot, ok := s.pool.TryAcquire(def.ResourcesPerJob, def.Requires.GPU == "required", allowed)
		if ok {
			s.activeBy[ref.Service]++ // reserved while we pull
		}
		s.mu.Unlock()
		if !ok {
			continue
		}
		del, err := q.Pull(ctx, []queue.ConsumerRef{ref}, 0)
		if err != nil {
			s.release(ref.Service, slot.ID)
			if !errors.Is(err, queue.ErrNoWork) {
				s.brokerTrouble("pulling work", err, "consumer", ref.Name())
			}
			continue
		}
		s.claim(ctx, q, del, slot)
	}
}

func (s *Server) release(service, slotID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pool.Release(slotID)
	s.activeBy[service] = max(s.activeBy[service]-1, 0)
}

// claim takes ownership of a delivered job: fence it in job_state, write the run directory, start the task host.
func (s *Server) claim(ctx context.Context, q queue.Queue, del queue.Delivery, slot jobs.Slot) {
	env := del.Envelope()
	id, attempt := env.JobID, del.Attempt()
	s.mu.Lock()
	running := s.held[id]
	s.mu.Unlock()
	if running != nil {
		s.rehold(ctx, running, del, slot)
		return
	}
	drop := func(why string) {
		s.release(env.Service, slot.ID)
		s.brokerTrouble("acking a job that will not run", del.Ack(ctx), "job_id", id)
		s.log.Info("delivered job dropped", "job_id", id, "reason", why)
	}
	st, rev, err := q.State(ctx, id)
	if errors.Is(err, queue.ErrNotFound) {
		st = queue.JobState{JobID: id, Caller: env.Caller, Service: env.Service, State: queue.StateQueued}
		rev, err = q.CreateState(ctx, st)
	}
	if err != nil {
		s.release(env.Service, slot.ID)
		s.brokerTrouble("reading job_state at claim", err, "job_id", id)
		elog.Ignore("requeueing after a job_state failure", del.Requeue(ctx, 5*time.Second))
		return
	}
	if queue.FinalState(st.State) {
		drop("already " + st.State)
		return
	}
	now := time.Now().UTC()
	finalBeforeStart := func(state string, kind jobs.ErrorKind, evType, msg string) {
		st.State, st.Reason, st.Seq = state, msg, st.Seq+1
		if _, err := q.CASState(ctx, st, rev); err != nil {
			s.release(env.Service, slot.ID)
			elog.Ignore("requeueing after a fence conflict", del.Requeue(ctx, time.Second))
			return
		}
		ev := &queue.Event{JobID: id, Caller: env.Caller, Seq: st.Seq, Type: evType, State: state, Attempt: attempt, Kind: string(kind), Message: msg}
		if evType == queue.EvCancelled {
			ev.By = msg
		}
		if s.emit(ctx, q, ev) != nil {
			s.release(env.Service, slot.ID)
			return // not acked: redelivered, and the final state is seen then
		}
		drop(msg)
		s.mu.Lock()
		if j := s.jobs[id]; j != nil {
			j.ErrorKind, j.ErrorMessage, j.ErrorStage, j.FinishedAt, j.Seq = kind, msg, j.State, &now, st.Seq
			j.Move(jobs.State(state), msg)
			s.save(j, true)
		}
		s.mu.Unlock()
	}
	if c, err := q.GetControl(ctx, id); err == nil && c.Cancel {
		finalBeforeStart(queue.StateCancelled, jobs.KindCancelled, queue.EvCancelled, "cancelled by "+c.By+" before it started")
		return
	}
	if now.After(env.ExpiresAt) {
		finalBeforeStart(queue.StateExpired, jobs.KindExpired, queue.EvExpired, "the envelope expired before a worker could start it")
		return
	}
	if env.Deadline != nil && now.After(*env.Deadline) {
		finalBeforeStart(queue.StateFailed, jobs.KindTimeout, queue.EvFailed, "deadline passed before the job could start")
		return
	}

	s.mu.Lock()
	key, snap := s.cfg.Server.Key, s.opt.Snapshot
	def := s.cat.Services[env.Service]
	engines := s.effective.Engines
	keep := s.cfg.Debug.KeepWorkDirs
	s.mu.Unlock()
	st.State, st.Attempt, st.Server, st.CodeVersion, st.Seq = queue.StateClaimed, attempt, key, snap.ID, st.Seq+1
	fence, err := q.CASState(ctx, st, rev)
	if err != nil {
		s.release(env.Service, slot.ID)
		elog.Ignore("requeueing after a fence conflict", del.Requeue(ctx, time.Second))
		return
	}

	s.mu.Lock()
	j := s.jobs[id]
	if j == nil {
		j = recordFor(env, now)
		s.jobs[id] = j
	}
	j.Attempt, j.Server, j.Band, j.CodeVersion, j.Seq = attempt, key, st.Band, snap.ID, st.Seq
	j.Slot, j.StartedAt, j.NextAttemptAfter, j.FinishedAt = &slot, &now, nil, nil
	j.Progress, j.Step, j.Note = 0, "", ""
	j.Move(jobs.Claimed, fmt.Sprintf("attempt %d on %s %s, code %s", attempt, key, slot.ID, snap.ID))
	s.save(j, true)
	taskJob := j.Clone()
	s.mu.Unlock()

	dir := filepath.Join(s.runsDir, id)
	elog.Check("clearing the run dir of an earlier attempt", os.RemoveAll(dir), "job_id", id)
	h := &held{dir: dir, del: del, service: env.Service, caller: env.Caller}
	h.rec = runs.Record{RunVersion: runs.Version, JobID: id, Caller: env.Caller, Service: env.Service, Snapshot: snap.ID,
		Claim: del.Handle(), Slot: slot, Seq: st.Seq, ClaimedAt: now}
	h.rec.Claim.FenceRev, h.rec.Claim.Attempt = fence, attempt
	task := runs.Task{RunVersion: runs.Version, Job: taskJob, Service: def, WorkDir: s.workDir, ResultsDir: s.resultsDir,
		LogPath: filepath.Join(s.jobLogDir, id+".log"), Engines: engines, AllowLocal: env.Caller == "local", KeepWorkDir: keep, Snapshot: snap.ID}
	launchErr := os.MkdirAll(dir, 0o700)
	if launchErr == nil {
		launchErr = runs.WriteJSON(dir, runs.TaskFile, task)
	}
	if launchErr == nil {
		launchErr = runs.WriteJSON(dir, runs.RunFile, h.rec)
	}
	if launchErr == nil {
		var pid int
		if pid, launchErr = taskhost.Launch(snap.Binary, dir, s.opt.TaskHostArgs); launchErr == nil {
			h.rec.TaskHost = runs.HostInfo{PID: pid, StartedAt: time.Now().UTC()}
			launchErr = runs.WriteJSON(dir, runs.RunFile, h.rec)
		}
	}
	if launchErr != nil {
		// Let supervise finish it through the normal path, as an infra failure on this machine.
		elog.Error("starting a task host", launchErr, "job_id", id)
		elog.Ignore("recording a launch failure", os.MkdirAll(dir, 0o700))
		elog.Ignore("recording a launch failure", runs.WriteJSON(dir, runs.ResultFile, runs.Result{Kind: jobs.KindInfra, Message: "could not start the task host: " + launchErr.Error(), Stage: string(jobs.Claimed)}))
		elog.Ignore("recording a launch failure", runs.WriteJSON(dir, runs.ExitFile, runs.Exit{Code: 2, At: time.Now().UTC()}))
	}
	s.mu.Lock()
	s.held[id] = h
	s.mu.Unlock()
	h.lastBeat = time.Now()
	s.brokerTrouble("renewing a claim", del.InProgress(ctx), "job_id", id)
	s.emit(ctx, q, &queue.Event{JobID: id, Caller: env.Caller, Seq: st.Seq, Type: queue.EvClaimed, State: queue.StateClaimed,
		Attempt: attempt, Server: key, CodeVersion: snap.ID})
	s.log.Info("job claimed", "job_id", id, "service", env.Service, "caller", env.Caller, "attempt", attempt, "slot", slot.ID,
		"band", st.Band, "code_version", snap.ID, "taskhost_pid", h.rec.TaskHost.PID)
}

// rehold handles the broker redelivering a job this worker is still running: the controller was away longer than
// the ack wait, so the old claim expired. The task host is fine, so keep it and hold the job through the new
// delivery instead of starting it twice (§9.4, §16.1).
func (s *Server) rehold(ctx context.Context, h *held, del queue.Delivery, slot jobs.Slot) {
	s.release(h.service, slot.ID)
	h.del = del
	h.rec.Claim.AckSubject = del.Handle().AckSubject
	elog.Check("writing run.json", runs.WriteJSON(h.dir, runs.RunFile, h.rec), "job_id", h.rec.JobID)
	h.lastBeat = time.Now()
	s.brokerTrouble("renewing a claim", del.InProgress(ctx), "job_id", h.rec.JobID)
	s.log.Warn("the broker redelivered a job this server is still running (its claim expired while the controller was away); kept the running task host",
		"job_id", h.rec.JobID, "delivery", del.Attempt())
}

// applyControl acts on a job_control change: cancel a running job through its task host, or a waiting one at once.
func (s *Server) applyControl(ctx context.Context, c queue.ControlChange) {
	if !c.Control.Cancel {
		return
	}
	reason := "cancelled by " + c.Control.By
	s.mu.Lock()
	h := s.held[c.JobID]
	q := s.q
	s.mu.Unlock()
	if h != nil {
		if !h.cancelSent {
			h.cancelSent = true
			elog.Check("writing a cancel file", runs.WriteJSON(h.dir, runs.CancelFile, runs.Cancel{Reason: reason, Kind: jobs.KindCancelled}), "job_id", c.JobID)
		}
		return
	}
	if q == nil {
		return
	}
	for range 3 { // a waiting job: compare-and-set to CANCELLED, retrying if admission moved it meanwhile
		st, rev, err := q.State(ctx, c.JobID)
		if err != nil || !(st.State == queue.StateSubmitted || st.State == queue.StateAdmitted || st.State == queue.StateQueued) {
			return // running elsewhere, final, or unknown: its holder handles it
		}
		st.State, st.Reason, st.Seq = queue.StateCancelled, reason+" before it started", st.Seq+1
		if _, err := q.CASState(ctx, st, rev); errors.Is(err, queue.ErrRevision) {
			continue
		} else if err != nil {
			s.brokerTrouble("cancelling a waiting job", err, "job_id", c.JobID)
			return
		}
		s.emit(ctx, q, &queue.Event{JobID: c.JobID, Caller: st.Caller, Seq: st.Seq, Type: queue.EvCancelled, State: queue.StateCancelled,
			Kind: string(jobs.KindCancelled), Message: st.Reason, By: c.Control.By})
		s.mu.Lock()
		if j := s.jobs[c.JobID]; j != nil && !j.State.Final() {
			now := time.Now().UTC()
			j.ErrorKind, j.ErrorMessage, j.ErrorStage, j.FinishedAt, j.Seq = jobs.KindCancelled, st.Reason, j.State, &now, st.Seq
			j.Move(jobs.Cancelled, st.Reason)
			s.save(j, true)
			s.stats.Cancelled[j.Service]++
		}
		s.mu.Unlock()
		s.log.Info("waiting job cancelled", "job_id", c.JobID, "by", c.Control.By)
		return
	}
}

// putServer writes this server's capability record every 10 s (§12; the key's TTL is 90 s).
func (s *Server) putServer(ctx context.Context) {
	s.mu.Lock()
	q := s.q
	if q == nil || time.Since(s.lastPut) < 10*time.Second {
		s.mu.Unlock()
		return
	}
	s.lastPut = time.Now()
	st, _ := s.stateLocked()
	rec := queue.ServerRecord{Key: s.cfg.Server.Key, Role: s.cfg.Queue.Role, Version: s.opt.Version, CodeVersion: s.opt.Snapshot.ID, State: st, At: time.Now().UTC()}
	for _, o := range s.offers {
		if o.Parallel > 0 {
			rec.Services = append(rec.Services, queue.ServiceOffer{Service: o.Service, Category: s.cat.Queue.CategoryOf(o.Service), Parallel: o.Parallel, Running: s.activeBy[o.Service]})
		}
	}
	sort.Slice(rec.Services, func(i, j int) bool { return rec.Services[i].Service < rec.Services[j].Service })
	s.mu.Unlock()
	s.brokerTrouble("publishing the servers record", q.PutServer(ctx, rec))
}
