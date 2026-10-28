package scheduler

// The task queue link (pm/task_queue.mdx §8 to §16, D-014). The broker (NATS JetStream in its own nats-server
// process) stores and delivers; this file decides: which lanes to pull and in what order, when a job is claimed
// (the job_state revision is the fencing token), what the caller is told (events), and whether a failure goes back
// to the queue, to DEAD, or is final. A queued job runs through the same slots, runners and journal as a direct
// job; only its intake and its settlement differ.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
	"github.com/ACT3ai/ai_processing_server/internal/runners"
)

// Broker connection states for /v1/health and /v1/broker.
const (
	BrokerDisabled   = "disabled"
	BrokerConnecting = "connecting"
	BrokerOK         = "ok"
	BrokerMissing    = "broker_missing"
	BrokerError      = "broker_error"
)

// eventSeqBase keeps event sequence numbers unique across attempts: attempt n numbers its events n*1000+1, ...
const eventSeqBase = 1000

// heldJob is one queued job this server holds a claim on.
type heldJob struct {
	d            queue.Delivery
	caller, band string
	attempt      int
	maxAttempts  int
	rev          uint64 // job_state revision this server wrote: the fencing token
	seq          int64
	lastProgress time.Time
}

type pubReq struct {
	ev   *queue.Event
	done chan error // nil: best effort
}

// BrokerHealth is the broker part of GET /v1/health.
type BrokerHealth struct {
	State   string `json:"state"`
	URL     string `json:"url,omitempty"`
	Version string `json:"server_version,omitempty"`
	Error   string `json:"error,omitempty"`
	Fix     string `json:"fix,omitempty"`
	Held    int    `json:"held_jobs"`
}

func (s *Server) brokerHealthLocked() *BrokerHealth {
	if s.opt.OpenQueue == nil {
		return &BrokerHealth{State: BrokerDisabled}
	}
	b := &BrokerHealth{State: s.qState, URL: s.cfg.Queue.BrokerURL, Version: s.qVersion, Error: s.qErr, Held: len(s.held)}
	if s.qState == BrokerMissing {
		b.Fix = "start this machine's nats-server: `just broker-install` once, `just broker-config`, then `just broker-run` " +
			"(or the OS service); direct mode (POST /v1/jobs) keeps working meanwhile"
	}
	return b
}

func (s *Server) setBroker(state, version, errText string) {
	s.mu.Lock()
	changed := s.qState != state || s.qErr != errText
	s.qState, s.qErr = state, errText
	if version != "" {
		s.qVersion = version
	}
	s.mu.Unlock()
	if !changed {
		return
	}
	switch state {
	case BrokerOK:
		s.log.Info("broker connected", "url", s.cfg.Queue.BrokerURL, "server_version", version)
	case BrokerMissing:
		s.log.Error("broker_missing: cannot reach nats-server; queued jobs wait until it is up (direct mode still works)",
			"url", s.cfg.Queue.BrokerURL, "error", errText, "fix", "just broker-config && just broker-run")
	case BrokerError:
		s.log.Warn("broker error", "error", errText)
	}
}

// queueMain connects (retrying every 5 s until the broker is up), then runs the queue loops until ctx ends.
func (s *Server) queueMain(ctx context.Context) {
	s.setBroker(BrokerConnecting, "", "")
	var q queue.Queue
	for q == nil {
		octx, cancel := context.WithTimeout(ctx, 10*time.Second)
		got, err := s.opt.OpenQueue(octx)
		cancel()
		if err == nil {
			q = got
			break
		}
		if ctx.Err() != nil {
			return
		}
		s.setBroker(BrokerMissing, "", err.Error())
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
	defer func() { elog.Ignore("closing the broker connection", q.Close()) }()
	st := q.Status(ctx)
	s.mu.Lock()
	s.q = q
	want := s.cfg.Queue.BrokerVersion
	s.mu.Unlock()
	s.setBroker(BrokerOK, st.ServerVersion, "")
	if want != "" && !strings.HasPrefix(strings.TrimPrefix(st.ServerVersion, "v"), want) {
		s.log.Warn("nats-server version is not the supported line", "server_version", st.ServerVersion, "supported", want+".x")
	}
	s.reclaimAtBoot(ctx, q)

	done := make(chan struct{})
	go func() { defer elog.Recover("publishing queue events"); s.publishLoop(ctx, q); close(done) }()
	go func() { defer elog.Recover("watching job_control"); s.controlLoop(ctx, q) }()
	go func() { defer elog.Recover("extending queue claims"); s.heartbeatLoop(ctx, q) }()
	s.pullLoop(ctx, q)
	<-done
}

// pullLoop takes one job at a time while some offered service has a free slot (§14.4: pull, not push).
func (s *Server) pullLoop(ctx context.Context, q queue.Queue) {
	for ctx.Err() == nil {
		s.mu.Lock()
		order := s.pullOrderLocked()
		s.mu.Unlock()
		if len(order) > 0 {
			d, err := q.Pull(ctx, order, 0)
			switch {
			case err != nil && ctx.Err() == nil:
				s.setBroker(BrokerError, "", err.Error())
				select {
				case <-ctx.Done():
				case <-time.After(2 * time.Second):
				}
				continue
			case d != nil:
				s.setBroker(BrokerOK, "", "")
				func() {
					defer elog.Recover("accepting a queued job")
					s.acceptDelivery(ctx, q, d)
				}()
				continue
			default:
				s.setBroker(BrokerOK, "", "")
			}
		}
		select {
		case <-ctx.Done():
		case <-s.qwake:
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// pullOrderLocked lists the lanes this server may pull now: bands highest first, then caller, then service.
// Weighted round robin over bands and caller shares are build phase Q2 (§14.4); strict band order is the v1 rule.
func (s *Server) pullOrderLocked() []queue.ConsumerRef {
	if s.draining || s.paused || s.diskLowLocked() {
		return nil
	}
	pendingBy, pending := map[string]int{}, 0
	for _, j := range s.jobs {
		if j.Claim != nil && (j.State == jobs.Pending || j.State == jobs.Retry) {
			pendingBy[j.Type]++
			pending++
		}
	}
	if s.active+pending >= s.cfg.Limits.MaxParallelJobs {
		return nil
	}
	var svcs []string
	for id, o := range s.offers {
		if _, p := s.pausedSvc[id]; p || o.Parallel == 0 {
			continue
		}
		if s.activeBy[id]+pendingBy[id] < o.Parallel {
			svcs = append(svcs, id)
		}
	}
	sort.Strings(svcs)
	return pullOrder(s.cfg.EnabledCallers(), svcs, s.cfg.LocalMode())
}

// pullOrder orders lanes by band, then caller, then service (§14.4 v1: strict band order). A lane above its caller's
// ceiling is pulled as if it were at the ceiling (§14.3), so a WTC job sent to p0 waits behind ACT3's p0 jobs. The local
// caller's lanes move with localMode (§14.5): all first, inside each band (share), or after every platform lane.
func pullOrder(callers, svcs []string, localMode string) []queue.ConsumerRef {
	rank := map[string]int{}
	for i, b := range queue.Bands {
		rank[b] = i
	}
	type lane struct {
		ref        queue.ConsumerRef
		group, eff int
	}
	var lanes []lane
	for _, band := range queue.Bands {
		for _, c := range callers {
			group := 1
			if c == "local" {
				switch localMode {
				case config.LocalFirst:
					group = 0
				case config.LocalIdleOnly:
					group = 2
				}
			}
			for _, svc := range svcs {
				lanes = append(lanes, lane{queue.ConsumerRef{Band: band, Caller: c, Service: svc}, group, rank[queue.ClampBand(c, band)]})
			}
		}
	}
	sort.SliceStable(lanes, func(i, j int) bool {
		if lanes[i].group != lanes[j].group {
			return lanes[i].group < lanes[j].group
		}
		return lanes[i].eff < lanes[j].eff
	})
	order := make([]queue.ConsumerRef, len(lanes))
	for i, l := range lanes {
		order[i] = l.ref
	}
	return order
}

// acceptDelivery claims a pulled job (fencing token), or settles it at once when it must not run.
func (s *Server) acceptDelivery(ctx context.Context, q queue.Queue, d queue.Delivery) {
	env := d.Envelope()
	attempt := d.Attempt()
	s.mu.Lock()
	def, known := s.cat.Services[env.Service]
	offered := s.offers[env.Service].Parallel > 0
	key := s.cfg.Server.Key
	s.mu.Unlock()
	band := queue.BandFor(env.Priority)
	base := queue.JobState{JobID: env.JobID, Caller: env.Caller, Service: env.Service, Band: band, Attempt: attempt, Server: key}
	h := &heldJob{d: d, caller: env.Caller, band: band, attempt: attempt, maxAttempts: maxAttempts(env, def.Retry.MaxAttempts),
		seq: int64(attempt) * eventSeqBase}

	if !known || !offered {
		// Pulled for a service this server no longer offers (a reload raced the pull): let another worker have it.
		s.log.Warn("queued job for a service not offered here; returned to the queue", "job_id", env.JobID, "service", env.Service)
		elog.Check("returning a job for an unoffered service", d.Requeue(ctx, 30*time.Second), "job_id", env.JobID)
		return
	}
	cur, rev, err := q.State(ctx, env.JobID)
	if err != nil && !errors.Is(err, queue.ErrNotFound) {
		s.log.Warn("cannot read job_state; returning the job", "job_id", env.JobID, "error", err)
		elog.Ignore("returning a job after a job_state read error", d.Requeue(ctx, 5*time.Second))
		return
	}
	if queue.FinalState(cur.State) {
		s.log.Info("queued job already finished elsewhere; dropping the duplicate delivery", "job_id", env.JobID, "state", cur.State)
		elog.Ignore("acking a duplicate delivery", d.Ack(ctx))
		return
	}
	h.rev = rev
	ctl, _ := q.GetControl(ctx, env.JobID)
	now := time.Now().UTC()
	switch {
	case ctl.Cancel:
		s.settleDirect(ctx, q, h, base, queue.StateCancelled, &queue.Event{Type: "cancelled", By: ctl.By}, "ack")
		return
	case env.Expired(now):
		s.settleDirect(ctx, q, h, base, queue.StateExpired, &queue.Event{Type: "expired", By: "queue"}, "ack")
		return
	case attempt > h.maxAttempts:
		msg := fmt.Sprintf("gave up after %d deliveries (the service allows %d attempts)", attempt-1, h.maxAttempts)
		s.settleDirect(ctx, q, h, base, queue.StateFailed, &queue.Event{Type: "failed", Kind: string(jobs.KindInfra), Message: msg}, "dead:"+msg)
		return
	}
	if queue.VendorModel(env.Model) {
		msg := "vendor API jobs run in ACT3 job_runner and appsrv, never on processing servers"
		s.settleDirect(ctx, q, h, base, queue.StateFailed, &queue.Event{Type: "failed", Kind: string(jobs.KindInvalidInput), Message: msg}, "dead:model_not_allowed")
		return
	}
	if ok, why := s.cfg.Models.Allowed(env.Model); !ok {
		if s.cfg.Queue.Role != "worker" {
			// Standalone: this machine is the only one pulling, so the job can never run. Say so now (§13.6).
			msg := fmt.Sprintf("model %q is not allowed on %s (%s)", env.Model, key, why)
			s.settleDirect(ctx, q, h, base, queue.StateFailed, &queue.Event{Type: "failed", Kind: string(jobs.KindInvalidInput), Message: msg}, "dead:model_not_allowed")
			return
		}
		// A worker in a fleet: another server may allow the model (§11.2). Back off so this one does not spin on it.
		s.log.Info("queued job needs a model this server refuses; returned to the queue", "job_id", env.JobID, "model", env.Model, "why", why)
		elog.Check("returning a job for a refused model", d.Requeue(ctx, time.Minute), "job_id", env.JobID)
		return
	}
	local, err := toLocalEnvelope(env)
	if err != nil {
		s.settleDirect(ctx, q, h, base, queue.StateFailed, &queue.Event{Type: "failed", Kind: string(jobs.KindInvalidInput), Message: err.Error()}, "dead:invalid_input")
		return
	}
	claimed := base
	claimed.State = queue.StateClaimed
	newRev, err := q.CASState(ctx, claimed, rev)
	if err != nil {
		s.log.Info("job_state changed while claiming; returning the job", "job_id", env.JobID, "error", err)
		elog.Ignore("returning a job after a lost claim race", d.Requeue(ctx, 2*time.Second))
		return
	}
	h.rev = newRev
	d.SetFence(newRev)
	handle := d.Handle()

	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.jobs[env.JobID]; ok && !old.State.Final() {
		// Redelivered to the server already running it (its claim lapsed): keep running, hold the new claim.
		if prev := s.held[env.JobID]; prev != nil {
			h.seq = max(h.seq, prev.seq)
		}
		s.held[env.JobID] = h
		old.Claim = &handle
		s.save(old, true)
		return
	}
	delete(s.jobs, env.JobID) // a finished local record of an earlier attempt
	retried, noRetry := attempt-1, 0
	j, err := s.acceptLocked(local, env.Caller, retried, &noRetry, &handle)
	if err != nil {
		s.log.Warn("queued job refused at intake", "job_id", env.JobID, "error", err)
		go s.settleDirect(context.WithoutCancel(ctx), q, h, base, queue.StateFailed,
			&queue.Event{Type: "failed", Kind: string(jobs.KindInvalidInput), Message: err.Error()}, "dead:invalid_input")
		return
	}
	s.held[env.JobID] = h
	s.eventLocked(j, "claimed", func(e *queue.Event) { e.State = queue.StateClaimed })
}

// settleDirect finishes a job that never reached a slot: write job_state, publish the final event, then act.
func (s *Server) settleDirect(ctx context.Context, q queue.Queue, h *heldJob, st queue.JobState, state string, ev *queue.Event, action string) {
	st.State = state
	ev.JobID, ev.Caller, ev.State, ev.Attempt, ev.Server, ev.Band = st.JobID, st.Caller, state, st.Attempt, st.Server, st.Band
	h.seq++
	ev.Seq = h.seq
	s.finishQueued(ctx, q, h, st, ev, action, 0)
}

// finishQueued is the one place a claim ends: fence check (CAS on job_state), event, then ack, requeue or dead.
// A stale fencing token means another worker holds the job now: this result is dropped and the claim left alone.
func (s *Server) finishQueued(ctx context.Context, q queue.Queue, h *heldJob, st queue.JobState, ev *queue.Event, action string, delay time.Duration) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	st.Kind, st.Message = ev.Kind, ev.Message
	if _, err := q.CASState(ctx, st, h.rev); err != nil {
		if errors.Is(err, queue.ErrFenced) {
			s.log.Warn("result fenced off: another worker holds this job now", "job_id", st.JobID, "state", st.State)
			elog.Warn("settling a queued job", err, "job_id", st.JobID, "outcome", st.State)
			return
		}
		s.log.Error("cannot write job_state; the broker will redeliver the job after the ack wait", "job_id", st.JobID, "error", err)
		elog.Error("settling a queued job", err, "job_id", st.JobID, "outcome", st.State)
		return
	}
	if err := s.publishSync(ctx, ev); err != nil {
		s.log.Error("cannot publish the final event", "job_id", st.JobID, "error", err)
		elog.Error("publishing a final queue event", err, "job_id", st.JobID, "type", ev.Type)
	}
	var err error
	switch {
	case action == "ack":
		err = h.d.Ack(ctx)
	case action == "requeue":
		err = h.d.Requeue(ctx, delay)
	case strings.HasPrefix(action, "dead:"):
		err = h.d.Dead(ctx, strings.TrimPrefix(action, "dead:"))
	}
	elog.Check("settling a queued job with the broker ("+action+")", err, "job_id", st.JobID)
	s.log.Info("queued job settled", "job_id", st.JobID, "caller", st.Caller, "state", st.State, "attempt", st.Attempt, "action", action)
}

// settleLocked is called by finishLocked when a queued job reaches a final local state. It hands the broker work
// to a goroutine the drain waits for, so a job's claim is always settled before the process exits.
func (s *Server) settleLocked(j *jobs.Job, retryable bool) {
	h := s.held[j.ID]
	if h == nil || s.q == nil {
		return
	}
	delete(s.held, j.ID)
	q := s.q
	st := queue.JobState{JobID: j.ID, Caller: h.caller, Service: j.Type, Band: h.band, Attempt: h.attempt, Server: s.cfg.Server.Key}
	h.seq++
	ev := &queue.Event{JobID: j.ID, Caller: h.caller, Seq: h.seq, Attempt: h.attempt, Server: st.Server, Band: h.band,
		Stage: string(j.ErrorStage), Kind: string(j.ErrorKind), Message: j.LastErr}
	action, delay := "ack", time.Duration(0)
	switch j.State {
	case jobs.Completed:
		st.State, ev.Type, ev.Kind, ev.Message, ev.Stage = queue.StateSucceeded, "succeeded", "", "", ""
		ev.Outputs, _ = json.Marshal(publicOutputs(j.Outputs))
		ev.Result = j.Result
	case jobs.Cancelled:
		st.State, ev.Type, ev.By = queue.StateCancelled, "cancelled", strings.TrimPrefix(j.LastErr, "cancelled by ")
	default:
		if retryable && h.attempt < h.maxAttempts {
			delay = backoff(h.attempt)
			if j.LastErr == errShutdown.Error() {
				delay = 0
			}
			next := time.Now().UTC().Add(delay)
			st.State, ev.Type, ev.NextAttemptAfter, action = queue.StateQueued, "requeued", &next, "requeue"
			j.Note = fmt.Sprintf("returned to the queue for attempt %d", h.attempt+1)
		} else {
			st.State, ev.Type, action = queue.StateFailed, "failed", "dead:"+string(j.ErrorKind)
		}
	}
	ev.State = st.State
	j.Claim = nil
	s.save(j, true)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer elog.Recover("settling a queued job", "job_id", st.JobID)
		s.finishQueued(context.Background(), q, h, st, ev, action, delay)
	}()
}

// publicOutputs is what a caller learns about outputs: names, sizes, hashes and where they went (no local paths).
func publicOutputs(outs []jobs.Output) []map[string]any {
	var r []map[string]any
	for _, o := range outs {
		m := map[string]any{"name": o.Name, "bytes": o.Bytes, "sha256": o.SHA256, "content_type": o.ContentType}
		if o.UploadedTo != "" {
			m["uploaded_to"] = o.UploadedTo
		}
		if o.Download != "" {
			m["download"] = o.Download // local API path on the server that ran the job
		}
		if o.Meta != nil {
			m["meta"] = o.Meta
		}
		r = append(r, m)
	}
	return r
}

// eventLocked queues a non-final event for a held job; it never blocks the scheduler (a full buffer drops it).
func (s *Server) eventLocked(j *jobs.Job, typ string, fill func(*queue.Event)) {
	h := s.held[j.ID]
	if h == nil || s.pub == nil {
		return
	}
	h.seq++
	ev := &queue.Event{JobID: j.ID, Caller: h.caller, Seq: h.seq, Type: typ, State: j.CallerState, Attempt: h.attempt,
		Server: s.cfg.Server.Key, Band: h.band, Stage: string(j.Stage), At: time.Now().UTC()}
	if fill != nil {
		fill(ev)
	}
	select {
	case s.pub <- pubReq{ev: ev}:
	default:
		s.log.Warn("event buffer full; dropped a non-final event", "job_id", j.ID, "type", typ)
	}
}

// progressEventLocked publishes progress at most every 5 s per job (§13.5).
func (s *Server) progressEventLocked(j *jobs.Job) {
	h := s.held[j.ID]
	if h == nil || time.Since(h.lastProgress) < 5*time.Second {
		return
	}
	h.lastProgress = time.Now()
	s.eventLocked(j, "progress", func(e *queue.Event) { e.Progress, e.Note = j.Progress, j.Note })
}

// publishSync sends an event through the publisher, behind every earlier event of the job, and waits.
func (s *Server) publishSync(ctx context.Context, ev *queue.Event) error {
	done := make(chan error, 1)
	select {
	case s.pub <- pubReq{ev: ev, done: done}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// publishLoop writes events in order. It keeps running through the drain (ctx ends after Shutdown).
func (s *Server) publishLoop(ctx context.Context, q queue.Queue) {
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-s.pub:
			pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			err := q.Publish(pctx, r.ev)
			cancel()
			if r.done != nil {
				r.done <- err
			} else if err != nil {
				s.log.Warn("event not published", "job_id", r.ev.JobID, "type", r.ev.Type, "error", err)
			}
		}
	}
}

// heartbeatLoop extends every held claim and refreshes this server's capability record.
func (s *Server) heartbeatLoop(ctx context.Context, q queue.Queue) {
	s.mu.Lock()
	every := s.cfg.Continuity.Heartbeat.Or(20 * time.Second)
	s.mu.Unlock()
	t := time.NewTicker(every)
	defer t.Stop()
	s.putServer(ctx, q)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s.mu.Lock()
		var ds []queue.Delivery
		for _, h := range s.held {
			ds = append(ds, h.d)
		}
		s.mu.Unlock()
		for _, d := range ds {
			if err := d.InProgress(ctx); err != nil && ctx.Err() == nil {
				s.log.Warn("could not extend a claim", "job_id", d.Envelope().JobID, "error", err)
			}
		}
		s.putServer(ctx, q)
	}
}

func (s *Server) putServer(ctx context.Context, q queue.Queue) {
	s.mu.Lock()
	st, _ := s.stateLocked()
	rec := queue.ServerRecord{Key: s.cfg.Server.Key, Role: s.cfg.Queue.Role, Version: s.opt.Version, State: st,
		Offers: map[string]int{}, Free: map[string]int{}}
	for id, o := range s.offers {
		if o.Parallel > 0 {
			rec.Offers[id] = o.Parallel
			rec.Free[id] = max(o.Parallel-s.activeBy[id], 0)
		}
	}
	s.mu.Unlock()
	if err := q.PutServer(ctx, rec); err != nil && ctx.Err() == nil {
		s.log.Warn("could not write the servers record", "error", err)
	}
}

// controlLoop applies cancel flags from job_control to jobs this server holds.
func (s *Server) controlLoop(ctx context.Context, q queue.Queue) {
	for ctx.Err() == nil {
		ch, err := q.WatchControl(ctx, nil)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
				continue
			}
		}
		for c := range ch {
			if !c.Cancel {
				continue
			}
			s.mu.Lock()
			_, mine := s.held[c.JobID]
			s.mu.Unlock()
			if mine {
				by := c.By
				if by == "" {
					by = "caller"
				}
				_, _ = s.Cancel(c.JobID, by)
			}
		}
	}
}

// reclaimAtBoot settles the claims a previous process held when it stopped (the journal saved their handles).
// A job whose runner can re-attach (runners.Reattacher: a detached task host, §9) runs again here under the same
// claim and attempt; any other job goes straight back to the queue instead of waiting out the ack wait.
func (s *Server) reclaimAtBoot(ctx context.Context, q queue.Queue) {
	s.mu.Lock()
	claims := s.reclaim
	s.reclaim = nil
	key := s.cfg.Server.Key
	s.mu.Unlock()
	for _, c := range claims {
		d, err := q.Resume(ctx, c)
		if err != nil {
			s.log.Info("claim from before the restart is gone (settled or redelivered)", "job_id", c.JobID, "error", err)
			continue
		}
		cur, rev, err := q.State(ctx, c.JobID)
		if err != nil || rev != c.FenceRev || cur.Server != key {
			s.log.Info("claim from before the restart was taken over; leaving it", "job_id", c.JobID)
			continue
		}
		h := &heldJob{d: d, caller: c.Caller, band: cur.Band, attempt: c.Attempt, rev: rev, seq: int64(c.Attempt)*eventSeqBase + eventSeqBase/2}
		if s.readopt(ctx, h, c) {
			continue
		}
		st := cur
		st.State = queue.StateQueued
		next := time.Now().UTC()
		ev := &queue.Event{JobID: c.JobID, Caller: c.Caller, Seq: h.seq, Type: "requeued", State: queue.StateQueued,
			Attempt: c.Attempt, Server: key, Band: cur.Band, Kind: string(jobs.KindInfra),
			Message: "server restarted during the job", NextAttemptAfter: &next}
		s.finishQueuedNow(ctx, q, h, st, ev)
	}
}

// readopt runs a reclaimed job again on this machine when its runner re-attaches; false means "requeue it".
func (s *Server) readopt(ctx context.Context, h *heldJob, c queue.ClaimHandle) bool {
	s.mu.Lock()
	def, ok := s.cat.Services[c.Service]
	r, _ := s.runners[def.Runner].(runners.Reattacher)
	offered := s.offers[c.Service].Parallel > 0
	s.mu.Unlock()
	if !ok || !offered || r == nil || !r.Reattachable() {
		return false
	}
	local, err := toLocalEnvelope(h.d.Envelope())
	if err != nil {
		return false
	}
	elog.Ignore("extending a re-adopted claim", h.d.InProgress(ctx))
	h.maxAttempts = maxAttempts(h.d.Envelope(), def.Retry.MaxAttempts)
	handle := h.d.Handle()
	handle.FenceRev = h.rev
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.jobs, c.JobID)
	noRetry := 0
	j, err := s.acceptLocked(local, c.Caller, c.Attempt-1, &noRetry, &handle)
	if err != nil {
		s.log.Warn("could not re-adopt a claimed job; returning it", "job_id", c.JobID, "error", err)
		return false
	}
	s.held[c.JobID] = h
	s.eventLocked(j, "claimed", func(e *queue.Event) { e.State, e.Note = queue.StateClaimed, "re-adopted after a restart" })
	s.log.Info("claimed job re-adopted after a restart", "job_id", c.JobID, "service", c.Service, "attempt", c.Attempt)
	return true
}

func (s *Server) finishQueuedNow(ctx context.Context, q queue.Queue, h *heldJob, st queue.JobState, ev *queue.Event) {
	// Events cannot go through publishLoop yet (it starts after reclaim); publish directly, in order.
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := q.CASState(pctx, st, h.rev); err != nil {
		s.log.Info("could not return a reclaimed job", "job_id", st.JobID, "error", err)
		return
	}
	elog.Check("publishing a requeued event at boot", q.Publish(pctx, ev), "job_id", st.JobID)
	elog.Check("returning a reclaimed job to the queue", h.d.Requeue(pctx, 0), "job_id", st.JobID)
	s.log.Info("claim from before the restart returned to the queue", "job_id", st.JobID, "attempt", st.Attempt)
}

// maxAttempts is the envelope's max_attempts, else the service's retry.max_attempts, at least 1.
func maxAttempts(e *queue.Envelope, service int) int {
	if e.MaxAttempts > 0 {
		return e.MaxAttempts
	}
	return max(service, 1)
}

// toLocalEnvelope turns a task envelope into the server's job envelope.
func toLocalEnvelope(e *queue.Envelope) (jobs.Envelope, error) {
	out := jobs.Envelope{JobID: e.JobID, Platform: e.Caller, Service: e.Service, PlatformJobType: e.CallerJobType,
		Priority: e.Priority, TraceID: e.TraceID, Tenant: e.Tenant, Deadline: e.Deadline, TimeoutSeconds: e.TimeoutSeconds,
		Params: e.Params}
	for name, raw := range map[string]json.RawMessage{"inputs": e.Inputs, "outputs": e.Outputs, "data_rules": e.DataRules} {
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		var dst any
		switch name {
		case "inputs":
			dst = &out.Inputs
		case "outputs":
			dst = &out.Outputs
		default:
			dst = &out.DataRules
		}
		if err := json.Unmarshal(raw, dst); err != nil {
			return out, fmt.Errorf("envelope %s: %v", name, err)
		}
	}
	for _, in := range out.Inputs {
		if in.Path != "" && e.Caller != "local" {
			return out, errors.New("only the local caller may pass inputs by path; other callers pass URLs")
		}
	}
	return out, nil
}

// ---- local API side ----

// QueueSubmit stores a job in the task queue as this machine's owner (caller "local", §14.5). Any server in the
// fleet that offers the service may run it, this one included.
func (s *Server) QueueSubmit(ctx context.Context, env *queue.Envelope) (queue.SubmitAck, error) {
	q, err := s.queueOrErr()
	if err != nil {
		return queue.SubmitAck{}, err
	}
	s.mu.Lock()
	_, known := s.cat.Services[env.Service]
	s.mu.Unlock()
	if !known {
		return queue.SubmitAck{}, fmt.Errorf("%w: %q is not in data/services", ErrUnknownService, env.Service)
	}
	env.Caller = "local"
	if env.JobID == "" {
		env.JobID = newID()
	}
	ack, err := q.Submit(ctx, env)
	if err != nil {
		return ack, err
	}
	// Admission creates job_state, create-only (§16.4), so GET /v1/queue/jobs/{id} answers QUEUED from the moment the
	// job is accepted. Without it a job waiting behind a busy slot reads as 404 and the caller takes it for lost
	// (2026-10-28: the Wan engine gave up on three live tasks within 22 s and they rendered for nobody). An existing
	// key (a claim that raced us, or a re-submit) wins: ErrFenced is the expected answer then.
	st := queue.JobState{JobID: env.JobID, Caller: env.Caller, Service: env.Service, State: queue.StateQueued, Band: ack.Band}
	if _, err := q.CASState(ctx, st, 0); err != nil && !errors.Is(err, queue.ErrFenced) {
		s.log.Warn("queued job has no job_state yet; it reads as not found until a server claims it", "job_id", env.JobID, "error", err)
	}
	return ack, nil
}

// QueueJob is GET /v1/queue/jobs/{id}: the job's job_state and every event the caller has been sent.
type QueueJob struct {
	State    queue.JobState `json:"job_state"`
	Revision uint64         `json:"revision"`
	Events   []queue.Event  `json:"events"`
	Control  queue.Control  `json:"control"` // cancel / pause flags set for the job
}

func (s *Server) QueueJob(ctx context.Context, id string) (*QueueJob, error) {
	q, err := s.queueOrErr()
	if err != nil {
		return nil, err
	}
	st, rev, err := q.State(ctx, id)
	if errors.Is(err, queue.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	evs, err := q.Events(ctx, st.Caller, id)
	if err != nil {
		return nil, err
	}
	if evs == nil {
		evs = []queue.Event{}
	}
	ctl, _ := q.GetControl(ctx, id)
	return &QueueJob{State: st, Revision: rev, Events: evs, Control: ctl}, nil
}

// QueueItem is one row of GET /v1/queue/jobs: a job the WORK stream holds, with its job_state and cancel flag.
type QueueItem struct {
	queue.WorkItem
	State   string `json:"state"`
	Attempt int    `json:"attempt,omitempty"`
	Server  string `json:"server,omitempty"`
	Cancel  bool   `json:"cancel,omitempty"`
	Held    bool   `json:"held"` // claimed by this server right now
}

// QueueList lists what the task queue holds (debugging: `ai_processing_server queue ls`).
func (s *Server) QueueList(ctx context.Context, limit int) ([]QueueItem, error) {
	q, err := s.queueOrErr()
	if err != nil {
		return nil, err
	}
	l, ok := q.(queue.Lister)
	if !ok {
		return nil, fmt.Errorf("%w: this queue backend cannot list its jobs", ErrInvalid)
	}
	items, err := l.List(ctx, limit)
	if err != nil {
		return nil, err
	}
	out := make([]QueueItem, 0, len(items))
	for _, it := range items {
		row := QueueItem{WorkItem: it}
		if st, _, err := q.State(ctx, it.JobID); err == nil {
			row.State, row.Attempt, row.Server = st.State, st.Attempt, st.Server
		}
		ctl, _ := q.GetControl(ctx, it.JobID)
		row.Cancel = ctl.Cancel
		s.mu.Lock()
		_, row.Held = s.held[it.JobID]
		s.mu.Unlock()
		out = append(out, row)
	}
	return out, nil
}

// inQueue reports whether the WORK stream still holds the job (a job_state write that failed at submit).
func inQueue(ctx context.Context, q queue.Queue, id string) bool {
	l, ok := q.(queue.Lister)
	if !ok {
		return false
	}
	items, err := l.List(ctx, 1000)
	if err != nil {
		return false
	}
	for _, it := range items {
		if it.JobID == id {
			return true
		}
	}
	return false
}

// QueueCancel sets the job's cancel flag; whichever server holds it (or pulls it later) stops it. A finished job is
// ErrFinal and a job the queue never had is ErrNotFound, so the caller is told, not answered "ok".
func (s *Server) QueueCancel(ctx context.Context, id, by string) error {
	q, err := s.queueOrErr()
	if err != nil {
		return err
	}
	st, rev, err := q.State(ctx, id)
	switch {
	case err == nil && queue.FinalState(st.State):
		return fmt.Errorf("%w: %s is %s since %s", ErrFinal, id, st.State, st.UpdatedAt.Format(time.RFC3339))
	case errors.Is(err, queue.ErrNotFound):
		if !inQueue(ctx, q, id) {
			return fmt.Errorf("%w: %s is not on the task queue", ErrNotFound, id)
		}
	case err != nil:
		return err
	}
	if err := q.SetControl(ctx, id, queue.Control{Cancel: true, By: by}); err != nil {
		return err
	}
	s.mu.Lock()
	_, mine := s.held[id]
	s.mu.Unlock()
	if mine {
		_, _ = s.Cancel(id, by)
	}
	if err == nil && st.State == queue.StateQueued {
		s.cancelWaiting(ctx, q, st, rev, by)
	}
	return nil
}

// cancelWaiting ends a job no server holds (§16.5) at once instead of when a slot frees: job_state QUEUED →
// CANCELLED (CAS) and a final `cancelled` event. Its WORK message stays; the server that pulls it reads the final
// state and acks it without running it. A claim that raced us fences the CAS off and the cancel flag stops it.
func (s *Server) cancelWaiting(ctx context.Context, q queue.Queue, st queue.JobState, rev uint64, by string) {
	st.State, st.Kind, st.Message, st.UpdatedAt = queue.StateCancelled, "", "cancelled before any server ran it", time.Time{}
	if _, err := q.CASState(ctx, st, rev); err != nil {
		s.log.Info("waiting job was claimed while cancelling; its cancel flag stops it", "job_id", st.JobID, "error", err)
		return
	}
	ev := &queue.Event{JobID: st.JobID, Caller: st.Caller, Seq: int64(st.Attempt)*eventSeqBase + eventSeqBase - 1,
		Type: "cancelled", State: queue.StateCancelled, Attempt: st.Attempt, Band: st.Band, By: by}
	if err := s.publishSync(ctx, ev); err != nil {
		elog.Error("publishing the cancelled event of a waiting job", err, "job_id", st.JobID)
	}
	s.log.Info("waiting job cancelled before it ran", "job_id", st.JobID, "by", by)
}

// BrokerView is GET /v1/broker.
type BrokerView struct {
	BrokerHealth
	Role    string               `json:"role"`
	Callers []string             `json:"callers"`
	Status  *queue.Status        `json:"status,omitempty"`
	Held    []string             `json:"held"`
	Servers []queue.ServerRecord `json:"servers"`
}

func (s *Server) Broker(ctx context.Context) BrokerView {
	s.mu.Lock()
	v := BrokerView{BrokerHealth: *s.brokerHealthLocked(), Role: s.cfg.Queue.Role, Callers: s.cfg.EnabledCallers(), Held: []string{}}
	for id := range s.held {
		v.Held = append(v.Held, id)
	}
	q := s.q
	s.mu.Unlock()
	sort.Strings(v.Held)
	if q != nil {
		st := q.Status(ctx)
		v.Status = &st
		v.Servers, _ = q.Servers(ctx)
	}
	if v.Servers == nil {
		v.Servers = []queue.ServerRecord{}
	}
	return v
}

// ErrBroker is returned to API callers when the queue is off or its broker is unreachable.
var ErrBroker = errors.New("task queue unavailable")

func (s *Server) queueOrErr() (queue.Queue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opt.OpenQueue == nil {
		return nil, fmt.Errorf("%w: queue.enabled is false in this_server.yaml", ErrBroker)
	}
	if s.q == nil {
		return nil, fmt.Errorf("%w: %s (%s)", ErrBroker, s.qState, s.qErr)
	}
	return s.q, nil
}
