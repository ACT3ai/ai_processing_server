package scheduler

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
	"github.com/ACT3ai/ai_processing_server/internal/queue/queuetest"
	"github.com/ACT3ai/ai_processing_server/internal/runners"
)

// flakyQ is the in-memory queue with a broker that can be "down", and with replies that can be "lost" (the write
// lands, the caller gets an error), the two ways a settlement goes wrong in production.
type flakyQ struct {
	*queuetest.Mem
	mu             sync.Mutex
	down           bool
	loseCASReply   int // the next n CAS writes land but report ErrUnavailable
	losePubReply   int // the next n publishes land but report ErrUnavailable
	succeededCalls int // publishes of a "succeeded" event that reached the store
}

func (f *flakyQ) isDown() bool   { f.mu.Lock(); defer f.mu.Unlock(); return f.down }
func (f *flakyQ) setDown(v bool) { f.mu.Lock(); f.down = v; f.mu.Unlock() }

func (f *flakyQ) CASState(ctx context.Context, st queue.JobState, rev uint64) (uint64, error) {
	if f.isDown() {
		return 0, queue.ErrUnavailable
	}
	n, err := f.Mem.CASState(ctx, st, rev)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err == nil && f.loseCASReply > 0 {
		f.loseCASReply--
		return 0, queue.ErrUnavailable
	}
	return n, err
}

func (f *flakyQ) State(ctx context.Context, id string) (queue.JobState, uint64, error) {
	if f.isDown() {
		return queue.JobState{}, 0, queue.ErrUnavailable
	}
	return f.Mem.State(ctx, id)
}

func (f *flakyQ) Publish(ctx context.Context, ev *queue.Event) error {
	if f.isDown() {
		return queue.ErrUnavailable
	}
	err := f.Mem.Publish(ctx, ev) // Mem dedupes ids forever; EVENTS only for 2 minutes, so count the calls

	f.mu.Lock()
	defer f.mu.Unlock()
	if ev.Type == "succeeded" {
		f.succeededCalls++
	}
	if err == nil && f.losePubReply > 0 {
		f.losePubReply--
		return queue.ErrUnavailable
	}
	return err
}

func (f *flakyQ) Events(ctx context.Context, caller, id string) ([]queue.Event, error) {
	if f.isDown() {
		return nil, queue.ErrUnavailable
	}
	return f.Mem.Events(ctx, caller, id)
}

func (f *flakyQ) Resume(ctx context.Context, h queue.ClaimHandle) (queue.Delivery, error) {
	if f.isDown() {
		return nil, queue.ErrUnavailable
	}
	return f.Mem.Resume(ctx, h)
}

var testLane = []queue.ConsumerRef{{Band: "p3", Caller: "local", Service: "sys.compute"}}

// newQueueServer is a scheduler with just enough wiring for the queue link: journal, publisher, outbox.
func newQueueServer(t *testing.T, q queue.Queue, state string) (*Server, context.CancelFunc) {
	t.Helper()
	jr, err := jobs.NewJournal(filepath.Join(state, "journal"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{opt: Options{StateDir: state}, cfg: &config.ThisServer{Server: config.Identity{Key: "TEST"}},
		cat: &config.Catalog{Services: map[string]config.ServiceDef{}}, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		jobs: map[string]*jobs.Job{}, held: map[string]*heldJob{}, settling: map[string]queue.Delivery{},
		settleRun: map[string]bool{}, pub: make(chan pubReq, 64), journal: jr, lastSave: map[string]time.Time{},
		q: q, qwake: make(chan struct{}, 1), wake: make(chan struct{}, 1)}
	s.settleCtx, s.stopSettle = context.WithCancel(ctx)
	all, _ := jr.LoadAll()
	for _, j := range all {
		s.jobs[j.ID] = j
	}
	go s.publishLoop(ctx, q)
	t.Cleanup(func() { s.stopSettle(); cancel(); s.swg.Wait() })
	return s, cancel
}

// claimOne submits a job, pulls it and claims it the way acceptDelivery does.
func claimOne(t *testing.T, q queue.Queue, id string) (queue.Delivery, uint64) {
	t.Helper()
	ctx := context.Background()
	if _, err := q.Submit(ctx, &queue.Envelope{JobID: id, Caller: "local", Service: "sys.compute", Priority: 30}); err != nil {
		t.Fatal(err)
	}
	d, err := q.Pull(ctx, testLane, time.Second)
	if err != nil || d == nil {
		t.Fatalf("pull: %v %v", d, err)
	}
	rev, err := q.CASState(ctx, queue.JobState{JobID: id, Caller: "local", Service: "sys.compute", Band: "p3",
		State: queue.StateClaimed, Attempt: 1, Server: "TEST"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	d.SetFence(rev)
	return d, rev
}

func finishLocally(s *Server, d queue.Delivery, rev uint64, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := d.Handle()
	now := time.Now().UTC()
	j := &jobs.Job{ID: id, Type: "sys.compute", Queue: "local", Claim: &h, CreatedAt: now, FinishedAt: &now,
		Outputs: []jobs.Output{{Name: "result", Bytes: 3, SHA256: "abc"}}}
	j.Move(jobs.Completed, jobs.StageDone, "done")
	s.jobs[id] = j
	s.held[id] = &heldJob{d: d, caller: "local", band: "p3", attempt: 1, maxAttempts: 2, rev: rev, seq: eventSeqBase}
	s.settleLocked(j, false)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func countType(evs []queue.Event, typ string) int {
	n := 0
	for _, e := range evs {
		if e.Type == typ {
			n++
		}
	}
	return n
}

// A 30-minute job finishes while the broker is down, the process restarts before the broker is back, and the broker
// redelivers the job: the outcome waits in the journal's outbox, the redelivery is settled from it instead of running
// the job again, and the caller gets exactly one "succeeded" (D-026).
func TestSettlementSurvivesBrokerOutageAndRestart(t *testing.T) {
	old := SettleGrace
	SettleGrace = 0
	defer func() { SettleGrace = old }()
	state := t.TempDir()
	q := &flakyQ{Mem: queuetest.NewMem()}
	ctx := context.Background()
	d, rev := claimOne(t, q, "j1")

	q.setDown(true)
	s1, stop1 := newQueueServer(t, q, state)
	finishLocally(s1, d, rev, "j1")
	time.Sleep(200 * time.Millisecond)
	if st, _, _ := q.Mem.State(ctx, "j1"); st.State != queue.StateClaimed {
		t.Fatalf("job_state written while the broker was down: %s", st.State)
	}
	s1.stopSettle() // the process stops; the outcome stays in the journal
	stop1()
	s1.swg.Wait()

	s2, _ := newQueueServer(t, q, state)
	if j := s2.jobs["j1"]; j == nil || j.Settle == nil || j.Settle.State.State != queue.StateSucceeded {
		t.Fatalf("the restarted process has no outbox entry for the finished job: %+v", j)
	}
	q.setDown(false)
	// The broker redelivers the job (its claim lapsed while the broker was down).
	d2, err := q.Mem.Resume(ctx, d.Handle())
	if err != nil {
		t.Fatal(err)
	}
	s2.acceptDelivery(ctx, q, d2)
	s2.resumeSettlements()
	waitFor(t, "the outcome to settle", func() bool {
		s2.mu.Lock()
		defer s2.mu.Unlock()
		return s2.jobs["j1"].Settle == nil
	})
	st, _, _ := q.Mem.State(ctx, "j1")
	evs, _ := q.Mem.Events(ctx, "local", "j1")
	if st.State != queue.StateSucceeded || countType(evs, "succeeded") != 1 {
		t.Fatalf("job_state %s, %d succeeded events; want SUCCEEDED and exactly 1", st.State, countType(evs, "succeeded"))
	}
	if s2.jobs["j1"].State != jobs.Completed {
		t.Fatal("the redelivery ran the job again")
	}
	if again, _ := q.Pull(ctx, testLane, 100*time.Millisecond); again != nil {
		t.Fatal("the settled job is still in the queue (not acked)")
	}
}

// Lost replies: the job_state write and the event publish both land but report an error. The retries must not
// publish a second "succeeded" (a double charge), and the job_state write must be recognized as our own.
func TestSettlementLostRepliesPublishOnce(t *testing.T) {
	q := &flakyQ{Mem: queuetest.NewMem()}
	ctx := context.Background()
	d, rev := claimOne(t, q, "j2")
	q.loseCASReply, q.losePubReply = 1, 1
	s, _ := newQueueServer(t, q, t.TempDir())
	finishLocally(s, d, rev, "j2")
	waitFor(t, "the outcome to settle", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.jobs["j2"].Settle == nil
	})
	st, _, _ := q.Mem.State(ctx, "j2")
	if st.State != queue.StateSucceeded {
		t.Fatalf("job_state %s: the lost CAS reply was taken for a fence", st.State)
	}
	if q.succeededCalls != 1 {
		t.Fatalf("succeeded published %d times, want 1", q.succeededCalls)
	}
}

// Another worker took the job over while this one's outcome waited: the outcome is dropped, nothing is published.
func TestSettlementFencedOffIsDropped(t *testing.T) {
	q := &flakyQ{Mem: queuetest.NewMem()}
	ctx := context.Background()
	d, rev := claimOne(t, q, "j3")
	q.setDown(true)
	s, _ := newQueueServer(t, q, t.TempDir())
	finishLocally(s, d, rev, "j3")
	if _, err := q.Mem.CASState(ctx, queue.JobState{JobID: "j3", State: queue.StateClaimed, Attempt: 2, Server: "OTHER"}, rev); err != nil {
		t.Fatal(err)
	}
	q.setDown(false)
	waitFor(t, "the outcome to be dropped", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.jobs["j3"].Settle == nil
	})
	evs, _ := q.Mem.Events(ctx, "local", "j3")
	if st, _, _ := q.Mem.State(ctx, "j3"); st.Server != "OTHER" || countType(evs, "succeeded") != 0 {
		t.Fatalf("fenced result was reported: state %+v, events %v", st, evs)
	}
}

// The boot reclaim keeps a claim while the broker cannot answer for it (it was dropped as "gone" before, and the
// sweep then cancelled its still-rendering task host as an orphan), and settles it once the broker is back.
func TestReclaimWaitsForTheBroker(t *testing.T) {
	old := ReclaimRetry
	ReclaimRetry = 50 * time.Millisecond
	defer func() { ReclaimRetry = old }()
	q := &flakyQ{Mem: queuetest.NewMem()}
	ctx := context.Background()
	d, rev := claimOne(t, q, "j4")
	h := d.Handle()
	h.FenceRev, h.Attempt = rev, 1
	s, _ := newQueueServer(t, q, t.TempDir())
	s.reclaim = []queue.ClaimHandle{h}
	q.setDown(true)
	done := make(chan struct{})
	go func() { s.reclaimAtBoot(ctx, q); close(done) }()
	time.Sleep(200 * time.Millisecond)
	s.mu.Lock()
	n := len(s.reclaim)
	s.mu.Unlock()
	if n != 1 {
		t.Fatal("the claim was dropped while the broker was unreachable")
	}
	q.setDown(false)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the reclaim did not finish after the broker came back")
	}
	if st, _, _ := q.Mem.State(ctx, "j4"); st.State != queue.StateQueued {
		t.Fatalf("job_state %s after the reclaim, want QUEUED (returned to the queue)", st.State)
	}
}

// Low disk on the state dir (run directories, segments, snapshots) parks new work with the reason on every waiting
// job and in /v1/health, instead of starting a render that dies when the disk fills.
func TestLowDiskParksWorkWithAReason(t *testing.T) {
	mon := hardware.NewMonitor(hardware.Options{}, hardware.Facts{})
	mon.SetLoad(hardware.Load{DiskFreeGB: 5, DiskPath: "/data/git_ignore"})
	s, _ := newQueueServer(t, queuetest.NewMem(), t.TempDir())
	s.opt.Monitor = mon
	s.started = true
	s.cfg.Limits = config.Limits{DiskMinFreeGB: 20, MaxParallelJobs: 2}
	j := &jobs.Job{ID: "w1", Type: "sys.compute", CreatedAt: time.Now().UTC()}
	j.Move(jobs.Pending, jobs.StageLeased, "accepted")
	s.jobs["w1"] = j
	s.admit()
	if j.State != jobs.Pending || !strings.Contains(j.Note, "disk free 5.0 GB at /data/git_ignore") {
		t.Fatalf("waiting job: state %s note %q", j.State, j.Note)
	}
	s.mu.Lock()
	st, why := s.stateLocked()
	order := s.pullOrderLocked()
	s.mu.Unlock()
	if st != StateDegraded || !strings.Contains(why, "disk_min_free_gb 20.0") || order != nil {
		t.Fatalf("health %s %q, pull order %v", st, why, order)
	}
}

// detachRunner stands in for a task-host runner: it renders until its context ends and never stops the render.
type detachRunner struct{ started chan struct{} }

func (detachRunner) Name() string                                        { return "detach" }
func (detachRunner) Reattachable() bool                                  { return true }
func (detachRunner) Check(context.Context, *jobs.Job, runners.Env) error { return nil }
func (r detachRunner) Run(ctx context.Context, _ *jobs.Job, _ runners.Env, _ runners.Reporter) (*runners.Result, error) {
	close(r.started)
	<-ctx.Done()
	return nil, context.Cause(ctx)
}

// `just update` mid-render (a stop signal, §9.5): the controller lets go of a queued task-host job at once, without
// finishing it, requeueing it or waiting out the drain timeout; the journal keeps it active with its claim so the
// next controller re-adopts it on attempt 1. Before, the stop waited 10 minutes, then failed the job as infra: it was
// requeued and rendered again elsewhere while its task host kept going here.
func TestStopLeavesATaskHostJobToTheNextController(t *testing.T) {
	q := queuetest.NewMem()
	state := t.TempDir()
	d, rev := claimOne(t, q, "r1")
	s, _ := newQueueServer(t, q, state)
	s.workDir, s.jobLogDir = filepath.Join(state, "work"), filepath.Join(state, "logs")
	s.cancels, s.activeBy = map[string]context.CancelCauseFunc{}, map[string]int{}
	s.jobsCtx, s.stopJobs = context.WithCancelCause(context.Background())
	s.pool = NewSlotPool(hardware.Facts{}, config.Limits{})
	r := detachRunner{started: make(chan struct{})}
	s.runners = map[string]runners.Runner{"detach": r}
	s.cat.Services["svc"] = config.ServiceDef{ID: "svc", Runner: "detach"}
	s.opt.Monitor = hardware.NewMonitor(hardware.Options{}, hardware.Facts{})
	h := d.Handle()
	s.mu.Lock()
	j := &jobs.Job{ID: "r1", Type: "svc", Queue: "local", Claim: &h, CreatedAt: time.Now().UTC(), Timeout: 3600}
	j.Move(jobs.Pending, jobs.StageLeased, "claimed")
	s.jobs["r1"] = j
	s.held["r1"] = &heldJob{d: d, caller: "local", band: "p3", attempt: 1, maxAttempts: 2, rev: rev}
	s.startLocked(j, jobs.Slot{ID: "cpu0"})
	s.mu.Unlock()
	<-r.started

	start := time.Now()
	s.Shutdown(time.Hour)
	if time.Since(start) > 5*time.Second {
		t.Fatalf("stop waited %s for a job its task host keeps running", time.Since(start))
	}
	jr, _ := jobs.NewJournal(filepath.Join(state, "journal"))
	all, _ := jr.LoadAll()
	if len(all) != 1 || all[0].State != jobs.Active || all[0].Claim == nil || all[0].Settle != nil {
		t.Fatalf("journal after stop: %+v", all)
	}
	if st, _, _ := q.State(context.Background(), "r1"); st.State != queue.StateClaimed {
		t.Fatalf("job_state %s after a stop; the claim must stay with this machine", st.State)
	}
}
