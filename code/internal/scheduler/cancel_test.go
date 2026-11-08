//go:build !windows

package scheduler

// CANCEL MODE in the scheduler (pm/cancel.mdx §5.6, §6, §11 tests C-S1..C-S4).

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/procs"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
	"github.com/ACT3ai/ai_processing_server/internal/queue/queuetest"
	"github.com/ACT3ai/ai_processing_server/internal/runners"
	"github.com/ACT3ai/ai_processing_server/internal/taskhost"
)

// stopRunner renders until cancelled, then files a stop report the way runners.stopTask does.
type stopRunner struct{ started chan struct{} }

func (stopRunner) Name() string                                        { return "stopper" }
func (stopRunner) Check(context.Context, *jobs.Job, runners.Env) error { return nil }
func (r stopRunner) Run(ctx context.Context, _ *jobs.Job, _ runners.Env, rep runners.Reporter) (*runners.Result, error) {
	close(r.started)
	<-ctx.Done()
	if sr, ok := rep.(runners.StopRecorder); ok {
		now := time.Now().UTC()
		sr.Stopped(jobs.StopReport{Profile: "seedvr2", AskedAt: now, StoppedAt: now, Graceful: true, TreeVerified: true, GPU: "released"})
	}
	return nil, context.Cause(ctx)
}

func newCancelServer(t *testing.T, q queue.Queue, id string) (*Server, stopRunner) {
	t.Helper()
	state := t.TempDir()
	d, rev := claimOne(t, q, id)
	s, _ := newQueueServer(t, q, state)
	s.workDir, s.jobLogDir = filepath.Join(state, "work"), filepath.Join(state, "logs")
	os.MkdirAll(s.jobLogDir, 0o700)
	s.cancels, s.activeBy = map[string]context.CancelCauseFunc{}, map[string]int{}
	s.jobsCtx, s.stopJobs = context.WithCancelCause(context.Background())
	s.pool = NewSlotPool(hardware.Facts{}, config.Limits{})
	r := stopRunner{started: make(chan struct{})}
	s.runners = map[string]runners.Runner{"stopper": r}
	s.cat.Services["svc"] = config.ServiceDef{ID: "svc", Runner: "stopper"}
	s.opt.Monitor = hardware.NewMonitor(hardware.Options{}, hardware.Facts{})
	s.stats = counters{Processed: map[string]int64{}, Failed: map[string]int64{}, Cancelled: map[string]int64{}, Retried: map[string]int64{}}
	h := d.Handle()
	s.mu.Lock()
	j := &jobs.Job{ID: id, Type: "svc", Queue: "local", Claim: &h, CreatedAt: time.Now().UTC(), Timeout: 3600}
	j.Move(jobs.Pending, jobs.StageLeased, "claimed")
	s.jobs[id] = j
	s.held[id] = &heldJob{d: d, caller: "local", band: "p3", attempt: 1, maxAttempts: 3, rev: rev}
	s.mu.Unlock()
	return s, r
}

func jobState(s *Server, id string) (jobs.State, *jobs.StopReport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[id]
	return j.State, j.Stop
}

// C-S1: a cancel flag the control watch never delivered (set while the controller was down, or while the watch was
// broken) is found by reconcileControl and stops the running job. Before cancel mode it was lost: the render ran on.
func TestMissedCancelIsAppliedByReconcile(t *testing.T) {
	q := queuetest.NewMem()
	s, r := newCancelServer(t, q, "c1")
	s.mu.Lock()
	s.startLocked(s.jobs["c1"], jobs.Slot{ID: "gpu0"})
	s.mu.Unlock()
	<-r.started
	if err := q.SetControl(context.Background(), "c1", queue.Control{Cancel: true, By: "production"}); err != nil {
		t.Fatal(err)
	}
	s.reconcileControl(context.Background(), q)
	waitFor(t, "the job to end cancelled", func() bool { st, _ := jobState(s, "c1"); return st == jobs.Cancelled })
	_, stop := jobState(s, "c1")
	if stop == nil || stop.Profile != "seedvr2" || !stop.TreeVerified {
		t.Fatalf("stop report: %+v", stop)
	}
	// C-S2: the final event says it was cancelled, by whom, and carries the stop report; it is never requeued.
	waitFor(t, "the cancelled event", func() bool {
		evs, _ := q.Events(context.Background(), "local", "c1")
		return countType(evs, "cancelled") == 1
	})
	evs, _ := q.Events(context.Background(), "local", "c1")
	if countType(evs, "requeued") != 0 {
		t.Fatal("a cancelled job was requeued")
	}
	for _, e := range evs {
		if e.Type != "cancelled" {
			continue
		}
		if e.By != "production" || !strings.Contains(e.Note, "seedvr2 stopped gracefully") {
			t.Fatalf("cancelled event: by %q note %q", e.By, e.Note)
		}
		var res struct {
			Stop jobs.StopReport `json:"stop"`
		}
		if json.Unmarshal(e.Result, &res) != nil || res.Stop.GPU != "released" {
			t.Fatalf("cancelled event result: %s", e.Result)
		}
	}
	if st, _, _ := q.State(context.Background(), "c1"); st.State != queue.StateCancelled {
		t.Fatalf("job_state %s, want CANCELLED", st.State)
	}
	// a second reconcile is a no-op
	s.reconcileControl(context.Background(), q)
}

// C-S3: a job cancelled while it waits for a slot, whose task host is already rendering (re-adopted after a
// restart), is stopped WITHOUT a slot and never started (CTO review M5): the run directory is stopped directly,
// verified and removed, and only then is the job finished.
func TestCancelOfAWaitingJobWithALiveTaskHostStopsItWithoutASlot(t *testing.T) {
	oldM := runners.StopMargin
	runners.StopMargin = 300 * time.Millisecond
	t.Cleanup(func() { runners.StopMargin = oldM })
	q := queuetest.NewMem()
	s, _ := newCancelServer(t, q, "c2")
	dir := filepath.Join(s.opt.StateDir, "runs", runners.RunDirName("c2"))
	os.MkdirAll(dir, 0o700)
	host := exec.Command("/bin/sh", "-c", `trap "" INT TERM; sleep 60`) // a task host that never stops by itself
	host.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	go host.Wait()
	t.Cleanup(func() { syscall.Kill(-host.Process.Pid, syscall.SIGKILL) })
	at, _ := procs.StartTime(host.Process.Pid)
	b, _ := json.Marshal(taskhost.Task{RunVersion: 1, JobID: "c2", Argv: []string{"true"}, Output: filepath.Join(dir, "out.mp4"),
		Cancel: &taskhost.CancelSpec{Profile: "fake", GraceSeconds: 0.2}})
	os.WriteFile(filepath.Join(dir, taskhost.TaskFile), b, 0o600)
	b, _ = json.Marshal(taskhost.Heartbeat{PID: host.Process.Pid, Boot: taskhost.BootID(), StartedAt: at, At: time.Now()})
	os.WriteFile(filepath.Join(dir, taskhost.HeartbeatFile), b, 0o600)

	if _, err := s.Cancel("c2", "staff"); err != nil {
		t.Fatal(err)
	}
	if st, _ := jobState(s, "c2"); st == jobs.Cancelled {
		t.Fatal("finished at once: the live task host would be orphaned")
	}
	s.mu.Lock()
	_, stopping := s.cancelPending["c2"]
	s.mu.Unlock()
	if !stopping {
		t.Fatal("not marked as stopping: admit could start it")
	}
	waitFor(t, "the run directory to be stopped and the job cancelled", func() bool { st, _ := jobState(s, "c2"); return st == jobs.Cancelled })
	_, stop := jobState(s, "c2")
	if stop == nil || !stop.Escalated || !stop.TreeVerified {
		t.Fatalf("stop report %+v", stop)
	}
	if syscall.Kill(host.Process.Pid, 0) == nil {
		t.Fatal("the task host survived")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("run directory kept after a verified stop")
	}
}

// C-S4: a waiting job with no task host is cancelled at once, and its stop report says it never started.
func TestCancelOfAWaitingJobFinishesAtOnce(t *testing.T) {
	q := queuetest.NewMem()
	s, _ := newCancelServer(t, q, "c3")
	if _, err := s.Cancel("c3", "staff"); err != nil {
		t.Fatal(err)
	}
	st, stop := jobState(s, "c3")
	if st != jobs.Cancelled || stop == nil || stop.Graceful || !strings.Contains(stop.Summary(), "before it started") {
		t.Fatalf("state %s stop %+v", st, stop)
	}
}
