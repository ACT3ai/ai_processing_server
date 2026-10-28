// Package scheduler is the controller (pm/task_queue.mdx §8, §9; spec §9, §10, §15.2): our coordination logic
// over the task queue. It admits submissions (standalone role), pulls work in priority order only when a slot is
// free, starts each job in a detached task host from a frozen code snapshot, turns the task host's files into
// events, renews the claim, fences the result, and re-adopts running jobs after a restart.
//
// The controller keeps nothing that lives only in memory: queued jobs are in the broker, running jobs in their task
// hosts and run directories, and the job records the local API serves in the journal. It can be killed at any
// moment and a new one carries on.
package scheduler

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/capability"
	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/errlog"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/priority"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
	"github.com/ACT3ai/ai_processing_server/internal/runners"
	"github.com/ACT3ai/ai_processing_server/internal/runs"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
)

// Health states (spec §12.4).
const (
	StateStarting = "STARTING"
	StateOnline   = "ONLINE"
	StateBusy     = "BUSY"
	StateDegraded = "DEGRADED"
	StatePaused   = "PAUSED"
	StateDraining = "DRAINING"
)

// Errors returned to API callers.
var (
	ErrUnknownService = errors.New("unknown service")
	ErrNotOffered     = errors.New("service not offered on this server")
	ErrNotFound       = errors.New("job not found")
	ErrConflict       = errors.New("job id already exists")
	ErrFinal          = errors.New("job already finished")
	ErrInvalid        = errors.New("invalid job")
	ErrBrokerMissing  = errors.New("broker_missing")
)

// elog records failures that should not happen (pm/errors.mdx): the journal, run dirs, the broker, panics.
var elog = errlog.For("scheduler")

// Loader re-reads this_server.yaml and data/ for a reload. It returns an error instead of a partial config.
type Loader func() (*config.ThisServer, *config.Catalog, []string, error)

// OpenQueue connects to the broker. It is retried every few seconds until it succeeds.
type OpenQueue func(ctx context.Context, cfg *config.ThisServer, cat *config.Catalog) (queue.Queue, error)

// Options construct a Server.
type Options struct {
	Server       *config.ThisServer
	Catalog      *config.Catalog
	Monitor      *hardware.Monitor
	StateDir     string // data/git_ignore
	Loader       Loader
	OpenQueue    OpenQueue
	Snapshot     *snapshot.Snapshot // the frozen copy of this binary new jobs run from (§9.2)
	TaskHostArgs []string           // passed to every task host (--error-dir, --log-dir, --root)
	BrokerFix    string             // the command that starts the broker, shown when it is missing
	Version      string
	GitSHA       string
	Warnings     []string
	Logger       *slog.Logger
	// Intervals; zero means the spec default.
	TelemetryEvery time.Duration
	SweepEvery     time.Duration
	HardwareEvery  time.Duration
}

// Server is one controller.
type Server struct {
	mu        sync.Mutex
	opt       Options
	cfg       *config.ThisServer
	cat       *config.Catalog
	effective hardware.Facts
	drift     []hardware.Drift
	offers    map[string]capability.Offer
	pool      *SlotPool
	jobs      map[string]*jobs.Job
	held      map[string]*held // running here: claim, task host, run dir
	activeBy  map[string]int
	pausedSvc map[string]string // service → who paused it
	draining  bool
	started   bool
	warnings  []string
	journal   *jobs.Journal
	wake      chan struct{}
	log       *slog.Logger
	bootAt    time.Time
	stats     counters
	lastSave  map[string]time.Time

	q         queue.Queue // nil while the broker is missing
	brokerErr string
	readopted bool
	ctrl      <-chan queue.ControlChange
	lastPut   time.Time
	lastConn  time.Time

	workDir, resultsDir, jobLogDir, runsDir, snapDir string
}

type counters struct {
	Processed map[string]int64 // service → finished here (any outcome)
	Failed    map[string]int64
	Cancelled map[string]int64
	Retried   map[string]int64
	Readopted int64
	Lost      int64 // task hosts found gone at re-adoption (readopt_lost)
}

// New builds the controller and loads the journal. It does not connect to the broker or start any loop.
func New(o Options) (*Server, error) {
	if o.TelemetryEvery == 0 {
		o.TelemetryEvery = 5 * time.Second
	}
	if o.SweepEvery == 0 {
		o.SweepEvery = 10 * time.Minute
	}
	if o.HardwareEvery == 0 {
		o.HardwareEvery = 10 * time.Minute
	}
	s := &Server{
		opt: o, cfg: o.Server, cat: o.Catalog, jobs: map[string]*jobs.Job{}, held: map[string]*held{}, activeBy: map[string]int{},
		pausedSvc: map[string]string{}, wake: make(chan struct{}, 1), log: o.Logger.With("ctx", "scheduler"),
		bootAt: time.Now(), lastSave: map[string]time.Time{}, warnings: o.Warnings, brokerErr: "not connected yet",
		stats: counters{Processed: map[string]int64{}, Failed: map[string]int64{}, Cancelled: map[string]int64{}, Retried: map[string]int64{}},
	}
	s.workDir = s.cfg.Paths.WorkDir
	if s.workDir == "" {
		s.workDir = filepath.Join(o.StateDir, "work")
	}
	s.workDir = config.ExpandHome(s.workDir)
	s.resultsDir = filepath.Join(o.StateDir, "results")
	s.jobLogDir = filepath.Join(o.StateDir, "logs", "jobs")
	s.runsDir = filepath.Join(o.StateDir, "runs")
	s.snapDir = filepath.Join(o.StateDir, "snapshots")
	for _, d := range []string{s.workDir, s.resultsDir, s.jobLogDir, s.runsDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, err
		}
	}
	// state/jobs/ holds task-queue records; the v0.1 journal in state/journal/ (Asynq-shaped) is left untouched.
	j, err := jobs.NewJournal(filepath.Join(o.StateDir, "state", "jobs"))
	if err != nil {
		return nil, err
	}
	s.journal = j
	s.recompute()
	s.load()
	return s, nil
}

// recompute derives effective hardware, offers and pool size. Caller holds mu (or is New).
func (s *Server) recompute() {
	s.effective, s.drift = hardware.Effective(s.opt.Monitor.Detected(), s.cfg.Hardware)
	offers := capability.ComputeOffers(capability.Input{Catalog: s.cat, Server: s.cfg, Facts: s.effective, Runners: runners.Names()})
	s.offers = map[string]capability.Offer{}
	for _, o := range offers {
		s.offers[o.Service] = o
	}
	if s.pool == nil {
		s.pool = NewSlotPool(s.effective, s.cfg.Limits)
	} else {
		s.pool.Resize(s.effective, s.cfg.Limits)
	}
	for _, d := range s.drift {
		if d.Level == "error" {
			s.log.Error("hardware drift", "detail", d.Message)
		} else {
			s.log.Info("hardware drift", "detail", d.Message)
		}
	}
}

// load reads the journal. Records of jobs that were running stay as they are: their task hosts are re-adopted
// once the broker is reachable (§9.4), and waiting jobs are still in the broker.
func (s *Server) load() {
	all, errs := s.journal.LoadAll()
	for _, e := range errs {
		s.log.Warn("journal entry unreadable", "error", e)
		elog.Error("reading a journal entry at boot", e)
	}
	for _, j := range all {
		s.jobs[j.ID] = j
	}
	if len(all) > 0 {
		s.log.Info("journal loaded", "jobs", len(all))
	}
}

// Run starts the loops and blocks until ctx ends. Call Shutdown first to stop pulling.
func (s *Server) Run(ctx context.Context) {
	s.mu.Lock()
	s.started = true
	s.mu.Unlock()
	s.log.Info("controller started", "offers", s.offerSummary(), "code_version", s.opt.Snapshot.ID)
	s.opt.Monitor.SampleLoad(ctx)
	tWork := time.NewTicker(200 * time.Millisecond)
	tTele := time.NewTicker(s.opt.TelemetryEvery)
	tSweep := time.NewTicker(s.opt.SweepEvery)
	tHW := time.NewTicker(s.opt.HardwareEvery)
	defer func() { tWork.Stop(); tTele.Stop(); tSweep.Stop(); tHW.Stop() }()
	s.safely("connecting to the broker", func() { s.connect(ctx) })
	s.sweep()
	tick := func() {
		s.safely("connecting to the broker", func() { s.connect(ctx) })
		s.safely("re-adopting running jobs", func() { s.readopt(ctx) })
		s.safely("supervising running jobs", func() { s.supervise(ctx) })
		s.safely("admitting submissions", func() { s.admitSubmissions(ctx) })
		s.safely("pulling work", func() { s.pull(ctx) })
		s.safely("publishing this server's record", func() { s.putServer(ctx) })
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
			tick()
		case <-tWork.C:
			tick()
		case c, ok := <-s.ctrlChan():
			if !ok { // the watch ended (connection closed): stop selecting on it
				s.mu.Lock()
				s.ctrl = nil
				s.mu.Unlock()
				continue
			}
			s.safely("applying a control change", func() { s.applyControl(ctx, c) })
		case <-tTele.C:
			s.safely("sampling load telemetry", func() { s.opt.Monitor.SampleLoad(ctx) })
		case <-tSweep.C:
			s.safely("sweeping old jobs, run dirs and snapshots", s.sweep)
		case <-tHW.C:
			s.safely("refreshing hardware detection", func() { s.RefreshHardware(ctx) })
		}
	}
}

func (s *Server) ctrlChan() <-chan queue.ControlChange {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ctrl // nil blocks forever until the broker is connected
}

// safely runs one step of the main loop; a panic is recorded in error.err and the loop keeps going.
func (s *Server) safely(attempting string, fn func()) {
	defer elog.Recover(attempting)
	fn()
}

func (s *Server) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Server) offerSummary() string {
	var on []string
	for id, o := range s.offers {
		if o.Parallel > 0 {
			on = append(on, fmt.Sprintf("%s×%d", id, o.Parallel))
		}
	}
	sort.Strings(on)
	return fmt.Sprint(on)
}

func (s *Server) queueNow() queue.Queue {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.q
}

// Submit accepts a job from the local API: it builds the task envelope and stores it on SUBMIT (§10.1, caller
// `local`). The owner's fence applies first: a service that is off here, or that this hardware cannot run, is
// refused up front rather than left waiting forever.
func (s *Server) Submit(ctx context.Context, env jobs.Envelope) (*jobs.Job, error) {
	s.mu.Lock()
	q := s.q
	def, ok := s.cat.Services[env.Service]
	if !ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: %q is not in data/services", ErrUnknownService, env.Service)
	}
	if o := s.offers[env.Service]; o.Parallel == 0 {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: %s: %s", ErrNotOffered, env.Service, o.Reason)
	}
	if env.JobID == "" {
		env.JobID = newID()
	} else if !queue.ValidToken(env.JobID, 64) {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: job_id must be 1-64 characters of A-Z a-z 0-9 - _", ErrInvalid)
	}
	if _, dup := s.jobs[env.JobID]; dup {
		s.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrConflict, env.JobID)
	}
	s.mu.Unlock()
	if q == nil {
		return nil, fmt.Errorf("%w: %s", ErrBrokerMissing, s.brokerMessage())
	}
	seen := map[string]bool{}
	for _, in := range env.Inputs {
		if in.Name == "" || seen[in.Name] {
			return nil, fmt.Errorf("%w: every input needs a unique name", ErrInvalid)
		}
		seen[in.Name] = true
	}
	now := time.Now().UTC()
	env.EnvelopeVersion = queue.EnvelopeVersion
	env.Caller, env.Service = "local", def.ID
	env.Category = s.cat.Queue.CategoryOf(def.ID)
	env.Priority = priority.Clamp(env.Priority)
	env.IssuedAt = now
	if env.ExpiresAt.IsZero() {
		env.ExpiresAt = now.Add(7 * 24 * time.Hour)
	}
	if env.MaxAttempts <= 0 {
		env.MaxAttempts = max(def.Retry.MaxAttempts, 1)
	}
	env.MaxAttempts = min(env.MaxAttempts, 26)
	if env.TimeoutSeconds <= 0 {
		env.TimeoutSeconds = int64(def.Timeouts.Run.Or(time.Hour) / time.Second)
	}
	env.Reply.Events = queue.EventSubject(env.Caller, env.JobID)
	if err := env.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	// Create-only job_state first: a job id used before (even days ago) is a conflict, not a second job (§16.4).
	if _, err := q.CreateState(ctx, queue.JobState{JobID: env.JobID, Caller: env.Caller, Service: env.Service, State: queue.StateSubmitted}); err != nil {
		if errors.Is(err, queue.ErrExists) {
			return nil, fmt.Errorf("%w: %s (job_state already holds it)", ErrConflict, env.JobID)
		}
		return nil, fmt.Errorf("%w: %v", ErrBrokerMissing, err)
	}
	if _, err := q.Submit(ctx, &env); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBrokerMissing, err)
	}
	j := recordFor(&env, now)
	j.Move(jobs.Submitted, "stored on SUBMIT")
	s.mu.Lock()
	s.jobs[j.ID] = j
	s.save(j, true)
	out := j.Redacted()
	s.mu.Unlock()
	s.log.Info("job submitted", "job_id", j.ID, "service", j.Service, "caller", j.Caller, "priority", j.Priority)
	s.poke()
	return out, nil
}

// recordFor builds the local record of a job from its envelope.
func recordFor(env *jobs.Envelope, now time.Time) *jobs.Job {
	return &jobs.Job{ID: env.JobID, Caller: env.Caller, Category: env.Category, Service: env.Service, Model: env.Model,
		Priority: env.Priority, MaxAttempts: max(env.MaxAttempts, 1), Deadline: env.Deadline, TimeoutSeconds: env.TimeoutSeconds,
		CreatedAt: now, Envelope: *env}
}

// Get returns one job (URLs redacted).
func (s *Server) Get(id string) (*jobs.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return j.Redacted(), nil
}

// Filter selects jobs for List.
type Filter struct{ State, Service, Caller string }

// List returns jobs newest first.
func (s *Server) List(f Filter) []*jobs.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*jobs.Job
	for _, j := range s.jobs {
		if (f.State == "" || string(j.State) == f.State) && (f.Service == "" || j.Service == f.Service) && (f.Caller == "" || j.Caller == f.Caller) {
			out = append(out, j.Redacted())
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].CreatedAt.After(out[b].CreatedAt) })
	return out
}

// Events returns a job's events from the broker's EVENTS stream, as its caller sees them (§13.5).
func (s *Server) Events(ctx context.Context, id string) ([]queue.Event, error) {
	s.mu.Lock()
	j, ok := s.jobs[id]
	q := s.q
	s.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	if q == nil {
		return nil, fmt.Errorf("%w: %s", ErrBrokerMissing, s.brokerMessage())
	}
	return q.Events(ctx, j.Caller, id)
}

// Cancel sets job_control cancel (§16.5). A waiting job is cancelled at once; a running one is stopped by its task
// host and uploads nothing.
func (s *Server) Cancel(ctx context.Context, id, by string) (*jobs.Job, error) {
	s.mu.Lock()
	j, ok := s.jobs[id]
	q := s.q
	if ok && j.State.Final() {
		out := j.Redacted()
		s.mu.Unlock()
		return out, ErrFinal
	}
	s.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	if q == nil {
		return nil, fmt.Errorf("%w: %s", ErrBrokerMissing, s.brokerMessage())
	}
	c := queue.Control{Cancel: true, By: by, At: time.Now().UTC()}
	if err := q.SetControl(ctx, id, c); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBrokerMissing, err)
	}
	s.log.Info("job cancel requested", "job_id", id, "by", by)
	s.applyControl(ctx, queue.ControlChange{JobID: id, Control: c})
	return s.Get(id)
}

// PauseService / ResumeService are the owner's pause (spec §10.4: a local pause wins). A paused service is not
// pulled; its jobs wait in the broker.
func (s *Server) PauseService(id, by string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cat.Services[id]; !ok {
		return ErrUnknownService
	}
	s.pausedSvc[id] = by
	s.log.Info("service paused", "service", id, "by", by)
	return nil
}

func (s *Server) ResumeService(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cat.Services[id]; !ok {
		return ErrUnknownService
	}
	delete(s.pausedSvc, id)
	s.log.Info("service resumed", "service", id)
	s.poke()
	return nil
}

// Drain stops pulling new work; running jobs finish. Resume undoes Drain.
func (s *Server) Drain() {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	s.log.Info("draining: no new jobs will start")
}

func (s *Server) Resume() {
	s.mu.Lock()
	s.draining = false
	s.mu.Unlock()
	s.log.Info("resumed: taking jobs")
	s.poke()
}

// Reload re-reads this_server.yaml and data/ all-or-nothing (spec §6.7).
func (s *Server) Reload() ([]string, error) {
	if s.opt.Loader == nil {
		return nil, errors.New("reload not configured")
	}
	cfg, cat, warns, err := s.opt.Loader()
	if err != nil {
		s.log.Warn("reload rejected; keeping the running config", "error", err)
		return nil, err
	}
	s.mu.Lock()
	s.cfg, s.cat, s.warnings = cfg, cat, warns
	s.recompute()
	s.mu.Unlock()
	s.log.Info("config reloaded", "offers", s.offerSummary())
	s.poke()
	return warns, nil
}

// RefreshHardware re-runs detection and recomputes offers.
func (s *Server) RefreshHardware(ctx context.Context) {
	s.opt.Monitor.Refresh(ctx)
	s.mu.Lock()
	s.recompute()
	s.mu.Unlock()
	s.poke()
}

// Shutdown is the controller half of a code update (§9.5): stop pulling, write every run.json, and close the
// broker connection without nacking anything. Task hosts are never signalled; they keep running and the next
// controller re-adopts them.
func (s *Server) Shutdown() (stillRunning int) {
	s.Drain()
	s.mu.Lock()
	for _, h := range s.held {
		elog.Check("writing run.json at shutdown", runs.WriteJSON(h.dir, runs.RunFile, h.rec), "job_id", h.rec.JobID)
	}
	for _, j := range s.jobs {
		s.save(j, true)
	}
	stillRunning = len(s.held)
	q := s.q
	s.q = nil
	s.mu.Unlock()
	if q != nil {
		elog.Ignore("closing the broker connection", q.Close())
	}
	return stillRunning
}

// sweep removes final jobs older than 24 hours, their results and run dirs, old job logs, orphan work dirs and
// unused code snapshots (spec §9.6, pm/task_queue.mdx §9.2).
func (s *Server) sweep() {
	s.mu.Lock()
	cut := time.Now().Add(-24 * time.Hour)
	for id, j := range s.jobs {
		if j.State.Final() && j.FinishedAt != nil && j.FinishedAt.Before(cut) {
			delete(s.jobs, id)
			elog.Ignore("deleting an old journal entry", s.journal.Delete(id), "job_id", id)
			elog.Ignore("deleting old results", os.RemoveAll(filepath.Join(s.resultsDir, id)), "job_id", id)
		}
	}
	live := map[string]bool{}
	inUse := map[string]bool{s.opt.Snapshot.ID: true}
	for id, h := range s.held {
		live[id] = true
		inUse[h.rec.Snapshot] = true
	}
	keep := s.cfg.Continuity.KeepSnapshots
	s.mu.Unlock()
	for _, id := range runs.List(s.runsDir) {
		dir := filepath.Join(s.runsDir, id)
		if !live[id] && runs.Exists(dir, runs.HandledFile) {
			if info, err := os.Stat(filepath.Join(dir, runs.HandledFile)); err == nil && info.ModTime().Before(cut) {
				elog.Ignore("deleting an old run dir", os.RemoveAll(dir), "job_id", id)
			}
		}
	}
	removeOld(s.workDir, 48*time.Hour, live)
	removeOld(s.jobLogDir, 7*24*time.Hour, nil)
	if removed := snapshot.GC(s.snapDir, keep, inUse); len(removed) > 0 {
		s.log.Info("old code snapshots removed", "snapshots", removed)
	}
}

func removeOld(dir string, age time.Duration, keep map[string]bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	cut := time.Now().Add(-age)
	for _, e := range entries {
		if keep[e.Name()] {
			continue
		}
		if info, err := e.Info(); err == nil && info.ModTime().Before(cut) {
			elog.Ignore("sweeping an old file", os.RemoveAll(filepath.Join(dir, e.Name())), "path", filepath.Join(dir, e.Name()))
		}
	}
}

// save writes the journal entry; progress-only updates are throttled to one write per 2 seconds.
func (s *Server) save(j *jobs.Job, force bool) {
	if !force && time.Since(s.lastSave[j.ID]) < 2*time.Second {
		return
	}
	s.lastSave[j.ID] = time.Now()
	if err := s.journal.Save(j); err != nil {
		s.log.Error("journal save failed", "job_id", j.ID, "error", err)
		elog.Error("saving the job journal", err, "job_id", j.ID, "state", j.State)
	}
}

func newID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
