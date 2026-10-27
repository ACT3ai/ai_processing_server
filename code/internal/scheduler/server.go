// Package scheduler is our coordination logic (spec §9, §10, §15.2): it owns the job table, the slot pool,
// admission by effective priority, the per-job lifecycle, retries, cancel, pause, drain and crash recovery.
// The transport underneath (direct mode, and the NATS JetStream task queue in queue.go) never decides any of this.
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
	"github.com/ACT3ai/ai_processing_server/internal/queue"
	"github.com/ACT3ai/ai_processing_server/internal/runners"
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

	errCancelled = errors.New("cancelled by the machine owner")
	errTimeout   = errors.New("job exceeded its timeout")
	errShutdown  = errors.New("server shutting down")
)

// elog records failures that should not happen (pm/errors.mdx): the journal, work dirs, panics, stuck shutdowns.
var elog = errlog.For("scheduler")

// Loader re-reads this_server.yaml and data/ for a reload. It returns an error instead of a partial config.
type Loader func() (*config.ThisServer, *config.Catalog, []string, error)

// Options construct a Server.
type Options struct {
	Server   *config.ThisServer
	Catalog  *config.Catalog
	Monitor  *hardware.Monitor
	StateDir string // data/git_ignore
	Loader   Loader
	Version  string
	GitSHA   string
	Warnings []string // config warnings to report (legacy file, unknown services)
	Logger   *slog.Logger
	// OpenQueue connects to the task queue (this machine's nats-server). nil: queue.enabled is false, direct mode only.
	OpenQueue func(context.Context) (queue.Queue, error)
	// Intervals; zero means the spec default.
	TelemetryEvery time.Duration
	SweepEvery     time.Duration
	HardwareEvery  time.Duration
}

// Server is one running processing server.
type Server struct {
	mu        sync.Mutex
	opt       Options
	cfg       *config.ThisServer
	cat       *config.Catalog
	effective hardware.Facts
	drift     []hardware.Drift
	offers    map[string]capability.Offer
	runners   map[string]runners.Runner
	pool      *SlotPool
	jobs      map[string]*jobs.Job
	cancels   map[string]context.CancelCauseFunc
	activeBy  map[string]int
	active    int
	pausedSvc map[string]string // service → who paused it
	paused    bool
	draining  bool
	started   bool
	warnings  []string
	journal   *jobs.Journal
	wake      chan struct{}
	jobsCtx   context.Context
	stopJobs  context.CancelCauseFunc
	wg        sync.WaitGroup
	log       *slog.Logger
	bootAt    time.Time
	stats     counters
	lastSave  map[string]time.Time

	workDir, resultsDir, jobLogDir string

	// Task queue link (queue.go). q is nil until the broker answers.
	q        queue.Queue
	qState   string
	qErr     string
	qVersion string
	held     map[string]*heldJob
	qwake    chan struct{}
	pub      chan pubReq
	reclaim  []queue.ClaimHandle
}

type counters struct {
	Processed map[string]int64 // service → finished (any outcome)
	Failed    map[string]int64 // service → archived
	Cancelled map[string]int64
	Retried   map[string]int64
}

// New builds the server, recovers the journal, and computes offers. It does not start any loop.
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
		opt: o, cfg: o.Server, cat: o.Catalog, runners: runners.All(),
		jobs: map[string]*jobs.Job{}, cancels: map[string]context.CancelCauseFunc{}, activeBy: map[string]int{},
		pausedSvc: map[string]string{}, wake: make(chan struct{}, 1), log: o.Logger.With("ctx", "scheduler"),
		bootAt: time.Now(), lastSave: map[string]time.Time{}, warnings: o.Warnings,
		held: map[string]*heldJob{}, qwake: make(chan struct{}, 1), pub: make(chan pubReq, 1024), qState: BrokerDisabled,
		stats: counters{map[string]int64{}, map[string]int64{}, map[string]int64{}, map[string]int64{}},
	}
	if o.OpenQueue != nil {
		s.qState = BrokerConnecting // until queueMain's first attempt answers; never "disabled" while enabled
	}
	s.workDir = s.cfg.Paths.WorkDir
	if s.workDir == "" {
		s.workDir = filepath.Join(o.StateDir, "work")
	}
	s.workDir = config.ExpandHome(s.workDir)
	s.resultsDir = filepath.Join(o.StateDir, "results")
	s.jobLogDir = filepath.Join(o.StateDir, "logs", "jobs")
	for _, d := range []string{s.workDir, s.resultsDir, s.jobLogDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, err
		}
	}
	j, err := jobs.NewJournal(filepath.Join(o.StateDir, "state", "journal"))
	if err != nil {
		return nil, err
	}
	s.journal = j
	s.jobsCtx, s.stopJobs = context.WithCancelCause(context.Background())
	s.recompute()
	s.recover()
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

// recover applies spec §9.5: every journal entry not in a final state failed with `infra` because the server
// restarted; its work dir is deleted. Final entries are loaded so the API can still answer for them.
func (s *Server) recover() {
	all, errs := s.journal.LoadAll()
	for _, e := range errs {
		s.log.Warn("journal entry unreadable", "error", e)
		elog.Error("reading a journal entry at boot", e)
	}
	now := time.Now().UTC()
	for _, j := range all {
		if !j.State.Final() {
			stage := j.Stage
			j.ErrorKind, j.ErrorStage = jobs.KindInfra, stage
			j.LastErr = fmt.Sprintf("server restarted during %s", stage)
			j.LastFailedAt, j.FinishedAt = &now, &now
			j.Slot = nil
			if j.Claim != nil { // a queued job: its claim goes back to the queue once the broker answers
				s.reclaim = append(s.reclaim, *j.Claim)
				j.Claim = nil
			}
			j.Move(jobs.Archived, jobs.StageFailed, j.LastErr)
			elog.Check("deleting the work dir of a job interrupted by a restart", os.RemoveAll(filepath.Join(s.workDir, j.ID)), "job_id", j.ID)
			if err := s.journal.Save(j); err != nil {
				s.log.Error("journal save failed", "job_id", j.ID, "error", err)
				elog.Error("saving the job journal at boot", err, "job_id", j.ID)
			}
			s.log.Error("job failed at boot", "job_id", j.ID, "service", j.Type, "reason", j.LastErr)
		}
		s.jobs[j.ID] = j
	}
	if len(all) > 0 {
		s.log.Info("journal recovered", "jobs", len(all))
	}
}

// Run starts the loops and blocks until ctx ends. Call Shutdown to drain first.
func (s *Server) Run(ctx context.Context) {
	s.mu.Lock()
	s.started = true
	s.mu.Unlock()
	s.log.Info("scheduler started", "offers", s.offerSummary())
	s.opt.Monitor.SampleLoad(ctx)
	tAdmit := time.NewTicker(250 * time.Millisecond)
	tTele := time.NewTicker(s.opt.TelemetryEvery)
	tSweep := time.NewTicker(s.opt.SweepEvery)
	tHW := time.NewTicker(s.opt.HardwareEvery)
	defer func() { tAdmit.Stop(); tTele.Stop(); tSweep.Stop(); tHW.Stop() }()
	s.sweep()
	if s.opt.OpenQueue != nil {
		go func() {
			defer elog.Recover("running the task queue link")
			s.queueMain(ctx)
		}()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
			s.safely("admitting waiting jobs", s.admit)
		case <-tAdmit.C:
			s.safely("admitting waiting jobs", s.admit)
		case <-tTele.C:
			s.safely("sampling load telemetry", func() { s.opt.Monitor.SampleLoad(ctx) })
		case <-tSweep.C:
			s.safely("sweeping old jobs and work dirs", s.sweep)
		case <-tHW.C:
			s.safely("refreshing hardware detection", func() { s.RefreshHardware(ctx) })
		}
	}
}

// safely runs one step of the main loop; a panic is recorded in error.err and the loop keeps going, so one bad
// tick never stops the scheduler.
func (s *Server) safely(attempting string, fn func()) {
	defer elog.Recover(attempting)
	fn()
}

func (s *Server) poke() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
	select {
	case s.qwake <- struct{}{}:
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

// Submit accepts a job in direct mode (POST /v1/jobs). The owner's fence applies: a service that is off, or that
// this hardware cannot run, is refused up front rather than left waiting forever.
func (s *Server) Submit(env jobs.Envelope, platform string) (*jobs.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.acceptLocked(env, platform, 0, nil, nil)
	if err != nil {
		return nil, err
	}
	return j.Redacted(), nil
}

// acceptLocked validates and stores a new job. retried and maxRetry come from the queue for queued jobs (the queue
// owns their retries, so maxRetry is 0); claim is the broker claim, nil for direct jobs.
func (s *Server) acceptLocked(env jobs.Envelope, platform string, retried int, maxRetryOverride *int, claim *queue.ClaimHandle) (*jobs.Job, error) {
	def, ok := s.cat.Services[env.Service]
	if !ok {
		return nil, fmt.Errorf("%w: %q is not in data/services", ErrUnknownService, env.Service)
	}
	o := s.offers[env.Service]
	if o.Parallel == 0 {
		return nil, fmt.Errorf("%w: %s: %s", ErrNotOffered, env.Service, o.Reason)
	}
	id := env.JobID
	if id == "" {
		id = newID()
	} else if !validID(id) {
		return nil, fmt.Errorf("%w: job_id must be 1-64 characters of A-Z a-z 0-9 - _", ErrInvalid)
	}
	if _, dup := s.jobs[id]; dup {
		return nil, fmt.Errorf("%w: %s", ErrConflict, id)
	}
	seen := map[string]bool{}
	for _, in := range env.Inputs {
		if in.Name == "" || seen[in.Name] {
			return nil, fmt.Errorf("%w: every input needs a unique name", ErrInvalid)
		}
		seen[in.Name] = true
	}
	env.JobID, env.Service = id, def.ID
	if env.Platform == "" {
		env.Platform = platform
	}
	env.Priority = min(max(env.Priority, 0), 300) // spec §10.2 clamp
	maxRetry := max(def.Retry.MaxAttempts-1, 0)
	if env.MaxRetry != nil {
		maxRetry = min(max(*env.MaxRetry, 0), 25)
	}
	if maxRetryOverride != nil {
		maxRetry = *maxRetryOverride
	}
	timeout := def.Timeouts.Run.Or(time.Hour)
	if env.TimeoutSeconds > 0 {
		timeout = time.Duration(env.TimeoutSeconds) * time.Second
	}
	now := time.Now().UTC()
	j := &jobs.Job{
		ID: id, Queue: env.Platform, Type: def.ID, Payload: env, MaxRetry: maxRetry,
		Timeout: int64(timeout / time.Second), Deadline: env.Deadline, Priority: env.Priority, CreatedAt: now,
		Retried: retried, Claim: claim,
	}
	note := "accepted"
	if claim != nil {
		note = fmt.Sprintf("claimed from the task queue (caller %s, attempt %d)", platform, retried+1)
	}
	j.Move(jobs.Pending, jobs.StageLeased, note)
	j.EffectivePriority = s.effPriority(j, now)
	s.jobs[id] = j
	s.save(j, true)
	s.log.Info("job accepted", "job_id", id, "service", j.Type, "platform", j.Queue, "priority", j.Priority, "queued", claim != nil)
	s.poke()
	return j, nil
}

// effPriority is spec §10.3: priority + platform weight + owner bias + one point per waiting minute (max 60).
func (s *Server) effPriority(j *jobs.Job, now time.Time) int {
	wait := int(now.Sub(j.CreatedAt).Minutes())
	return j.Priority + s.offers[j.Type].LocalBias + min(wait, 60)
}

// admit starts the highest effective-priority waiting jobs that fit (spec §9.3). A job that does not fit is
// skipped so a smaller one behind it can use the free capacity.
func (s *Server) admit() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining || s.paused {
		return
	}
	now := time.Now().UTC()
	var cands []*jobs.Job
	for _, j := range s.jobs {
		if j.State != jobs.Pending && j.State != jobs.Retry {
			continue
		}
		if j.NextProcessAt != nil && now.Before(*j.NextProcessAt) {
			continue
		}
		if j.Deadline != nil && now.After(*j.Deadline) {
			s.finishLocked(j, jobs.Archived, jobs.KindTimeout, j.Stage, "deadline passed before the job could start", nil)
			continue
		}
		j.EffectivePriority = s.effPriority(j, now)
		cands = append(cands, j)
	}
	sort.Slice(cands, func(a, b int) bool {
		if cands[a].EffectivePriority != cands[b].EffectivePriority {
			return cands[a].EffectivePriority > cands[b].EffectivePriority
		}
		return cands[a].CreatedAt.Before(cands[b].CreatedAt)
	})
	diskLow := s.diskLowLocked()
	for _, j := range cands {
		if s.active >= s.cfg.Limits.MaxParallelJobs {
			return
		}
		if diskLow {
			return
		}
		if _, p := s.pausedSvc[j.Type]; p {
			continue
		}
		o := s.offers[j.Type]
		if o.Parallel == 0 || s.activeBy[j.Type] >= o.Parallel {
			continue
		}
		def := s.cat.Services[j.Type]
		var allowed []int
		if set := s.cfg.Service(j.Type); set != nil {
			allowed = set.GPUs
		}
		slot, ok := s.pool.TryAcquire(def.ResourcesPerJob, def.Requires.GPU == "required", allowed)
		if !ok {
			continue
		}
		s.startLocked(j, slot)
	}
}

func (s *Server) diskLowLocked() bool {
	free := s.opt.Monitor.Load().DiskFreeGB
	return free > 0 && free < s.cfg.Limits.DiskMinFreeGB
}

func (s *Server) startLocked(j *jobs.Job, slot jobs.Slot) {
	now := time.Now().UTC()
	j.Slot = &slot
	j.StartedAt = &now
	j.NextProcessAt = nil
	j.Progress, j.Step, j.Note = 0, "", ""
	attempt := j.Retried + 1
	j.Move(jobs.Active, jobs.StageStaging, fmt.Sprintf("attempt %d on %s", attempt, slot.ID))
	s.activeBy[j.Type]++
	s.active++
	ctx, cancel := context.WithCancelCause(s.jobsCtx)
	s.cancels[j.ID] = cancel
	s.save(j, true)
	s.eventLocked(j, "stage", nil)
	s.log.Info("job start", "job_id", j.ID, "service", j.Type, "platform", j.Queue, "slot", slot.ID,
		"cores", slot.Cores, "attempt", attempt, "effective_priority", j.EffectivePriority)
	s.wg.Add(1)
	go s.execute(ctx, cancel, j.Clone(), time.Duration(j.Timeout)*time.Second)
}

// Get returns one job (URLs redacted).
func (s *Server) Get(id string) (*jobs.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	if j.State == jobs.Pending || j.State == jobs.Retry {
		j.EffectivePriority = s.effPriority(j, time.Now().UTC())
	}
	return j.Redacted(), nil
}

// Filter selects jobs for List.
type Filter struct{ State, Service, Queue string }

// List returns jobs newest first.
func (s *Server) List(f Filter) []*jobs.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*jobs.Job
	for _, j := range s.jobs {
		if (f.State == "" || string(j.State) == f.State) && (f.Service == "" || j.Type == f.Service) && (f.Queue == "" || j.Queue == f.Queue) {
			out = append(out, j.Redacted())
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].CreatedAt.After(out[b].CreatedAt) })
	return out
}

// Cancel stops a job. Waiting jobs are cancelled at once; running jobs are stopped and upload nothing.
func (s *Server) Cancel(id, by string) (*jobs.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	if j.State.Final() {
		return j.Redacted(), ErrFinal
	}
	if j.State == jobs.Active {
		if c := s.cancels[id]; c != nil {
			c(fmt.Errorf("%w (%s)", errCancelled, by))
		}
		s.log.Info("job cancel requested", "job_id", id, "by", by)
		return j.Redacted(), nil
	}
	s.finishLocked(j, jobs.Cancelled, jobs.KindCancelled, j.Stage, "cancelled by "+by+" before it started", nil)
	return j.Redacted(), nil
}

// PauseService / ResumeService are the owner's pause (spec §10.4: a local pause wins).
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

// Drain stops taking new work; running jobs finish. Resume undoes Drain.
func (s *Server) Drain() {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	s.log.Info("draining: no new jobs will start")
}

func (s *Server) Resume() {
	s.mu.Lock()
	s.draining, s.paused = false, false
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

// Shutdown drains, waits up to timeout for running jobs, then stops the rest as `infra` "server shutting down".
func (s *Server) Shutdown(timeout time.Duration) {
	s.Drain()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return
	case <-time.After(timeout):
	}
	s.log.Warn("drain timeout reached; stopping running jobs", "timeout", timeout)
	s.stopJobs(errShutdown)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		s.log.Error("jobs did not stop within 30s of shutdown")
		elog.Error("stopping running jobs at shutdown", errors.New("jobs did not stop within 30 s of being cancelled"))
	}
}

// sweep removes final jobs older than 24 hours, their results, old job logs, and orphan work dirs (spec §9.6).
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
	for id, j := range s.jobs {
		if j.State == jobs.Active {
			live[id] = true
		}
	}
	s.mu.Unlock()
	removeOld(s.workDir, 48*time.Hour, live)
	removeOld(s.jobLogDir, 7*24*time.Hour, nil)
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

func validID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}
