package scheduler

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/capability"
	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
)

// Read-only views for the local API. Each takes the lock briefly and returns copies.

// Health is GET /v1/health.
type Health struct {
	OK      bool   `json:"ok"`
	State   string `json:"state"`
	Reason  string `json:"state_reason,omitempty"`
	Key     string `json:"key"`
	Version string `json:"version"`
	GitSHA  string `json:"git_sha"`
	UptimeS int64  `json:"uptime_s"`
	PID     int    `json:"pid"`
	// Broker is the task queue connection (pm/task_queue.mdx §8.2). Its state is never a reason to stop direct
	// mode, so it does not change OK or State; broker_missing names the fix.
	Broker *BrokerHealth `json:"broker"`
}

func (s *Server) stateLocked() (string, string) {
	switch {
	case !s.started:
		return StateStarting, "booting"
	case s.draining:
		return StateDraining, "drain requested; running jobs finish, no new ones start"
	case s.paused:
		return StatePaused, "paused"
	case s.diskLowLocked():
		return StateDegraded, fmt.Sprintf("disk free %.1f GB is under limits.disk_min_free_gb %.1f", s.opt.Monitor.Load().DiskFreeGB, s.cfg.Limits.DiskMinFreeGB)
	case s.active >= s.cfg.Limits.MaxParallelJobs:
		return StateBusy, "all slots busy"
	}
	return StateOnline, ""
}

func (s *Server) Health() Health {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, why := s.stateLocked()
	return Health{OK: true, State: st, Reason: why, Key: s.cfg.Server.Key, Version: s.opt.Version, GitSHA: s.opt.GitSHA,
		UptimeS: int64(time.Since(s.bootAt).Seconds()), PID: os.Getpid(), Broker: s.brokerHealthLocked()}
}

// ServerInfo is GET /v1/server.
type ServerInfo struct {
	Health
	Identity  config.Identity             `json:"server"`
	Platforms map[string]map[string]any   `json:"platforms"`
	Limits    config.Limits               `json:"limits"`
	Slots     PoolState                   `json:"slots"`
	Active    int                         `json:"active_jobs"`
	Paused    map[string]string           `json:"paused_services"`
	Warnings  []string                    `json:"warnings"`
	Load      hardware.Load               `json:"load"`
	Dirs      map[string]string           `json:"dirs"`
	Transport map[string]string           `json:"transport"`
	Offers    map[string]int              `json:"offers_parallel"`
	Extra     map[string]capability.Offer `json:"-"`
}

func (s *Server) Info() ServerInfo {
	h := s.Health()
	s.mu.Lock()
	defer s.mu.Unlock()
	plat := map[string]map[string]any{}
	for name, p := range s.cfg.Platforms {
		plat[name] = map[string]any{"enabled": p.Enabled, "base_url": p.BaseURL, "share": p.Share,
			"connected": false, "note": "superseded: platforms submit through the task queue (callers, task_queue.mdx §10)"}
	}
	paused := map[string]string{}
	for k, v := range s.pausedSvc {
		paused[k] = v
	}
	offers := map[string]int{}
	for id, o := range s.offers {
		offers[id] = o.Parallel
	}
	return ServerInfo{
		Health: h, Identity: s.cfg.Server, Platforms: plat, Limits: s.cfg.Limits, Slots: s.pool.Snapshot(),
		Active: s.active, Paused: paused, Warnings: append([]string{}, s.warnings...), Load: s.opt.Monitor.Load(),
		Dirs:      map[string]string{"work": s.workDir, "results": s.resultsDir, "job_logs": s.jobLogDir},
		Transport: transportLocked(s),
		Offers:    offers,
	}
}

// HardwareView is GET /v1/hardware.
type HardwareView struct {
	Effective hardware.Facts   `json:"effective"`
	Detected  hardware.Facts   `json:"detected"`
	Declared  *hardware.Facts  `json:"declared"`
	Drift     []hardware.Drift `json:"drift"`
}

func (s *Server) Hardware() HardwareView {
	s.mu.Lock()
	defer s.mu.Unlock()
	return HardwareView{Effective: s.effective, Detected: s.opt.Monitor.Detected(), Declared: s.cfg.Hardware, Drift: append([]hardware.Drift{}, s.drift...)}
}

// Offers is GET /v1/capabilities, sorted by service id.
func (s *Server) Offers() []capability.Offer {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]capability.Offer, 0, len(s.offers))
	for _, o := range s.offers {
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Service < out[j].Service })
	return out
}

// ServiceView is one row of GET /v1/services.
type ServiceView struct {
	capability.Offer
	PausedBy string `json:"paused_by,omitempty"`
	Running  int    `json:"running"`
	Waiting  int    `json:"waiting"`
}

func (s *Server) Services() []ServiceView {
	offers := s.Offers()
	s.mu.Lock()
	defer s.mu.Unlock()
	waiting := map[string]int{}
	for _, j := range s.jobs {
		if j.State == jobs.Pending || j.State == jobs.Retry {
			waiting[j.Type]++
		}
	}
	var out []ServiceView
	for _, o := range offers {
		out = append(out, ServiceView{Offer: o, PausedBy: s.pausedSvc[o.Service], Running: s.activeBy[o.Service], Waiting: waiting[o.Service]})
	}
	return out
}

// QueueInfo mirrors Asynq's QueueInfo counters, per queue (platform) and per type (service).
type QueueInfo struct {
	Queue     string `json:"queue"`
	Size      int    `json:"size"` // pending + active + retry
	Pending   int    `json:"pending"`
	Active    int    `json:"active"`
	Retry     int    `json:"retry"`
	Completed int    `json:"completed"`
	Archived  int    `json:"archived"`
	Cancelled int    `json:"cancelled"`
	Paused    bool   `json:"paused"`
}

func (s *Server) Queues() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	byQueue := map[string]*QueueInfo{}
	byType := map[string]*QueueInfo{}
	add := func(m map[string]*QueueInfo, k string, st jobs.State) {
		q := m[k]
		if q == nil {
			q = &QueueInfo{Queue: k}
			m[k] = q
		}
		switch st {
		case jobs.Pending:
			q.Pending++
		case jobs.Active:
			q.Active++
		case jobs.Retry:
			q.Retry++
		case jobs.Completed:
			q.Completed++
		case jobs.Archived:
			q.Archived++
		case jobs.Cancelled:
			q.Cancelled++
		}
		q.Size = q.Pending + q.Active + q.Retry
	}
	for _, j := range s.jobs {
		add(byQueue, j.Queue, j.State)
		add(byType, j.Type, j.State)
	}
	for k, q := range byType {
		_, q.Paused = s.pausedSvc[k]
	}
	flat := func(m map[string]*QueueInfo) []QueueInfo {
		var out []QueueInfo
		for _, q := range m {
			out = append(out, *q)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Queue < out[j].Queue })
		return out
	}
	return map[string]any{"queues": flat(byQueue), "types": flat(byType)}
}

// OutputPath returns the local file of a kept output.
func (s *Server) OutputPath(id, name string) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return "", "", ErrNotFound
	}
	for _, o := range j.Outputs {
		if o.Name == name && o.Download != "" {
			return filepath.Join(s.resultsDir, id, o.File), o.ContentType, nil
		}
	}
	return "", "", ErrNotFound
}

// LogPath returns the per-job log file.
func (s *Server) LogPath(id string) (string, error) {
	s.mu.Lock()
	_, ok := s.jobs[id]
	s.mu.Unlock()
	if !ok {
		return "", ErrNotFound
	}
	return filepath.Join(s.jobLogDir, id+".log"), nil
}

// Metrics renders Prometheus text format (spec §13 /metrics).
func (s *Server) Metrics() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b strings.Builder
	st, _ := s.stateLocked()
	fmt.Fprintf(&b, "# TYPE aps_up gauge\naps_up{key=%q,state=%q} 1\n", s.cfg.Server.Key, st)
	pool := s.pool.Snapshot()
	fmt.Fprintf(&b, "# TYPE aps_slots_cores gauge\naps_slots_cores{kind=\"total\"} %d\naps_slots_cores{kind=\"used\"} %d\n", pool.CoresTotal, pool.CoresUsed)
	fmt.Fprintf(&b, "# TYPE aps_active_jobs gauge\naps_active_jobs %d\n", s.active)
	counts := map[[2]string]int{}
	for _, j := range s.jobs {
		counts[[2]string{j.Type, string(j.State)}]++
	}
	keys := make([][2]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i][0]+keys[i][1] < keys[j][0]+keys[j][1] })
	b.WriteString("# TYPE aps_jobs gauge\n")
	for _, k := range keys {
		fmt.Fprintf(&b, "aps_jobs{service=%q,state=%q} %d\n", k[0], k[1], counts[k])
	}
	for name, m := range map[string]map[string]int64{"processed": s.stats.Processed, "failed": s.stats.Failed, "cancelled": s.stats.Cancelled, "retried": s.stats.Retried} {
		fmt.Fprintf(&b, "# TYPE aps_jobs_%s_total counter\n", name)
		svcs := make([]string, 0, len(m))
		for k := range m {
			svcs = append(svcs, k)
		}
		sort.Strings(svcs)
		for _, svc := range svcs {
			fmt.Fprintf(&b, "aps_jobs_%s_total{service=%q} %d\n", name, svc, m[svc])
		}
	}
	return b.String()
}

func transportLocked(s *Server) map[string]string {
	t := map[string]string{"intake": "direct (POST /v1/jobs)", "local_table": "in memory + journal; job JSON uses Asynq TaskInfo names plus caller_state"}
	if s.opt.OpenQueue != nil {
		t["intake"] = "direct (POST /v1/jobs) and task queue (NATS JetStream, " + s.cfg.Queue.BrokerURL + ")"
		t["queue"] = s.qState
	} else {
		t["queue"] = "disabled (queue.enabled is false)"
	}
	return t
}
