package scheduler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/capability"
	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

// Read-only views for the local API. Each takes the lock briefly and returns copies.

// Health is GET /v1/health.
type Health struct {
	OK      bool       `json:"ok"`
	State   string     `json:"state"`
	Reason  string     `json:"state_reason,omitempty"`
	Key     string     `json:"key"`
	Version string     `json:"version"`
	GitSHA  string     `json:"git_sha"`
	UptimeS int64      `json:"uptime_s"`
	PID     int        `json:"pid"`
	Code    string     `json:"code_version"` // the snapshot new jobs run from
	Running int        `json:"running_jobs"`
	Broker  BrokerView `json:"broker"`
}

func (s *Server) stateLocked() (string, string) {
	switch {
	case !s.started:
		return StateStarting, "booting"
	case s.q == nil:
		return StateDegraded, "broker_missing: " + s.brokerErr + "; fix: " + s.opt.BrokerFix
	case !s.q.Info().Connected:
		return StateDegraded, "broker connection lost; reconnecting (running jobs continue)"
	case s.draining:
		return StateDraining, "drain requested; running jobs finish, no new ones start"
	case s.diskLowLocked():
		return StateDegraded, fmt.Sprintf("disk free %.1f GB is under limits.disk_min_free_gb %.1f", s.opt.Monitor.Load().DiskFreeGB, s.cfg.Limits.DiskMinFreeGB)
	case len(s.held) >= s.cfg.Limits.MaxParallelJobs:
		return StateBusy, "all slots busy"
	}
	return StateOnline, ""
}

func (s *Server) diskLowLocked() bool {
	free := s.opt.Monitor.Load().DiskFreeGB
	return free > 0 && free < s.cfg.Limits.DiskMinFreeGB
}

// BrokerView is the broker part of health (pm/task_queue.mdx §8.2).
type BrokerView struct {
	State string `json:"state"` // ok | broker_missing | reconnecting
	queue.BrokerInfo
	Error string `json:"error,omitempty"`
	Fix   string `json:"fix,omitempty"`
}

func (s *Server) brokerViewLocked() BrokerView {
	if s.q == nil {
		return BrokerView{State: "broker_missing", BrokerInfo: queue.BrokerInfo{URL: s.cfg.Queue.BrokerURL}, Error: s.brokerErr, Fix: s.opt.BrokerFix}
	}
	info := s.q.Info()
	if !info.Connected {
		return BrokerView{State: "reconnecting", BrokerInfo: info}
	}
	return BrokerView{State: "ok", BrokerInfo: info}
}

func (s *Server) Health() Health {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, why := s.stateLocked()
	return Health{OK: true, State: st, Reason: why, Key: s.cfg.Server.Key, Version: s.opt.Version, GitSHA: s.opt.GitSHA,
		UptimeS: int64(time.Since(s.bootAt).Seconds()), PID: os.Getpid(), Code: s.opt.Snapshot.ID, Running: len(s.held), Broker: s.brokerViewLocked()}
}

// ServerInfo is GET /v1/server.
type ServerInfo struct {
	Health
	Identity   config.Identity                 `json:"server"`
	Platforms  map[string]map[string]any       `json:"platforms"`
	Queue      config.QueueSettings            `json:"queue"`
	Continuity config.Continuity               `json:"continuity"`
	Callers    map[string]config.CallerSetting `json:"callers"`
	Limits     config.Limits                   `json:"limits"`
	Slots      PoolState                       `json:"slots"`
	Active     int                             `json:"active_jobs"`
	Paused     map[string]string               `json:"paused_services"`
	Warnings   []string                        `json:"warnings"`
	Load       hardware.Load                   `json:"load"`
	Dirs       map[string]string               `json:"dirs"`
	Transport  map[string]string               `json:"transport"`
	Offers     map[string]int                  `json:"offers_parallel"`
	Extra      map[string]capability.Offer     `json:"-"`
}

func (s *Server) Info() ServerInfo {
	h := s.Health()
	s.mu.Lock()
	defer s.mu.Unlock()
	plat := map[string]map[string]any{}
	for name, p := range s.cfg.Platforms {
		plat[name] = map[string]any{"enabled": p.Enabled, "base_url": p.BaseURL, "share": p.Share,
			"connected": false, "note": "platforms submit through the task queue hub in phase Q3; today jobs arrive through the local API"}
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
		Active: len(s.held), Queue: s.cfg.Queue, Continuity: s.cfg.Continuity, Callers: s.cfg.Callers, Paused: paused, Warnings: append([]string{}, s.warnings...), Load: s.opt.Monitor.Load(),
		Dirs: map[string]string{"work": s.workDir, "results": s.resultsDir, "job_logs": s.jobLogDir, "runs": s.runsDir, "snapshots": s.snapDir,
			"code_snapshot": s.opt.Snapshot.Dir},
		Transport: map[string]string{"intake": "local API (POST /v1/jobs) → SUBMIT", "queue": "NATS JetStream, a separate nats-server service (pm/task_queue.mdx)",
			"broker_url": s.cfg.Queue.BrokerURL, "role": s.cfg.Queue.Role},
		Offers: offers,
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
		if j.State.Waiting() {
			waiting[j.Service]++
		}
	}
	var out []ServiceView
	for _, o := range offers {
		out = append(out, ServiceView{Offer: o, PausedBy: s.pausedSvc[o.Service], Running: s.activeBy[o.Service], Waiting: waiting[o.Service]})
	}
	return out
}

// Queues is GET /v1/queues: the broker's work depth per consumer (band × caller × service), and this server's
// job records counted by state.
func (s *Server) Queues(ctx context.Context) map[string]any {
	s.mu.Lock()
	q := s.q
	byState := map[string]map[string]int{} // service → state → count
	for _, j := range s.jobs {
		if byState[j.Service] == nil {
			byState[j.Service] = map[string]int{}
		}
		byState[j.Service][string(j.State)]++
	}
	paused := map[string]bool{}
	for k := range s.pausedSvc {
		paused[k] = true
	}
	broker := s.brokerViewLocked()
	s.mu.Unlock()
	type row struct {
		queue.ConsumerRef
		Consumer string `json:"consumer"`
		Subject  string `json:"subject"`
		Stored   int    `json:"stored"` // waiting plus held by a worker
	}
	work := []row{}
	if q != nil {
		if depth, err := q.Depth(ctx); err == nil {
			for ref, n := range depth {
				work = append(work, row{ref, ref.Name(), ref.Subject(), n})
			}
		}
	}
	sort.Slice(work, func(i, j int) bool { return work[i].Subject < work[j].Subject })
	return map[string]any{"broker": broker, "work": work, "jobs_by_service": byState, "paused_services": paused}
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
	fmt.Fprintf(&b, "# TYPE aps_active_jobs gauge\naps_active_jobs %d\n", len(s.held))
	broker := 0
	if s.q != nil && s.q.Info().Connected {
		broker = 1
	}
	fmt.Fprintf(&b, "# TYPE aps_broker_connected gauge\naps_broker_connected %d\n", broker)
	fmt.Fprintf(&b, "# TYPE aps_readopted_total counter\naps_readopted_total %d\n", s.stats.Readopted)
	fmt.Fprintf(&b, "# TYPE aps_readopt_lost_total counter\naps_readopt_lost_total %d\n", s.stats.Lost)
	counts := map[[2]string]int{}
	for _, j := range s.jobs {
		counts[[2]string{j.Service, string(j.State)}]++
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
