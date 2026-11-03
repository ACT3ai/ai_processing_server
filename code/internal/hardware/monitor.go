package hardware

import (
	"context"
	"sync"
	"time"
)

// Load is live telemetry, refreshed every few seconds and never written to yaml (spec §7.5).
type Load struct {
	At         time.Time `json:"at"`
	CPULoad1   float64   `json:"cpu_load_1m"`
	DiskFreeGB float64   `json:"disk_free_gb"`        // the lower of the work dir's and the state dir's disks
	DiskPath   string    `json:"disk_path,omitempty"` // the directory DiskFreeGB was measured on
}

// Monitor holds the latest detection and telemetry for the running server.
type Monitor struct {
	mu       sync.RWMutex
	opts     Options
	detected Facts
	load     Load
}

// SetLoad replaces the telemetry sample (tests: a full disk without filling one).
func (m *Monitor) SetLoad(l Load) {
	m.mu.Lock()
	m.load = l
	m.mu.Unlock()
}

func NewMonitor(opts Options, initial Facts) *Monitor {
	return &Monitor{opts: opts, detected: initial}
}

func (m *Monitor) Detected() Facts {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.detected
}

func (m *Monitor) Load() Load {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.load
}

// Refresh re-runs full detection.
func (m *Monitor) Refresh(ctx context.Context) Facts {
	f := Detect(ctx, m.opts)
	m.mu.Lock()
	m.detected = f
	m.mu.Unlock()
	return f
}

// SampleLoad takes one telemetry sample.
func (m *Monitor) SampleLoad(ctx context.Context) {
	l := Load{At: time.Now(), CPULoad1: readLoad(ctx)}
	for _, dir := range []string{m.opts.WorkDir, m.opts.StateDir} {
		if dir == "" {
			continue
		}
		if d, err := diskUsage(dir); err == nil && (l.DiskPath == "" || d.FreeGB < l.DiskFreeGB) {
			l.DiskFreeGB, l.DiskPath = d.FreeGB, dir
		}
	}
	m.mu.Lock()
	m.load = l
	m.mu.Unlock()
}
