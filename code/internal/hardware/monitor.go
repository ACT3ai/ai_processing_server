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
	DiskFreeGB float64   `json:"disk_free_gb"`
}

// Monitor holds the latest detection and telemetry for the running server.
type Monitor struct {
	mu       sync.RWMutex
	opts     Options
	detected Facts
	load     Load
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
	if d, err := diskUsage(m.opts.WorkDir); err == nil {
		l.DiskFreeGB = d.FreeGB
	}
	m.mu.Lock()
	m.load = l
	m.mu.Unlock()
}
