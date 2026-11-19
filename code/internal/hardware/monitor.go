package hardware

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Load is live telemetry, refreshed every few seconds and never written to yaml (spec §7.5).
type Load struct {
	At         time.Time `json:"at"`
	CPULoad1   float64   `json:"cpu_load_1m"`
	DiskFreeGB float64   `json:"disk_free_gb"`        // the lower of the work dir's and the state dir's disks
	DiskPath   string    `json:"disk_path,omitempty"` // the directory DiskFreeGB was measured on
	// GPUBusyPct is the busiest NVIDIA GPU's utilization (nvidia-smi), -1 when this machine has none or it could not
	// be read. The idle-only gate (ServiceSetting.IdleOnly) reads it: someone gaming or rendering on the 5090 is busy.
	GPUBusyPct float64 `json:"gpu_busy_pct"`
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
	l := Load{At: time.Now(), CPULoad1: readLoad(ctx), GPUBusyPct: readGPUBusy(ctx)}
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

// readGPUBusy returns the highest utilization.gpu of the NVIDIA GPUs, or -1 (no nvidia-smi, or it failed).
func readGPUBusy(ctx context.Context) float64 {
	smi, err := exec.LookPath("nvidia-smi")
	if err != nil {
		return -1
	}
	out, err := run(ctx, smi, "--query-gpu=utilization.gpu", "--format=csv,noheader,nounits")
	if err != nil {
		return -1
	}
	return ParseGPUBusy(out)
}

// ParseGPUBusy reads nvidia-smi's utilization lines ("12\n0\n") and returns the highest, or -1.
func ParseGPUBusy(out string) float64 {
	best := -1.0
	for _, l := range strings.Split(out, "\n") {
		if v, err := strconv.ParseFloat(strings.TrimSpace(l), 64); err == nil && v > best {
			best = v
		}
	}
	return best
}
