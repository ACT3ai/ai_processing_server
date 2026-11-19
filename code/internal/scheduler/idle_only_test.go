package scheduler

import (
	"strings"
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/capability"
	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/queue/queuetest"
)

// An idle-only service (B5090's act3.llm_batch, Bryan 2026-11-19) is pulled only while the machine is truly idle: no
// job running, the NVIDIA GPU quiet, the CPU quiet. Ordinary services keep being pulled.
func TestIdleOnlyServiceIsPulledOnlyWhenIdle(t *testing.T) {
	mon := hardware.NewMonitor(hardware.Options{}, hardware.Facts{})
	s, _ := newQueueServer(t, queuetest.NewMem(), t.TempDir())
	s.opt.Monitor = mon
	s.started = true
	s.cfg.Limits = config.Limits{MaxParallelJobs: 4}
	s.cfg.Services = []config.ServiceSetting{{ID: "act3.llm_batch", Enabled: true, IdleOnly: true}, {ID: "sys.compute", Enabled: true}}
	s.offers = map[string]capability.Offer{"act3.llm_batch": {Service: "act3.llm_batch", Parallel: 1}, "sys.compute": {Service: "sys.compute", Parallel: 2}}
	s.effective.CPU.LogicalCores = 32
	lanes := func() string {
		s.mu.Lock()
		defer s.mu.Unlock()
		var out []string
		for _, l := range s.pullOrderLocked() {
			out = append(out, l.Service)
		}
		return strings.Join(out, ",")
	}

	mon.SetLoad(hardware.Load{GPUBusyPct: 2, CPULoad1: 1})
	if got := lanes(); !strings.Contains(got, "act3.llm_batch") {
		t.Fatalf("idle machine: %s", got)
	}
	mon.SetLoad(hardware.Load{GPUBusyPct: 85, CPULoad1: 1}) // someone renders or games on the 5090
	if got := lanes(); strings.Contains(got, "act3.llm_batch") || !strings.Contains(got, "sys.compute") {
		t.Fatalf("busy GPU: %s", got)
	}
	mon.SetLoad(hardware.Load{GPUBusyPct: -1, CPULoad1: 20}) // no NVIDIA, CPU busy
	if got := lanes(); strings.Contains(got, "act3.llm_batch") {
		t.Fatalf("busy CPU: %s", got)
	}
	mon.SetLoad(hardware.Load{GPUBusyPct: -1, CPULoad1: 0.5})
	s.mu.Lock()
	if s.activeBy == nil {
		s.activeBy = map[string]int{}
	}
	s.active, s.activeBy["sys.compute"] = 1, 1 // another job of ours is running here
	s.mu.Unlock()
	if got := lanes(); strings.Contains(got, "act3.llm_batch") || !strings.Contains(got, "sys.compute") {
		t.Fatalf("a job running: %s", got)
	}
}
