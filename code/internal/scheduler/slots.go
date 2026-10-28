package scheduler

import (
	"fmt"
	"slices"
	"sort"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
)

// SlotPool hands out reservations of CPU cores, RAM and one GPU's memory (spec §9.3, §15.2).
// It never blocks: the admit loop decides the order and asks. Each GPU is its own pool; cards are never summed.
type SlotPool struct {
	cores int
	ram   float64
	gpus  []gpuCap
	held  map[string]jobs.Slot
	seq   int
}

type gpuCap struct {
	Index  int     `json:"index"`
	MemGB  float64 `json:"mem_gb"`
	UsedGB float64 `json:"used_gb"`
}

// PoolState is a snapshot for the API and metrics.
type PoolState struct {
	CoresTotal int      `json:"cores_total"`
	CoresUsed  int      `json:"cores_used"`
	RAMTotalGB float64  `json:"ram_total_gb"`
	RAMUsedGB  float64  `json:"ram_used_gb"`
	GPUs       []gpuCap `json:"gpus"`
	Held       int      `json:"held"`
}

// NewSlotPool sizes the pool from effective hardware minus the owner's reserves.
func NewSlotPool(f hardware.Facts, l config.Limits) *SlotPool {
	p := &SlotPool{held: map[string]jobs.Slot{}}
	p.Resize(f, l)
	return p
}

// Resize changes totals (reload, hardware refresh) and keeps the slots already held.
func (p *SlotPool) Resize(f hardware.Facts, l config.Limits) {
	p.cores = max(1, f.CPU.LogicalCores*(100-l.CPUReservePct)/100)
	p.ram = max(0, f.RAMGB-l.RAMReserveGB)
	p.gpus = nil
	for _, g := range f.GPUs {
		mem := g.VRAMGB - l.VRAMHeadroomGB
		if g.VRAMGB == 0 {
			mem = g.UnifiedMemoryGB - l.RAMReserveGB
		}
		p.gpus = append(p.gpus, gpuCap{Index: g.Index, MemGB: max(0, mem)})
	}
	for _, s := range p.held {
		if s.GPU != nil {
			for i := range p.gpus {
				if p.gpus[i].Index == *s.GPU {
					p.gpus[i].UsedGB += s.VRAMGB
				}
			}
		}
	}
}

func (p *SlotPool) used() (int, float64) {
	var c int
	var r float64
	for _, s := range p.held {
		c += s.Cores
		r += s.RAMGB
	}
	return c, r
}

// TryAcquire reserves req if it fits now. A GPU job gets the allowed card with the most free memory that fits.
func (p *SlotPool) TryAcquire(req config.Resources, needGPU bool, allowed []int) (jobs.Slot, bool) {
	cores, ram := p.used()
	need := max(req.CPUCores, 1)
	if cores+need > p.cores || (req.RAMGB > 0 && ram+req.RAMGB > p.ram+1e-9) {
		return jobs.Slot{}, false
	}
	slot := jobs.Slot{Cores: need, RAMGB: req.RAMGB}
	if needGPU {
		best := -1
		for i, g := range p.gpus {
			if len(allowed) > 0 && !slices.Contains(allowed, g.Index) {
				continue
			}
			if g.MemGB-g.UsedGB+1e-9 < req.VRAMGB {
				continue
			}
			if best < 0 || g.MemGB-g.UsedGB > p.gpus[best].MemGB-p.gpus[best].UsedGB {
				best = i
			}
		}
		if best < 0 {
			return jobs.Slot{}, false
		}
		p.gpus[best].UsedGB += req.VRAMGB
		idx := p.gpus[best].Index
		slot.GPU, slot.VRAMGB = &idx, req.VRAMGB
	}
	p.seq++
	slot.ID = fmt.Sprintf("slot-%d", p.seq)
	p.held[slot.ID] = slot
	return slot, true
}

// Release returns a slot.
func (p *SlotPool) Release(id string) {
	s, ok := p.held[id]
	if !ok {
		return
	}
	delete(p.held, id)
	if s.GPU != nil {
		for i := range p.gpus {
			if p.gpus[i].Index == *s.GPU {
				p.gpus[i].UsedGB = max(0, p.gpus[i].UsedGB-s.VRAMGB)
			}
		}
	}
}

func (p *SlotPool) Snapshot() PoolState {
	c, r := p.used()
	g := append([]gpuCap(nil), p.gpus...)
	sort.Slice(g, func(i, j int) bool { return g[i].Index < g[j].Index })
	return PoolState{CoresTotal: p.cores, CoresUsed: c, RAMTotalGB: p.ram, RAMUsedGB: r, GPUs: g, Held: len(p.held)}
}
