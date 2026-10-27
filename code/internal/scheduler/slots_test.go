package scheduler

import (
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
)

func TestSlotPoolPerCardAndRelease(t *testing.T) {
	f := hardware.Facts{CPU: hardware.CPU{LogicalCores: 20}, RAMGB: 64,
		GPUs: []hardware.GPU{{Index: 0, VRAMGB: 16}, {Index: 1, VRAMGB: 16}}}
	l := config.Limits{CPUReservePct: 0, RAMReserveGB: 0, VRAMHeadroomGB: 1}
	p := NewSlotPool(f, l)
	req := config.Resources{CPUCores: 2, RAMGB: 4, VRAMGB: 14}
	a, ok1 := p.TryAcquire(req, true, nil)
	b, ok2 := p.TryAcquire(req, true, nil)
	_, ok3 := p.TryAcquire(req, true, nil)
	if !ok1 || !ok2 || ok3 {
		t.Fatalf("two 15 GB cards hold exactly two 14 GB jobs: %v %v %v", ok1, ok2, ok3)
	}
	if *a.GPU == *b.GPU {
		t.Fatal("both jobs on the same card")
	}
	p.Release(a.ID)
	if _, ok := p.TryAcquire(req, true, []int{*a.GPU}); !ok {
		t.Fatal("released card not reusable")
	}
	if _, ok := p.TryAcquire(config.Resources{CPUCores: 2}, true, []int{*b.GPU}); ok == false {
		// zero-VRAM request on a busy card still fits memory-wise
		t.Fatal("zero-VRAM request should fit")
	}
}

func TestSlotPoolCores(t *testing.T) {
	p := NewSlotPool(hardware.Facts{CPU: hardware.CPU{LogicalCores: 10}, RAMGB: 8}, config.Limits{CPUReservePct: 20})
	n := 0
	for {
		if _, ok := p.TryAcquire(config.Resources{CPUCores: 3, RAMGB: 1}, false, nil); !ok {
			break
		}
		n++
	}
	if n != 2 { // 8 usable cores / 3
		t.Fatalf("want 2 slots, got %d", n)
	}
}
