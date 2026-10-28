package scheduler

import (
	"strings"
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

func lanes(order []queue.ConsumerRef) string {
	var b []string
	for _, r := range order {
		b = append(b, r.Band+"."+r.Caller)
	}
	return strings.Join(b, " ")
}

func TestPullOrderLocalModesAndCeilings(t *testing.T) {
	callers, svcs := []string{"act3", "local", "wtc"}, []string{"s"}
	share := lanes(pullOrder(callers, svcs, config.LocalShare))
	// WTC's p0 lane is pulled as p1 (its ceiling): after every real p0 lane, first among the p1 lanes.
	if want := "p0.act3 p0.local p0.wtc p1.act3 p1.local p1.wtc p2.act3"; !strings.HasPrefix(share, want) {
		t.Fatalf("share order = %s", share)
	}
	first := lanes(pullOrder(callers, svcs, config.LocalFirst))
	if !strings.HasPrefix(first, "p0.local p1.local p2.local p3.local p4.local p0.act3") {
		t.Fatalf("first order = %s", first)
	}
	idle := lanes(pullOrder(callers, svcs, config.LocalIdleOnly))
	if !strings.HasSuffix(idle, "p0.local p1.local p2.local p3.local p4.local") {
		t.Fatalf("idle_only order = %s", idle)
	}
}
