package priority

import (
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/config"
)

var bands = []config.Band{{Name: "p0", MinPriority: 150}, {Name: "p1", MinPriority: 75}, {Name: "p2", MinPriority: 50},
	{Name: "p3", MinPriority: 25}, {Name: "p4", MinPriority: 0}}

func TestBandForEveryThreshold(t *testing.T) {
	for p, want := range map[int]string{0: "p4", 24: "p4", 25: "p3", 49: "p3", 50: "p2", 74: "p2", 75: "p1", 149: "p1", 150: "p0", 300: "p0"} {
		if got := BandFor(bands, p); got != want {
			t.Errorf("BandFor(%d) = %s, want %s", p, got, want)
		}
	}
	if Clamp(-5) != 0 || Clamp(999) != 300 {
		t.Error("clamp")
	}
}

func TestCapBand(t *testing.T) {
	if b, capped := CapBand(bands, "p0", "p1"); b != "p1" || !capped {
		t.Errorf("p0 under ceiling p1 = %s %v", b, capped)
	}
	if b, capped := CapBand(bands, "p3", "p1"); b != "p3" || capped {
		t.Errorf("p3 under ceiling p1 = %s %v", b, capped)
	}
}

func TestPullOrderIsBandThenCallerThenBias(t *testing.T) {
	order := PullOrder(bands[:2], []string{"local", "act3"}, []Candidate{{"sys.compute", 0}, {"sys.self_test", 30}})
	want := []string{"work.p0.local.sys.self_test", "work.p0.local.sys.compute", "work.p0.act3.sys.self_test", "work.p0.act3.sys.compute",
		"work.p1.local.sys.self_test", "work.p1.local.sys.compute", "work.p1.act3.sys.self_test", "work.p1.act3.sys.compute"}
	if len(order) != len(want) {
		t.Fatalf("got %d refs", len(order))
	}
	for i, r := range order {
		if r.Subject() != want[i] {
			t.Errorf("%d: %s, want %s", i, r.Subject(), want[i])
		}
	}
}
