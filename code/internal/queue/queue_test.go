package queue

import (
	"testing"
	"time"
)

func TestBandFor(t *testing.T) {
	cases := map[int]string{300: "p0", 150: "p0", 149: "p1", 75: "p1", 74: "p2", 50: "p2", 49: "p3", 25: "p3", 24: "p4", 0: "p4"}
	for prio, want := range cases {
		if got := BandFor(prio); got != want {
			t.Errorf("BandFor(%d) = %s, want %s", prio, got, want)
		}
	}
}

func TestConsumerRefNames(t *testing.T) {
	r := ConsumerRef{Band: "p2", Caller: "act3", Service: "act3.video-merge"}
	if r.Subject() != "work.p2.act3.act3.video-merge" || r.Durable() != "w_p2_act3_act3_video_merge" {
		t.Fatalf("subject %q durable %q", r.Subject(), r.Durable())
	}
}

func TestNormalize(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	e := Envelope{JobID: "j1", Caller: "local", Service: "sys.compute", Priority: 999}
	if err := e.Normalize(now); err != nil {
		t.Fatal(err)
	}
	if e.Priority != 300 || e.EnvelopeVersion != 1 || !e.IssuedAt.Equal(now) || e.ExpiresAt.Sub(now) != 24*time.Hour || e.Reply.Events != "evt.local.j1" {
		t.Fatalf("normalized = %+v", e)
	}
	if e.Expired(now.Add(23*time.Hour)) || !e.Expired(now.Add(25*time.Hour)) {
		t.Fatal("Expired wrong")
	}
	bad := Envelope{EnvelopeVersion: 2, JobID: "j1", Caller: "local", Service: "s"}
	if err := bad.Normalize(now); err == nil {
		t.Fatal("envelope_version 2 accepted")
	}
}

func TestFinalState(t *testing.T) {
	for _, s := range []string{StateSucceeded, StateFailed, StateCancelled, StateExpired, StateRejected} {
		if !FinalState(s) {
			t.Errorf("%s should be final", s)
		}
	}
	for _, s := range []string{StateQueued, StateClaimed, StateRunning, StateUploading} {
		if FinalState(s) {
			t.Errorf("%s should not be final", s)
		}
	}
}
