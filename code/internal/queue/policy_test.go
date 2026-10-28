package queue

import (
	"errors"
	"testing"
	"time"
)

func TestCallerCeilings(t *testing.T) {
	cases := []struct {
		caller   string
		in, want int
	}{
		{"act3", 300, 300}, {"local", 200, 200}, // p0 allowed
		{"wtc", 300, 149}, {"wtc", 150, 149}, {"wtc", 149, 149}, {"wtc", 20, 20}, // WTC tops out at p1
		{"partner", 120, 49}, {"partner", 30, 30}, // anyone else tops out at p3
	}
	for _, c := range cases {
		if got := ClampPriority(c.caller, c.in); got != c.want {
			t.Errorf("ClampPriority(%s, %d) = %d, want %d", c.caller, c.in, got, c.want)
		}
	}
	if ClampBand("wtc", "p0") != "p1" || ClampBand("wtc", "p2") != "p2" || ClampBand("act3", "p0") != "p0" || ClampBand("x", "p1") != "p3" {
		t.Fatal("ClampBand")
	}
}

func TestNormalizeClampsAndRefusesVendor(t *testing.T) {
	e := &Envelope{JobID: "j1", Caller: "wtc", Service: "wtc.transcription", Priority: 250}
	if err := e.Normalize(time.Now()); err != nil || e.Priority != 149 || BandFor(e.Priority) != "p1" {
		t.Fatalf("wtc p0 not clamped: %v %d", err, e.Priority)
	}
	v := &Envelope{JobID: "j2", Caller: "act3", Service: "act3.video_gen_open_source", Model: "vendor:veo3"}
	if err := v.Normalize(time.Now()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("vendor model accepted: %v", err)
	}
	c := &Envelope{JobID: "j3", Caller: "act3", Service: "act3.x", Category: "vendor_api"}
	if err := c.Normalize(time.Now()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("vendor_api category accepted: %v", err)
	}
}
