package config

import (
	"strings"
	"testing"
	"time"
)

func TestQueueValidation(t *testing.T) {
	cat := &Catalog{Services: map[string]ServiceDef{}}
	base := func() *ThisServer {
		ts := defaults()
		ts.Server.Key = "TEST1"
		ts.Queue.Enabled = true
		return &ts
	}
	if errs, _ := Validate(base(), cat); len(errs) != 0 {
		t.Fatalf("defaults with the queue on should validate: %v", errs)
	}
	cases := map[string]func(*ThisServer){
		"queue.role hub":       func(ts *ThisServer) { ts.Queue.Role = "hub" },
		"queue.role \"x\"":     func(ts *ThisServer) { ts.Queue.Role = "x" },
		"queue.broker_url":     func(ts *ThisServer) { ts.Queue.BrokerURL = "" },
		"continuity.heartbeat": func(ts *ThisServer) { ts.Continuity.Heartbeat = Duration{4 * time.Minute} },
		"callers.Bad Caller":   func(ts *ThisServer) { ts.Callers = map[string]CallerSetting{"Bad Caller": {Enabled: true}} },
	}
	for want, mutate := range cases {
		ts := base()
		mutate(ts)
		errs, _ := Validate(ts, cat)
		if len(errs) == 0 || !strings.Contains(strings.Join(errs, "|"), want) {
			t.Errorf("%s: errs = %v", want, errs)
		}
	}
	off := base()
	off.Queue = Queue{} // a file without a queue section: off, nothing to validate
	if errs, _ := Validate(off, cat); len(errs) != 0 {
		t.Fatalf("queue off should not be validated: %v", errs)
	}
}

func TestEnabledCallers(t *testing.T) {
	ts := defaults()
	if got := strings.Join(ts.EnabledCallers(), ","); got != "act3,wtc,local" {
		t.Fatalf("default callers = %s", got)
	}
	ts.Callers = map[string]CallerSetting{"wtc": {Enabled: true}, "act3": {Enabled: false}, "local": {Enabled: true}}
	if got := strings.Join(ts.EnabledCallers(), ","); got != "local,wtc" {
		t.Fatalf("callers = %s", got)
	}
}

func TestLocalModeAndModels(t *testing.T) {
	ts := defaults()
	if ts.LocalMode() != LocalShare {
		t.Fatalf("default local mode = %s", ts.LocalMode())
	}
	ts.Callers = map[string]CallerSetting{"local": {Enabled: true, Mode: LocalFirst}}
	if ts.LocalMode() != LocalFirst {
		t.Fatal("mode not read")
	}
	ts.Server.Key, ts.Queue.Enabled = "TEST1", true
	ts.Callers["local"] = CallerSetting{Enabled: true, Mode: "always"}
	ts.Models = ModelRules{Allow: []string{"[bad"}}
	errs, _ := Validate(&ts, &Catalog{Services: map[string]ServiceDef{}})
	all := strings.Join(errs, "|")
	if !strings.Contains(all, "callers.local.mode") || !strings.Contains(all, "not a valid pattern") {
		t.Fatalf("errs = %v", errs)
	}

	m := ModelRules{Allow: []string{"wan2.*", "seedvr2*"}, Deny: []string{"wan2.1*"}}
	for model, want := range map[string]bool{
		"": true, "wan2.2-a14b": true, "seedvr2-best": true, "wan2.1-14b": false, "flux-dev": false, "vendor:veo3": false,
	} {
		if got, why := m.Allowed(model); got != want {
			t.Errorf("Allowed(%q) = %v (%s), want %v", model, got, why, want)
		}
	}
	if ok, _ := (ModelRules{}).Allowed("vendor:runway"); ok {
		t.Error("vendor models must be refused even with no rules")
	}
	if ok, _ := (ModelRules{}).Allowed("anything"); !ok {
		t.Error("empty allow list allows every non-vendor model")
	}
}
