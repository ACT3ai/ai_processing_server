package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// problems 2026_oct_27 #2: the owner turns the model services and the queue on with one command; comments and
// hand edits survive, a service missing from an older file is added, and an old file without queue: gets one.
func TestSetEnabled(t *testing.T) {
	cat := catalog(t)
	path := filepath.Join(t.TempDir(), "this_server.yaml")
	if _, err := InitThisServer(InitOptions{Path: path, Catalog: cat, Facts: laptop, Version: "test", CanRun: canRunAll}); err != nil {
		t.Fatal(err)
	}
	orig, _ := os.ReadFile(path)
	// An older file: no queue section, no upscale service, a hand comment the edit must keep.
	text := string(orig)
	qs, qe := block(strings.Split(text, "\n"), "queue")
	lines := strings.Split(text, "\n")
	text = strings.Join(append(lines[:qs:qs], lines[qe:]...), "\n")
	text = strings.Replace(text, "  - id: act3.upscale_open_source\n    enabled: false\n", "", 1)
	text = strings.Replace(text, "services:", "services:   # Bryan: keep this comment", 1)
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	ts, err := Load(path)
	if err != nil || ts.Queue.Enabled || ts.Service("act3.upscale_open_source") != nil {
		t.Fatalf("setup: queue=%v upscale=%v err=%v", ts.Queue.Enabled, ts.Service("act3.upscale_open_source"), err)
	}

	did, err := SetEnabled(path, cat, []string{"queue", "act3.video_gen_open_source", "act3.upscale_open_source"}, true)
	if err != nil {
		t.Fatal(err)
	}
	ts, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !ts.Queue.Enabled || ts.Queue.BrokerURL == "" {
		t.Errorf("queue not enabled: %+v (%v)", ts.Queue, did)
	}
	for _, id := range []string{"act3.video_gen_open_source", "act3.upscale_open_source"} {
		if s := ts.Service(id); s == nil || !s.Enabled {
			t.Errorf("%s not enabled: %+v (%v)", id, s, did)
		}
	}
	after, _ := os.ReadFile(path)
	if !strings.Contains(string(after), "# Bryan: keep this comment") {
		t.Error("hand comment lost")
	}
	baks, _ := filepath.Glob(path + ".bak-*")
	if len(baks) != 1 {
		t.Errorf("want one backup, got %v", baks)
	}

	// Again: nothing changes, no new backup. Then off.
	if _, err := SetEnabled(path, cat, []string{"act3.upscale_open_source"}, true); err != nil {
		t.Fatal(err)
	}
	if again, _ := os.ReadFile(path); string(again) != string(after) {
		t.Error("a no-op enable rewrote the file")
	}
	if _, err := SetEnabled(path, cat, []string{"act3.upscale_open_source"}, false); err != nil {
		t.Fatal(err)
	}
	if ts, _ = Load(path); ts.Service("act3.upscale_open_source").Enabled {
		t.Error("disable did not take")
	}
	if _, err := SetEnabled(path, cat, []string{"nope.service"}, true); err == nil {
		t.Error("an unknown service must be refused")
	}
}
