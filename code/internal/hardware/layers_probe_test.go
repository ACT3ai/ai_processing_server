//go:build !windows

package hardware

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// fakeTool writes an executable that leaves a marker file when it runs.
func fakeTool(t *testing.T, dir, name, version string) (bin, marker string) {
	t.Helper()
	marker = filepath.Join(dir, name+".ran")
	bin = filepath.Join(dir, name)
	if err := os.WriteFile(bin, []byte("#!/bin/sh\ntouch '"+marker+"'\necho '"+version+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, marker
}

// act3.layers_render's engines: the motion binaries come from $ACT3_MOTION_BIN_DIR and run --version only when the
// service is enabled; Chromium is found ($ACT3_CHROMIUM) and never started.
func TestLayersEnginesProbe(t *testing.T) {
	dir := t.TempDir()
	video, vMark := fakeTool(t, dir, "act3-motion-video", "act3-motion-video 0.1.0 (gpu: false)")
	audio, _ := fakeTool(t, dir, "act3-motion-audio", "act3-motion-audio 0.1.0")
	chrome, cMark := fakeTool(t, dir, "headless_shell", "Chromium 1.0")
	t.Setenv("ACT3_MOTION_BIN_DIR", dir)
	t.Setenv("ACT3_CHROMIUM", chrome)

	f := Facts{Engines: map[string]*Engine{}}
	detectEngines(context.Background(), &f, nil, nil)
	if e := f.Engines["act3_motion_video"]; e == nil || e.Path != video || e.Version != "unknown" {
		t.Fatalf("act3_motion_video without opt-in: %+v", e)
	}
	if e := f.Engines["act3_motion_audio"]; e == nil || e.Path != audio {
		t.Fatalf("act3_motion_audio: %+v", e)
	}
	if _, err := os.Stat(vMark); err == nil {
		t.Fatal("act3-motion-video ran although act3.layers_render is not enabled")
	}
	if e := f.Engines["chromium"]; e == nil || e.Path != chrome {
		t.Fatalf("chromium: %+v", e)
	}

	f = Facts{Engines: map[string]*Engine{}}
	detectEngines(context.Background(), &f, nil, map[string]bool{"act3_motion_video": true, "act3_motion_audio": true})
	if e := f.Engines["act3_motion_video"]; e == nil || e.Version != "0.1.0" {
		t.Fatalf("act3_motion_video opted in: %+v", e)
	}
	if _, err := os.Stat(cMark); err == nil {
		t.Fatal("a probe started Chromium")
	}

	// this_server.yaml engines: wins over the environment
	other := t.TempDir()
	v2, _ := fakeTool(t, other, "act3-motion-video", "act3-motion-video 0.2.0")
	f = Facts{Engines: map[string]*Engine{}}
	detectEngines(context.Background(), &f, map[string]string{"act3_motion_video": v2}, nil)
	if e := f.Engines["act3_motion_video"]; e == nil || e.Path != v2 {
		t.Fatalf("override: %+v", e)
	}
}
