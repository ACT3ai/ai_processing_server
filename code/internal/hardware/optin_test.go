//go:build !windows

package hardware

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// A fake blender that leaves a marker file when it is executed. Bryan, 2026-11-19: a server that does not run
// act3.blender_render must never start Blender, not even `blender --version`.
func fakeBlender(t *testing.T) (bin, marker string) {
	t.Helper()
	dir := t.TempDir()
	marker = filepath.Join(dir, "ran")
	bin = filepath.Join(dir, "blender")
	script := "#!/bin/sh\ntouch '" + marker + "'\necho 'Blender 5.3.0'\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, marker
}

func TestBlenderNotExecutedWithoutOptIn(t *testing.T) {
	bin, marker := fakeBlender(t)
	f := Facts{Engines: map[string]*Engine{}}
	detectEngines(context.Background(), &f, map[string]string{"blender": bin}, nil)
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("blender was executed although act3.blender_render is not enabled")
	}
	e := f.Engines["blender"]
	if e == nil || e.Path != bin || e.Version != "unknown" {
		t.Fatalf("blender should be found by path only with version unknown, got %+v", e)
	}
}

func TestBlenderExecutedWhenOptedIn(t *testing.T) {
	bin, marker := fakeBlender(t)
	f := Facts{Engines: map[string]*Engine{}}
	detectEngines(context.Background(), &f, map[string]string{"blender": bin}, map[string]bool{"blender": true})
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("blender should be executed for its version when act3.blender_render is enabled")
	}
	if e := f.Engines["blender"]; e == nil || e.Version == "unknown" {
		t.Fatalf("expected a parsed version, got %+v", e)
	}
}
