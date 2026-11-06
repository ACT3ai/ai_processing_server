package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
)

// A staged input keeps its source's extension: SeedVR2 refused an extensionless source_video (2026-11-04).
func TestFetchKeepsTheExtension(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "clip.MP4")
	if err := os.WriteFile(src, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	got, _, _, err := Fetch(context.Background(), jobs.Input{Name: "source_video", Path: src}, out, true)
	if err != nil || got != filepath.Join(out, "source_video.mp4") {
		t.Fatalf("%q %v", got, err)
	}
	got, _, _, err = Fetch(context.Background(), jobs.Input{Name: "first_frame.png", Path: src}, out, true)
	if err != nil || got != filepath.Join(out, "first_frame.png") {
		t.Fatalf("a name with its own extension keeps it: %q %v", got, err)
	}
	for in, want := range map[jobs.Input]string{
		{Name: "a", URL: "https://s3/x/clip.mov?X-Amz-Signature=abc"}: ".mov",
		{Name: "a", URL: "https://s3/x/noext"}:                        "",
		{Name: "a", Path: "/x/weird.ext!!"}:                           "",
	} {
		if got := sourceExt(in); got != want {
			t.Errorf("%+v: %q, want %q", in, got, want)
		}
	}
}
