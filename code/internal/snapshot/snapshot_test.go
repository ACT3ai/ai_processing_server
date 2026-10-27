package snapshot

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, p, s string) {
	t.Helper()
	os.MkdirAll(filepath.Dir(p), 0o700)
	if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A pull that changes a runner script makes a new snapshot; the old one keeps the old script byte for byte.
func TestPullDoesNotChangeARunningSnapshot(t *testing.T) {
	root, base := t.TempDir(), t.TempDir()
	bin := filepath.Join(root, "ai_processing_server")
	script := filepath.Join(root, "code", "scripts", "video_gen", "wan22", "render_wan.py")
	write(t, bin, "binary v1")
	write(t, script, "print('v1')")
	write(t, filepath.Join(root, "data", "services", "act3.x.yaml"), "id: act3.x")
	write(t, filepath.Join(root, "code", "scripts", "video_gen", "__pycache__", "x.pyc"), "junk")

	s1, err := Ensure(root, bin, base)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := Ensure(root, bin, base)
	if again.Key != s1.Key {
		t.Fatal("same tree, different key")
	}
	write(t, script, "print('v2')") // the git pull
	s2, err := Ensure(root, bin, base)
	if err != nil || s2.Key == s1.Key {
		t.Fatalf("pull not detected: %v", err)
	}
	if b, _ := os.ReadFile(s1.Path("code/scripts/video_gen/wan22/render_wan.py")); string(b) != "print('v1')" {
		t.Fatalf("old snapshot changed: %q", b)
	}
	if b, _ := os.ReadFile(s2.Binary()); string(b) != "binary v1" {
		t.Fatal("binary not copied")
	}
	if _, err := os.Stat(s1.Path("code/scripts/video_gen/__pycache__/x.pyc")); err == nil {
		t.Fatal("pycache copied")
	}
	if _, err := os.Stat(s1.Path("data/services/act3.x.yaml")); err != nil {
		t.Fatal("catalog not copied")
	}
	write(t, bin, "binary v2") // a rebuild alone is also a new snapshot
	s3, _ := Ensure(root, bin, base)
	if s3.Key == s2.Key {
		t.Fatal("rebuild not detected")
	}
	if got := Prune(base, 1, map[string]bool{s1.Key: true}); len(got) != 1 || got[0] != s2.Key {
		t.Fatalf("prune removed %v", got)
	}
}
