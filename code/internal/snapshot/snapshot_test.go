package snapshot

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureReusesAndGCKeepsInUse(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	for _, d := range []string{"services", "queue"} {
		os.MkdirAll(filepath.Join(data, d), 0o700)
		os.WriteFile(filepath.Join(data, d, "x.yaml"), []byte("a: 1\n"), 0o600)
	}
	exe := filepath.Join(dir, "bin_a")
	os.WriteFile(exe, []byte("binary A"), 0o700)
	root := filepath.Join(dir, "snapshots")
	a, err := Ensure(root, exe, "abc1234", data)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(a.Binary); err != nil || string(b) != "binary A" {
		t.Fatalf("snapshot binary %q %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(a.Dir, "data", "services", "x.yaml")); err != nil {
		t.Fatal("catalogs not copied")
	}
	again, err := Ensure(root, exe, "abc1234", data)
	if err != nil || again.ID != a.ID || !again.CreatedAt.Equal(a.CreatedAt) {
		t.Fatalf("same binary did not reuse the snapshot: %+v %v", again, err)
	}
	os.WriteFile(exe, []byte("binary B"), 0o700) // a rebuild on the same commit (dirty tree)
	b, err := Ensure(root, exe, "abc1234", data)
	if err != nil || b.ID == a.ID {
		t.Fatalf("a different binary must get its own snapshot: %v", err)
	}
	if removed := GC(root, 0, map[string]bool{a.ID: true}); len(removed) != 1 || removed[0] != b.ID {
		t.Fatalf("GC removed %v; must keep the one in use", removed)
	}
	if _, err := os.Stat(a.Binary); err != nil {
		t.Fatal("the in-use snapshot was deleted")
	}
}
