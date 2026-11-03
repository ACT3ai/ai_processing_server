package snapshot

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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

// A git pull that lands between hashing and copying never yields a snapshot that mixes old and new files: Ensure
// sees the copy no longer matches the hash and starts over, so the snapshot's content is exactly its key's.
func TestPullDuringCopyIsNeverMixed(t *testing.T) {
	root, base := t.TempDir(), t.TempDir()
	bin := filepath.Join(root, "ai_processing_server")
	a := filepath.Join(root, "code", "scripts", "a.py")
	b := filepath.Join(root, "code", "scripts", "b.py")
	write(t, bin, "binary")
	write(t, a, "a v1")
	write(t, b, "b v1")
	pulled := false
	beforeCopy = func() {
		if !pulled {
			pulled = true
			write(t, a, "a v2") // the pull lands mid-snapshot
			write(t, b, "b v2")
		}
	}
	defer func() { beforeCopy = nil }()
	s, err := Ensure(root, bin, base)
	if err != nil {
		t.Fatal(err)
	}
	ga, _ := os.ReadFile(s.Path("code/scripts/a.py"))
	gb, _ := os.ReadFile(s.Path("code/scripts/b.py"))
	if string(ga) != "a v2" || string(gb) != "b v2" {
		t.Fatalf("snapshot holds a=%q b=%q", ga, gb)
	}
	again, _ := Ensure(root, bin, base)
	if again.Key != s.Key {
		t.Fatalf("the snapshot's key does not match its content: %s vs %s", s.Key, again.Key)
	}
}

// A snapshot whose files lost their data (a power cut right after it was built) is set aside and rebuilt, never
// handed to a job; Prune later removes the damaged copy and other hidden leftovers.
func TestDamagedSnapshotIsRebuilt(t *testing.T) {
	root, base := t.TempDir(), t.TempDir()
	bin := filepath.Join(root, "ai_processing_server")
	script := filepath.Join(root, "code", "scripts", "run.py")
	write(t, bin, "binary")
	write(t, script, "print('render')")
	s, err := Ensure(root, bin, base)
	if err != nil {
		t.Fatal(err)
	}
	p := s.Path("code/scripts/run.py")
	os.Chmod(p, 0o600)
	os.Truncate(p, 0)
	s2, err := Ensure(root, bin, base)
	if err != nil || s2.Key != s.Key {
		t.Fatalf("rebuild: %v %s", err, s2.Key)
	}
	if b, _ := os.ReadFile(p); string(b) != "print('render')" {
		t.Fatalf("damaged snapshot reused: %q", b)
	}
	old := LeftoverAge
	LeftoverAge = 0
	defer func() { LeftoverAge = old }()
	os.MkdirAll(filepath.Join(base, ".building-123"), 0o700)
	time.Sleep(10 * time.Millisecond)
	Prune(base, 5, nil)
	ents, _ := os.ReadDir(base)
	if len(ents) != 1 || ents[0].Name() != s.Key {
		var names []string
		for _, e := range ents {
			names = append(names, e.Name())
		}
		t.Fatalf("after Prune: %v", names)
	}
}
