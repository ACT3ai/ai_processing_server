package keys

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// A key dir inside a git repo must be git-ignored there (the act3ai-credentials layout: /keys/ ignored).
func TestInitKeyRefusesCommittableDir(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	repo := t.TempDir()
	if out, err := exec.Command("git", "init", "-q", repo).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v %s", err, out)
	}
	if err := os.WriteFile(filepath.Join(repo, ".gitignore"), []byte("/keys/\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	tracked := filepath.Join(repo, "ACT3", "server")
	if _, err := InitKey(tracked); err == nil {
		t.Fatal("created a private key where git could commit it")
	}
	if _, err := os.Stat(filepath.Join(tracked, PrivateFile)); err == nil {
		t.Fatal("key file written despite the refusal")
	}

	ignored := filepath.Join(repo, "keys", "ai_processing_server", "TEST1")
	r, err := InitKey(ignored)
	if err != nil || !r.Created {
		t.Fatalf("ignored dir: %v created=%v", err, r.Created)
	}
	if _, err := LoadKey(r.Key.Path); err != nil {
		t.Fatalf("load from ignored dir: %v", err)
	}

	// The ignore rule removed later: loading must refuse too.
	if err := os.WriteFile(filepath.Join(repo, ".gitignore"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadKey(r.Key.Path); err == nil {
		t.Fatal("loaded a key that git could now commit")
	}
}

func TestCheckNotCommittableOutsideRepo(t *testing.T) {
	if enclosingRepo(t.TempDir()) != "" {
		t.Skip("the temp dir is inside a git repo on this machine")
	}
	if err := CheckNotCommittable(filepath.Join(t.TempDir(), "k", PrivateFile)); err != nil {
		t.Fatal(err)
	}
}

// A stray ".git" folder git does not recognise (one sat in macOS $TMPDIR holding only a config file, 2026-10-27)
// is not a repo: the guard must not refuse every key dir under it.
func TestStrayDotGitIsNotARepo(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".git", "config"), []byte("junk\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := CheckNotCommittable(filepath.Join(dir, "keys", PrivateFile)); err != nil {
		t.Fatalf("stray .git treated as a repo: %v", err)
	}
}
