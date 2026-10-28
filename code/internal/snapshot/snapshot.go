// Package snapshot makes the frozen, read-only copy of the code a job runs from (pm/task_queue.mdx §9.2), so a
// `git pull` and `just build` during a 30-minute render can never change the code under it.
//
// Today every runner is compiled into the binary, so a snapshot is the binary itself plus the data/ catalogs a
// runner reads. TypeScript bundles and Python runner trees will be added to the same directory when those runners
// exist; the id already covers them because it hashes the binary that would launch them.
package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/errlog"
)

var elog = errlog.For("snapshot")

// Snapshot is one frozen copy.
type Snapshot struct {
	ID           string    `json:"id"` // <git_sha>-<first 8 hex of the binary's sha256>
	GitSHA       string    `json:"git_sha"`
	BinarySHA256 string    `json:"binary_sha256"`
	CreatedAt    time.Time `json:"created_at"`
	Dir          string    `json:"-"`
	Binary       string    `json:"-"` // the task host shim to launch
}

// BinaryName is the executable's file name on this OS.
func BinaryName() string {
	if runtime.GOOS == "windows" {
		return "ai_processing_server.exe"
	}
	return "ai_processing_server"
}

// Ensure returns the snapshot of the running binary, building it under root (data/git_ignore/snapshots) the first
// time. Call it once at startup, before anything can rebuild the binary on disk: the copy is made from the file the
// process was started from, and a later `just build` replaces that file.
func Ensure(root, exe, gitSHA, dataDir string) (*Snapshot, error) {
	sum, err := hashFile(exe)
	if err != nil {
		return nil, fmt.Errorf("hash %s: %w", exe, err)
	}
	if gitSHA == "" {
		gitSHA = "unknown"
	}
	s := &Snapshot{ID: gitSHA + "-" + sum[:8], GitSHA: gitSHA, BinarySHA256: sum}
	s.Dir = filepath.Join(root, s.ID)
	s.Binary = filepath.Join(s.Dir, BinaryName())
	if b, err := os.ReadFile(filepath.Join(s.Dir, "snapshot.json")); err == nil {
		var have Snapshot
		if json.Unmarshal(b, &have) == nil && have.BinarySHA256 == sum {
			s.CreatedAt = have.CreatedAt
			return s, nil
		}
	}
	tmp, err := os.MkdirTemp(root, ".building-")
	if err != nil {
		if err := os.MkdirAll(root, 0o700); err != nil {
			return nil, err
		}
		if tmp, err = os.MkdirTemp(root, ".building-"); err != nil {
			return nil, err
		}
	}
	defer func() { elog.Ignore("removing a half-built snapshot", os.RemoveAll(tmp)) }()
	if err := copyFile(exe, filepath.Join(tmp, BinaryName()), 0o555); err != nil {
		return nil, err
	}
	// The binary has the same hash as the running process only if the file was not replaced while we copied it.
	if got, err := hashFile(filepath.Join(tmp, BinaryName())); err != nil || got != sum {
		return nil, fmt.Errorf("%s changed while it was being copied (a build running?); restart the server", exe)
	}
	for _, sub := range []string{"services", "queue"} {
		if err := copyTree(filepath.Join(dataDir, sub), filepath.Join(tmp, "data", sub)); err != nil {
			return nil, err
		}
	}
	s.CreatedAt = time.Now().UTC()
	b, _ := json.MarshalIndent(s, "", "  ")
	if err := os.WriteFile(filepath.Join(tmp, "snapshot.json"), b, 0o444); err != nil {
		return nil, err
	}
	if err := os.RemoveAll(s.Dir); err != nil { // a broken earlier attempt
		return nil, err
	}
	if err := os.Rename(tmp, s.Dir); err != nil {
		return nil, err
	}
	return s, nil
}

// GC removes snapshots that are not in use and not among the newest keep (§9.2).
func GC(root string, keep int, inUse map[string]bool) (removed []string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	type snap struct {
		id string
		at time.Time
	}
	var all []snap
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		at := time.Time{}
		if info, err := e.Info(); err == nil {
			at = info.ModTime()
		}
		all = append(all, snap{e.Name(), at})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].at.After(all[j].at) })
	for i, s := range all {
		if i < keep || inUse[s.id] {
			continue
		}
		p := filepath.Join(root, s.id)
		makeWritable(p)
		if elog.Check("removing an old code snapshot", os.RemoveAll(p), "path", p) == nil {
			removed = append(removed, s.id)
		}
	}
	return removed
}

// makeWritable lets RemoveAll delete the read-only files of a snapshot (needed on Windows).
func makeWritable(dir string) {
	filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err == nil {
			os.Chmod(p, 0o700)
		}
		return nil
	})
}

func hashFile(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o700)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Chmod(dst, mode)
}

func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o700)
		}
		return copyFile(p, filepath.Join(dst, rel), 0o444)
	})
}
