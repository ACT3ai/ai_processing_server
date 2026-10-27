// Package snapshot freezes the code a task runs: a read-only copy of the binary (the task host shim), the runner
// scripts under code/scripts/ and the service catalogs, keyed by their content (pm/task_queue.mdx §9.2,
// pm/task_authoring.mdx R1–R2). A task started from a snapshot never sees a later git pull: the pull changes the
// working tree, the next job builds a new snapshot, and the running one keeps reading its own copy.
//
// The key is a content hash, not the git commit, so a dirty working tree and a rebuilt binary are both caught and
// two identical trees share one snapshot.
package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// Dirs copied from the source root into every snapshot, at the same relative paths.
var Dirs = []string{filepath.Join("code", "scripts"), filepath.Join("data", "services")}

// BinaryName is the binary's name inside a snapshot.
func BinaryName() string {
	if runtime.GOOS == "windows" {
		return "ai_processing_server.exe"
	}
	return "ai_processing_server"
}

// Snapshot is one frozen copy.
type Snapshot struct {
	Key string // 12 hex characters of the content hash
	Dir string // absolute path: <base>/<key>
}

// Binary is the task host shim inside the snapshot.
func (s Snapshot) Binary() string { return filepath.Join(s.Dir, BinaryName()) }

// Path returns rel (slash-separated, relative to the repo root) inside the snapshot.
func (s Snapshot) Path(rel string) string { return filepath.Join(s.Dir, filepath.FromSlash(rel)) }

var mu sync.Mutex

// Ensure returns the snapshot of srcRoot (the repo checkout) and binary under base, building it on first use.
// Building is atomic: a half-copied snapshot is never visible under its key.
func Ensure(srcRoot, binary, base string) (Snapshot, error) {
	mu.Lock()
	defer mu.Unlock()
	files, err := list(srcRoot)
	if err != nil {
		return Snapshot{}, err
	}
	key, err := hashAll(binary, srcRoot, files)
	if err != nil {
		return Snapshot{}, err
	}
	s := Snapshot{Key: key, Dir: filepath.Join(base, key)}
	if _, err := os.Stat(filepath.Join(s.Dir, ".complete")); err == nil {
		now := time.Now()
		os.Chtimes(s.Dir, now, now) // last used, for Prune
		return s, nil
	}
	if err := os.MkdirAll(base, 0o700); err != nil {
		return Snapshot{}, err
	}
	tmp, err := os.MkdirTemp(base, ".building-")
	if err != nil {
		return Snapshot{}, err
	}
	defer os.RemoveAll(tmp)
	if err := copyFile(binary, filepath.Join(tmp, BinaryName()), 0o500); err != nil {
		return Snapshot{}, fmt.Errorf("snapshot binary: %w", err)
	}
	for _, rel := range files {
		if err := copyFile(filepath.Join(srcRoot, rel), filepath.Join(tmp, rel), 0o400); err != nil {
			return Snapshot{}, fmt.Errorf("snapshot %s: %w", rel, err)
		}
	}
	if err := os.WriteFile(filepath.Join(tmp, ".complete"), []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o400); err != nil {
		return Snapshot{}, err
	}
	os.RemoveAll(s.Dir) // an incomplete leftover from a crash
	if err := os.Rename(tmp, s.Dir); err != nil {
		return Snapshot{}, err
	}
	return s, nil
}

// Prune deletes snapshots beyond the newest keep, never one in inUse.
func Prune(base string, keep int, inUse map[string]bool) (removed []string) {
	mu.Lock()
	defer mu.Unlock()
	ents, err := os.ReadDir(base)
	if err != nil {
		return nil
	}
	type snap struct {
		name string
		at   time.Time
	}
	var all []snap
	for _, e := range ents {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if fi, err := e.Info(); err == nil {
			all = append(all, snap{e.Name(), fi.ModTime()})
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].at.After(all[j].at) })
	for i, s := range all {
		if i < keep || inUse[s.name] {
			continue
		}
		dir := filepath.Join(base, s.name)
		makeWritable(dir)
		if os.RemoveAll(dir) == nil {
			removed = append(removed, s.name)
		}
	}
	return removed
}

// list returns the files under Dirs, relative to root, sorted.
func list(root string) ([]string, error) {
	var out []string
	for _, d := range Dirs {
		err := filepath.WalkDir(filepath.Join(root, d), func(p string, e fs.DirEntry, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			name := e.Name()
			if e.IsDir() {
				if name == "__pycache__" || name == "node_modules" || (strings.HasPrefix(name, ".") && p != filepath.Join(root, d)) {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".pyc") || !e.Type().IsRegular() {
				return nil
			}
			rel, _ := filepath.Rel(root, p)
			out = append(out, rel)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(out)
	return out, nil
}

func hashAll(binary, root string, files []string) (string, error) {
	h := sha256.New()
	add := func(name, path string) error {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		fmt.Fprintf(h, "%s\x00", filepath.ToSlash(name))
		_, err = io.Copy(h, f)
		return err
	}
	if err := add("<binary>", binary); err != nil {
		return "", err
	}
	for _, rel := range files {
		if err := add(rel, filepath.Join(root, rel)); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:12], nil
}

func copyFile(src, dst string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
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

func makeWritable(dir string) {
	filepath.WalkDir(dir, func(p string, e fs.DirEntry, err error) error {
		if err == nil {
			if e.IsDir() {
				os.Chmod(p, 0o700)
			} else {
				os.Chmod(p, 0o600)
			}
		}
		return nil
	})
}
