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
	"encoding/json"
	"errors"
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

	"github.com/ACT3ai/ai_processing_server/internal/errlog"
	"github.com/ACT3ai/ai_processing_server/internal/fsx"
)

var elog = errlog.For("snapshot")

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

// EnsureTries bounds how often Ensure starts over because the tree changed while it was being copied.
var EnsureTries = 5

// manifest is the .complete file: what the snapshot must hold. Ensure re-checks the sizes before reusing a snapshot,
// so one whose data never reached the disk (a power cut right after it was built) is rebuilt, not run.
type manifest struct {
	Key   string           `json:"key"`
	Files map[string]int64 `json:"files"` // relative path (slashes) → bytes; the binary is BinaryName()
	At    time.Time        `json:"at"`
}

// Ensure returns the snapshot of srcRoot (the repo checkout) and binary under base, building it on first use.
//
// Building is atomic and consistent: the files are copied into a hidden temp directory while they are hashed a
// second time, and the copy is kept only if that second hash equals the first. A git pull that lands while the
// snapshot is copied therefore never produces a snapshot that mixes old and new files (Ensure starts over instead).
// Every file and directory is fsynced before the rename publishes the snapshot under its key.
func Ensure(srcRoot, binary, base string) (Snapshot, error) {
	mu.Lock()
	defer mu.Unlock()
	if err := os.MkdirAll(base, 0o700); err != nil {
		return Snapshot{}, err
	}
	var lastErr error
	for range max(EnsureTries, 1) {
		s, err := ensureOnce(srcRoot, binary, base)
		if err == nil {
			return s, nil
		}
		lastErr = err
		if !errors.Is(err, errTreeChanged) {
			return Snapshot{}, err
		}
		time.Sleep(200 * time.Millisecond) // let the pull finish
	}
	return Snapshot{}, fmt.Errorf("the code tree kept changing while it was snapshotted (a git pull in progress?): %w", lastErr)
}

// beforeCopy is a test hook: it runs between hashing the tree and copying it (where a git pull can land).
var beforeCopy func()

var errTreeChanged = errors.New("the code tree changed while it was being snapshotted")

func ensureOnce(srcRoot, binary, base string) (Snapshot, error) {
	files, err := list(srcRoot)
	if err != nil {
		return Snapshot{}, err
	}
	key, err := hashAll(binary, srcRoot, files)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: %v", errTreeChanged, err) // a file vanished between listing and hashing
	}
	s := Snapshot{Key: key, Dir: filepath.Join(base, key)}
	switch err := verify(s); {
	case err == nil:
		now := time.Now()
		if cerr := os.Chtimes(s.Dir, now, now); cerr != nil { // last used, for Prune
			return Snapshot{}, cerr
		}
		return s, nil
	case !os.IsNotExist(err):
		// Present but not whole (a crash mid-build, or data a power cut lost): set it aside for Prune, build again.
		if rerr := os.Rename(s.Dir, filepath.Join(base, fmt.Sprintf(".broken-%s-%d", key, time.Now().UnixNano()))); rerr != nil {
			return Snapshot{}, fmt.Errorf("snapshot %s is damaged (%v) and cannot be moved aside: %w", key, err, rerr)
		}
	}
	if beforeCopy != nil {
		beforeCopy()
	}
	tmp, err := os.MkdirTemp(base, ".building-")
	if err != nil {
		return Snapshot{}, err
	}
	defer os.RemoveAll(tmp) // after a successful rename tmp no longer exists
	m := manifest{Key: key, Files: map[string]int64{}, At: time.Now().UTC()}
	h := sha256.New()
	copyOne := func(name, src, dst string, mode os.FileMode) error {
		fmt.Fprintf(h, "%s\x00", filepath.ToSlash(name))
		n, err := copyFile(src, dst, mode, h)
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(name)
		if name == "<binary>" {
			rel = BinaryName()
		}
		m.Files[rel] = n
		return nil
	}
	if err := copyOne("<binary>", binary, filepath.Join(tmp, BinaryName()), 0o500); err != nil {
		return Snapshot{}, fmt.Errorf("snapshot binary: %w", err)
	}
	for _, rel := range files {
		if err := copyOne(rel, filepath.Join(srcRoot, rel), filepath.Join(tmp, rel), 0o400); err != nil {
			if os.IsNotExist(err) {
				return Snapshot{}, fmt.Errorf("%w: %s vanished", errTreeChanged, rel)
			}
			return Snapshot{}, fmt.Errorf("snapshot %s: %w", rel, err)
		}
	}
	if got := hex.EncodeToString(h.Sum(nil))[:12]; got != key {
		return Snapshot{}, fmt.Errorf("%w (hashed %s, copied %s)", errTreeChanged, key, got)
	}
	if err := syncTree(tmp); err != nil {
		return Snapshot{}, err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return Snapshot{}, err
	}
	if err := fsx.WriteDurable(filepath.Join(tmp, ".complete"), b, 0o400); err != nil {
		return Snapshot{}, err
	}
	if err := os.Rename(tmp, s.Dir); err != nil {
		return Snapshot{}, err
	}
	return s, fsx.SyncDir(base)
}

// verify: nil when the snapshot is complete and every file has its recorded size; os.ErrNotExist when there is no
// snapshot under the key at all. A .complete from before manifests (a timestamp) is trusted as before.
func verify(s Snapshot) error {
	if _, err := os.Stat(s.Dir); err != nil {
		return err
	}
	b, err := os.ReadFile(filepath.Join(s.Dir, ".complete"))
	if err != nil {
		return fmt.Errorf("no .complete: %v", err) // a leftover of an interrupted build (never os.ErrNotExist here)
	}
	var m manifest
	if json.Unmarshal(b, &m) != nil || m.Files == nil {
		return nil
	}
	for rel, n := range m.Files {
		fi, err := os.Stat(filepath.Join(s.Dir, filepath.FromSlash(rel)))
		if err != nil || fi.Size() != n {
			return fmt.Errorf("%s: want %d bytes", rel, n)
		}
	}
	return nil
}

// syncTree fsyncs every directory under dir (files were synced as they were copied).
func syncTree(dir string) error {
	return filepath.WalkDir(dir, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			return fsx.SyncDir(p)
		}
		return nil
	})
}

// LeftoverAge: hidden .building-, .broken- and .deleting- directories older than this are removed by Prune.
var LeftoverAge = time.Hour

// Prune deletes snapshots beyond the newest keep, never one in inUse. A snapshot is first renamed to a hidden
// .deleting- name, so a removal cut short (a crash, a file still open on Windows) never leaves a partial snapshot
// under its key for Ensure to hand to a job. It also removes hidden leftovers of interrupted builds and deletes.
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
		if !e.IsDir() {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		if strings.HasPrefix(e.Name(), ".") {
			if time.Since(fi.ModTime()) > LeftoverAge {
				dir := filepath.Join(base, e.Name())
				makeWritable(dir)
				elog.Ignore("removing a leftover snapshot directory", os.RemoveAll(dir), "dir", dir)
			}
			continue
		}
		all = append(all, snap{e.Name(), fi.ModTime()})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].at.After(all[j].at) })
	for i, s := range all {
		if i < keep || inUse[s.name] {
			continue
		}
		dir := filepath.Join(base, s.name)
		gone := filepath.Join(base, fmt.Sprintf(".deleting-%s-%d", s.name, time.Now().UnixNano()))
		if err := os.Rename(dir, gone); err != nil {
			elog.Warn("moving an old snapshot aside to delete it", err, "snapshot", s.name)
			continue
		}
		removed = append(removed, s.name)
		makeWritable(gone)
		elog.Ignore("deleting an old snapshot", os.RemoveAll(gone), "dir", gone) // a leftover is retried next Prune
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

// copyFile copies src to dst (fsynced, then mode), feeding the bytes it copied into h. It returns the size.
func copyFile(src, dst string, mode os.FileMode, h io.Writer) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return 0, err
	}
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(io.MultiWriter(out, h), in)
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return 0, err
	}
	return n, os.Chmod(dst, mode)
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
