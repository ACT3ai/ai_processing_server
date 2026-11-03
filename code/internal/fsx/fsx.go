// Package fsx writes the small control files the server and its task hosts depend on (job journal, task.json,
// checkpoints, exit.json, snapshot manifests) so that neither a crash nor a power cut leaves a torn or empty file
// under the real name (pm/task_queue.mdx §9, D-026).
//
// WriteAtomic: a unique temp file in the same directory, then rename. A reader never sees half a file, and two
// writers never share a temp name. Enough for files that are rewritten every few seconds (heartbeat.json).
//
// WriteDurable: WriteAtomic plus fsync of the file before the rename and of the directory after it. Without the
// file fsync, a power cut can persist the rename but not the data (a zero-length or zero-filled journal entry,
// checkpoint or exit.json after the reboot); without the directory fsync the rename itself can be lost.
package fsx

import (
	"os"
	"path/filepath"
	"runtime"
)

// WriteAtomic writes data to path through a temp file and a rename. No fsync.
func WriteAtomic(path string, data []byte, perm os.FileMode) error {
	return write(path, data, perm, false)
}

// WriteDurable writes data to path so it survives a power cut once WriteDurable returns.
func WriteDurable(path string, data []byte, perm os.FileMode) error {
	return write(path, data, perm, true)
}

func write(path string, data []byte, perm os.FileMode, durable bool) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		if !ok {
			f.Close()
			os.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		return err
	}
	if durable {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp, perm); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	if durable {
		return SyncDir(dir)
	}
	return nil
}

// SyncFile flushes an existing file's data to disk (a render output before its checkpoint is written).
func SyncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// SyncDir makes a rename or a new entry in dir durable. Windows cannot open a directory for sync; NTFS journals
// metadata itself, so it is a no-op there.
func SyncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
