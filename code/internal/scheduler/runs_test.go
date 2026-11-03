package scheduler

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/taskhost"
)

func writeRun(t *testing.T, dir, jobID, snap string, beat time.Time, pid int) {
	t.Helper()
	os.MkdirAll(dir, 0o700)
	b, _ := json.Marshal(taskhost.Task{RunVersion: 1, JobID: jobID, Snapshot: snap, Argv: []string{"true"}})
	os.WriteFile(filepath.Join(dir, taskhost.TaskFile), b, 0o600)
	b, _ = json.Marshal(taskhost.Heartbeat{PID: pid, At: beat})
	os.WriteFile(filepath.Join(dir, taskhost.HeartbeatFile), b, 0o600)
	for _, f := range []string{taskhost.TaskFile, taskhost.HeartbeatFile} {
		os.Chtimes(filepath.Join(dir, f), beat, beat)
	}
}

// The run-dir sweep: a task host rendering a job nobody holds is cancelled (it would keep a GPU the slot pool thinks
// is free), a held job's task host is left alone, an old dead run directory is removed, and old snapshots are pruned
// except the newest SnapshotKeep and any a kept run directory names.
func TestSweepRunsCancelsOrphansAndPrunes(t *testing.T) {
	state := t.TempDir()
	runs := filepath.Join(state, "runs")
	now := time.Now().UTC()
	// A running task host is a live process (Alive checks the pid since D-026): this test process stands in for it.
	writeRun(t, filepath.Join(runs, "orphan"), "orphan", "s1", now, os.Getpid())
	writeRun(t, filepath.Join(runs, "held"), "held", "s0", now, os.Getpid())
	writeRun(t, filepath.Join(runs, "old"), "old", "s2", now.Add(-3*time.Hour), 999997)
	snaps := filepath.Join(state, "snapshots")
	for i, k := range []string{"s0", "s1", "s2", "s3", "s4", "s5", "s6"} {
		os.MkdirAll(filepath.Join(snaps, k), 0o700)
		at := now.Add(-time.Duration(i) * time.Hour) // s0 newest ... s6 oldest
		os.Chtimes(filepath.Join(snaps, k), at, at)
	}
	s := &Server{opt: Options{StateDir: state}, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		jobs: map[string]*jobs.Job{"held": {ID: "held", State: jobs.Active}}, held: map[string]*heldJob{}}

	s.sweepRuns() // before the boot reclaim settled: nothing is cancelled yet
	if _, err := os.Stat(filepath.Join(runs, "orphan", taskhost.CancelFile)); err == nil {
		t.Fatal("cancelled a task host before the boot reclaim settled its claim")
	}
	s.reclaimed = true
	s.sweepRuns()
	if _, err := os.Stat(filepath.Join(runs, "orphan", taskhost.CancelFile)); err != nil {
		t.Fatal("the orphan task host was not cancelled")
	}
	if _, err := os.Stat(filepath.Join(runs, "held", taskhost.CancelFile)); err == nil {
		t.Fatal("cancelled the task host of a job this server holds")
	}
	if _, err := os.Stat(filepath.Join(runs, "old")); !os.IsNotExist(err) {
		t.Fatal("the old dead run directory was kept")
	}
	for k, want := range map[string]bool{"s0": true, "s1": true, "s2": true, "s3": false, "s5": false, "s6": false} {
		_, err := os.Stat(filepath.Join(snaps, k))
		if (err == nil) != want {
			t.Errorf("snapshot %s kept=%v, want %v", k, err == nil, want)
		}
	}
}
