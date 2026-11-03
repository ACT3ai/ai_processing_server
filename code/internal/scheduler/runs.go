package scheduler

import (
	"os"
	"path/filepath"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
	"github.com/ACT3ai/ai_processing_server/internal/taskhost"
)

// Run directories and code snapshots (pm/task_queue.mdx §9; pm/task_authoring.mdx R10). A detached task host
// outlives the controller on purpose, so the controller must also find the ones nothing wants any more:
//
//   - a task host still rendering a job this server no longer holds (a direct job failed at boot, a claim another
//     machine took over while this one was down) is cancelled, so it stops holding a GPU the slot pool thinks is
//     free;
//   - a finished or dead run directory nothing holds is removed after RunKeep (CheckpointKeep when it has
//     checkpoints, so a redelivery to this machine can still resume it);
//   - code snapshots beyond the newest SnapshotKeep are pruned, never one a run directory still names.
var (
	RunKeep        = time.Hour
	CheckpointKeep = 24 * time.Hour
	SnapshotKeep   = 3
)

// sweepRuns is part of the sweep, and runs once more right after the boot reclaim settles the old claims.
func (s *Server) sweepRuns() {
	if s.opt.StateDir == "" {
		return
	}
	runsDir := filepath.Join(s.opt.StateDir, "runs")
	ents, err := os.ReadDir(runsDir)
	if err != nil && !os.IsNotExist(err) {
		elog.Error("listing run directories", err, "dir", runsDir)
		return
	}
	s.mu.Lock()
	settled := s.reclaimed // until the boot reclaim ran, a claim from before the restart may still re-attach
	wanted := map[string]bool{}
	for id, j := range s.jobs {
		if !j.State.Final() {
			wanted[id] = true
		}
	}
	for id := range s.held {
		wanted[id] = true
	}
	for _, c := range s.reclaim {
		wanted[c.JobID] = true
	}
	s.mu.Unlock()

	inUse := map[string]bool{}
	for _, e := range ents {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(runsDir, e.Name())
		t, terr := taskhost.ReadTask(dir)
		if wanted[e.Name()] || (terr == nil && wanted[t.JobID]) {
			if terr == nil {
				inUse[t.Snapshot] = true
			}
			continue
		}
		switch taskhost.Alive(dir) {
		case taskhost.Running:
			if terr == nil {
				inUse[t.Snapshot] = true
			}
			if settled {
				s.log.Warn("cancelling a task host for a job this server no longer holds", "job_id", e.Name(), "dir", dir)
				elog.Check("cancelling an orphan task host", taskhost.Cancel(dir), "dir", dir)
			}
		default:
			keep := RunKeep
			if done, _ := taskhost.Checkpoints(dir); done > 0 {
				keep = CheckpointKeep
			}
			if time.Since(lastTouched(dir)) < keep {
				if terr == nil {
					inUse[t.Snapshot] = true
				}
				continue
			}
			s.log.Info("removing an old run directory nothing holds", "dir", dir)
			elog.Ignore("removing an old run directory", os.RemoveAll(dir), "dir", dir)
		}
	}
	for _, k := range snapshot.Prune(filepath.Join(s.opt.StateDir, "snapshots"), SnapshotKeep, inUse) {
		s.log.Info("pruned an old code snapshot", "snapshot", k)
	}
}

// lastTouched is the newest modification time of the run directory's control files.
func lastTouched(dir string) time.Time {
	var t time.Time
	for _, f := range []string{taskhost.TaskFile, taskhost.HeartbeatFile, taskhost.ExitFile, taskhost.EventsFile, taskhost.CancelFile} {
		if fi, err := os.Stat(filepath.Join(dir, f)); err == nil && fi.ModTime().After(t) {
			t = fi.ModTime()
		}
	}
	if t.IsZero() {
		if fi, err := os.Stat(dir); err == nil {
			t = fi.ModTime()
		}
	}
	return t
}
