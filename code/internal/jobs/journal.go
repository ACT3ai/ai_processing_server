package jobs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ACT3ai/ai_processing_server/internal/fsx"
)

// Journal keeps one JSON file per job under state/journal/, written atomically before the server acts on a
// state change, so a crash leaves a truthful record (spec §9.5). Final jobs stay for 24 hours so the API can
// still answer for them after a restart; the sweeper removes them.
type Journal struct{ dir string }

func NewJournal(dir string) (*Journal, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Journal{dir: dir}, nil
}

func (j *Journal) path(id string) string { return filepath.Join(j.dir, id+".json") }

// Save writes the job atomically and durably (temp file, fsync, rename, directory fsync: fsx.WriteDurable). A plain
// rename could survive a power cut without its data, leaving an empty entry and losing the job's claim (D-026).
func (j *Journal) Save(job *Job) error {
	b, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		return err
	}
	return fsx.WriteDurable(j.path(job.ID), b, 0o600)
}

// Delete removes a job's entry.
func (j *Journal) Delete(id string) error {
	err := os.Remove(j.path(id))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// LoadAll reads every entry. Unreadable files are returned as errors and skipped.
func (j *Journal) LoadAll() ([]*Job, []error) {
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return nil, []error{err}
	}
	var out []*Job
	var errs []error
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(j.dir, e.Name()))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var job Job
		if err := json.Unmarshal(b, &job); err != nil {
			errs = append(errs, fmt.Errorf("journal %s: %w", e.Name(), err))
			continue
		}
		out = append(out, &job)
	}
	return out, errs
}
