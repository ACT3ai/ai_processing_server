// Package runners holds one Runner per kind of work. Runners never upload and never call a platform: the job
// lifecycle in the scheduler does staging, upload and reporting, so every runner gets the same retry, cancel
// and cleanup behavior (spec §8.4).
package runners

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
)

// Runner does one kind of work. One implementation per runner name in the service catalog.
type Runner interface {
	// Name is the catalog's runner field: "compute", "render", "ffmpeg", ...
	Name() string
	// Check validates params and the local environment without doing work. Inputs are not staged yet.
	Check(ctx context.Context, job *jobs.Job, env Env) error
	// Run does the work, honors ctx cancellation, reports progress, and writes files only under env.OutDir.
	Run(ctx context.Context, job *jobs.Job, env Env, rep Reporter) (*Result, error)
}

// Reattacher is implemented by runners whose Run is idempotent per job id: after a restart, Run re-attaches to a
// detached task host still working on the job, or collects the output it already finished. For such runners a
// queued job held at restart runs again here under the same claim and attempt instead of going back to the queue
// (pm/task_queue.mdx §9.4).
type Reattacher interface {
	Reattachable() bool
}

// Env is what the lifecycle hands a runner.
type Env struct {
	WorkDir string            // data/git_ignore/work/<job_id>/
	OutDir  string            // WorkDir/out: every output goes here
	Inputs  map[string]string // staged input name → local path
	Slot    jobs.Slot
	Engines map[string]*hardware.Engine
	Logger  *slog.Logger
}

// Reporter carries progress back to the job record and the per-job log.
type Reporter interface {
	Stage(name string)
	Progress(fraction float64, note string)
	Log(line string)
}

// Result is what a successful run produced.
type Result struct {
	Outputs []jobs.Output // Name, Path, ContentType, Meta; the lifecycle fills size, sha256 and destination
	Data    any           // stored as the job's `result` JSON
}

// All returns the runners built into this binary, by name.
func All() map[string]Runner {
	m := map[string]Runner{}
	for _, r := range []Runner{Compute{}, Render{}, FFmpeg{}, OSSVideo{}, OSSUpscale{}} {
		m[r.Name()] = r
	}
	return m
}

// Names returns the set of built runner names (for capability offers).
func Names() map[string]bool {
	m := map[string]bool{}
	for n := range All() {
		m[n] = true
	}
	return m
}

// decodeParams unmarshals job params into v, classifying failure as invalid_input.
func decodeParams(job *jobs.Job, v any) error {
	if len(job.Envelope.Params) == 0 {
		return nil
	}
	if err := json.Unmarshal(job.Envelope.Params, v); err != nil {
		return jobs.Wrap(jobs.KindInvalidInput, err, "params do not match this service")
	}
	return nil
}

// writeJSONOutput writes v as name.json into env.OutDir and returns the output.
func writeJSONOutput(env Env, name string, v any) (jobs.Output, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return jobs.Output{}, jobs.Wrap(jobs.KindInternal, err, "encode result")
	}
	p := filepath.Join(env.OutDir, name+".json")
	if err := os.WriteFile(p, b, 0o600); err != nil {
		return jobs.Output{}, jobs.Wrap(jobs.KindInfra, err, "write result")
	}
	return jobs.Output{Name: name, Path: p, ContentType: "application/json"}, nil
}
