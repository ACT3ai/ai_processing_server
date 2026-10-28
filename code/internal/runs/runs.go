// Package runs is the run directory contract between the controller and a task host (pm/task_queue.mdx §9.3,
// §9.4; schema data/schemas/run_record.v1.schema.json). One directory per running job,
// data/git_ignore/runs/<job_id>/:
//
//	task.json       written by the controller before launch: everything the task host needs (it reads no config)
//	run.json        the controller's claim record: ack subject, fencing revision, slot, task host pid
//	events.jsonl    appended by the task host: stage, step and progress lines
//	heartbeat.json  rewritten by the task host every 2 s
//	cancel          written by the controller to stop the job
//	fence_request   written by the task host before it uploads; answered by fence_ok or fence_lost
//	result.json     written by the task host when it is done (success or failure)
//	exit.json       written by the task host last; its presence means the task host has finished
//	handled         written by the controller once the final event is published and the job acked
//
// Files are written with a temp file and rename, so a reader never sees half a file.
package runs

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

// Version is the run directory contract version. A controller reads this version and the previous one (§9.6).
const Version = 1

// File names.
const (
	TaskFile      = "task.json"
	RunFile       = "run.json"
	EventsFile    = "events.jsonl"
	HeartbeatFile = "heartbeat.json"
	CancelFile    = "cancel"
	FenceRequest  = "fence_request"
	FenceOK       = "fence_ok"
	FenceLost     = "fence_lost"
	ResultFile    = "result.json"
	ExitFile      = "exit.json"
	HandledFile   = "handled"
	HostLog       = "taskhost.log"
)

// HeartbeatEvery is how often a task host rewrites heartbeat.json; StaleAfter is when a silent one counts as gone.
const (
	HeartbeatEvery = 2 * time.Second
	StaleAfter     = 10 * time.Second
)

// Task is task.json.
type Task struct {
	RunVersion  int                         `json:"run_version"`
	Job         *jobs.Job                   `json:"job"` // envelope, attempt, slot, timeout
	Service     config.ServiceDef           `json:"service"`
	WorkDir     string                      `json:"work_dir"`
	ResultsDir  string                      `json:"results_dir"` // outputs kept on this server go to <results_dir>/<job_id>/
	LogPath     string                      `json:"log_path"`
	Engines     map[string]*hardware.Engine `json:"engines"`
	AllowLocal  bool                        `json:"allow_local_inputs"` // caller `local`: inputs may be local paths
	KeepWorkDir bool                        `json:"keep_work_dir"`
	Snapshot    string                      `json:"snapshot"`
}

// Record is run.json.
type Record struct {
	RunVersion int               `json:"run_version"`
	JobID      string            `json:"job_id"`
	Caller     string            `json:"caller"`
	Service    string            `json:"service"`
	Snapshot   string            `json:"snapshot"`
	Claim      queue.ClaimHandle `json:"claim"` // ack subject, fencing revision, attempt
	Slot       jobs.Slot         `json:"slot"`
	TaskHost   HostInfo          `json:"taskhost"`
	EventsRead int               `json:"events_read"` // lines of events.jsonl already applied and published
	Seq        int64             `json:"seq"`         // last event seq published
	ClaimedAt  time.Time         `json:"claimed_at"`
}

type HostInfo struct {
	PID       int       `json:"pid"`
	StartedAt time.Time `json:"started_at"`
}

// Line is one line of events.jsonl.
type Line struct {
	Type     string    `json:"type"` // stage | step | progress
	State    string    `json:"state,omitempty"`
	Step     string    `json:"step,omitempty"`
	Progress float64   `json:"progress,omitempty"`
	Note     string    `json:"note,omitempty"`
	At       time.Time `json:"at"`
}

// Heartbeat is heartbeat.json.
type Heartbeat struct {
	PID       int       `json:"pid"`
	StartedAt time.Time `json:"started_at"`
	At        time.Time `json:"at"`
}

// Result is result.json.
type Result struct {
	OK      bool            `json:"ok"`
	Fenced  bool            `json:"fenced,omitempty"` // another worker owns the job; nothing was delivered
	Outputs []jobs.Output   `json:"outputs,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	Kind    jobs.ErrorKind  `json:"kind,omitempty"`
	Message string          `json:"message,omitempty"`
	Stage   string          `json:"stage,omitempty"`
	Seconds float64         `json:"seconds"`
}

// Exit is exit.json.
type Exit struct {
	Code int       `json:"code"`
	At   time.Time `json:"at"`
}

// Cancel is the cancel file.
type Cancel struct {
	Reason string         `json:"reason"`
	Kind   jobs.ErrorKind `json:"kind"`
}

// WriteJSON writes v to dir/name atomically.
func WriteJSON(dir, name string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, "."+name+".tmp")
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, name))
}

// ReadJSON reads dir/name into v.
func ReadJSON(dir, name string, v any) error {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// Exists reports whether dir/name exists.
func Exists(dir, name string) bool {
	_, err := os.Stat(filepath.Join(dir, name))
	return err == nil
}

// ReadLines returns the lines of events.jsonl from index from on, and the new total. A torn last line (the task
// host is mid-write) is left for the next read.
func ReadLines(dir string, from int) ([]Line, int, error) {
	f, err := os.Open(filepath.Join(dir, EventsFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, from, nil
	}
	if err != nil {
		return nil, from, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	var out []Line
	n := 0
	for sc.Scan() {
		var l Line
		if json.Unmarshal(sc.Bytes(), &l) != nil {
			break // torn line: stop here
		}
		if n >= from {
			out = append(out, l)
		}
		n++
	}
	return out, max(n, from), sc.Err()
}

// Alive reports whether the task host recorded in rec is still the same running process: its pid is alive and it
// rewrote heartbeat.json recently with that pid. A reused pid never rewrites our heartbeat, so it reads as gone
// (§9.4 step 2).
func Alive(dir string, rec *Record) bool {
	var hb Heartbeat
	if err := ReadJSON(dir, HeartbeatFile, &hb); err != nil {
		// Not started yet: trust the pid for a short grace period after launch.
		return rec.TaskHost.PID > 0 && pidAlive(rec.TaskHost.PID) && time.Since(rec.ClaimedAt) < StaleAfter
	}
	if hb.PID != rec.TaskHost.PID || time.Since(hb.At) > StaleAfter {
		return false
	}
	return pidAlive(hb.PID)
}

// List returns the job ids that have a run directory under root.
func List(root string) []string {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out
}
