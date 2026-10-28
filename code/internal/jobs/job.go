// Package jobs defines a job on this server: the envelope a platform (or the owner, in direct mode) sends, the
// record the server keeps, the state machine, error kinds and the on-disk journal (spec §9, §12).
//
// The local record keeps Asynq's TaskInfo field and state names (id, queue, type, payload, state, max_retry,
// retried, last_err, ...; pm/Decisions.yaml D-008). The task system is NATS JetStream (D-014), which has no job
// model of its own, so the caller-facing contract is pm/task_queue.mdx §13.4: every record also carries
// caller_state (QUEUED, CLAIMED, STAGING, RUNNING, UPLOADING, SUCCEEDED, FAILED, CANCELLED), the same value the
// queue's job_state and events use. Server-specific fields (stage, progress, priority, outputs) sit alongside.
package jobs

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/logging"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

// State is the task-system state (Asynq names, plus "cancelled").
type State string

const (
	Pending   State = "pending"   // waiting for a slot
	Active    State = "active"    // holding a slot: staging, running, uploading
	Retry     State = "retry"     // failed, will run again at next_process_at
	Completed State = "completed" // done; result and outputs available
	Archived  State = "archived"  // failed for good (Asynq's name for a dead task)
	Cancelled State = "cancelled" // stopped on request; nothing uploaded
)

// Final reports whether no more work will happen for this job.
func (s State) Final() bool { return s == Completed || s == Archived || s == Cancelled }

// Stage is the server lifecycle position inside a state (spec §9.2).
type Stage string

const (
	StageLeased    Stage = "LEASED"
	StageStaging   Stage = "STAGING"
	StageRunning   Stage = "RUNNING"
	StageUploading Stage = "UPLOADING"
	StageReporting Stage = "REPORTING"
	StageDone      Stage = "DONE"
	StageFailed    Stage = "FAILED"
	StageCancelled Stage = "CANCELLED"
)

// PlatformStatus maps to ACT3's JobStatus (spec §9.2 table).
func PlatformStatus(st State, sg Stage) string {
	switch st {
	case Pending, Retry:
		return "INQUEUE"
	case Active:
		return "PROGRESS"
	case Completed:
		return "COMPLETED"
	case Cancelled:
		return "CANCELLED"
	default:
		return "ERROR"
	}
}

// CallerState maps the local state and stage to the caller-facing lifecycle (pm/task_queue.mdx §13.4).
func CallerState(st State, sg Stage) string {
	switch st {
	case Pending, Retry:
		return queue.StateQueued
	case Completed:
		return queue.StateSucceeded
	case Cancelled:
		return queue.StateCancelled
	case Archived:
		return queue.StateFailed
	}
	switch sg {
	case StageStaging:
		return queue.StateStaging
	case StageRunning:
		return queue.StateRunning
	case StageUploading, StageReporting:
		return queue.StateUploading
	}
	return queue.StateClaimed
}

// Envelope is what a platform sends (spec §9.1). In direct mode the owner sends the same shape.
type Envelope struct {
	JobID           string          `json:"job_id,omitempty"`
	Platform        string          `json:"platform,omitempty"`
	Service         string          `json:"service"`
	PlatformJobType string          `json:"platform_job_type,omitempty"`
	Priority        int             `json:"priority"`
	TraceID         string          `json:"trace_id,omitempty"`
	Tenant          map[string]any  `json:"tenant,omitempty"`
	Deadline        *time.Time      `json:"deadline,omitempty"`
	TimeoutSeconds  int64           `json:"timeout_seconds,omitempty"`
	MaxRetry        *int            `json:"max_retry,omitempty"` // nil: the service catalog's retry.max_attempts − 1
	Inputs          []Input         `json:"inputs,omitempty"`
	Params          json.RawMessage `json:"params,omitempty"`
	Outputs         []OutputSpec    `json:"outputs,omitempty"`
	DataRules       DataRules       `json:"data_rules"`
}

// Input is a file the job needs. URL is http(s); Path is a local file (direct mode only).
type Input struct {
	Name   string `json:"name"`
	URL    string `json:"url,omitempty"`
	Path   string `json:"path,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
	Bytes  int64  `json:"bytes,omitempty"`
}

// OutputSpec says where a named output goes. No Upload: the server keeps it for GET /v1/jobs/{id}/outputs/{name}.
type OutputSpec struct {
	Name        string  `json:"name"`
	ContentType string  `json:"content_type,omitempty"`
	Upload      *Upload `json:"upload,omitempty"`
}

type Upload struct {
	Method string `json:"method"`
	URL    string `json:"url"`
}

type DataRules struct {
	CustomerData      bool `json:"customer_data"`
	RetainAfterUpload bool `json:"retain_after_upload"`
}

// Output is one produced file, as reported in the job record.
type Output struct {
	Name        string         `json:"name"`
	File        string         `json:"file"`
	Bytes       int64          `json:"bytes"`
	SHA256      string         `json:"sha256"`
	ContentType string         `json:"content_type"`
	UploadedTo  string         `json:"uploaded_to,omitempty"` // host and path only, never the signed query
	Download    string         `json:"download,omitempty"`    // local API path when kept on this server
	Meta        map[string]any `json:"meta,omitempty"`
	Path        string         `json:"-"` // local file
}

// Slot is the hardware a running job holds (spec §15.2).
type Slot struct {
	ID     string  `json:"id"`
	GPU    *int    `json:"gpu,omitempty"`
	Cores  int     `json:"cores"`
	RAMGB  float64 `json:"ram_gb"`
	VRAMGB float64 `json:"vram_gb,omitempty"`
}

// Transition is one entry of a job's history.
type Transition struct {
	At    time.Time `json:"at"`
	State State     `json:"state"`
	Stage Stage     `json:"stage"`
	Note  string    `json:"note,omitempty"`
}

// Job is the record the server keeps and the API returns.
type Job struct {
	// Asynq TaskInfo-compatible fields.
	ID            string          `json:"id"`
	Queue         string          `json:"queue"` // platform: act3 | wtc | direct
	Type          string          `json:"type"`  // service id
	Payload       Envelope        `json:"payload"`
	State         State           `json:"state"`
	MaxRetry      int             `json:"max_retry"`
	Retried       int             `json:"retried"`
	LastErr       string          `json:"last_err,omitempty"`
	LastFailedAt  *time.Time      `json:"last_failed_at,omitempty"`
	Timeout       int64           `json:"timeout_seconds"`
	Deadline      *time.Time      `json:"deadline,omitempty"`
	NextProcessAt *time.Time      `json:"next_process_at,omitempty"`
	CompletedAt   *time.Time      `json:"completed_at,omitempty"`
	Result        json.RawMessage `json:"result,omitempty"`

	// Server fields.
	PlatformStatus    string       `json:"platform_status"`
	CallerState       string       `json:"caller_state"` // pm/task_queue.mdx §13.4
	Priority          int          `json:"priority"`
	EffectivePriority int          `json:"effective_priority"`
	Stage             Stage        `json:"stage"`
	Step              string       `json:"step,omitempty"` // the runner's own step: compute:primes, render, encode
	Progress          float64      `json:"progress"`
	Note              string       `json:"note,omitempty"`
	ErrorKind         ErrorKind    `json:"error_kind,omitempty"`
	ErrorStage        Stage        `json:"error_stage,omitempty"`
	CreatedAt         time.Time    `json:"created_at"`
	StartedAt         *time.Time   `json:"started_at,omitempty"`
	FinishedAt        *time.Time   `json:"finished_at,omitempty"`
	Slot              *Slot        `json:"slot,omitempty"`
	Outputs           []Output     `json:"outputs,omitempty"`
	History           []Transition `json:"history"`

	// Claim is set when the job came from the task queue: the broker claim this server holds, saved in the
	// journal so a restarted process can settle it at once instead of waiting out the ack wait (task_queue.mdx §9.4).
	Claim *queue.ClaimHandle `json:"claim,omitempty"`
}

// Move records a state/stage change in the history.
func (j *Job) Move(st State, sg Stage, note string) {
	j.State, j.Stage = st, sg
	j.PlatformStatus = PlatformStatus(st, sg)
	j.CallerState = CallerState(st, sg)
	j.History = append(j.History, Transition{At: time.Now().UTC(), State: st, Stage: sg, Note: note})
}

// Clone returns a deep-enough copy for handing out of the scheduler lock.
func (j *Job) Clone() *Job {
	c := *j
	c.Outputs = append([]Output(nil), j.Outputs...)
	c.History = append([]Transition(nil), j.History...)
	c.Payload.Inputs = append([]Input(nil), j.Payload.Inputs...)
	c.Payload.Outputs = append([]OutputSpec(nil), j.Payload.Outputs...)
	if j.Slot != nil {
		s := *j.Slot
		c.Slot = &s
	}
	if j.Claim != nil {
		h := *j.Claim
		c.Claim = &h
	}
	return &c
}

// Redacted strips signed query strings from every URL, for the API and logs (spec §16.1).
func (j *Job) Redacted() *Job {
	c := j.Clone()
	for i := range c.Payload.Inputs {
		c.Payload.Inputs[i].URL = logging.RedactURL(c.Payload.Inputs[i].URL)
	}
	for i := range c.Payload.Outputs {
		if u := c.Payload.Outputs[i].Upload; u != nil {
			c.Payload.Outputs[i].Upload = &Upload{Method: u.Method, URL: logging.RedactURL(u.URL)}
		}
	}
	return c
}

// ErrorKind classifies every failure once, at the runner boundary (spec §12.2).
type ErrorKind string

const (
	KindInvalidInput  ErrorKind = "invalid_input"
	KindContentPolicy ErrorKind = "content_policy"
	KindTransient     ErrorKind = "transient"
	KindTimeout       ErrorKind = "timeout"
	KindInfra         ErrorKind = "infra"
	KindInternal      ErrorKind = "internal"
	KindCancelled     ErrorKind = "cancelled"
)

// Error is a classified job failure.
type Error struct {
	Kind    ErrorKind
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Kind, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Kind, e.Message)
}

func (e *Error) Unwrap() error { return e.Err }

// Errf builds a classified error.
func Errf(kind ErrorKind, format string, args ...any) *Error {
	return &Error{Kind: kind, Message: fmt.Sprintf(format, args...)}
}

// Wrap classifies err with a human sentence.
func Wrap(kind ErrorKind, err error, msg string) *Error {
	return &Error{Kind: kind, Message: msg, Err: err}
}
