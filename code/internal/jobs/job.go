// Package jobs defines a job as this server keeps and reports it: the task envelope it was submitted with, its
// state, error kinds, outputs, and the on-disk journal (spec §9, §12).
//
// The record is a projection of the task queue's own data (pm/task_queue.mdx §13, D-Q1): the envelope is the
// broker message, the state names and fields are the job_state KV and the events callers read on
// evt.<caller>.<job_id>. The local API passes them through with no renames (D-008).
package jobs

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/logging"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

// State is the lifecycle state callers see (pm/task_queue.mdx §13.4).
type State string

const (
	Submitted State = queue.StateSubmitted // stored on SUBMIT, waiting for admission
	Admitted  State = queue.StateAdmitted  // passed admission (momentary; then QUEUED)
	Queued    State = queue.StateQueued    // on WORK in a band, waiting for a worker with a free slot
	Claimed   State = queue.StateClaimed   // a worker holds it; its task host is starting
	Staging   State = queue.StateStaging   // fetching inputs
	Running   State = queue.StateRunning   // the runner is working
	Uploading State = queue.StateUploading // delivering outputs
	Succeeded State = queue.StateSucceeded
	Failed    State = queue.StateFailed
	Cancelled State = queue.StateCancelled
	Expired   State = queue.StateExpired
	Rejected  State = queue.StateRejected
)

// Final reports whether no more work will happen for this job.
func (s State) Final() bool { return queue.FinalState(string(s)) }

// Holding reports whether the job holds a slot on this machine.
func (s State) Holding() bool { return s == Claimed || s == Staging || s == Running || s == Uploading }

// Waiting reports whether the job is in the queue, not yet on a machine.
func (s State) Waiting() bool { return s == Submitted || s == Admitted || s == Queued }

// PlatformStatus maps to ACT3's JobStatus (spec §9.2 table).
func PlatformStatus(st State) string {
	switch {
	case st.Waiting():
		return "INQUEUE"
	case st.Holding():
		return "PROGRESS"
	case st == Succeeded:
		return "COMPLETED"
	case st == Cancelled:
		return "CANCELLED"
	default:
		return "ERROR"
	}
}

// The envelope types live in package queue; these names keep runners and storage readable.
type (
	Envelope   = queue.Envelope
	Input      = queue.Input
	OutputSpec = queue.OutputSpec
	Upload     = queue.Upload
	DataRules  = queue.DataRules
)

// Output is one produced file, as reported in the job record and the `succeeded` event.
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
	At      time.Time `json:"at"`
	State   State     `json:"state"`
	Attempt int       `json:"attempt,omitempty"`
	Note    string    `json:"note,omitempty"`
}

// Job is the record this server keeps for every job it accepted or claimed, and what the local API returns.
type Job struct {
	ID          string `json:"job_id"`
	Caller      string `json:"caller"`
	Category    string `json:"category"`
	Service     string `json:"service"`
	Model       string `json:"model,omitempty"`
	Priority    int    `json:"priority"`
	Band        string `json:"band,omitempty"`
	State       State  `json:"state"`
	Attempt     int    `json:"attempt"`      // deliveries so far (1 while the first attempt runs)
	MaxAttempts int    `json:"max_attempts"` // from the envelope, else the service catalog
	Server      string `json:"server,omitempty"`

	PlatformStatus string  `json:"platform_status"`
	Step           string  `json:"step,omitempty"` // the runner's own step: compute:primes, render, encode
	Progress       float64 `json:"progress"`
	Note           string  `json:"note,omitempty"`

	ErrorKind    ErrorKind `json:"error_kind,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
	ErrorStage   State     `json:"error_stage,omitempty"`

	CreatedAt        time.Time  `json:"created_at"`
	StartedAt        *time.Time `json:"started_at,omitempty"`
	FinishedAt       *time.Time `json:"finished_at,omitempty"`
	NextAttemptAfter *time.Time `json:"next_attempt_after,omitempty"`
	Deadline         *time.Time `json:"deadline,omitempty"`
	TimeoutSeconds   int64      `json:"timeout_seconds"`

	CodeVersion string          `json:"code_version,omitempty"` // the snapshot that ran it (§9.2)
	Outputs     []Output        `json:"outputs,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	Seq         int64           `json:"events_seq"` // last event seq published for this job
	Slot        *Slot           `json:"slot,omitempty"`
	Envelope    Envelope        `json:"envelope"`
	History     []Transition    `json:"history"`
}

// Move records a state change in the history.
func (j *Job) Move(st State, note string) {
	j.State = st
	j.PlatformStatus = PlatformStatus(st)
	j.History = append(j.History, Transition{At: time.Now().UTC(), State: st, Attempt: j.Attempt, Note: note})
}

// Clone returns a deep-enough copy for handing out of the scheduler lock.
func (j *Job) Clone() *Job {
	c := *j
	c.Outputs = append([]Output(nil), j.Outputs...)
	c.History = append([]Transition(nil), j.History...)
	c.Envelope.Inputs = append([]Input(nil), j.Envelope.Inputs...)
	c.Envelope.Outputs = append([]OutputSpec(nil), j.Envelope.Outputs...)
	if j.Slot != nil {
		s := *j.Slot
		c.Slot = &s
	}
	return &c
}

// Redacted strips signed query strings from every URL, for the API and logs (spec §16.1).
func (j *Job) Redacted() *Job {
	c := j.Clone()
	RedactEnvelope(&c.Envelope)
	return c
}

// RedactEnvelope strips signed query strings from an envelope's URLs in place.
func RedactEnvelope(e *Envelope) {
	for i := range e.Inputs {
		e.Inputs[i].URL = logging.RedactURL(e.Inputs[i].URL)
	}
	for i := range e.Outputs {
		if u := e.Outputs[i].Upload; u != nil {
			e.Outputs[i].Upload = &Upload{Method: u.Method, URL: logging.RedactURL(u.URL)}
		}
	}
}

// ErrorKind classifies every failure once, at the runner boundary (spec §12.2, pm/task_queue.mdx §13.6).
type ErrorKind string

const (
	KindInvalidInput  ErrorKind = "invalid_input"
	KindContentPolicy ErrorKind = "content_policy"
	KindTransient     ErrorKind = "transient"
	KindTimeout       ErrorKind = "timeout"
	KindInfra         ErrorKind = "infra"
	KindInternal      ErrorKind = "internal"
	KindCancelled     ErrorKind = "cancelled"
	KindExpired       ErrorKind = "expired"
	// Admission rejections (§13.6).
	KindNotAllowed   ErrorKind = "caller_not_allowed"
	KindNoCapable    ErrorKind = "no_capable_server"
	KindBadSignature ErrorKind = "bad_signature"
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
