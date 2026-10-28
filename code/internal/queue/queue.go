// Package queue is the only door between our code and the task system (pm/task_queue.mdx §13.7). Everything above
// it (admission, the scheduler, the local API) uses these types; only queue/natsq knows NATS JetStream exists, and
// queue/queuetest is an in-memory implementation that passes the same contract tests.
//
// The broker owns transport and durability: storing jobs, one delivery per worker, redelivery, KV with
// compare-and-set. Our code owns coordination: admission, priority, slots, retry policy (§8.3).
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// EnvelopeVersion is the task envelope this binary writes and reads (data/schemas/task_envelope.v1.schema.json).
const EnvelopeVersion = 1

// Errors every implementation returns for the same situations.
var (
	ErrNotFound     = errors.New("not found")
	ErrExists       = errors.New("already exists")
	ErrRevision     = errors.New("revision moved: another claim owns this job") // fencing failed (§16.3)
	ErrNoWork       = errors.New("no work")
	ErrBrokerAbsent = errors.New("broker not reachable")
)

// Envelope is one job as callers submit it (§13.2). Media never rides on the bus: inputs and outputs are URLs
// (or local paths, for the local caller only).
type Envelope struct {
	EnvelopeVersion int             `json:"envelope_version"`
	JobID           string          `json:"job_id"`
	Caller          string          `json:"caller"`
	Category        string          `json:"category"`
	Service         string          `json:"service"`
	Model           string          `json:"model,omitempty"`
	CallerJobType   string          `json:"caller_job_type,omitempty"`
	Priority        int             `json:"priority"`
	IssuedAt        time.Time       `json:"issued_at"`
	ExpiresAt       time.Time       `json:"expires_at"`
	Deadline        *time.Time      `json:"deadline,omitempty"`
	TraceID         string          `json:"trace_id,omitempty"`
	Tenant          map[string]any  `json:"tenant,omitempty"`
	Estimate        map[string]any  `json:"estimate,omitempty"`
	TimeoutSeconds  int64           `json:"timeout_seconds,omitempty"` // 0: the service catalog's timeouts.run
	MaxAttempts     int             `json:"max_attempts,omitempty"`    // 0: the service catalog's retry.max_attempts
	Inputs          []Input         `json:"inputs,omitempty"`
	Params          json.RawMessage `json:"params,omitempty"`
	Outputs         []OutputSpec    `json:"outputs,omitempty"`
	DataRules       DataRules       `json:"data_rules"`
	Reply           Reply           `json:"reply"`
}

// Input is a file the job needs. URL is http(s); Path is a local file (caller `local` only).
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

type Reply struct {
	Events  string  `json:"events"`
	Webhook *string `json:"webhook"`
}

// Validate checks what every implementation and admission rely on.
func (e *Envelope) Validate() error {
	switch {
	case e.EnvelopeVersion != EnvelopeVersion:
		return fmt.Errorf("envelope_version %d is not supported (want %d)", e.EnvelopeVersion, EnvelopeVersion)
	case !ValidToken(e.JobID, 64):
		return errors.New("job_id must be 1-64 characters of A-Z a-z 0-9 - _")
	case !ValidToken(e.Caller, 32):
		return errors.New("caller must be 1-32 characters of A-Z a-z 0-9 - _")
	case e.Service == "" || strings.ContainsAny(e.Service, " *>"):
		return errors.New("service is required and may not contain spaces, * or >")
	case e.IssuedAt.IsZero() || e.ExpiresAt.IsZero():
		return errors.New("issued_at and expires_at are required")
	case e.ExpiresAt.Sub(e.IssuedAt) > 7*24*time.Hour:
		return errors.New("expires_at may be at most 7 days after issued_at")
	}
	return nil
}

// ValidToken reports whether s is 1..max characters of A-Z a-z 0-9 - _ (safe as a subject token and a KV key).
func ValidToken(s string, max int) bool {
	if len(s) == 0 || len(s) > max {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

// States as callers see them (§13.4).
const (
	StateSubmitted = "SUBMITTED"
	StateAdmitted  = "ADMITTED"
	StateQueued    = "QUEUED"
	StateClaimed   = "CLAIMED"
	StateStaging   = "STAGING"
	StateRunning   = "RUNNING"
	StateUploading = "UPLOADING"
	StateSucceeded = "SUCCEEDED"
	StateFailed    = "FAILED"
	StateCancelled = "CANCELLED"
	StateExpired   = "EXPIRED"
	StateRejected  = "REJECTED"
)

// FinalState reports whether no more work will happen for a job in state s.
func FinalState(s string) bool {
	switch s {
	case StateSucceeded, StateFailed, StateCancelled, StateExpired, StateRejected:
		return true
	}
	return false
}

// Event types (§13.5).
const (
	EvAdmitted  = "admitted"
	EvRejected  = "rejected"
	EvClaimed   = "claimed"
	EvStage     = "stage"
	EvProgress  = "progress"
	EvRequeued  = "requeued"
	EvSucceeded = "succeeded"
	EvFailed    = "failed"
	EvCancelled = "cancelled"
	EvExpired   = "expired"
)

// Event is one lifecycle message on evt.<caller>.<job_id>. Callers dedupe by (job_id, seq): delivery is at least once.
type Event struct {
	JobID            string             `json:"job_id"`
	Caller           string             `json:"caller"`
	Seq              int64              `json:"seq"`
	Type             string             `json:"type"`
	State            string             `json:"state"`
	Attempt          int                `json:"attempt,omitempty"`
	Server           string             `json:"server,omitempty"`
	Band             string             `json:"band,omitempty"`
	Stage            string             `json:"stage,omitempty"`
	Step             string             `json:"step,omitempty"`
	Progress         *float64           `json:"progress,omitempty"`
	Note             string             `json:"note,omitempty"`
	Reason           string             `json:"reason,omitempty"`
	Kind             string             `json:"kind,omitempty"`
	Message          string             `json:"message,omitempty"`
	NextAttemptAfter *time.Time         `json:"next_attempt_after,omitempty"`
	Outputs          json.RawMessage    `json:"outputs,omitempty"`
	Result           json.RawMessage    `json:"result,omitempty"`
	Timings          map[string]float64 `json:"timings,omitempty"`
	CodeVersion      string             `json:"code_version,omitempty"`
	By               string             `json:"by,omitempty"`
	Detail           string             `json:"detail,omitempty"`
	At               time.Time          `json:"at"`
}

// Final reports whether this event ends the job.
func (e *Event) Final() bool {
	switch e.Type {
	case EvRejected, EvSucceeded, EvFailed, EvCancelled, EvExpired:
		return true
	}
	return false
}

// JobState is the job_state KV value. Its KV revision is the fencing token (§16.3).
type JobState struct {
	JobID       string    `json:"job_id"`
	Caller      string    `json:"caller"`
	Service     string    `json:"service"`
	State       string    `json:"state"`
	Band        string    `json:"band,omitempty"`
	Attempt     int       `json:"attempt"`
	Server      string    `json:"server,omitempty"`
	CodeVersion string    `json:"code_version,omitempty"`
	Reason      string    `json:"reason,omitempty"`
	Seq         int64     `json:"seq"` // last event seq published, so the next holder continues the sequence
	UpdatedAt   time.Time `json:"updated_at"`
}

// Control is the job_control KV value: flags a caller or the owner sets on a live job (§16.5).
type Control struct {
	Cancel bool      `json:"cancel"`
	By     string    `json:"by,omitempty"`
	At     time.Time `json:"at"`
}

// ControlChange is one update seen by WatchControl.
type ControlChange struct {
	JobID   string
	Control Control
}

// ServerRecord is one machine's entry in the `servers` KV (§12.1).
type ServerRecord struct {
	Key         string         `json:"key"`
	Role        string         `json:"role"`
	Version     string         `json:"version"`
	CodeVersion string         `json:"code_version"`
	State       string         `json:"state"`
	Services    []ServiceOffer `json:"services"`
	At          time.Time      `json:"at"`
}

type ServiceOffer struct {
	Service  string `json:"service"`
	Category string `json:"category"`
	Parallel int    `json:"parallel"`
	Running  int    `json:"running"`
}

// ConsumerRef names one work consumer: one per (band, caller, service) (§13.1).
type ConsumerRef struct {
	Band    string `json:"band"`
	Caller  string `json:"caller"`
	Service string `json:"service"`
}

// Subject is the work subject this consumer reads.
func (c ConsumerRef) Subject() string { return WorkSubject(c.Band, c.Caller, c.Service) }

// Name is the durable consumer name (subject tokens joined by _, since names may not hold dots).
func (c ConsumerRef) Name() string {
	return "w_" + c.Band + "_" + c.Caller + "_" + strings.ReplaceAll(c.Service, ".", "_")
}

// Subject helpers (§13.1).
func SubmitSubject(caller, category string) string { return "submit." + caller + "." + category }
func WorkSubject(band, caller, service string) string {
	return "work." + band + "." + caller + "." + service
}
func EventSubject(caller, jobID string) string    { return "evt." + caller + "." + jobID }
func DeadSubject(caller, service string) string   { return "dead." + caller + "." + service }

// ParseWorkSubject splits work.<band>.<caller>.<service> (the service id itself holds dots).
func ParseWorkSubject(subj string) (ConsumerRef, bool) {
	parts := strings.SplitN(subj, ".", 4)
	if len(parts) != 4 || parts[0] != "work" {
		return ConsumerRef{}, false
	}
	return ConsumerRef{Band: parts[1], Caller: parts[2], Service: parts[3]}, true
}

// SubmitAck says the job is durably stored.
type SubmitAck struct {
	Stream    string `json:"stream"`
	Seq       uint64 `json:"seq"`
	Duplicate bool   `json:"duplicate"`
}

// ClaimHandle is everything needed to keep holding a job after a controller restart: for NATS, the ack subject.
// It is stored on disk in run.json, never only in memory (§9.4).
type ClaimHandle struct {
	JobID      string `json:"job_id"`
	AckSubject string `json:"ack_subject"`
	FenceRev   uint64 `json:"fence_rev"`
	Attempt    int    `json:"attempt"`
}

// Submission is one raw job on SUBMIT waiting for admission. Exactly one of Ack, Term must be called.
type Submission interface {
	Envelope() (*Envelope, error) // a decode error means the message is garbage: Term it
	Ack(ctx context.Context) error
	Term(ctx context.Context) error
}

// Delivery is one job held by this worker. Exactly one of Ack, Requeue, Dead must be called.
type Delivery interface {
	Envelope() *Envelope
	Attempt() int
	InProgress(ctx context.Context) error                   // extends the ack wait; called every heartbeat
	Ack(ctx context.Context) error                          // done (succeeded, failed for good, or cancelled)
	Requeue(ctx context.Context, delay time.Duration) error // let a worker try again after delay
	Dead(ctx context.Context, reason string) error          // stop delivering; copy to DEAD
	Handle() ClaimHandle                                    // saved to run.json before the task host starts
}

// Queue is the broker seam.
type Queue interface {
	// Submit stores a job on SUBMIT. Re-submitting the same job_id inside the dedupe window is a no-op.
	Submit(ctx context.Context, env *Envelope) (SubmitAck, error)
	// NextSubmission is admission's side of SUBMIT: at most one raw job, waiting at most wait.
	NextSubmission(ctx context.Context, wait time.Duration) (Submission, error)
	// Enqueue puts an admitted job on work.<band>.<caller>.<service>.
	Enqueue(ctx context.Context, env *Envelope, band string) error

	// Pull fetches at most one job from the first consumer in order that has work. The scheduler computes the order
	// (§14.4); the queue never decides priority. ErrNoWork when every consumer is empty.
	Pull(ctx context.Context, order []ConsumerRef, wait time.Duration) (Delivery, error)
	// Depth is the number of messages stored per work consumer, including ones held by a worker.
	Depth(ctx context.Context) (map[ConsumerRef]int, error)
	// Resume rebuilds a Delivery from the handle a previous controller saved, keeping the same claim (§9.4).
	Resume(ctx context.Context, h ClaimHandle, env *Envelope) (Delivery, error)

	// Publish writes an event for the caller. Final events are written before Ack.
	Publish(ctx context.Context, ev *Event) error
	// Events reads a job's events in order (the local API's view of the event stream).
	Events(ctx context.Context, caller, jobID string) ([]Event, error)

	// CreateState creates job_state create-only (idempotent submission, §16.4). ErrExists if present.
	CreateState(ctx context.Context, s JobState) (uint64, error)
	// State reads job_state; the revision is the fencing token.
	State(ctx context.Context, jobID string) (JobState, uint64, error)
	// CASState writes job_state only if its revision is still rev. ErrRevision otherwise.
	CASState(ctx context.Context, s JobState, rev uint64) (uint64, error)

	// SetControl and WatchControl carry cancel requests to whoever holds a job.
	SetControl(ctx context.Context, jobID string, c Control) error
	GetControl(ctx context.Context, jobID string) (Control, error)
	WatchControl(ctx context.Context) (<-chan ControlChange, error)

	// PutServer writes this server's capability record; Servers reads the fleet's.
	PutServer(ctx context.Context, rec ServerRecord) error
	Servers(ctx context.Context) ([]ServerRecord, error)

	// Info describes the broker for health and status.
	Info() BrokerInfo
	Close() error
}

// BrokerInfo is what health reports about the broker.
type BrokerInfo struct {
	Kind      string `json:"kind"` // "nats" | "memory"
	URL       string `json:"url,omitempty"`
	Connected bool   `json:"connected"`
	Version   string `json:"version,omitempty"`
}
