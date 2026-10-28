// Package queue is the broker seam (pm/task_queue.mdx §13): the task contract every job follows (envelope, events,
// job state, claims) and the Queue interface that is the only door between our code and the task system.
//
// The task system is NATS JetStream running as its own nats-server process (pm/Decisions.yaml D-014). Only
// queue/natsq knows that; queue/queuetest is an in-memory implementation that passes the same contract tests.
// Coordination stays ours: the scheduler decides what to pull and in which order, the queue only stores and delivers.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// EnvelopeVersion is the task envelope version this binary writes and accepts (§13.2).
const EnvelopeVersion = 1

// MaxEnvelopeBytes caps an envelope (§13.2); media never rides on the bus.
const MaxEnvelopeBytes = 256 << 10

// Errors every implementation returns for the same situations.
var (
	ErrNotFound    = errors.New("not found")
	ErrFenced      = errors.New("job_state changed since it was read (fencing token is stale)")
	ErrInvalid     = errors.New("invalid envelope")
	ErrUnavailable = errors.New("broker unavailable")
)

// Envelope is the task envelope v1 (§13.2).
type Envelope struct {
	EnvelopeVersion int             `json:"envelope_version"`
	JobID           string          `json:"job_id"`
	Caller          string          `json:"caller"`
	Category        string          `json:"category,omitempty"`
	Service         string          `json:"service"`
	Model           string          `json:"model,omitempty"`
	CallerJobType   string          `json:"caller_job_type,omitempty"`
	Priority        int             `json:"priority"`
	IssuedAt        time.Time       `json:"issued_at"`
	ExpiresAt       time.Time       `json:"expires_at"`
	Deadline        *time.Time      `json:"deadline,omitempty"`
	TimeoutSeconds  int64           `json:"timeout_seconds,omitempty"`
	MaxAttempts     int             `json:"max_attempts,omitempty"` // 0: the service catalog's retry.max_attempts; at most 10
	TraceID         string          `json:"trace_id,omitempty"`
	Tenant          map[string]any  `json:"tenant,omitempty"`
	Estimate        map[string]any  `json:"estimate,omitempty"`
	Inputs          json.RawMessage `json:"inputs,omitempty"`
	Params          json.RawMessage `json:"params,omitempty"`
	Outputs         json.RawMessage `json:"outputs,omitempty"`
	DataRules       json.RawMessage `json:"data_rules,omitempty"`
	Reply           Reply           `json:"reply"`
}

// Reply says where the caller listens.
type Reply struct {
	Events  string `json:"events"`
	Webhook string `json:"webhook,omitempty"`
}

// Normalize fills defaults and checks the rules of §13.2. now is the submission time.
func (e *Envelope) Normalize(now time.Time) error {
	if e.EnvelopeVersion == 0 {
		e.EnvelopeVersion = EnvelopeVersion
	}
	if e.EnvelopeVersion != EnvelopeVersion {
		return fmt.Errorf("%w: envelope_version %d is not supported (want %d)", ErrInvalid, e.EnvelopeVersion, EnvelopeVersion)
	}
	if !ValidToken(e.JobID, 64) {
		return fmt.Errorf("%w: job_id must be 1-64 characters of A-Z a-z 0-9 - _", ErrInvalid)
	}
	if !ValidToken(e.Caller, 32) {
		return fmt.Errorf("%w: caller must be 1-32 characters of A-Z a-z 0-9 - _", ErrInvalid)
	}
	if e.Service == "" || strings.ContainsAny(e.Service, " *>\t\r\n") {
		return fmt.Errorf("%w: service is required and may not contain spaces, * or >", ErrInvalid)
	}
	if e.IssuedAt.IsZero() {
		e.IssuedAt = now.UTC()
	}
	if e.ExpiresAt.IsZero() {
		e.ExpiresAt = e.IssuedAt.Add(24 * time.Hour)
	}
	if e.ExpiresAt.Sub(e.IssuedAt) > 7*24*time.Hour {
		return fmt.Errorf("%w: expires_at may be at most 7 days after issued_at", ErrInvalid)
	}
	if VendorModel(e.Model) || e.Category == "vendor_api" {
		return fmt.Errorf("%w: vendor API jobs (%s) run in ACT3 job_runner and appsrv, never on processing servers", ErrInvalid, e.Model)
	}
	e.Priority = ClampPriority(e.Caller, min(max(e.Priority, 0), 300))
	e.MaxAttempts = min(max(e.MaxAttempts, 0), 10)
	if e.Reply.Events == "" {
		e.Reply.Events = EventSubject(e.Caller, e.JobID)
	}
	return nil
}

// Expired reports whether the envelope may no longer start.
func (e *Envelope) Expired(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && now.After(e.ExpiresAt)
}

// ValidToken is the character rule for ids that become subject tokens and KV keys.
func ValidToken(s string, maxLen int) bool {
	if len(s) == 0 || len(s) > maxLen {
		return false
	}
	return strings.IndexFunc(s, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_')
	}) < 0
}

// Bands, highest first (§14.2).
var Bands = []string{"p0", "p1", "p2", "p3", "p4"}

// bandFloor is the lowest priority in each band (§14.2).
var bandFloor = map[string]int{"p0": 150, "p1": 75, "p2": 50, "p3": 25, "p4": 0}

// CallerCeilings is the highest band each caller may use (§14.3, decided 2026-10-27 in §24 Q4, D-021): p0 is for ACT3
// users watching a render and for the machine's own owner; WTC tops out at p1; any caller not listed at p3.
var CallerCeilings = map[string]string{"act3": "p0", "local": "p0", "wtc": "p1"}

// CeilingFor returns the highest band a caller may use.
func CeilingFor(caller string) string {
	if b, ok := CallerCeilings[caller]; ok {
		return b
	}
	return "p3"
}

// ClampBand returns band, lowered to the caller's ceiling when it is above it.
func ClampBand(caller, band string) string {
	if c := CeilingFor(caller); bandFloor[band] > bandFloor[c] {
		return c
	}
	return band
}

// ClampPriority lowers a priority to the top of the caller's ceiling band (§14.3).
func ClampPriority(caller string, priority int) int {
	c := CeilingFor(caller)
	i := slicesIndex(Bands, c)
	if i <= 0 || priority < bandFloor[Bands[i-1]] {
		return priority
	}
	return bandFloor[Bands[i-1]] - 1
}

func slicesIndex(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}

// VendorModel reports a hosted-AI model ("vendor:veo3"). Vendor API jobs stay in ACT3 job_runner and appsrv; processing
// servers never run them (task_queue.mdx §24 Q5, D-021).
func VendorModel(model string) bool { return strings.HasPrefix(model, "vendor:") }

// BandFor maps a 0..300 priority to its band (§14.2).
func BandFor(priority int) string {
	switch {
	case priority >= 150:
		return "p0"
	case priority >= 75:
		return "p1"
	case priority >= 50:
		return "p2"
	case priority >= 25:
		return "p3"
	default:
		return "p4"
	}
}

// ConsumerRef names one work consumer: one (band, caller, service) lane (§13.1).
type ConsumerRef struct {
	Band, Caller, Service string
}

// Subject is the work subject of the lane: work.<band>.<caller>.<service>.
func (c ConsumerRef) Subject() string { return "work." + c.Band + "." + c.Caller + "." + c.Service }

// Durable is the shared durable consumer name of the lane (no dots allowed in consumer names).
func (c ConsumerRef) Durable() string {
	return "w_" + c.Band + "_" + c.Caller + "_" + strings.NewReplacer(".", "_", "-", "_").Replace(c.Service)
}

// EventSubject is evt.<caller>.<job_id> (§13.1).
func EventSubject(caller, jobID string) string { return "evt." + caller + "." + jobID }

// DeadSubject is dead.<caller>.<service> (§13.1).
func DeadSubject(caller, service string) string { return "dead." + caller + "." + service }

// Caller-visible lifecycle states (§13.4).
const (
	StateSubmitted = "SUBMITTED"
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

// FinalState reports whether nothing more will happen to a job in this state.
func FinalState(s string) bool {
	switch s {
	case StateSucceeded, StateFailed, StateCancelled, StateExpired, StateRejected:
		return true
	}
	return false
}

// JobState is the job_state KV record. Its KV revision is the fencing token (§16.3).
type JobState struct {
	JobID     string    `json:"job_id"`
	Caller    string    `json:"caller"`
	Service   string    `json:"service"`
	State     string    `json:"state"`
	Band      string    `json:"band,omitempty"`
	Attempt   int       `json:"attempt"`
	Server    string    `json:"server,omitempty"`
	Kind      string    `json:"kind,omitempty"`
	Message   string    `json:"message,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Event is one lifecycle event on evt.<caller>.<job_id> (§13.5).
type Event struct {
	JobID            string          `json:"job_id"`
	Caller           string          `json:"caller"`
	Seq              int64           `json:"seq"`
	Type             string          `json:"type"`
	State            string          `json:"state"`
	Attempt          int             `json:"attempt,omitempty"`
	Server           string          `json:"server,omitempty"`
	Band             string          `json:"band,omitempty"`
	Stage            string          `json:"stage,omitempty"`
	Progress         float64         `json:"progress,omitempty"`
	Note             string          `json:"note,omitempty"`
	Kind             string          `json:"kind,omitempty"`
	Message          string          `json:"message,omitempty"`
	By               string          `json:"by,omitempty"`
	NextAttemptAfter *time.Time      `json:"next_attempt_after,omitempty"`
	Outputs          json.RawMessage `json:"outputs,omitempty"`
	Result           json.RawMessage `json:"result,omitempty"`
	At               time.Time       `json:"at"`
}

// Final reports whether this event ends the job for the caller.
func (e *Event) Final() bool {
	switch e.Type {
	case "succeeded", "failed", "cancelled", "expired", "rejected":
		return true
	}
	return false
}

// SubmitAck is returned once the job is durably stored.
type SubmitAck struct {
	JobID     string `json:"job_id"`
	Band      string `json:"band"`
	Subject   string `json:"subject"`
	Sequence  uint64 `json:"sequence"`
	Duplicate bool   `json:"duplicate"`
}

// Control is the job_control record for one job.
type Control struct {
	Cancel bool   `json:"cancel,omitempty"`
	Pause  bool   `json:"pause,omitempty"`
	By     string `json:"by,omitempty"`
}

// ControlChange is one change seen by WatchControl.
type ControlChange struct {
	JobID string
	Control
}

// ServerRecord is a worker's capability record in the servers KV (§12), refreshed well inside its TTL.
type ServerRecord struct {
	Key       string         `json:"key"`
	Role      string         `json:"role"`
	Version   string         `json:"version"`
	State     string         `json:"state"`
	Offers    map[string]int `json:"offers"` // service → parallel slots offered
	Free      map[string]int `json:"free"`   // service → slots free now
	UpdatedAt time.Time      `json:"updated_at"`
}

// Status describes the broker connection for /v1/health and /v1/queue.
type Status struct {
	Kind          string            `json:"kind"` // nats | memory
	URL           string            `json:"url,omitempty"`
	Connected     bool              `json:"connected"`
	ServerVersion string            `json:"server_version,omitempty"`
	JetStream     bool              `json:"jetstream"`
	Streams       map[string]uint64 `json:"streams,omitempty"` // stream → messages stored
	Waiting       map[string]uint64 `json:"waiting,omitempty"` // work subject → messages (queued or claimed)
	Error         string            `json:"error,omitempty"`
}

// Queue is the only door between our code and the task system (§13.7).
type Queue interface {
	// Submit stores a job (normalized envelope) and returns after it is durable. Re-submitting a job_id inside the
	// dedupe window is a no-op that reports Duplicate.
	Submit(ctx context.Context, env *Envelope) (SubmitAck, error)

	// Pull fetches at most one job from the first lane in order that has work. The scheduler computes the order
	// (§14.4); the queue never decides priority. With wait 0 it does not block. No work is (nil, nil).
	Pull(ctx context.Context, order []ConsumerRef, wait time.Duration) (Delivery, error)

	// Publish writes an event for the caller. Final events are written before Ack.
	Publish(ctx context.Context, ev *Event) error

	// Events reads every stored event of one job, in order.
	Events(ctx context.Context, caller, jobID string) ([]Event, error)

	// State reads job_state; a job never written is (JobState{}, 0, ErrNotFound). CASState writes it only if the
	// revision is still rev (0 = create), returning the new revision, the fencing token (§16.3).
	State(ctx context.Context, jobID string) (JobState, uint64, error)
	CASState(ctx context.Context, s JobState, rev uint64) (uint64, error)

	// SetControl and WatchControl carry cancel and pause flags for live jobs. jobIDs nil watches every job.
	SetControl(ctx context.Context, jobID string, c Control) error
	GetControl(ctx context.Context, jobID string) (Control, error)
	WatchControl(ctx context.Context, jobIDs []string) (<-chan ControlChange, error)

	// PutServer writes this server's capability record; Servers reads the fleet's live records.
	PutServer(ctx context.Context, rec ServerRecord) error
	Servers(ctx context.Context) ([]ServerRecord, error)

	// Resume rebuilds a Delivery from a handle a previous process saved, so the claim is kept (or released at
	// once) without waiting for the ack wait to expire (§9.4).
	Resume(ctx context.Context, h ClaimHandle) (Delivery, error)

	// Status reports the broker connection; it never blocks longer than ctx.
	Status(ctx context.Context) Status

	Close() error
}

// ClaimHandle is everything needed to keep holding a job after a restart: for NATS, the ack subject and the
// fencing revision. It is stored on disk (the job journal), never only in memory.
type ClaimHandle struct {
	JobID      string `json:"job_id"`
	Caller     string `json:"caller"`
	Service    string `json:"service"`
	AckSubject string `json:"ack_subject"`
	FenceRev   uint64 `json:"fence_rev"`
	Attempt    int    `json:"attempt"`
}

// Delivery is one job held by this worker. Exactly one of Ack, Requeue, Dead must be called.
type Delivery interface {
	Envelope() *Envelope
	Attempt() int
	InProgress(ctx context.Context) error                   // extends the ack wait; called every heartbeat
	Ack(ctx context.Context) error                          // done: succeeded, failed for good, or cancelled
	Requeue(ctx context.Context, delay time.Duration) error // let a worker try again after delay
	Dead(ctx context.Context, reason string) error          // stop delivering; copy to DEAD
	Handle() ClaimHandle                                    // saved before the job starts
	SetFence(rev uint64)                                    // record the fencing revision in the handle
}
