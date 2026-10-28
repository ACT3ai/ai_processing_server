// Package queuetest is an in-memory queue.Queue for unit tests of the scheduler and admission (pm/task_queue.mdx
// §19, acceptance Q8). It keeps the broker's semantics that our code relies on: one delivery at a time per job,
// redelivery after the ack wait, delayed requeue, message-id dedupe, create-only and compare-and-set KV, and claims
// held through an ack subject that survives a "restart" (Resume).
package queuetest

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

// Mem is the in-memory queue. The zero value is not usable; call New.
type Mem struct {
	mu       sync.Mutex
	ackWait  time.Duration
	seq      uint64
	msgIDs   map[string]bool
	submit   []*msg
	work     map[queue.ConsumerRef][]*msg
	byAck    map[string]*msg
	dead     []*msg
	events   map[string][]queue.Event
	evIDs    map[string]bool
	state    map[string]kvEntry
	control  map[string]queue.Control
	servers  map[string]queue.ServerRecord
	watchers []chan queue.ControlChange
	kvRev    uint64
}

type kvEntry struct {
	val queue.JobState
	rev uint64
}

type msg struct {
	env       queue.Envelope
	ref       queue.ConsumerRef
	delivered int
	availAt   time.Time
	heldTil   time.Time // zero: not held
	ack       string
	done      bool
}

var _ queue.Queue = (*Mem)(nil)

// New returns an empty queue with the given ack wait (redelivery after a silent worker).
func New(ackWait time.Duration) *Mem {
	return &Mem{ackWait: ackWait, msgIDs: map[string]bool{}, work: map[queue.ConsumerRef][]*msg{}, byAck: map[string]*msg{},
		events: map[string][]queue.Event{}, evIDs: map[string]bool{}, state: map[string]kvEntry{}, control: map[string]queue.Control{},
		servers: map[string]queue.ServerRecord{}}
}

func (m *Mem) Info() queue.BrokerInfo { return queue.BrokerInfo{Kind: "memory", Connected: true} }
func (m *Mem) Close() error           { return nil }

func (m *Mem) Submit(_ context.Context, env *queue.Envelope) (queue.SubmitAck, error) {
	if err := env.Validate(); err != nil {
		return queue.SubmitAck{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.msgIDs["s:"+env.JobID] {
		return queue.SubmitAck{Stream: "SUBMIT", Duplicate: true}, nil
	}
	m.msgIDs["s:"+env.JobID] = true
	m.seq++
	m.submit = append(m.submit, &msg{env: *env})
	return queue.SubmitAck{Stream: "SUBMIT", Seq: m.seq}, nil
}

type sub struct {
	m   *Mem
	msg *msg
}

func (s sub) Envelope() (*queue.Envelope, error) { e := s.msg.env; return &e, nil }
func (s sub) Ack(context.Context) error          { s.m.dropSubmit(s.msg); return nil }
func (s sub) Term(context.Context) error         { s.m.dropSubmit(s.msg); return nil }

func (m *Mem) dropSubmit(x *msg) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, y := range m.submit {
		if y == x {
			m.submit = append(m.submit[:i], m.submit[i+1:]...)
			return
		}
	}
}

func (m *Mem) NextSubmission(ctx context.Context, wait time.Duration) (queue.Submission, error) {
	deadline := time.Now().Add(wait)
	for {
		m.mu.Lock()
		now := time.Now()
		for _, x := range m.submit {
			if x.heldTil.Before(now) {
				x.heldTil = now.Add(30 * time.Second)
				m.mu.Unlock()
				return sub{m, x}, nil
			}
		}
		m.mu.Unlock()
		if time.Now().After(deadline) {
			return nil, queue.ErrNoWork
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (m *Mem) Enqueue(_ context.Context, env *queue.Envelope, band string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.msgIDs["w:"+env.JobID] {
		return nil
	}
	m.msgIDs["w:"+env.JobID] = true
	ref := queue.ConsumerRef{Band: band, Caller: env.Caller, Service: env.Service}
	m.work[ref] = append(m.work[ref], &msg{env: *env, ref: ref})
	return nil
}

func (m *Mem) Depth(context.Context) (map[queue.ConsumerRef]int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[queue.ConsumerRef]int{}
	for ref, l := range m.work {
		if len(l) > 0 {
			out[ref] = len(l)
		}
	}
	return out, nil
}

func (m *Mem) Pull(_ context.Context, order []queue.ConsumerRef, _ time.Duration) (queue.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for _, ref := range order {
		for _, x := range m.work[ref] {
			if x.availAt.After(now) || (!x.heldTil.IsZero() && x.heldTil.After(now)) {
				continue
			}
			x.delivered++
			x.heldTil = now.Add(m.ackWait)
			m.seq++
			x.ack = fmt.Sprintf("$JS.ACK.MEM.%s.%d.%d", ref.Name(), x.delivered, m.seq)
			m.byAck[x.ack] = x
			e := x.env
			return &del{m: m, env: &e, h: queue.ClaimHandle{JobID: e.JobID, AckSubject: x.ack, Attempt: x.delivered}}, nil
		}
	}
	return nil, queue.ErrNoWork
}

func (m *Mem) Resume(_ context.Context, h queue.ClaimHandle, env *queue.Envelope) (queue.Delivery, error) {
	if !strings.HasPrefix(h.AckSubject, "$JS.ACK.") {
		return nil, fmt.Errorf("claim handle for %s has no ack subject", h.JobID)
	}
	return &del{m: m, env: env, h: h}, nil
}

type del struct {
	m   *Mem
	env *queue.Envelope
	h   queue.ClaimHandle
}

func (d *del) Envelope() *queue.Envelope { return d.env }
func (d *del) Attempt() int              { return d.h.Attempt }
func (d *del) Handle() queue.ClaimHandle { return d.h }

// live returns the message if this ack subject still holds the claim (a stale ack is ignored, as JetStream does).
func (d *del) live() *msg {
	x := d.m.byAck[d.h.AckSubject]
	if x == nil || x.done || x.ack != d.h.AckSubject {
		return nil
	}
	return x
}

func (d *del) InProgress(context.Context) error {
	d.m.mu.Lock()
	defer d.m.mu.Unlock()
	if x := d.live(); x != nil {
		x.heldTil = time.Now().Add(d.m.ackWait)
	}
	return nil
}

func (d *del) remove(x *msg) {
	x.done = true
	l := d.m.work[x.ref]
	for i, y := range l {
		if y == x {
			d.m.work[x.ref] = append(l[:i], l[i+1:]...)
			break
		}
	}
}

func (d *del) Ack(context.Context) error {
	d.m.mu.Lock()
	defer d.m.mu.Unlock()
	if x := d.live(); x != nil {
		d.remove(x)
	}
	return nil
}

func (d *del) Requeue(_ context.Context, delay time.Duration) error {
	d.m.mu.Lock()
	defer d.m.mu.Unlock()
	if x := d.live(); x != nil {
		x.heldTil = time.Time{}
		x.availAt = time.Now().Add(delay)
		x.ack = ""
	}
	return nil
}

func (d *del) Dead(context.Context, string) error {
	d.m.mu.Lock()
	defer d.m.mu.Unlock()
	if x := d.live(); x != nil {
		d.remove(x)
		d.m.dead = append(d.m.dead, x)
	}
	return nil
}

// Dead returns the job ids copied to DEAD, for tests.
func (m *Mem) DeadJobs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, x := range m.dead {
		out = append(out, x.env.JobID)
	}
	return out
}

func (m *Mem) Publish(_ context.Context, ev *queue.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := fmt.Sprintf("%s.%d", ev.JobID, ev.Seq)
	if m.evIDs[id] {
		return nil
	}
	m.evIDs[id] = true
	k := queue.EventSubject(ev.Caller, ev.JobID)
	m.events[k] = append(m.events[k], *ev)
	return nil
}

func (m *Mem) Events(_ context.Context, caller, jobID string) ([]queue.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]queue.Event{}, m.events[queue.EventSubject(caller, jobID)]...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

func (m *Mem) CreateState(_ context.Context, s queue.JobState) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.state[s.JobID]; ok {
		return 0, queue.ErrExists
	}
	m.kvRev++
	s.UpdatedAt = time.Now().UTC()
	m.state[s.JobID] = kvEntry{s, m.kvRev}
	return m.kvRev, nil
}

func (m *Mem) State(_ context.Context, id string) (queue.JobState, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.state[id]
	if !ok {
		return queue.JobState{}, 0, queue.ErrNotFound
	}
	return e.val, e.rev, nil
}

func (m *Mem) CASState(_ context.Context, s queue.JobState, rev uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.state[s.JobID]; !ok || e.rev != rev {
		return 0, queue.ErrRevision
	}
	m.kvRev++
	s.UpdatedAt = time.Now().UTC()
	m.state[s.JobID] = kvEntry{s, m.kvRev}
	return m.kvRev, nil
}

func (m *Mem) SetControl(_ context.Context, id string, c queue.Control) error {
	m.mu.Lock()
	m.control[id] = c
	ws := append([]chan queue.ControlChange{}, m.watchers...)
	m.mu.Unlock()
	for _, w := range ws {
		select {
		case w <- queue.ControlChange{JobID: id, Control: c}:
		default:
		}
	}
	return nil
}

func (m *Mem) GetControl(_ context.Context, id string) (queue.Control, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.control[id]
	if !ok {
		return queue.Control{}, queue.ErrNotFound
	}
	return c, nil
}

func (m *Mem) WatchControl(ctx context.Context) (<-chan queue.ControlChange, error) {
	ch := make(chan queue.ControlChange, 64)
	m.mu.Lock()
	m.watchers = append(m.watchers, ch)
	m.mu.Unlock()
	go func() {
		<-ctx.Done()
		m.mu.Lock()
		defer m.mu.Unlock()
		for i, w := range m.watchers {
			if w == ch {
				m.watchers = append(m.watchers[:i], m.watchers[i+1:]...)
				close(ch)
				return
			}
		}
	}()
	return ch, nil
}

func (m *Mem) PutServer(_ context.Context, rec queue.ServerRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.servers[rec.Key] = rec
	return nil
}

func (m *Mem) Servers(context.Context) ([]queue.ServerRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []queue.ServerRecord{}
	for _, r := range m.servers {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// Snapshot is the queue's content as JSON, for debugging a failed test.
func (m *Mem) Snapshot() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, _ := json.MarshalIndent(map[string]any{"submit": len(m.submit), "work": len(m.work), "state": m.state}, "", "  ")
	return string(b)
}
