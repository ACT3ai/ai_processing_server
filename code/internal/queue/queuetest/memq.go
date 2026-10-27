// Package queuetest holds an in-memory queue.Queue and the contract tests every implementation must pass
// (pm/task_queue.mdx §21.3 Q8). The in-memory queue has no ack wait: a claim lasts until it is settled.
package queuetest

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

type msg struct {
	seq       uint64
	ref       queue.ConsumerRef
	env       queue.Envelope
	delivered int
	held      bool
	notBefore time.Time
}

type kvEntry struct {
	val []byte
	rev uint64
}

// Mem is the in-memory queue.
type Mem struct {
	mu       sync.Mutex
	seq      uint64
	work     []*msg
	ids      map[string]bool
	events   map[string][]queue.Event
	eventIDs map[string]bool
	dead     []queue.Envelope
	state    map[string]kvEntry
	control  map[string]queue.Control
	servers  map[string]queue.ServerRecord
	watchers []chan queue.ControlChange
	rev      uint64
}

// NewMem returns an empty in-memory queue.
func NewMem() *Mem {
	return &Mem{ids: map[string]bool{}, events: map[string][]queue.Event{}, eventIDs: map[string]bool{},
		state: map[string]kvEntry{}, control: map[string]queue.Control{}, servers: map[string]queue.ServerRecord{}}
}

func (m *Mem) Submit(_ context.Context, env *queue.Envelope) (queue.SubmitAck, error) {
	if err := env.Normalize(time.Now()); err != nil {
		return queue.SubmitAck{}, err
	}
	ref := queue.ConsumerRef{Band: queue.BandFor(env.Priority), Caller: env.Caller, Service: env.Service}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ids[env.JobID] {
		return queue.SubmitAck{JobID: env.JobID, Band: ref.Band, Subject: ref.Subject(), Duplicate: true}, nil
	}
	m.seq++
	m.ids[env.JobID] = true
	m.work = append(m.work, &msg{seq: m.seq, ref: ref, env: *env})
	return queue.SubmitAck{JobID: env.JobID, Band: ref.Band, Subject: ref.Subject(), Sequence: m.seq}, nil
}

func (m *Mem) Pull(ctx context.Context, order []queue.ConsumerRef, wait time.Duration) (queue.Delivery, error) {
	deadline := time.Now().Add(wait)
	for {
		if d := m.pullOnce(order); d != nil {
			return d, nil
		}
		if wait <= 0 || time.Now().After(deadline) {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (m *Mem) pullOnce(order []queue.ConsumerRef) queue.Delivery {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for _, ref := range order {
		for _, w := range m.work {
			if w.ref == ref && !w.held && !now.Before(w.notBefore) {
				w.held = true
				w.delivered++
				env := w.env
				return &memDelivery{m: m, w: w, env: &env, h: queue.ClaimHandle{JobID: env.JobID, Caller: env.Caller,
					Service: env.Service, AckSubject: "mem." + strconv.FormatUint(w.seq, 10), Attempt: w.delivered}}
			}
		}
	}
	return nil
}

func (m *Mem) Publish(_ context.Context, ev *queue.Event) error {
	if ev.At.IsZero() {
		ev.At = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	id := ev.JobID + "#" + strconv.FormatInt(ev.Seq, 10)
	if m.eventIDs[id] {
		return nil
	}
	m.eventIDs[id] = true
	k := ev.Caller + "." + ev.JobID
	m.events[k] = append(m.events[k], *ev)
	return nil
}

func (m *Mem) Events(_ context.Context, caller, jobID string) ([]queue.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.events[caller+"."+jobID]), nil
}

func (m *Mem) State(_ context.Context, jobID string) (queue.JobState, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.state[jobID]
	if !ok {
		return queue.JobState{}, 0, queue.ErrNotFound
	}
	var s queue.JobState
	_ = json.Unmarshal(e.val, &s)
	return s, e.rev, nil
}

func (m *Mem) CASState(_ context.Context, s queue.JobState, rev uint64) (uint64, error) {
	if s.UpdatedAt.IsZero() {
		s.UpdatedAt = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state[s.JobID].rev != rev {
		return 0, queue.ErrFenced
	}
	m.rev++
	b, _ := json.Marshal(s)
	m.state[s.JobID] = kvEntry{val: b, rev: m.rev}
	return m.rev, nil
}

func (m *Mem) SetControl(_ context.Context, jobID string, c queue.Control) error {
	m.mu.Lock()
	m.control[jobID] = c
	ws := slices.Clone(m.watchers)
	m.mu.Unlock()
	for _, w := range ws {
		select {
		case w <- queue.ControlChange{JobID: jobID, Control: c}:
		default:
		}
	}
	return nil
}

func (m *Mem) GetControl(_ context.Context, jobID string) (queue.Control, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.control[jobID], nil
}

func (m *Mem) WatchControl(ctx context.Context, jobIDs []string) (<-chan queue.ControlChange, error) {
	in := make(chan queue.ControlChange, 64)
	out := make(chan queue.ControlChange, 64)
	m.mu.Lock()
	m.watchers = append(m.watchers, in)
	m.mu.Unlock()
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				m.mu.Lock()
				m.watchers = slices.DeleteFunc(m.watchers, func(c chan queue.ControlChange) bool { return c == in })
				m.mu.Unlock()
				return
			case c := <-in:
				if len(jobIDs) == 0 || slices.Contains(jobIDs, c.JobID) {
					out <- c
				}
			}
		}
	}()
	return out, nil
}

func (m *Mem) PutServer(_ context.Context, rec queue.ServerRecord) error {
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = time.Now().UTC()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.servers[rec.Key] = rec
	return nil
}

func (m *Mem) Servers(context.Context) ([]queue.ServerRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []queue.ServerRecord
	for _, r := range m.servers {
		if time.Since(r.UpdatedAt) < 90*time.Second {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Key < out[b].Key })
	return out, nil
}

func (m *Mem) Resume(_ context.Context, h queue.ClaimHandle) (queue.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, w := range m.work {
		if "mem."+strconv.FormatUint(w.seq, 10) == h.AckSubject && w.env.JobID == h.JobID {
			env := w.env
			return &memDelivery{m: m, w: w, env: &env, h: h}, nil
		}
	}
	return nil, fmt.Errorf("%w: job %s is no longer queued", queue.ErrNotFound, h.JobID)
}

func (m *Mem) Status(context.Context) queue.Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	w := map[string]uint64{}
	for _, x := range m.work {
		w[x.ref.Subject()]++
	}
	return queue.Status{Kind: "memory", Connected: true, JetStream: true, Waiting: w,
		Streams: map[string]uint64{"WORK": uint64(len(m.work)), "DEAD": uint64(len(m.dead))}}
}

func (m *Mem) Close() error { return nil }

// Dead returns the envelopes copied to DEAD.
func (m *Mem) Dead() []queue.Envelope {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.dead)
}

type memDelivery struct {
	m    *Mem
	w    *msg
	env  *queue.Envelope
	h    queue.ClaimHandle
	done bool
}

func (d *memDelivery) Envelope() *queue.Envelope        { return d.env }
func (d *memDelivery) Attempt() int                     { return d.h.Attempt }
func (d *memDelivery) Handle() queue.ClaimHandle        { return d.h }
func (d *memDelivery) SetFence(rev uint64)              { d.h.FenceRev = rev }
func (d *memDelivery) InProgress(context.Context) error { return nil }

func (d *memDelivery) settle(fn func()) error {
	d.m.mu.Lock()
	defer d.m.mu.Unlock()
	if d.done {
		return nil
	}
	d.done = true
	fn()
	return nil
}

func (d *memDelivery) remove() {
	d.m.work = slices.DeleteFunc(d.m.work, func(x *msg) bool { return x == d.w })
}

func (d *memDelivery) Ack(context.Context) error { return d.settle(d.remove) }

func (d *memDelivery) Requeue(_ context.Context, delay time.Duration) error {
	return d.settle(func() { d.w.held = false; d.w.notBefore = time.Now().Add(delay) })
}

func (d *memDelivery) Dead(_ context.Context, _ string) error {
	return d.settle(func() { d.m.dead = append(d.m.dead, *d.env); d.remove() })
}
