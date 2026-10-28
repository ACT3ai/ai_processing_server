// Package natsq is the NATS JetStream adapter behind queue.Queue (pm/task_queue.mdx §13). It is only a client: the
// nats-server runs as its own OS service (§8.2) and outlives this process, so queued jobs and claims survive every
// controller restart.
//
// Claims are held through ack subjects ($JS.ACK...), which are not tied to the connection that received the job:
// a restarted controller keeps the same claim by publishing to the stored subject (§9.4).
package natsq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/errlog"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var elog = errlog.For("natsq")

// StreamDef and KVDef mirror data/queue/streams.yaml.
type StreamDef struct {
	Name            string        `yaml:"name"`
	Subjects        []string      `yaml:"subjects"`
	Retention       string        `yaml:"retention"` // workqueue | limits
	MaxAge          time.Duration `yaml:"max_age"`
	DuplicateWindow time.Duration `yaml:"duplicate_window"`
}

type KVDef struct {
	Bucket  string        `yaml:"bucket"`
	TTL     time.Duration `yaml:"ttl"`
	History int           `yaml:"history"`
}

// Options open a connection.
type Options struct {
	URL        string
	Name       string // connection name, shown by the broker's monitoring
	CredsFile  string // optional NATS .creds file (phase Q3 accounts)
	AckWait    time.Duration
	MaxDeliver int
	Streams    []StreamDef
	KV         []KVDef
	Logger     *slog.Logger
}

// Q implements queue.Queue over one NATS connection.
type Q struct {
	o    Options
	nc   *nats.Conn
	js   jetstream.JetStream
	kv   map[string]jetstream.KeyValue
	log  *slog.Logger
	mu   sync.Mutex
	cons map[string]jetstream.Consumer
	adm  jetstream.Consumer
}

var _ queue.Queue = (*Q)(nil)

// Open connects, checks JetStream, and creates or updates every stream and bucket in o. It fails fast (2 s) when
// the broker is not running, wrapping queue.ErrBrokerAbsent, so the caller can report `broker_missing`.
func Open(ctx context.Context, o Options) (*Q, error) {
	if o.AckWait <= 0 {
		o.AckWait = 5 * time.Minute
	}
	if o.MaxDeliver == 0 {
		o.MaxDeliver = 10
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	log := o.Logger.With("ctx", "natsq")
	opts := []nats.Option{
		nats.Name(o.Name), nats.Timeout(2 * time.Second), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Warn("broker connection lost; reconnecting", "error", err)
			}
		}),
		nats.ReconnectHandler(func(c *nats.Conn) { log.Info("broker reconnected", "url", c.ConnectedUrlRedacted()) }),
	}
	if o.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(o.CredsFile))
	}
	nc, err := nats.Connect(o.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("%w at %s: %v", queue.ErrBrokerAbsent, o.URL, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	q := &Q{o: o, nc: nc, js: js, kv: map[string]jetstream.KeyValue{}, log: log, cons: map[string]jetstream.Consumer{}}
	if err := q.ensure(ctx); err != nil {
		nc.Close()
		return nil, err
	}
	return q, nil
}

func (q *Q) ensure(ctx context.Context) error {
	if _, err := q.js.AccountInfo(ctx); err != nil {
		return fmt.Errorf("the broker at %s has no JetStream (start it with the config from `just broker-config`): %w", q.o.URL, err)
	}
	for _, s := range q.o.Streams {
		cfg := jetstream.StreamConfig{Name: s.Name, Subjects: s.Subjects, Storage: jetstream.FileStorage,
			MaxAge: s.MaxAge, Duplicates: s.DuplicateWindow, MaxMsgSize: 1 << 20}
		if s.Retention == "workqueue" {
			cfg.Retention = jetstream.WorkQueuePolicy
		}
		if _, err := q.js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("stream %s: %w", s.Name, err)
		}
	}
	for _, k := range q.o.KV {
		kv, err := q.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: k.Bucket, TTL: k.TTL,
			History: uint8(max(k.History, 1)), Storage: jetstream.FileStorage})
		if err != nil {
			return fmt.Errorf("kv %s: %w", k.Bucket, err)
		}
		q.kv[k.Bucket] = kv
	}
	return nil
}

func (q *Q) bucket(name string) (jetstream.KeyValue, error) {
	kv := q.kv[name]
	if kv == nil {
		return nil, fmt.Errorf("kv bucket %s is not in data/queue/streams.yaml", name)
	}
	return kv, nil
}

// Info implements queue.Queue.
func (q *Q) Info() queue.BrokerInfo {
	return queue.BrokerInfo{Kind: "nats", URL: q.o.URL, Connected: q.nc.IsConnected(), Version: q.nc.ConnectedServerVersion()}
}

func (q *Q) Close() error {
	// Drain would nack nothing either way; Close leaves every held claim with the broker (§9.5).
	q.nc.Close()
	return nil
}

// ---- submission and admission ----

func headers(env *queue.Envelope) nats.Header {
	h := nats.Header{}
	h.Set("X-Caller", env.Caller)
	h.Set("X-Category", env.Category)
	h.Set("X-Service", env.Service)
	h.Set("X-Model", env.Model)
	h.Set("X-Priority", strconv.Itoa(env.Priority))
	h.Set("X-Trace-Id", env.TraceID)
	h.Set("X-Envelope-Version", strconv.Itoa(env.EnvelopeVersion))
	return h
}

func (q *Q) Submit(ctx context.Context, env *queue.Envelope) (queue.SubmitAck, error) {
	if err := env.Validate(); err != nil {
		return queue.SubmitAck{}, err
	}
	b, err := json.Marshal(env)
	if err != nil {
		return queue.SubmitAck{}, err
	}
	m := &nats.Msg{Subject: queue.SubmitSubject(env.Caller, env.Category), Data: b, Header: headers(env)}
	ack, err := q.js.PublishMsg(ctx, m, jetstream.WithMsgID(env.JobID))
	if err != nil {
		return queue.SubmitAck{}, err
	}
	return queue.SubmitAck{Stream: ack.Stream, Seq: ack.Sequence, Duplicate: ack.Duplicate}, nil
}

func (q *Q) admission(ctx context.Context) (jetstream.Consumer, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.adm != nil {
		return q.adm, nil
	}
	c, err := q.js.CreateOrUpdateConsumer(ctx, "SUBMIT", jetstream.ConsumerConfig{Durable: "admission",
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: 30 * time.Second, FilterSubject: "submit.>", MaxDeliver: -1})
	if err != nil {
		return nil, err
	}
	q.adm = c
	return c, nil
}

type submission struct{ m jetstream.Msg }

func (s submission) Envelope() (*queue.Envelope, error) {
	var env queue.Envelope
	if err := json.Unmarshal(s.m.Data(), &env); err != nil {
		return nil, err
	}
	return &env, nil
}
func (s submission) Ack(ctx context.Context) error  { return s.m.DoubleAck(ctx) }
func (s submission) Term(ctx context.Context) error { return s.m.Term() }

func (q *Q) NextSubmission(ctx context.Context, wait time.Duration) (queue.Submission, error) {
	c, err := q.admission(ctx)
	if err != nil {
		return nil, err
	}
	batch, err := c.Fetch(1, jetstream.FetchMaxWait(max(wait, 10*time.Millisecond)))
	if err != nil {
		return nil, err
	}
	for m := range batch.Messages() {
		return submission{m}, nil
	}
	if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
		return nil, err
	}
	return nil, queue.ErrNoWork
}

func (q *Q) Enqueue(ctx context.Context, env *queue.Envelope, band string) error {
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	ref := queue.ConsumerRef{Band: band, Caller: env.Caller, Service: env.Service}
	if _, err := q.consumer(ctx, ref); err != nil {
		return err
	}
	h := headers(env)
	h.Set("X-Band", band)
	h.Set("X-Admitted-At", time.Now().UTC().Format(time.RFC3339Nano))
	_, err = q.js.PublishMsg(ctx, &nats.Msg{Subject: ref.Subject(), Data: b, Header: h}, jetstream.WithMsgID(env.JobID+".w"))
	return err
}

// ---- work ----

func (q *Q) consumer(ctx context.Context, ref queue.ConsumerRef) (jetstream.Consumer, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if c := q.cons[ref.Name()]; c != nil {
		return c, nil
	}
	c, err := q.js.CreateOrUpdateConsumer(ctx, "WORK", jetstream.ConsumerConfig{Durable: ref.Name(), FilterSubject: ref.Subject(),
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: q.o.AckWait, MaxDeliver: q.o.MaxDeliver, MaxAckPending: -1})
	if err != nil {
		return nil, fmt.Errorf("work consumer %s: %w", ref.Name(), err)
	}
	q.cons[ref.Name()] = c
	return c, nil
}

func (q *Q) Depth(ctx context.Context) (map[queue.ConsumerRef]int, error) {
	s, err := q.js.Stream(ctx, "WORK")
	if err != nil {
		return nil, err
	}
	info, err := s.Info(ctx, jetstream.WithSubjectFilter("work.>"))
	if err != nil {
		return nil, err
	}
	out := map[queue.ConsumerRef]int{}
	for subj, n := range info.State.Subjects {
		if ref, ok := queue.ParseWorkSubject(subj); ok {
			out[ref] = int(n)
		}
	}
	return out, nil
}

func (q *Q) Pull(ctx context.Context, order []queue.ConsumerRef, wait time.Duration) (queue.Delivery, error) {
	depth, err := q.Depth(ctx)
	if err != nil {
		return nil, err
	}
	for _, ref := range order {
		if depth[ref] == 0 {
			continue
		}
		c, err := q.consumer(ctx, ref)
		if err != nil {
			return nil, err
		}
		batch, err := c.FetchNoWait(1)
		if err != nil {
			return nil, err
		}
		for m := range batch.Messages() {
			md, err := m.Metadata()
			if err != nil {
				return nil, err
			}
			var env queue.Envelope
			if err := json.Unmarshal(m.Data(), &env); err != nil {
				// Not ours to run: stop it being delivered again, and say so.
				elog.Error("decoding a work message", err, "subject", m.Subject())
				_ = m.Term()
				continue
			}
			return &delivery{q: q, env: &env, h: queue.ClaimHandle{JobID: env.JobID, AckSubject: m.Reply(), Attempt: int(md.NumDelivered)}}, nil
		}
	}
	return nil, queue.ErrNoWork
}

func (q *Q) Resume(_ context.Context, h queue.ClaimHandle, env *queue.Envelope) (queue.Delivery, error) {
	if !strings.HasPrefix(h.AckSubject, "$JS.ACK.") {
		return nil, fmt.Errorf("claim handle for %s has no ack subject", h.JobID)
	}
	return &delivery{q: q, env: env, h: h}, nil
}

// delivery works only through the ack subject, so a fresh delivery and a resumed one behave the same.
type delivery struct {
	q   *Q
	env *queue.Envelope
	h   queue.ClaimHandle
}

func (d *delivery) Envelope() *queue.Envelope { return d.env }
func (d *delivery) Attempt() int              { return d.h.Attempt }
func (d *delivery) Handle() queue.ClaimHandle { return d.h }

func (d *delivery) InProgress(context.Context) error {
	return d.q.nc.Publish(d.h.AckSubject, []byte("+WPI"))
}

// ack sends a control payload and waits for the broker's confirmation (a double ack).
func (d *delivery) ack(ctx context.Context, payload string) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	_, err := d.q.nc.RequestWithContext(ctx, d.h.AckSubject, []byte(payload))
	return err
}

func (d *delivery) Ack(ctx context.Context) error { return d.ack(ctx, "+ACK") }

func (d *delivery) Requeue(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return d.ack(ctx, "-NAK")
	}
	return d.ack(ctx, fmt.Sprintf(`-NAK {"delay": %d}`, delay.Nanoseconds()))
}

func (d *delivery) Dead(ctx context.Context, reason string) error {
	b, err := json.Marshal(d.env)
	if err != nil {
		return err
	}
	h := headers(d.env)
	h.Set("X-Dead-Reason", reason)
	h.Set("X-Attempts", strconv.Itoa(d.h.Attempt))
	if _, err := d.q.js.PublishMsg(ctx, &nats.Msg{Subject: queue.DeadSubject(d.env.Caller, d.env.Service), Data: b, Header: h},
		jetstream.WithMsgID(d.env.JobID+".dead")); err != nil {
		return err
	}
	return d.ack(ctx, "+TERM "+reason)
}

// ---- events ----

func (q *Q) Publish(ctx context.Context, ev *queue.Event) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	_, err = q.js.PublishMsg(ctx, &nats.Msg{Subject: queue.EventSubject(ev.Caller, ev.JobID), Data: b},
		jetstream.WithMsgID(ev.JobID+"."+strconv.FormatInt(ev.Seq, 10)))
	return err
}

func (q *Q) Events(ctx context.Context, caller, jobID string) ([]queue.Event, error) {
	subj := queue.EventSubject(caller, jobID)
	s, err := q.js.Stream(ctx, "EVENTS")
	if err != nil {
		return nil, err
	}
	info, err := s.Info(ctx, jetstream.WithSubjectFilter(subj))
	if err != nil {
		return nil, err
	}
	n := int(info.State.Subjects[subj])
	if n == 0 {
		return []queue.Event{}, nil
	}
	c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{FilterSubjects: []string{subj}})
	if err != nil {
		return nil, err
	}
	out := make([]queue.Event, 0, n)
	for len(out) < n {
		batch, err := c.Fetch(n-len(out), jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			return out, err
		}
		got := 0
		for m := range batch.Messages() {
			got++
			var ev queue.Event
			if json.Unmarshal(m.Data(), &ev) == nil {
				out = append(out, ev)
			}
		}
		if got == 0 {
			break
		}
	}
	return out, nil
}

// ---- job_state and job_control ----

func (q *Q) CreateState(ctx context.Context, s queue.JobState) (uint64, error) {
	kv, err := q.bucket("job_state")
	if err != nil {
		return 0, err
	}
	b, _ := json.Marshal(s)
	rev, err := kv.Create(ctx, s.JobID, b)
	if errors.Is(err, jetstream.ErrKeyExists) {
		return 0, queue.ErrExists
	}
	return rev, err
}

func (q *Q) State(ctx context.Context, jobID string) (queue.JobState, uint64, error) {
	kv, err := q.bucket("job_state")
	if err != nil {
		return queue.JobState{}, 0, err
	}
	e, err := kv.Get(ctx, jobID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return queue.JobState{}, 0, queue.ErrNotFound
	}
	if err != nil {
		return queue.JobState{}, 0, err
	}
	var s queue.JobState
	if err := json.Unmarshal(e.Value(), &s); err != nil {
		return queue.JobState{}, 0, err
	}
	return s, e.Revision(), nil
}

func (q *Q) CASState(ctx context.Context, s queue.JobState, rev uint64) (uint64, error) {
	kv, err := q.bucket("job_state")
	if err != nil {
		return 0, err
	}
	s.UpdatedAt = time.Now().UTC()
	b, _ := json.Marshal(s)
	n, err := kv.Update(ctx, s.JobID, b, rev)
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.Is(err, jetstream.ErrKeyExists) || (errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence) {
			return 0, queue.ErrRevision
		}
		return 0, err
	}
	return n, nil
}

func (q *Q) SetControl(ctx context.Context, jobID string, c queue.Control) error {
	kv, err := q.bucket("job_control")
	if err != nil {
		return err
	}
	b, _ := json.Marshal(c)
	_, err = kv.Put(ctx, jobID, b)
	return err
}

func (q *Q) GetControl(ctx context.Context, jobID string) (queue.Control, error) {
	kv, err := q.bucket("job_control")
	if err != nil {
		return queue.Control{}, err
	}
	e, err := kv.Get(ctx, jobID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return queue.Control{}, queue.ErrNotFound
	}
	if err != nil {
		return queue.Control{}, err
	}
	var c queue.Control
	return c, json.Unmarshal(e.Value(), &c)
}

func (q *Q) WatchControl(ctx context.Context) (<-chan queue.ControlChange, error) {
	kv, err := q.bucket("job_control")
	if err != nil {
		return nil, err
	}
	w, err := kv.WatchAll(ctx, jetstream.UpdatesOnly())
	if err != nil {
		return nil, err
	}
	out := make(chan queue.ControlChange, 64)
	elog.Go("watching job_control", func() {
		defer close(out)
		defer w.Stop()
		for e := range w.Updates() {
			if e == nil || e.Operation() != jetstream.KeyValuePut {
				continue
			}
			var c queue.Control
			if json.Unmarshal(e.Value(), &c) == nil {
				select {
				case out <- queue.ControlChange{JobID: e.Key(), Control: c}:
				case <-ctx.Done():
					return
				}
			}
		}
	})
	return out, nil
}

// ---- servers ----

func (q *Q) PutServer(ctx context.Context, rec queue.ServerRecord) error {
	kv, err := q.bucket("servers")
	if err != nil {
		return err
	}
	b, _ := json.Marshal(rec)
	_, err = kv.Put(ctx, rec.Key, b)
	return err
}

func (q *Q) Servers(ctx context.Context) ([]queue.ServerRecord, error) {
	kv, err := q.bucket("servers")
	if err != nil {
		return nil, err
	}
	keys, err := kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return []queue.ServerRecord{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := []queue.ServerRecord{}
	for _, k := range keys {
		if e, err := kv.Get(ctx, k); err == nil {
			var r queue.ServerRecord
			if json.Unmarshal(e.Value(), &r) == nil {
				out = append(out, r)
			}
		}
	}
	return out, nil
}
