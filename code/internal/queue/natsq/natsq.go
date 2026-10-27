// Package natsq is the NATS JetStream adapter behind queue.Queue (pm/task_queue.mdx §13, D-014). It is a client of
// this machine's own nats-server service; it never embeds a server. Streams and KV buckets follow §13.1 and are
// created (or updated) on Open, so a fresh nats-server needs nothing but JetStream turned on.
package natsq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/queue"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Stream and bucket names (§13.1).
const (
	StreamWork    = "WORK"
	StreamEvents  = "EVENTS"
	StreamDead    = "DEAD"
	BucketState   = "job_state"
	BucketControl = "job_control"
	BucketServers = "servers"
)

// MinServerVersion is the oldest nats-server this adapter supports (work-queue streams, KV, NAK with delay).
const MinServerVersion = "2.10.0"

// Options configure Open.
type Options struct {
	URL             string        // nats://127.0.0.1:4222
	CredentialsFile string        // NATS .creds file (decentralized auth, §15.2); empty for a local standalone broker
	Name            string        // connection name, shows in nats-server's connz
	AckWait         time.Duration // how long a claim lasts without InProgress (continuity.ack_wait, default 5m)
	Replicas        int           // stream replicas (1 standalone, 3 on a hub cluster)
	ConnectTimeout  time.Duration
}

// Queue is a connected adapter. It is safe for concurrent use.
type Queue struct {
	opt      Options
	nc       *nats.Conn
	js       jetstream.JetStream
	work     jetstream.Stream
	state    jetstream.KeyValue
	control  jetstream.KeyValue
	servers  jetstream.KeyValue
	mu       sync.Mutex
	consumer map[string]jetstream.Consumer
}

// Open connects to the broker, checks its version and JetStream, and makes sure every stream and bucket exists.
func Open(ctx context.Context, o Options) (*Queue, error) {
	if o.AckWait <= 0 {
		o.AckWait = 5 * time.Minute
	}
	if o.Replicas <= 0 {
		o.Replicas = 1
	}
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = 3 * time.Second
	}
	opts := []nats.Option{nats.Name(o.Name), nats.Timeout(o.ConnectTimeout), nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second), nats.RetryOnFailedConnect(false)}
	if o.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(o.CredentialsFile))
	}
	nc, err := nats.Connect(o.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot connect to nats-server at %s: %v", queue.ErrUnavailable, o.URL, err)
	}
	q := &Queue{opt: o, nc: nc, consumer: map[string]jetstream.Consumer{}}
	if err := q.setup(ctx); err != nil {
		nc.Close()
		return nil, err
	}
	return q, nil
}

func (q *Queue) setup(ctx context.Context) error {
	if v := q.nc.ConnectedServerVersion(); !VersionAtLeast(v, MinServerVersion) {
		return fmt.Errorf("%w: nats-server %s at %s is older than %s", queue.ErrUnavailable, v, q.opt.URL, MinServerVersion)
	}
	js, err := jetstream.New(q.nc)
	if err != nil {
		return err
	}
	if _, err := js.AccountInfo(ctx); err != nil {
		return fmt.Errorf("%w: JetStream is not enabled on nats-server at %s (start it with -js or jetstream {} in its config): %v",
			queue.ErrUnavailable, q.opt.URL, err)
	}
	q.js = js
	streams := []jetstream.StreamConfig{
		{Name: StreamWork, Subjects: []string{"work.>"}, Retention: jetstream.WorkQueuePolicy,
			Duplicates: 2 * time.Hour, MaxMsgSize: queue.MaxEnvelopeBytes + 4096, Replicas: q.opt.Replicas,
			Description: "admitted jobs waiting for a worker (task_queue.mdx §13.1)"},
		{Name: StreamEvents, Subjects: []string{"evt.>"}, Retention: jetstream.LimitsPolicy, MaxAge: 7 * 24 * time.Hour,
			Replicas: q.opt.Replicas, Description: "lifecycle events and results per job"},
		{Name: StreamDead, Subjects: []string{"dead.>"}, Retention: jetstream.LimitsPolicy, MaxAge: 30 * 24 * time.Hour,
			Replicas: q.opt.Replicas, Description: "jobs that exhausted their attempts or failed for good"},
	}
	for _, cfg := range streams {
		s, err := js.CreateOrUpdateStream(ctx, cfg)
		if err != nil {
			return fmt.Errorf("creating stream %s: %w", cfg.Name, err)
		}
		if cfg.Name == StreamWork {
			q.work = s
		}
	}
	kvs := []struct {
		dst *jetstream.KeyValue
		cfg jetstream.KeyValueConfig
	}{
		{&q.state, jetstream.KeyValueConfig{Bucket: BucketState, TTL: 30 * 24 * time.Hour, History: 1, Replicas: q.opt.Replicas}},
		{&q.control, jetstream.KeyValueConfig{Bucket: BucketControl, TTL: 7 * 24 * time.Hour, History: 1, Replicas: q.opt.Replicas}},
		{&q.servers, jetstream.KeyValueConfig{Bucket: BucketServers, TTL: 90 * time.Second, History: 1, Replicas: q.opt.Replicas}},
	}
	for _, k := range kvs {
		kv, err := js.CreateOrUpdateKeyValue(ctx, k.cfg)
		if err != nil {
			return fmt.Errorf("creating KV bucket %s: %w", k.cfg.Bucket, err)
		}
		*k.dst = kv
	}
	return nil
}

// Close drains nothing: claims are kept by the broker until their ack wait, which is the point (§9).
func (q *Queue) Close() error {
	q.nc.Close()
	return nil
}

// Submit publishes the envelope to its lane. Standalone admission is inline: the band comes from the priority
// (§14.2). Hub admission (signatures, caller policy, SUBMIT stream) is build phase Q2/Q3.
func (q *Queue) Submit(ctx context.Context, env *queue.Envelope) (queue.SubmitAck, error) {
	if err := env.Normalize(time.Now()); err != nil {
		return queue.SubmitAck{}, err
	}
	data, err := json.Marshal(env)
	if err != nil {
		return queue.SubmitAck{}, err
	}
	if len(data) > queue.MaxEnvelopeBytes {
		return queue.SubmitAck{}, fmt.Errorf("%w: envelope is %d bytes; the limit is %d (pass big params by URL)", queue.ErrInvalid, len(data), queue.MaxEnvelopeBytes)
	}
	ref := queue.ConsumerRef{Band: queue.BandFor(env.Priority), Caller: env.Caller, Service: env.Service}
	m := nats.NewMsg(ref.Subject())
	m.Data = data
	m.Header.Set(jetstream.MsgIDHeader, env.JobID)
	m.Header.Set("X-Caller", env.Caller)
	m.Header.Set("X-Category", env.Category)
	m.Header.Set("X-Service", env.Service)
	m.Header.Set("X-Model", env.Model)
	m.Header.Set("X-Priority", strconv.Itoa(env.Priority))
	m.Header.Set("X-Trace-Id", env.TraceID)
	m.Header.Set("X-Envelope-Version", strconv.Itoa(env.EnvelopeVersion))
	m.Header.Set("X-Band", ref.Band)
	m.Header.Set("X-Admitted-At", time.Now().UTC().Format(time.RFC3339Nano))
	ack, err := q.js.PublishMsg(ctx, m)
	if err != nil {
		return queue.SubmitAck{}, q.unavailable("publishing a job", err)
	}
	return queue.SubmitAck{JobID: env.JobID, Band: ref.Band, Subject: ref.Subject(), Sequence: ack.Sequence, Duplicate: ack.Duplicate}, nil
}

// Pull looks at which lanes hold messages (one stream info call), then fetches from the first lane in order that
// has work. A lane whose messages are all claimed by other workers returns nothing and the next lane is tried.
func (q *Queue) Pull(ctx context.Context, order []queue.ConsumerRef, wait time.Duration) (queue.Delivery, error) {
	deadline := time.Now().Add(wait)
	for {
		d, err := q.pullOnce(ctx, order)
		if d != nil || err != nil || wait <= 0 || time.Now().After(deadline) {
			return d, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(min(250*time.Millisecond, time.Until(deadline))):
		}
	}
}

func (q *Queue) pullOnce(ctx context.Context, order []queue.ConsumerRef) (queue.Delivery, error) {
	waiting, err := q.waiting(ctx)
	if err != nil {
		return nil, err
	}
	for _, ref := range order {
		if waiting[ref.Subject()] == 0 {
			continue
		}
		c, err := q.consumerFor(ctx, ref)
		if err != nil {
			return nil, err
		}
		batch, err := c.FetchNoWait(1)
		if err != nil {
			return nil, q.unavailable("fetching from "+ref.Durable(), err)
		}
		for msg := range batch.Messages() {
			return q.toDelivery(msg)
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) {
			return nil, q.unavailable("fetching from "+ref.Durable(), err)
		}
	}
	return nil, nil
}

// waiting returns work subject → stored messages (queued or claimed but not yet acked).
func (q *Queue) waiting(ctx context.Context) (map[string]uint64, error) {
	info, err := q.work.Info(ctx, jetstream.WithSubjectFilter("work.>"))
	if err != nil {
		return nil, q.unavailable("reading the WORK stream", err)
	}
	return info.State.Subjects, nil
}

func (q *Queue) consumerFor(ctx context.Context, ref queue.ConsumerRef) (jetstream.Consumer, error) {
	q.mu.Lock()
	c, ok := q.consumer[ref.Durable()]
	q.mu.Unlock()
	if ok {
		return c, nil
	}
	c, err := q.work.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: ref.Durable(), FilterSubject: ref.Subject(), AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: q.opt.AckWait, MaxDeliver: -1, MaxAckPending: 10000, InactiveThreshold: 0,
		Description: "shared work lane " + ref.Subject(),
	})
	if err != nil {
		return nil, q.unavailable("creating consumer "+ref.Durable(), err)
	}
	q.mu.Lock()
	q.consumer[ref.Durable()] = c
	q.mu.Unlock()
	return c, nil
}

func (q *Queue) toDelivery(msg jetstream.Msg) (queue.Delivery, error) {
	var env queue.Envelope
	if err := json.Unmarshal(msg.Data(), &env); err != nil {
		// A message nobody can read must not loop forever: terminate it and say so.
		_ = msg.Term()
		return nil, fmt.Errorf("work message on %s is not a task envelope (terminated): %w", msg.Subject(), err)
	}
	attempt := 1
	if md, err := msg.Metadata(); err == nil {
		attempt = int(md.NumDelivered)
	}
	return &delivery{q: q, env: &env, data: msg.Data(), h: queue.ClaimHandle{JobID: env.JobID, Caller: env.Caller,
		Service: env.Service, AckSubject: msg.Reply(), Attempt: attempt}}, nil
}

// Resume rebuilds a delivery from its ack subject: the stream sequence in it locates the original message.
func (q *Queue) Resume(ctx context.Context, h queue.ClaimHandle) (queue.Delivery, error) {
	ack, err := ParseAckSubject(h.AckSubject)
	if err != nil {
		return nil, err
	}
	raw, err := q.work.GetMsg(ctx, ack.StreamSeq)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return nil, fmt.Errorf("%w: job %s is no longer in the WORK stream (acked, or redelivered and finished elsewhere)", queue.ErrNotFound, h.JobID)
		}
		return nil, q.unavailable("reading a claimed job", err)
	}
	var env queue.Envelope
	if err := json.Unmarshal(raw.Data, &env); err != nil {
		return nil, err
	}
	if env.JobID != h.JobID {
		return nil, fmt.Errorf("%w: WORK sequence %d holds job %s, not %s", queue.ErrNotFound, ack.StreamSeq, env.JobID, h.JobID)
	}
	return &delivery{q: q, env: &env, data: raw.Data, h: h}, nil
}

// Publish writes one event to evt.<caller>.<job_id>. The message id (job, seq) makes a repeated final event a no-op.
func (q *Queue) Publish(ctx context.Context, ev *queue.Event) error {
	if ev.At.IsZero() {
		ev.At = time.Now().UTC()
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	m := nats.NewMsg(queue.EventSubject(ev.Caller, ev.JobID))
	m.Data = data
	m.Header.Set(jetstream.MsgIDHeader, ev.JobID+"#"+strconv.FormatInt(ev.Seq, 10))
	if _, err := q.js.PublishMsg(ctx, m); err != nil {
		return q.unavailable("publishing an event", err)
	}
	return nil
}

// Events reads the job's events with an ordered consumer from the start of its subject.
func (q *Queue) Events(ctx context.Context, caller, jobID string) ([]queue.Event, error) {
	subj := queue.EventSubject(caller, jobID)
	s, err := q.js.Stream(ctx, StreamEvents)
	if err != nil {
		return nil, q.unavailable("opening the EVENTS stream", err)
	}
	info, err := s.Info(ctx, jetstream.WithSubjectFilter(subj))
	if err != nil {
		return nil, q.unavailable("reading the EVENTS stream", err)
	}
	n := int(info.State.Subjects[subj])
	if n == 0 {
		return nil, nil
	}
	c, err := q.js.OrderedConsumer(ctx, StreamEvents, jetstream.OrderedConsumerConfig{FilterSubjects: []string{subj}})
	if err != nil {
		return nil, q.unavailable("reading events", err)
	}
	var out []queue.Event
	for len(out) < n {
		batch, err := c.Fetch(n-len(out), jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			return out, q.unavailable("reading events", err)
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

// State reads job_state.
func (q *Queue) State(ctx context.Context, jobID string) (queue.JobState, uint64, error) {
	e, err := q.state.Get(ctx, jobID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return queue.JobState{}, 0, queue.ErrNotFound
	}
	if err != nil {
		return queue.JobState{}, 0, q.unavailable("reading job_state", err)
	}
	var s queue.JobState
	if err := json.Unmarshal(e.Value(), &s); err != nil {
		return queue.JobState{}, 0, fmt.Errorf("job_state %s is not JSON: %w", jobID, err)
	}
	return s, e.Revision(), nil
}

// CASState writes job_state only if its revision is still rev (0: only if absent).
func (q *Queue) CASState(ctx context.Context, s queue.JobState, rev uint64) (uint64, error) {
	if s.UpdatedAt.IsZero() {
		s.UpdatedAt = time.Now().UTC()
	}
	data, err := json.Marshal(s)
	if err != nil {
		return 0, err
	}
	var n uint64
	if rev == 0 {
		n, err = q.state.Create(ctx, s.JobID, data)
	} else {
		n, err = q.state.Update(ctx, s.JobID, data, rev)
	}
	if err != nil {
		var apiErr *jetstream.APIError
		if errors.Is(err, jetstream.ErrKeyExists) || errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
			return 0, queue.ErrFenced
		}
		return 0, q.unavailable("writing job_state", err)
	}
	return n, nil
}

// SetControl writes the job's control flags.
func (q *Queue) SetControl(ctx context.Context, jobID string, c queue.Control) error {
	data, _ := json.Marshal(c)
	if _, err := q.control.Put(ctx, jobID, data); err != nil {
		return q.unavailable("writing job_control", err)
	}
	return nil
}

// GetControl reads the job's control flags (zero when none were set).
func (q *Queue) GetControl(ctx context.Context, jobID string) (queue.Control, error) {
	e, err := q.control.Get(ctx, jobID)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return queue.Control{}, nil
	}
	if err != nil {
		return queue.Control{}, q.unavailable("reading job_control", err)
	}
	var c queue.Control
	_ = json.Unmarshal(e.Value(), &c)
	return c, nil
}

// WatchControl streams changes to job_control made after the call. The channel closes when ctx ends.
func (q *Queue) WatchControl(ctx context.Context, jobIDs []string) (<-chan queue.ControlChange, error) {
	w, err := q.control.WatchAll(ctx, jetstream.UpdatesOnly())
	if err != nil {
		return nil, q.unavailable("watching job_control", err)
	}
	want := map[string]bool{}
	for _, id := range jobIDs {
		want[id] = true
	}
	out := make(chan queue.ControlChange, 64)
	go func() {
		defer close(out)
		defer func() { _ = w.Stop() }()
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-w.Updates():
				if !ok {
					return
				}
				if e == nil || e.Operation() != jetstream.KeyValuePut || (len(want) > 0 && !want[e.Key()]) {
					continue
				}
				var c queue.Control
				if json.Unmarshal(e.Value(), &c) != nil {
					continue
				}
				select {
				case out <- queue.ControlChange{JobID: e.Key(), Control: c}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// PutServer writes this server's capability record (TTL 90 s on the bucket).
func (q *Queue) PutServer(ctx context.Context, rec queue.ServerRecord) error {
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = time.Now().UTC()
	}
	data, _ := json.Marshal(rec)
	if _, err := q.servers.Put(ctx, rec.Key, data); err != nil {
		return q.unavailable("writing the servers record", err)
	}
	return nil
}

// Servers lists every live capability record.
func (q *Queue) Servers(ctx context.Context) ([]queue.ServerRecord, error) {
	keys, err := q.servers.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, q.unavailable("listing servers", err)
	}
	var out []queue.ServerRecord
	for _, k := range keys {
		e, err := q.servers.Get(ctx, k)
		if err != nil {
			continue
		}
		var r queue.ServerRecord
		if json.Unmarshal(e.Value(), &r) == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

// Status reports the connection, server version and stream sizes.
func (q *Queue) Status(ctx context.Context) queue.Status {
	st := queue.Status{Kind: "nats", URL: q.opt.URL, Connected: q.nc.IsConnected(), ServerVersion: q.nc.ConnectedServerVersion()}
	if !st.Connected {
		st.Error = "not connected to nats-server (reconnecting)"
		return st
	}
	st.Streams = map[string]uint64{}
	for _, name := range []string{StreamWork, StreamEvents, StreamDead} {
		s, err := q.js.Stream(ctx, name)
		if err != nil {
			st.Error = err.Error()
			return st
		}
		info, err := s.Info(ctx)
		if err != nil {
			st.Error = err.Error()
			return st
		}
		st.Streams[name] = info.State.Msgs
	}
	st.JetStream = true
	if w, err := q.waiting(ctx); err == nil {
		st.Waiting = w
	}
	return st
}

func (q *Queue) unavailable(what string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if !q.nc.IsConnected() || errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrNoResponders) || errors.Is(err, nats.ErrTimeout) {
		return fmt.Errorf("%w: %s: %v", queue.ErrUnavailable, what, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}

// delivery is one held job. Acks are published to the delivery's ack subject, which is not tied to the connection
// that received the job, so a restarted process holding the saved handle keeps the same claim (§9.4).
type delivery struct {
	q    *Queue
	env  *queue.Envelope
	data []byte
	mu   sync.Mutex
	h    queue.ClaimHandle
	done bool
}

func (d *delivery) Envelope() *queue.Envelope { return d.env }
func (d *delivery) Attempt() int              { return d.h.Attempt }
func (d *delivery) Handle() queue.ClaimHandle {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.h
}
func (d *delivery) SetFence(rev uint64) {
	d.mu.Lock()
	d.h.FenceRev = rev
	d.mu.Unlock()
}

func (d *delivery) InProgress(ctx context.Context) error {
	return d.send(ctx, []byte("+WPI"), false, false)
}

func (d *delivery) Ack(ctx context.Context) error { return d.send(ctx, []byte("+ACK"), true, true) }

func (d *delivery) Requeue(ctx context.Context, delay time.Duration) error {
	body := []byte("-NAK")
	if delay > 0 {
		body = fmt.Appendf(nil, `-NAK {"delay": %d}`, delay.Nanoseconds())
	}
	return d.send(ctx, body, false, true)
}

func (d *delivery) Dead(ctx context.Context, reason string) error {
	m := nats.NewMsg(queue.DeadSubject(d.env.Caller, d.env.Service))
	m.Data = d.data
	m.Header.Set("X-Job-Id", d.env.JobID)
	m.Header.Set("X-Reason", reason)
	m.Header.Set("X-Attempt", strconv.Itoa(d.h.Attempt))
	m.Header.Set(jetstream.MsgIDHeader, "dead#"+d.env.JobID+"#"+strconv.Itoa(d.h.Attempt))
	if _, err := d.q.js.PublishMsg(ctx, m); err != nil {
		return d.q.unavailable("copying a job to DEAD", err)
	}
	return d.send(ctx, []byte("+TERM "+reason), false, true)
}

// send publishes an ack body. final marks the delivery settled; confirm waits for the server's reply.
func (d *delivery) send(ctx context.Context, body []byte, confirm, final bool) error {
	d.mu.Lock()
	if d.done {
		d.mu.Unlock()
		return nil
	}
	if final {
		d.done = true
	}
	subj := d.h.AckSubject
	d.mu.Unlock()
	if subj == "" {
		return errors.New("delivery has no ack subject")
	}
	if confirm {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := d.q.nc.RequestWithContext(cctx, subj, body); err != nil {
			return d.q.unavailable("acknowledging job "+d.env.JobID, err)
		}
		return nil
	}
	if err := d.q.nc.Publish(subj, body); err != nil {
		return d.q.unavailable("acknowledging job "+d.env.JobID, err)
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return d.q.nc.FlushWithContext(fctx)
}

// AckInfo is what an ack subject says about its message.
type AckInfo struct {
	Stream, Consumer string
	Delivered        uint64
	StreamSeq        uint64
}

// ParseAckSubject reads both ack subject formats:
// $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending> and
// $JS.ACK.<domain>.<account hash>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<ts>.<pending>[.<token>].
func ParseAckSubject(s string) (AckInfo, error) {
	t := strings.Split(s, ".")
	if len(t) < 9 || t[0] != "$JS" || t[1] != "ACK" {
		return AckInfo{}, fmt.Errorf("%q is not a JetStream ack subject", s)
	}
	if len(t) > 9 {
		if len(t) < 11 {
			return AckInfo{}, fmt.Errorf("%q is not a JetStream ack subject", s)
		}
		t = append(t[:2], t[4:]...) // drop domain and account hash
	}
	del, err1 := strconv.ParseUint(t[4], 10, 64)
	seq, err2 := strconv.ParseUint(t[5], 10, 64)
	if err1 != nil || err2 != nil {
		return AckInfo{}, fmt.Errorf("%q has no numeric delivery and sequence tokens", s)
	}
	return AckInfo{Stream: t[2], Consumer: t[3], Delivered: del, StreamSeq: seq}, nil
}

// VersionAtLeast compares dotted versions ("2.15.2" >= "2.10.0"); a leading v and a -suffix are ignored.
func VersionAtLeast(have, want string) bool {
	parse := func(v string) [3]int {
		v = strings.TrimPrefix(v, "v")
		if i := strings.IndexAny(v, "-+"); i >= 0 {
			v = v[:i]
		}
		var out [3]int
		for i, p := range strings.SplitN(v, ".", 3) {
			out[i], _ = strconv.Atoi(p)
		}
		return out
	}
	h, w := parse(have), parse(want)
	for i := range h {
		if h[i] != w[i] {
			return h[i] > w[i]
		}
	}
	return true
}
