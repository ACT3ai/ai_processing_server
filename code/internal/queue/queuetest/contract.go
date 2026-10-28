package queuetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

// Contract runs the behavior every queue.Queue must have (pm/task_queue.mdx §13.7, acceptance Q8) against q.
// ackWait must be the ack wait q was built with.
func Contract(t *testing.T, q queue.Queue, ackWait time.Duration) {
	ctx := context.Background()
	env := func(id string) *queue.Envelope {
		now := time.Now().UTC()
		return &queue.Envelope{EnvelopeVersion: 1, JobID: id, Caller: "local", Category: "system", Service: "sys.compute",
			Priority: 10, IssuedAt: now, ExpiresAt: now.Add(time.Hour), Params: []byte(`{"op":"sleep"}`)}
	}
	ref := queue.ConsumerRef{Band: "p4", Caller: "local", Service: "sys.compute"}

	t.Run("submit is idempotent by job id and admission reads it once", func(t *testing.T) {
		if _, err := q.Submit(ctx, env("c-sub-1")); err != nil {
			t.Fatal(err)
		}
		ack, err := q.Submit(ctx, env("c-sub-1"))
		if err != nil || !ack.Duplicate {
			t.Fatalf("second submit: ack %+v err %v, want a duplicate", ack, err)
		}
		sub, err := q.NextSubmission(ctx, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		got, err := sub.Envelope()
		if err != nil || got.JobID != "c-sub-1" || string(got.Params) != `{"op":"sleep"}` {
			t.Fatalf("envelope %+v err %v", got, err)
		}
		if err := sub.Ack(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := q.NextSubmission(ctx, 50*time.Millisecond); !errors.Is(err, queue.ErrNoWork) {
			t.Fatalf("after ack: %v, want ErrNoWork", err)
		}
	})

	t.Run("invalid envelopes are refused", func(t *testing.T) {
		bad := env("c-bad")
		bad.JobID = "has spaces"
		if _, err := q.Submit(ctx, bad); err == nil {
			t.Fatal("a bad job id was accepted")
		}
	})

	t.Run("one delivery at a time; ack removes it", func(t *testing.T) {
		if err := q.Enqueue(ctx, env("c-work-1"), "p4"); err != nil {
			t.Fatal(err)
		}
		d, err := q.Pull(ctx, []queue.ConsumerRef{ref}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if d.Envelope().JobID != "c-work-1" || d.Attempt() != 1 {
			t.Fatalf("got %s attempt %d", d.Envelope().JobID, d.Attempt())
		}
		if _, err := q.Pull(ctx, []queue.ConsumerRef{ref}, 0); !errors.Is(err, queue.ErrNoWork) {
			t.Fatalf("a held job was delivered twice: %v", err)
		}
		if err := d.Ack(ctx); err != nil {
			t.Fatal(err)
		}
		if depth, _ := q.Depth(ctx); depth[ref] != 0 {
			t.Fatalf("depth after ack = %d", depth[ref])
		}
	})

	t.Run("requeue with delay comes back as the next attempt", func(t *testing.T) {
		if err := q.Enqueue(ctx, env("c-work-2"), "p4"); err != nil {
			t.Fatal(err)
		}
		d, err := q.Pull(ctx, []queue.ConsumerRef{ref}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Requeue(ctx, 300*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if _, err := q.Pull(ctx, []queue.ConsumerRef{ref}, 0); !errors.Is(err, queue.ErrNoWork) {
			t.Fatalf("delivered before its delay: %v", err)
		}
		var d2 queue.Delivery
		deadline := time.Now().Add(5 * time.Second)
		for d2 == nil && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
			d2, _ = q.Pull(ctx, []queue.ConsumerRef{ref}, 0)
		}
		if d2 == nil || d2.Attempt() != 2 {
			t.Fatalf("redelivery %v", d2)
		}
		if err := d2.Ack(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a resumed claim (after a restart) still acks the same delivery", func(t *testing.T) {
		if err := q.Enqueue(ctx, env("c-work-3"), "p4"); err != nil {
			t.Fatal(err)
		}
		d, err := q.Pull(ctx, []queue.ConsumerRef{ref}, 0)
		if err != nil {
			t.Fatal(err)
		}
		h := d.Handle()
		r, err := q.Resume(ctx, h, d.Envelope())
		if err != nil {
			t.Fatal(err)
		}
		if err := r.InProgress(ctx); err != nil {
			t.Fatal(err)
		}
		if err := r.Ack(ctx); err != nil {
			t.Fatal(err)
		}
		if depth, _ := q.Depth(ctx); depth[ref] != 0 {
			t.Fatalf("depth after a resumed ack = %d", depth[ref])
		}
		if _, err := q.Resume(ctx, queue.ClaimHandle{JobID: "x"}, d.Envelope()); err == nil {
			t.Fatal("a handle without an ack subject resumed")
		}
	})

	t.Run("a silent worker loses the job after the ack wait", func(t *testing.T) {
		if err := q.Enqueue(ctx, env("c-work-4"), "p4"); err != nil {
			t.Fatal(err)
		}
		d, err := q.Pull(ctx, []queue.ConsumerRef{ref}, 0)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(ackWait + 500*time.Millisecond)
		d2, err := q.Pull(ctx, []queue.ConsumerRef{ref}, 0)
		if err != nil {
			t.Fatalf("not redelivered after the ack wait: %v", err)
		}
		if d2.Attempt() != 2 || d.Handle().AckSubject == d2.Handle().AckSubject {
			t.Fatalf("redelivery attempt %d", d2.Attempt())
		}
		if err := d2.Dead(ctx, "contract test"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("job_state is create-only and fenced by revision", func(t *testing.T) {
		st := queue.JobState{JobID: "c-state-1", Caller: "local", Service: "sys.compute", State: queue.StateSubmitted}
		rev, err := q.CreateState(ctx, st)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := q.CreateState(ctx, st); !errors.Is(err, queue.ErrExists) {
			t.Fatalf("second create: %v, want ErrExists", err)
		}
		st.State = queue.StateClaimed
		rev2, err := q.CASState(ctx, st, rev)
		if err != nil {
			t.Fatal(err)
		}
		st.State = queue.StateSucceeded
		if _, err := q.CASState(ctx, st, rev); !errors.Is(err, queue.ErrRevision) {
			t.Fatalf("a stale revision wrote: %v", err)
		}
		got, gotRev, err := q.State(ctx, "c-state-1")
		if err != nil || got.State != queue.StateClaimed || gotRev != rev2 {
			t.Fatalf("state %+v rev %d err %v", got, gotRev, err)
		}
		if _, _, err := q.State(ctx, "c-none"); !errors.Is(err, queue.ErrNotFound) {
			t.Fatalf("missing key: %v", err)
		}
	})

	t.Run("events come back in order, deduplicated by seq", func(t *testing.T) {
		for _, n := range []int64{1, 2, 2, 3} {
			if err := q.Publish(ctx, &queue.Event{JobID: "c-ev-1", Caller: "local", Seq: n, Type: queue.EvProgress, At: time.Now()}); err != nil {
				t.Fatal(err)
			}
		}
		evs, err := q.Events(ctx, "local", "c-ev-1")
		if err != nil || len(evs) != 3 || evs[0].Seq != 1 || evs[2].Seq != 3 {
			t.Fatalf("events %+v err %v", evs, err)
		}
		if evs, _ := q.Events(ctx, "wtc", "c-ev-1"); len(evs) != 0 {
			t.Fatal("another caller's subject returned events")
		}
	})

	t.Run("control changes reach a watcher", func(t *testing.T) {
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		ch, err := q.WatchControl(wctx)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		if err := q.SetControl(ctx, "c-ctl-1", queue.Control{Cancel: true, By: "test"}); err != nil {
			t.Fatal(err)
		}
		select {
		case c := <-ch:
			if c.JobID != "c-ctl-1" || !c.Control.Cancel {
				t.Fatalf("got %+v", c)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("no control change seen")
		}
		if c, err := q.GetControl(ctx, "c-ctl-1"); err != nil || c.By != "test" {
			t.Fatalf("get %+v %v", c, err)
		}
	})

	t.Run("server records", func(t *testing.T) {
		if err := q.PutServer(ctx, queue.ServerRecord{Key: "CONTRACT", State: "ONLINE", At: time.Now()}); err != nil {
			t.Fatal(err)
		}
		srvs, err := q.Servers(ctx)
		if err != nil || len(srvs) != 1 || srvs[0].Key != "CONTRACT" {
			t.Fatalf("servers %+v err %v", srvs, err)
		}
	})
}
