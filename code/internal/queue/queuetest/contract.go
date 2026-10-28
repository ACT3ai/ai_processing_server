package queuetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

// RunContract runs the behavior every queue.Queue must have (pm/task_queue.mdx §13, §16). open returns a fresh,
// empty queue for each subtest.
func RunContract(t *testing.T, open func(t *testing.T) queue.Queue) {
	ctx := context.Background()
	lane := func(band, svc string) queue.ConsumerRef {
		return queue.ConsumerRef{Band: band, Caller: "local", Service: svc}
	}
	env := func(id, svc string, prio int) *queue.Envelope {
		return &queue.Envelope{JobID: id, Caller: "local", Service: svc, Priority: prio, Params: []byte(`{"op":"sleep"}`)}
	}
	pull := func(t *testing.T, q queue.Queue, order ...queue.ConsumerRef) queue.Delivery {
		t.Helper()
		d, err := q.Pull(ctx, order, 2*time.Second)
		if err != nil {
			t.Fatalf("pull: %v", err)
		}
		return d
	}

	t.Run("submit_pull_ack_once", func(t *testing.T) {
		q := open(t)
		ack, err := q.Submit(ctx, env("job-1", "sys.compute", 30))
		if err != nil {
			t.Fatal(err)
		}
		if ack.Band != "p3" || ack.Subject != "work.p3.local.sys.compute" || ack.Duplicate {
			t.Fatalf("ack = %+v", ack)
		}
		d := pull(t, q, lane("p3", "sys.compute"))
		if d == nil || d.Envelope().JobID != "job-1" || d.Attempt() != 1 {
			t.Fatalf("delivery = %+v", d)
		}
		if string(d.Envelope().Params) != `{"op":"sleep"}` || d.Envelope().Reply.Events != "evt.local.job-1" {
			t.Fatalf("envelope not carried intact: %+v", d.Envelope())
		}
		if err := d.Ack(ctx); err != nil {
			t.Fatal(err)
		}
		if again, _ := q.Pull(ctx, []queue.ConsumerRef{lane("p3", "sys.compute")}, 300*time.Millisecond); again != nil {
			t.Fatalf("acked job delivered again: %s", again.Envelope().JobID)
		}
	})

	t.Run("duplicate_submit_is_noop", func(t *testing.T) {
		q := open(t)
		if _, err := q.Submit(ctx, env("dup-1", "sys.compute", 30)); err != nil {
			t.Fatal(err)
		}
		ack, err := q.Submit(ctx, env("dup-1", "sys.compute", 30))
		if err != nil || !ack.Duplicate {
			t.Fatalf("second submit = %+v, %v; want duplicate", ack, err)
		}
		d := pull(t, q, lane("p3", "sys.compute"))
		_ = d.Ack(ctx)
		if again, _ := q.Pull(ctx, []queue.ConsumerRef{lane("p3", "sys.compute")}, 300*time.Millisecond); again != nil {
			t.Fatal("duplicate submission was stored twice")
		}
	})

	t.Run("invalid_envelope_rejected", func(t *testing.T) {
		q := open(t)
		for _, e := range []*queue.Envelope{
			{JobID: "", Caller: "local", Service: "sys.compute"},
			{JobID: "a.b", Caller: "local", Service: "sys.compute"},
			{JobID: "ok", Caller: "local", Service: "sys.*"},
			{JobID: "ok", Caller: "local", Service: "sys.compute", IssuedAt: time.Now(), ExpiresAt: time.Now().Add(8 * 24 * time.Hour)},
		} {
			if _, err := q.Submit(ctx, e); !errors.Is(err, queue.ErrInvalid) {
				t.Errorf("Submit(%+v) = %v, want ErrInvalid", e, err)
			}
		}
	})

	t.Run("pull_follows_given_order", func(t *testing.T) {
		q := open(t)
		_, _ = q.Submit(ctx, env("low", "sys.compute", 0))
		_, _ = q.Submit(ctx, env("high", "sys.compute", 200))
		order := []queue.ConsumerRef{lane("p0", "sys.compute"), lane("p4", "sys.compute")}
		first, second := pull(t, q, order...), pull(t, q, order...)
		if first == nil || second == nil || first.Envelope().JobID != "high" || second.Envelope().JobID != "low" {
			t.Fatalf("order wrong: %v then %v", first, second)
		}
		_, _ = first.Ack(ctx), second.Ack(ctx)
		if d, _ := q.Pull(ctx, []queue.ConsumerRef{lane("p3", "other.service")}, 0); d != nil {
			t.Fatal("pulled from a lane with no work")
		}
	})

	t.Run("held_job_not_given_twice", func(t *testing.T) {
		q := open(t)
		_, _ = q.Submit(ctx, env("held", "sys.compute", 30))
		d := pull(t, q, lane("p3", "sys.compute"))
		if other, _ := q.Pull(ctx, []queue.ConsumerRef{lane("p3", "sys.compute")}, 300*time.Millisecond); other != nil {
			t.Fatal("a held job was delivered to a second puller")
		}
		_ = d.InProgress(ctx)
		_ = d.Ack(ctx)
	})

	t.Run("requeue_redelivers_with_next_attempt", func(t *testing.T) {
		q := open(t)
		_, _ = q.Submit(ctx, env("rq", "sys.compute", 30))
		d := pull(t, q, lane("p3", "sys.compute"))
		if err := d.Requeue(ctx, 200*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if early, _ := q.Pull(ctx, []queue.ConsumerRef{lane("p3", "sys.compute")}, 0); early != nil {
			t.Fatal("requeued job came back before its delay")
		}
		d2 := pull(t, q, lane("p3", "sys.compute"))
		if d2 == nil || d2.Attempt() != 2 {
			t.Fatalf("redelivery = %v, want attempt 2", d2)
		}
		_ = d2.Ack(ctx)
	})

	t.Run("dead_stops_delivery", func(t *testing.T) {
		q := open(t)
		_, _ = q.Submit(ctx, env("dead", "sys.compute", 30))
		d := pull(t, q, lane("p3", "sys.compute"))
		if err := d.Dead(ctx, "invalid_input"); err != nil {
			t.Fatal(err)
		}
		if again, _ := q.Pull(ctx, []queue.ConsumerRef{lane("p3", "sys.compute")}, 300*time.Millisecond); again != nil {
			t.Fatal("dead job delivered again")
		}
	})

	t.Run("fencing_token", func(t *testing.T) {
		q := open(t)
		if _, _, err := q.State(ctx, "f1"); !errors.Is(err, queue.ErrNotFound) {
			t.Fatalf("State of unknown job = %v, want ErrNotFound", err)
		}
		r1, err := q.CASState(ctx, queue.JobState{JobID: "f1", Caller: "local", State: queue.StateClaimed, Attempt: 1, Server: "A"}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := q.CASState(ctx, queue.JobState{JobID: "f1", State: queue.StateClaimed}, 0); !errors.Is(err, queue.ErrFenced) {
			t.Fatalf("second create = %v, want ErrFenced", err)
		}
		r2, err := q.CASState(ctx, queue.JobState{JobID: "f1", Caller: "local", State: queue.StateClaimed, Attempt: 2, Server: "B"}, r1)
		if err != nil {
			t.Fatal(err)
		}
		// A stale worker A still holding r1 must not be able to report a result.
		if _, err := q.CASState(ctx, queue.JobState{JobID: "f1", State: queue.StateSucceeded, Server: "A"}, r1); !errors.Is(err, queue.ErrFenced) {
			t.Fatalf("stale CAS = %v, want ErrFenced", err)
		}
		s, rev, err := q.State(ctx, "f1")
		if err != nil || rev != r2 || s.Server != "B" || s.Attempt != 2 {
			t.Fatalf("State = %+v rev %d (want rev %d), %v", s, rev, r2, err)
		}
	})

	t.Run("events_in_order_and_idempotent", func(t *testing.T) {
		q := open(t)
		for i, typ := range []string{"claimed", "stage", "progress", "succeeded"} {
			ev := &queue.Event{JobID: "e1", Caller: "local", Seq: int64(i + 1), Type: typ}
			if err := q.Publish(ctx, ev); err != nil {
				t.Fatal(err)
			}
		}
		_ = q.Publish(ctx, &queue.Event{JobID: "e1", Caller: "local", Seq: 4, Type: "succeeded"}) // repeated final event
		evs, err := q.Events(ctx, "local", "e1")
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) != 4 || evs[0].Type != "claimed" || evs[3].Type != "succeeded" || !evs[3].Final() {
			t.Fatalf("events = %+v", evs)
		}
		if other, _ := q.Events(ctx, "wtc", "e1"); len(other) != 0 {
			t.Fatal("another caller's events were readable")
		}
	})

	t.Run("control_watch", func(t *testing.T) {
		q := open(t)
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		ch, err := q.WatchControl(wctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		if err := q.SetControl(ctx, "c1", queue.Control{Cancel: true, By: "test"}); err != nil {
			t.Fatal(err)
		}
		select {
		case c := <-ch:
			if c.JobID != "c1" || !c.Cancel || c.By != "test" {
				t.Fatalf("change = %+v", c)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("no control change seen")
		}
		if c, _ := q.GetControl(ctx, "c1"); !c.Cancel {
			t.Fatal("GetControl lost the cancel flag")
		}
	})

	t.Run("resume_keeps_claim", func(t *testing.T) {
		q := open(t)
		_, _ = q.Submit(ctx, env("res", "sys.compute", 30))
		d := pull(t, q, lane("p3", "sys.compute"))
		d.SetFence(7)
		h := d.Handle()
		if h.FenceRev != 7 || h.AckSubject == "" {
			t.Fatalf("handle = %+v", h)
		}
		r, err := q.Resume(ctx, h)
		if err != nil {
			t.Fatal(err)
		}
		if r.Envelope().JobID != "res" || r.Handle().FenceRev != 7 {
			t.Fatalf("resumed = %+v", r.Handle())
		}
		if err := r.Ack(ctx); err != nil {
			t.Fatal(err)
		}
		if again, _ := q.Pull(ctx, []queue.ConsumerRef{lane("p3", "sys.compute")}, 300*time.Millisecond); again != nil {
			t.Fatal("job acked through a resumed claim was delivered again")
		}
		if _, err := q.Resume(ctx, h); err == nil {
			t.Fatal("resuming a finished claim should fail")
		}
	})

	t.Run("servers_and_status", func(t *testing.T) {
		q := open(t)
		if err := q.PutServer(ctx, queue.ServerRecord{Key: "TEST1", Role: "standalone", Offers: map[string]int{"sys.compute": 2}}); err != nil {
			t.Fatal(err)
		}
		recs, err := q.Servers(ctx)
		if err != nil || len(recs) != 1 || recs[0].Offers["sys.compute"] != 2 {
			t.Fatalf("Servers = %+v, %v", recs, err)
		}
		_, _ = q.Submit(ctx, env("st", "sys.compute", 30))
		st := q.Status(ctx)
		if !st.Connected || !st.JetStream || st.Waiting["work.p3.local.sys.compute"] != 1 {
			t.Fatalf("Status = %+v", st)
		}
	})
}
