package natsq

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/queue"
	"github.com/ACT3ai/ai_processing_server/internal/queue/queuetest"
)

// startBroker runs a real nats-server process with JetStream, as production does (task_queue.mdx §21.2: never
// embedded). Tests skip when nats-server is not on PATH.
func startBroker(t *testing.T) string {
	t.Helper()
	bin, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server not on PATH (install: brew install nats-server, or see pm/task_queue.mdx §18)")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	dir := t.TempDir()
	cmd := exec.Command(bin, "-a", "127.0.0.1", "-p", fmt.Sprint(port), "-js", "-sd", filepath.Join(dir, "js"))
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if os.Getenv("NATSQ_TEST_VERBOSE") == "" {
		cmd.Stdout, cmd.Stderr = nil, nil
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	for range 100 {
		if c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port)); err == nil {
			c.Close()
			return url
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("nats-server did not start")
	return ""
}

func open(t *testing.T, url string, ackWait time.Duration) *Queue {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	q, err := Open(ctx, Options{URL: url, Name: t.Name(), AckWait: ackWait})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

func TestNATSContract(t *testing.T) {
	queuetest.RunContract(t, func(t *testing.T) queue.Queue { return open(t, startBroker(t), time.Minute) })
}

// A claim not extended within the ack wait goes to another worker (§16.1); a claim extended does not.
func TestAckWaitRedelivers(t *testing.T) {
	url := startBroker(t)
	a, b := open(t, url, 2*time.Second), open(t, url, 2*time.Second)
	ctx := context.Background()
	lane := []queue.ConsumerRef{{Band: "p3", Caller: "local", Service: "sys.compute"}}
	for _, id := range []string{"kept", "lost"} {
		if _, err := a.Submit(ctx, &queue.Envelope{JobID: id, Caller: "local", Service: "sys.compute", Priority: 30}); err != nil {
			t.Fatal(err)
		}
	}
	d1, _ := a.Pull(ctx, lane, time.Second)
	d2, _ := a.Pull(ctx, lane, time.Second)
	if d1 == nil || d2 == nil {
		t.Fatal("two jobs not delivered")
	}
	kept, lost := d1, d2
	if d1.Envelope().JobID == "lost" {
		kept, lost = d2, d1
	}
	_ = lost // worker A "dies" with this one: no InProgress, no ack
	stop := time.After(3500 * time.Millisecond)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	var got queue.Delivery
	for got == nil {
		select {
		case <-tick.C:
			_ = kept.InProgress(ctx)
			got, _ = b.Pull(ctx, lane, 0)
		case <-stop:
			t.Fatal("job was not redelivered after the ack wait")
		}
	}
	if got.Envelope().JobID != "lost" || got.Attempt() != 2 {
		t.Fatalf("redelivered %s attempt %d, want lost attempt 2", got.Envelope().JobID, got.Attempt())
	}
	if again, _ := b.Pull(ctx, lane, 500*time.Millisecond); again != nil {
		t.Fatalf("job %s with an extended claim was redelivered", again.Envelope().JobID)
	}
	_ = got.Ack(ctx)
	_ = kept.Ack(ctx)
}

// A claim saved to disk by one process is settled by another connection (§9.4).
func TestResumeFromAnotherConnection(t *testing.T) {
	url := startBroker(t)
	ctx := context.Background()
	lane := []queue.ConsumerRef{{Band: "p3", Caller: "local", Service: "sys.compute"}}
	first := open(t, url, time.Minute)
	_, _ = first.Submit(ctx, &queue.Envelope{JobID: "r1", Caller: "local", Service: "sys.compute", Priority: 30})
	d, _ := first.Pull(ctx, lane, time.Second)
	h := d.Handle()
	_ = first.Close() // the old controller exits; the broker keeps the claim
	second := open(t, url, time.Minute)
	r, err := second.Resume(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Requeue(ctx, 0); err != nil {
		t.Fatal(err)
	}
	again, _ := second.Pull(ctx, lane, 2*time.Second)
	if again == nil || again.Envelope().JobID != "r1" || again.Attempt() != 2 {
		t.Fatalf("after resume+requeue got %v", again)
	}
	_ = again.Ack(ctx)
}

func TestOpenWithoutBroker(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	_, err := Open(context.Background(), Options{URL: fmt.Sprintf("nats://127.0.0.1:%d", port), ConnectTimeout: 500 * time.Millisecond})
	if err == nil {
		t.Fatal("Open succeeded with no broker")
	}
}

func TestParseAckSubject(t *testing.T) {
	v1, err := ParseAckSubject("$JS.ACK.WORK.w_p3_local_sys_compute.2.17.5.1730000000000000000.0")
	if err != nil || v1.Stream != "WORK" || v1.Delivered != 2 || v1.StreamSeq != 17 {
		t.Fatalf("v1 = %+v, %v", v1, err)
	}
	v2, err := ParseAckSubject("$JS.ACK.hub.ACCHASH.WORK.w_p0_act3_x.1.42.9.1730000000000000000.3.abc")
	if err != nil || v2.Stream != "WORK" || v2.Consumer != "w_p0_act3_x" || v2.StreamSeq != 42 {
		t.Fatalf("v2 = %+v, %v", v2, err)
	}
	if _, err := ParseAckSubject("mem.3"); err == nil {
		t.Fatal("accepted a non-ack subject")
	}
}

func TestVersionAtLeast(t *testing.T) {
	for _, c := range []struct {
		have, want string
		ok         bool
	}{{"2.15.2", "2.10.0", true}, {"2.10.0", "2.10.0", true}, {"2.9.25", "2.10.0", false}, {"v2.11.0-beta", "2.10.0", true}, {"", "2.10.0", false}} {
		if VersionAtLeast(c.have, c.want) != c.ok {
			t.Errorf("VersionAtLeast(%q, %q) != %v", c.have, c.want, c.ok)
		}
	}
}
