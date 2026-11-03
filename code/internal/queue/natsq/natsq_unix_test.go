//go:build !windows

package natsq

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

// brokerProc is a nats-server that a test can stop and start again on the same port and store, like a broker
// restart or a power cut on a worker.
type brokerProc struct {
	t        *testing.T
	bin, dir string
	port     int
	cmd      *exec.Cmd
	syncAll  bool // jetstream sync_interval: always, as data/queue/nats/*.conf.tmpl configure production
}

func newBrokerProc(t *testing.T) *brokerProc {
	t.Helper()
	bin, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server not on PATH")
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	b := &brokerProc{t: t, bin: bin, dir: t.TempDir(), port: port}
	b.start()
	t.Cleanup(b.stop)
	return b
}

func (b *brokerProc) url() string { return fmt.Sprintf("nats://127.0.0.1:%d", b.port) }

func (b *brokerProc) start() {
	b.cmd = exec.Command(b.bin, "-a", "127.0.0.1", "-p", fmt.Sprint(b.port), "-js", "-sd", filepath.Join(b.dir, "js"))
	if b.syncAll {
		conf := filepath.Join(b.dir, "nats.conf")
		body := fmt.Sprintf("host: 127.0.0.1\nport: %d\njetstream {\n  store_dir: %q\n  sync_interval: always\n}\n", b.port, filepath.Join(b.dir, "js"))
		if err := os.WriteFile(conf, []byte(body), 0o600); err != nil {
			b.t.Fatal(err)
		}
		b.cmd = exec.Command(b.bin, "-c", conf)
	}
	if err := b.cmd.Start(); err != nil {
		b.t.Fatal(err)
	}
	for range 100 {
		if c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", b.port)); err == nil {
			c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	b.t.Fatal("nats-server did not start")
}

func (b *brokerProc) stop() {
	if b.cmd != nil && b.cmd.Process != nil {
		_ = b.cmd.Process.Kill()
		_ = b.cmd.Wait()
		b.cmd = nil
	}
}

// A job that finishes while the broker is not answering (stalled: disk I/O, a long pause): the Ack fails, and the
// retry once the broker answers again really acks (D-026). Before, the failed Ack marked the delivery done, every retry
// was a silent no-op, and the job was redelivered and run a second time after the ack wait.
//
// (A hard broker restart is different: nats-server loses the claim's pending state, the old ack subject never
// answers, and the job is redelivered; the scheduler then settles it from job_state or its outbox without running it
// again. scheduler.TestSettlementSurvivesBrokerOutageAndRestart covers that path.)
func TestAckRetriedAfterBrokerStall(t *testing.T) {
	b := newBrokerProc(t)
	q := open(t, b.url(), time.Minute)
	ctx := context.Background()
	lane := []queue.ConsumerRef{{Band: "p3", Caller: "local", Service: "sys.compute"}}
	if _, err := q.Submit(ctx, &queue.Envelope{JobID: "late", Caller: "local", Service: "sys.compute", Priority: 30}); err != nil {
		t.Fatal(err)
	}
	d, err := q.Pull(ctx, lane, 2*time.Second)
	if err != nil || d == nil {
		t.Fatalf("pull: %v %v", d, err)
	}
	if err := b.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	if err := d.Ack(ctx); err == nil {
		t.Fatal("Ack with the broker stalled reported success")
	}
	if err := b.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	var ackErr error
	for range 10 {
		if ackErr = d.Ack(ctx); ackErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if ackErr != nil {
		t.Fatalf("Ack after the broker answered again: %v", ackErr)
	}
	waiting, err := q.waiting(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n := waiting[lane[0].Subject()]; n != 0 {
		t.Fatalf("the acked job is still in WORK (%d message): the retried Ack was a no-op", n)
	}
}

// The broker is killed right after it acknowledged 20 submissions: all 20 are there after the restart. The broker runs
// with sync_interval: always, as data/queue/nats/*.conf.tmpl configure every broker since D-026 (nats-server's default
// fsyncs every 2 minutes, so on a real power cut a submission acked just before it could vanish). A SIGKILL keeps the
// OS page cache, so this test covers the broker-crash path only; it cannot simulate the power loss itself.
//
// The claim's delivery state is not covered: nats-server writes consumer state asynchronously, so after a hard kill a
// job being rendered can be handed out again as a fresh delivery. On a standalone broker it comes back to the same
// server, which keeps running it (scheduler acceptDelivery); in a fleet the hub cluster's replicas keep the claim,
// and job_state fencing makes sure only one result is reported either way.
func TestSubmittedJobSurvivesAHardBrokerKill(t *testing.T) {
	bin, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server not on PATH")
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	b := &brokerProc{t: t, bin: bin, dir: t.TempDir(), port: port, syncAll: true}
	b.start()
	t.Cleanup(b.stop)
	q := open(t, b.url(), time.Minute)
	ctx := context.Background()
	lane := []queue.ConsumerRef{{Band: "p3", Caller: "local", Service: "sys.compute"}}
	for i := range 20 {
		if _, err := q.Submit(ctx, &queue.Envelope{JobID: fmt.Sprintf("pc%d", i), Caller: "local", Service: "sys.compute", Priority: 30}); err != nil {
			t.Fatal(err)
		}
	}
	b.stop() // SIGKILL right after the acks: nothing flushed but what was synced
	b.start()
	for i := 0; !q.nc.IsConnected() && i < 200; i++ {
		time.Sleep(50 * time.Millisecond)
	}
	other := open(t, b.url(), time.Minute)
	got := 0
	for {
		d, err := other.Pull(ctx, lane, time.Second)
		if err != nil || d == nil {
			break
		}
		got++
		_ = d.Ack(ctx)
	}
	if got != 20 {
		t.Fatalf("%d of 20 acknowledged submissions survived the broker's power cut", got)
	}
}
