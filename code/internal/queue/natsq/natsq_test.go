package natsq

import (
	"context"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/queue/queuetest"
)

// TestNATSContract runs the queue contract against a real nats-server process (pm/task_queue.mdx §21.2: tests start
// the broker as its own process, never embedded). Skipped when nats-server is not installed.
func TestNATSContract(t *testing.T) {
	bin, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server not on PATH (just broker-install, or brew install nats-server)")
	}
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	cmd := exec.Command(bin, "-js", "-a", "127.0.0.1", "-p", strconv.Itoa(port), "-sd", t.TempDir())
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	for range 100 {
		if c, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port)); err == nil {
			c.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	ackWait := 2 * time.Second
	q, err := Open(context.Background(), Options{URL: "nats://127.0.0.1:" + strconv.Itoa(port), Name: "contract", AckWait: ackWait,
		Streams: []StreamDef{
			{Name: "SUBMIT", Subjects: []string{"submit.>"}, Retention: "workqueue", DuplicateWindow: time.Hour},
			{Name: "WORK", Subjects: []string{"work.>"}, Retention: "workqueue", DuplicateWindow: time.Hour},
			{Name: "EVENTS", Subjects: []string{"evt.>"}, Retention: "limits", DuplicateWindow: time.Hour},
			{Name: "DEAD", Subjects: []string{"dead.>"}, Retention: "limits"},
		},
		KV: []KVDef{{Bucket: "servers", TTL: time.Minute}, {Bucket: "job_state", History: 5}, {Bucket: "job_control"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	queuetest.Contract(t, q, ackWait)
}

func TestOpenFailsFastWithoutABroker(t *testing.T) {
	start := time.Now()
	_, err := Open(context.Background(), Options{URL: "nats://127.0.0.1:1"})
	if err == nil {
		t.Fatal("connected to nothing")
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("took %s to fail", time.Since(start))
	}
}
