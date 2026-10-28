package brokerctl

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/config"
)

func TestWriteConfig(t *testing.T) {
	t.Setenv("APS_BROKER_DIR", t.TempDir())
	data := filepath.Join("..", "..", "..", "data")
	ts := &config.ThisServer{Server: config.Identity{Key: "B5090"}, Queue: config.Queue{Role: "standalone",
		BrokerURL: "nats://127.0.0.1:4333", StoreDir: filepath.Join(t.TempDir(), "js"), StoreMaxGB: 2}}
	p, err := WriteConfig(ts, data)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(p)
	for _, want := range []string{`server_name: "aps_b5090"`, "port: 4333", `http: "127.0.0.1:8333"`, "max_file_store: 2147483648", ts.Queue.StoreDir} {
		if !strings.Contains(string(b), want) {
			t.Errorf("standalone.conf lacks %q:\n%s", want, b)
		}
	}
	ts.Queue.Role = "worker"
	if _, err := WriteConfig(ts, data); err == nil {
		t.Fatal("worker without hub_url accepted")
	}
	ts.Queue.HubURL = "tls://queue.act3ai.com:7422"
	p, err = WriteConfig(ts, data)
	if err != nil {
		t.Fatal(err)
	}
	b, _ = os.ReadFile(p)
	if !strings.Contains(string(b), `url: "tls://queue.act3ai.com:7422"`) || !strings.Contains(string(b), `domain: "b5090"`) || strings.Contains(string(b), "credentials:") {
		t.Fatalf("worker.conf wrong:\n%s", b)
	}
	pin, err := LoadPin(data)
	if err != nil || pin.SupportedLine == "" || pin.Install["darwin"] == "" {
		t.Fatalf("pin = %+v, %v", pin, err)
	}
}
