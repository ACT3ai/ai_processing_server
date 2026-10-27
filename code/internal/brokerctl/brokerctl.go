// Package brokerctl manages this machine's nats-server from our side (pm/task_queue.mdx §8.2, §18): it writes the
// broker's config from this_server.yaml and data/queue/nats/*.tmpl (outside the repo, never hand edited) and checks
// that the running broker is reachable, has JetStream, and is a supported version. Installing nats-server as an OS
// service stays a one-time step per machine (data/queue/broker.yaml lists the command per OS).
package brokerctl

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
	"github.com/ACT3ai/ai_processing_server/internal/queue/natsq"
	"gopkg.in/yaml.v3"
)

// Pin is data/queue/broker.yaml.
type Pin struct {
	SchemaVersion int               `yaml:"schema_version"`
	SupportedLine string            `yaml:"supported_line"`
	MinVersion    string            `yaml:"min_version"`
	Install       map[string]string `yaml:"install"`
}

// LoadPin reads data/queue/broker.yaml.
func LoadPin(dataDir string) (Pin, error) {
	var p Pin
	b, err := os.ReadFile(filepath.Join(dataDir, "queue", "broker.yaml"))
	if err != nil {
		return p, err
	}
	if err := yaml.Unmarshal(b, &p); err != nil {
		return p, fmt.Errorf("data/queue/broker.yaml: %w", err)
	}
	return p, nil
}

// ConfigDir is where generated broker files live: ~/.act3_processing/nats (or $APS_BROKER_DIR).
func ConfigDir() string {
	if d := os.Getenv("APS_BROKER_DIR"); d != "" {
		return d
	}
	return config.ExpandHome("~/.act3_processing/nats")
}

type tmplData struct {
	ServerName, Host, StoreDir, HubURL, Credentials, Domain string
	Port, MonitorPort                                       int
	MaxFileStore                                            int64
}

// WriteConfig renders data/queue/nats/<role>.conf.tmpl into ConfigDir()/<role>.conf and returns its path.
func WriteConfig(ts *config.ThisServer, dataDir string) (string, error) {
	q := ts.Queue
	role := q.Role
	if role != "standalone" && role != "worker" {
		return "", fmt.Errorf("queue.role %q: broker config exists for standalone and worker", role)
	}
	u, err := url.Parse(q.BrokerURL)
	if err != nil || u.Hostname() == "" {
		return "", fmt.Errorf("queue.broker_url %q is not a URL like nats://127.0.0.1:4222", q.BrokerURL)
	}
	port := 4222
	if p := u.Port(); p != "" {
		if port, err = strconv.Atoi(p); err != nil {
			return "", fmt.Errorf("queue.broker_url port: %w", err)
		}
	}
	if role == "worker" && q.HubURL == "" {
		return "", fmt.Errorf("queue.role worker needs queue.hub_url (the hub's leaf-node URL)")
	}
	maxGB := q.StoreMaxGB
	if maxGB <= 0 {
		maxGB = 20
	}
	d := tmplData{ServerName: "aps_" + strings.ToLower(ts.Server.Key), Host: u.Hostname(), Port: port,
		MonitorPort: 8222 + (port - 4222), StoreDir: config.ExpandHome(q.StoreDir), HubURL: q.HubURL,
		Credentials: config.ExpandHome(q.CredentialsFile), Domain: strings.ToLower(ts.Server.Key),
		MaxFileStore: int64(maxGB * (1 << 30))}
	src := filepath.Join(dataDir, "queue", "nats", role+".conf.tmpl")
	raw, err := os.ReadFile(src)
	if err != nil {
		return "", err
	}
	t, err := template.New(role).Option("missingkey=error").Parse(string(raw))
	if err != nil {
		return "", fmt.Errorf("%s: %w", src, err)
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, d); err != nil {
		return "", fmt.Errorf("%s: %w", src, err)
	}
	dir := ConfigDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	if err := os.MkdirAll(d.StoreDir, 0o700); err != nil {
		return "", err
	}
	out := filepath.Join(dir, role+".conf")
	if err := os.WriteFile(out, buf.Bytes(), 0o600); err != nil {
		return "", err
	}
	return out, nil
}

// Check connects to the configured broker and reports its status plus warnings (version line).
func Check(ctx context.Context, ts *config.ThisServer, pin Pin) (queue.Status, []string, error) {
	q, err := natsq.Open(ctx, natsq.Options{URL: ts.Queue.BrokerURL, CredentialsFile: config.ExpandHome(ts.Queue.CredentialsFile),
		Name: "ai_processing_server broker check", AckWait: ts.Continuity.AckWait.Or(0)})
	if err != nil {
		return queue.Status{URL: ts.Queue.BrokerURL, Error: err.Error()}, nil, err
	}
	defer q.Close()
	st := q.Status(ctx)
	var warns []string
	v := strings.TrimPrefix(st.ServerVersion, "v")
	if pin.MinVersion != "" && !natsq.VersionAtLeast(v, pin.MinVersion) {
		return st, warns, fmt.Errorf("nats-server %s is older than %s (data/queue/broker.yaml)", v, pin.MinVersion)
	}
	line := ts.Queue.BrokerVersion
	if line == "" {
		line = pin.SupportedLine
	}
	if line != "" && !strings.HasPrefix(v, line+".") && v != line {
		warns = append(warns, fmt.Sprintf("nats-server %s is not the supported %s.x line", v, line))
	}
	return st, warns, nil
}
