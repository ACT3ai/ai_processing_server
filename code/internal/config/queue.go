package config

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"
)

// QueueSettings is this_server.yaml `queue:` (pm/task_queue.mdx §17.1).
type QueueSettings struct {
	Role            string  `yaml:"role" json:"role"`             // standalone | worker | hub (worker and hub: phase Q3)
	BrokerURL       string  `yaml:"broker_url" json:"broker_url"` // this machine's own nats-server service
	HubURL          string  `yaml:"hub_url" json:"hub_url,omitempty"`
	CredentialsFile string  `yaml:"credentials_file" json:"-"`
	BrokerVersion   string  `yaml:"broker_version" json:"broker_version,omitempty"`
	StoreDir        string  `yaml:"store_dir" json:"store_dir"`
	StoreMaxGB      float64 `yaml:"store_max_gb" json:"store_max_gb"`
	Advertise       string  `yaml:"advertise" json:"advertise,omitempty"`
}

// Continuity is this_server.yaml `continuity:` (§9).
type Continuity struct {
	AckWait        Duration `yaml:"ack_wait" json:"ack_wait"`               // must exceed the slowest controller restart
	Heartbeat      Duration `yaml:"heartbeat" json:"heartbeat"`             // InProgress interval
	TaskhostClaims bool     `yaml:"taskhost_claims" json:"taskhost_claims"` // self-claim mode: phase Q3
	KeepSnapshots  int      `yaml:"keep_snapshots" json:"keep_snapshots"`
	Node           string   `yaml:"node" json:"node,omitempty"` // pinned Node for TypeScript task hosts
}

// CallerSetting is one entry of this_server.yaml `callers:` (§14.5, §17.1).
type CallerSetting struct {
	Enabled            bool     `yaml:"enabled" json:"enabled"`
	Mode               string   `yaml:"mode,omitempty" json:"mode,omitempty"` // local only: first | share | idle_only
	Share              int      `yaml:"share" json:"share"`
	ReserveSlots       int      `yaml:"reserve_slots,omitempty" json:"reserve_slots,omitempty"`
	YieldToInteractive bool     `yaml:"yield_to_interactive,omitempty" json:"yield_to_interactive,omitempty"`
	Accept             []string `yaml:"accept,omitempty" json:"accept,omitempty"`
	Deny               []string `yaml:"deny,omitempty" json:"deny,omitempty"`
}

func defaultQueue() QueueSettings {
	return QueueSettings{Role: "standalone", BrokerURL: "nats://127.0.0.1:4244", StoreDir: "~/.act3_processing/nats/jetstream", StoreMaxGB: 20}
}

func defaultContinuity() Continuity {
	return Continuity{AckWait: Duration{5 * time.Minute}, Heartbeat: Duration{20 * time.Second}, KeepSnapshots: 5}
}

func defaultCallers() map[string]CallerSetting {
	return map[string]CallerSetting{"local": {Enabled: true, Mode: "first"}}
}

// validateQueue adds the queue sections' errors to Validate.
func validateQueue(ts *ThisServer, cat *Catalog) (errs, warns []string) {
	switch ts.Queue.Role {
	case "standalone":
	case "worker", "hub":
		errs = append(errs, fmt.Sprintf("queue.role %q arrives in phase Q3 (pm/task_queue.mdx §22); use standalone", ts.Queue.Role))
	default:
		errs = append(errs, fmt.Sprintf("queue.role %q must be standalone, worker or hub", ts.Queue.Role))
	}
	if ts.Queue.BrokerURL == "" {
		errs = append(errs, "queue.broker_url is required (default nats://127.0.0.1:4244)")
	}
	if ts.Continuity.AckWait.Duration < 2*time.Second {
		errs = append(errs, "continuity.ack_wait must be at least 2s (default 5m)")
	}
	if ts.Continuity.Heartbeat.Duration <= 0 || ts.Continuity.Heartbeat.Duration*2 > ts.Continuity.AckWait.Duration {
		errs = append(errs, "continuity.heartbeat must be positive and at most half of continuity.ack_wait")
	}
	if ts.Continuity.TaskhostClaims {
		warns = append(warns, "continuity.taskhost_claims (self-claim mode) arrives in phase Q3; ignored")
	}
	for id, c := range ts.Callers {
		if cat != nil && cat.Queue != nil && cat.Queue.Caller(id) == nil {
			warns = append(warns, fmt.Sprintf("callers.%s is not in data/queue/callers.yaml; ignored", id))
		}
		if id == "local" && c.Mode != "" && !slices.Contains([]string{"first", "share", "idle_only"}, c.Mode) {
			errs = append(errs, "callers.local.mode must be first, share or idle_only")
		}
		if c.Share < 0 || c.Share > 100 {
			errs = append(errs, fmt.Sprintf("callers.%s.share must be within 0..100", id))
		}
	}
	return errs, warns
}

// ---- data/queue/ ----

// QueueCatalog is the checked-in data/queue/ directory (§17.2).
type QueueCatalog struct {
	Streams    StreamsFile
	Bands      BandsFile
	Callers    CallersFile
	Categories map[string][]string // category → service ids
	Broker     BrokerFile
	Dir        string
}

type StreamsFile struct {
	SchemaVersion int `yaml:"schema_version"`
	Streams       []struct {
		Name            string        `yaml:"name"`
		Subjects        []string      `yaml:"subjects"`
		Retention       string        `yaml:"retention"`
		MaxAge          time.Duration `yaml:"max_age"`
		DuplicateWindow time.Duration `yaml:"duplicate_window"`
	} `yaml:"streams"`
	KV []struct {
		Bucket  string        `yaml:"bucket"`
		TTL     time.Duration `yaml:"ttl"`
		History int           `yaml:"history"`
	} `yaml:"kv"`
	Consumers struct {
		AckWait    time.Duration `yaml:"ack_wait"`
		MaxDeliver int           `yaml:"max_deliver"`
	} `yaml:"consumers"`
}

type Band struct {
	Name        string        `yaml:"name"`
	MinPriority int           `yaml:"min_priority"`
	Weight      int           `yaml:"weight"`
	MaxWait     time.Duration `yaml:"max_wait"`
}

type BandsFile struct {
	SchemaVersion int    `yaml:"schema_version"`
	Bands         []Band `yaml:"bands"`
	MaxPromotions int    `yaml:"max_promotions"`
}

type CallerDef struct {
	ID          string `yaml:"id"`
	DisplayName string `yaml:"display_name"`
	MaxBand     string `yaml:"max_band"`
}

type CallersFile struct {
	SchemaVersion int         `yaml:"schema_version"`
	Callers       []CallerDef `yaml:"callers"`
}

type BrokerDownload struct {
	File   string `yaml:"file"`
	SHA256 string `yaml:"sha256"`
}

type BrokerFile struct {
	SchemaVersion int                       `yaml:"schema_version"`
	Supported     string                    `yaml:"supported"`
	Pinned        string                    `yaml:"pinned"`
	Port          int                       `yaml:"port"`
	MonitorPort   int                       `yaml:"monitor_port"`
	Downloads     map[string]BrokerDownload `yaml:"downloads"`
	BaseURL       string                    `yaml:"base_url"`
}

// Caller returns the caller definition, or nil.
func (q *QueueCatalog) Caller(id string) *CallerDef {
	for i := range q.Callers.Callers {
		if q.Callers.Callers[i].ID == id {
			return &q.Callers.Callers[i]
		}
	}
	return nil
}

// CategoryOf returns the category a service belongs to (`system` when it is in none).
func (q *QueueCatalog) CategoryOf(service string) string {
	for cat, ids := range q.Categories {
		if slices.Contains(ids, service) {
			return cat
		}
	}
	return "system"
}

// LoadQueueCatalog reads data/queue/*.yaml.
func LoadQueueCatalog(dataDir string) (*QueueCatalog, error) {
	dir := filepath.Join(dataDir, "queue")
	q := &QueueCatalog{Dir: dir}
	var cats struct {
		SchemaVersion int                 `yaml:"schema_version"`
		Categories    map[string][]string `yaml:"categories"`
	}
	for name, v := range map[string]any{"streams.yaml": &q.Streams, "bands.yaml": &q.Bands, "callers.yaml": &q.Callers,
		"categories.yaml": &cats, "broker.yaml": &q.Broker} {
		if err := readYAML(filepath.Join(dir, name), v); err != nil {
			return nil, err
		}
	}
	q.Categories = cats.Categories
	if len(q.Bands.Bands) == 0 || len(q.Streams.Streams) == 0 {
		return nil, fmt.Errorf("%s: bands.yaml and streams.yaml must not be empty", dir)
	}
	for i := 1; i < len(q.Bands.Bands); i++ {
		if q.Bands.Bands[i].MinPriority >= q.Bands.Bands[i-1].MinPriority {
			return nil, fmt.Errorf("%s/bands.yaml: bands must be listed from most urgent (highest min_priority) down", dir)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "nats", "standalone.conf.tmpl")); err != nil {
		return nil, fmt.Errorf("%s/nats/standalone.conf.tmpl: %w", dir, err)
	}
	return q, nil
}
