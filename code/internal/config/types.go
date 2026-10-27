// Package config loads and validates this_server.yaml (owner-edited, per machine) and the checked-in
// data/ catalogs (spec §5, §6, §8). The only code that writes this_server.yaml is InitThisServer's create branch.
package config

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"gopkg.in/yaml.v3"
)

// SchemaVersion is the this_server.yaml / catalog major version this binary understands.
const SchemaVersion = 1

// ThisServer is data/git_ignore/this_server.yaml.
type ThisServer struct {
	SchemaVersion int                        `yaml:"schema_version" json:"schema_version"`
	CreatedAt     string                     `yaml:"created_at" json:"created_at"`
	CreatedBy     string                     `yaml:"created_by" json:"created_by"`
	Server        Identity                   `yaml:"server" json:"server"`
	Hardware      *hardware.Facts            `yaml:"hardware" json:"hardware"`
	Engines       map[string]string          `yaml:"engines" json:"engines,omitempty"`
	Services      []ServiceSetting           `yaml:"services" json:"services"`
	Platforms     map[string]PlatformSetting `yaml:"platforms" json:"platforms"`
	Limits        Limits                     `yaml:"limits" json:"limits"`
	Paths         Paths                      `yaml:"paths" json:"paths"`
	LocalAPI      LocalAPI                   `yaml:"local_api" json:"local_api"`
	Queue         Queue                      `yaml:"queue" json:"queue"`
	Continuity    Continuity                 `yaml:"continuity" json:"continuity"`
	Callers       map[string]CallerSetting   `yaml:"callers" json:"callers"`
	Models        ModelRules                 `yaml:"models" json:"models"`
	Debug         Debug                      `yaml:"debug" json:"debug"`
}

// Queue is the connection to the task system: NATS JetStream in its own nats-server process (pm/task_queue.mdx
// §8.2, §17.1, D-014). A file without a queue section has it off, so existing machines keep running direct mode.
type Queue struct {
	Enabled         bool    `yaml:"enabled" json:"enabled"`
	Role            string  `yaml:"role" json:"role"`             // standalone | worker | hub
	BrokerURL       string  `yaml:"broker_url" json:"broker_url"` // this machine's own nats-server
	HubURL          string  `yaml:"hub_url" json:"hub_url"`       // used by broker-config for a worker's leaf node
	CredentialsFile string  `yaml:"credentials_file" json:"-"`
	BrokerVersion   string  `yaml:"broker_version" json:"broker_version"` // supported nats-server line; others warn
	StoreDir        string  `yaml:"store_dir" json:"store_dir"`           // JetStream store, outside the repo
	StoreMaxGB      float64 `yaml:"store_max_gb" json:"store_max_gb"`
}

// Continuity holds the claim timing (pm/task_queue.mdx §9).
type Continuity struct {
	AckWait   Duration `yaml:"ack_wait" json:"ack_wait"`   // a claim not extended for this long goes to another worker
	Heartbeat Duration `yaml:"heartbeat" json:"heartbeat"` // InProgress interval per held job
}

// CallerSetting says whether this machine pulls a caller's jobs at all (pm/task_queue.mdx §17.1). Mode is read only
// for the local caller (§14.5). Shares and category accept/deny are build phase Q2.
type CallerSetting struct {
	Enabled bool   `yaml:"enabled" json:"enabled"`
	Mode    string `yaml:"mode,omitempty" json:"mode,omitempty"` // local only: first | share | idle_only
}

// Local job modes (pm/task_queue.mdx §14.5; defaults per machine decided in §24 Q3, D-021).
const (
	LocalFirst    = "first"     // the owner's jobs are pulled before any platform job
	LocalShare    = "share"     // the owner's jobs take their turn inside each band
	LocalIdleOnly = "idle_only" // the owner's jobs run only when no platform job waits
)

// LocalMode is how this machine ranks its owner's queued jobs; share when unset.
func (t *ThisServer) LocalMode() string {
	if m := t.Callers["local"].Mode; m != "" {
		return m
	}
	return LocalShare
}

// ModelRules is the owner's model fence (pm/task_queue.mdx §11.2): deny wins; an empty allow list allows every model.
// Patterns use path.Match syntax ("wan2.*", "seedvr2-*"). Vendor models are refused everywhere (D-021).
type ModelRules struct {
	Allow []string `yaml:"allow" json:"allow"`
	Deny  []string `yaml:"deny" json:"deny"`
}

// Allowed reports whether this machine may run model, and why not. A job with no model is allowed.
func (m ModelRules) Allowed(model string) (bool, string) {
	if model == "" {
		return true, ""
	}
	if strings.HasPrefix(model, "vendor:") {
		return false, "vendor API models never run on processing servers"
	}
	for _, p := range m.Deny {
		if ok, _ := path.Match(p, model); ok {
			return false, "models.deny has " + p
		}
	}
	if len(m.Allow) == 0 {
		return true, ""
	}
	for _, p := range m.Allow {
		if ok, _ := path.Match(p, model); ok {
			return true, ""
		}
	}
	return false, "not in models.allow"
}

// DefaultCallers are the callers pulled when the file has no callers section.
var DefaultCallers = []string{"act3", "wtc", "local"}

// EnabledCallers returns the callers this machine pulls for, sorted.
func (t *ThisServer) EnabledCallers() []string {
	if len(t.Callers) == 0 {
		return append([]string(nil), DefaultCallers...)
	}
	var out []string
	for id, c := range t.Callers {
		if c.Enabled {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

type Identity struct {
	Key         string  `yaml:"key" json:"key"`
	DisplayName string  `yaml:"display_name" json:"display_name"`
	Profile     string  `yaml:"profile" json:"profile"`
	Family      string  `yaml:"family" json:"family"`
	Site        string  `yaml:"site" json:"site"`
	BoundTo     BoundTo `yaml:"bound_to" json:"bound_to"`
}

type BoundTo struct {
	Hostname  string `yaml:"hostname" json:"hostname"`
	MachineID string `yaml:"machine_id" json:"-"`
}

// ServiceSetting is one entry of the owner's allow-list (spec §6.4).
type ServiceSetting struct {
	ID          string `yaml:"id" json:"id"`
	Enabled     bool   `yaml:"enabled" json:"enabled"`
	MaxParallel int    `yaml:"max_parallel,omitempty" json:"max_parallel,omitempty"`
	GPUs        []int  `yaml:"gpus,omitempty" json:"gpus,omitempty"`
	LocalBias   int    `yaml:"local_bias,omitempty" json:"local_bias,omitempty"`
}

type PlatformSetting struct {
	Enabled         bool   `yaml:"enabled" json:"enabled"`
	BaseURL         string `yaml:"base_url" json:"base_url"`
	CredentialsFile string `yaml:"credentials_file" json:"-"`
	Share           int    `yaml:"share" json:"share"`
}

type Limits struct {
	MaxParallelJobs int     `yaml:"max_parallel_jobs" json:"max_parallel_jobs"`
	CPUReservePct   int     `yaml:"cpu_reserve_pct" json:"cpu_reserve_pct"`
	RAMReserveGB    float64 `yaml:"ram_reserve_gb" json:"ram_reserve_gb"`
	VRAMHeadroomGB  float64 `yaml:"vram_headroom_gb" json:"vram_headroom_gb"`
	DiskMinFreeGB   float64 `yaml:"disk_min_free_gb" json:"disk_min_free_gb"`
	GPUTempMaxC     int     `yaml:"gpu_temp_max_c" json:"gpu_temp_max_c"`
	DrainTimeoutS   int     `yaml:"drain_timeout_s" json:"drain_timeout_s"`
}

type Paths struct {
	WorkDir    string  `yaml:"work_dir" json:"work_dir"`
	CacheDir   string  `yaml:"cache_dir" json:"cache_dir"`
	CacheMaxGB float64 `yaml:"cache_max_gb" json:"cache_max_gb"`
}

type LocalAPI struct {
	Listen          string `yaml:"listen" json:"listen"`
	TokenFile       string `yaml:"token_file" json:"-"`
	AllowDirectJobs bool   `yaml:"allow_direct_jobs" json:"allow_direct_jobs"`
}

type Debug struct {
	KeepWorkDirs bool `yaml:"keep_work_dirs" json:"keep_work_dirs"`
}

// DefaultLimits are applied before parsing, so a missing key keeps its default.
func DefaultLimits() Limits {
	return Limits{MaxParallelJobs: 4, CPUReservePct: 20, RAMReserveGB: 16, VRAMHeadroomGB: 1.5,
		DiskMinFreeGB: 20, GPUTempMaxC: 85, DrainTimeoutS: 600}
}

func defaults() ThisServer {
	return ThisServer{
		Limits:   DefaultLimits(),
		LocalAPI: LocalAPI{Listen: "127.0.0.1:9440", TokenFile: "~/.credentials/ai_processing_server_local.token"},
		Paths:    Paths{CacheMaxGB: 200},
		Queue: Queue{Role: "standalone", BrokerURL: "nats://127.0.0.1:4222", BrokerVersion: "2.15",
			StoreDir: "~/.act3_processing/nats/jetstream", StoreMaxGB: 20},
		Continuity: Continuity{AckWait: Duration{5 * time.Minute}, Heartbeat: Duration{20 * time.Second}},
	}
}

// Service returns the owner's setting for id, or nil when the service is not listed (= off, spec §6.4).
func (t *ThisServer) Service(id string) *ServiceSetting {
	for i := range t.Services {
		if t.Services[i].ID == id {
			return &t.Services[i]
		}
	}
	return nil
}

// ---- data/ catalogs ----

// ServiceDef is one data/services/<id>.yaml (spec §8.2).
type ServiceDef struct {
	SchemaVersion      int             `yaml:"schema_version" json:"-"`
	ID                 string          `yaml:"id" json:"id"`
	Platform           string          `yaml:"platform" json:"platform"`
	Title              string          `yaml:"title" json:"title"`
	Runner             string          `yaml:"runner" json:"runner"`
	DefaultEnabled     bool            `yaml:"default_enabled" json:"default_enabled"`
	Requires           Requires        `yaml:"requires" json:"requires"`
	ResourcesPerJob    Resources       `yaml:"resources_per_job" json:"resources_per_job"`
	MaxParallelDefault int             `yaml:"max_parallel_default" json:"max_parallel_default"`
	Timeouts           ServiceTimeouts `yaml:"timeouts" json:"timeouts"`
	Retry              RetryPolicy     `yaml:"retry" json:"retry"`
	PlatformJobTypes   []string        `yaml:"platform_job_types" json:"platform_job_types,omitempty"`
}

type Requires struct {
	Engines         map[string]string `yaml:"engines" json:"engines,omitempty"`
	GPU             string            `yaml:"gpu" json:"gpu"` // none | optional | required
	Compute         []string          `yaml:"compute" json:"compute,omitempty"`
	MinVRAMGB       float64           `yaml:"min_vram_gb" json:"min_vram_gb,omitempty"`
	MinUnifiedMemGB float64           `yaml:"min_unified_memory_gb" json:"min_unified_memory_gb,omitempty"`
}

// Resources is what one running job of a service holds (a slot, spec §9.3).
type Resources struct {
	CPUCores int     `yaml:"cpu_cores" json:"cpu_cores"`
	RAMGB    float64 `yaml:"ram_gb" json:"ram_gb"`
	VRAMGB   float64 `yaml:"vram_gb" json:"vram_gb"`
}

type ServiceTimeouts struct {
	StageInputs Duration `yaml:"stage_inputs" json:"stage_inputs"`
	Run         Duration `yaml:"run" json:"run"`
	Upload      Duration `yaml:"upload" json:"upload"`
}

type RetryPolicy struct {
	MaxAttempts int      `yaml:"max_attempts" json:"max_attempts"`
	RetryOn     []string `yaml:"retry_on" json:"retry_on"`
}

// KnownServers is data/servers/known_servers.yaml (spec §6.6).
type KnownServers struct {
	SchemaVersion int           `yaml:"schema_version"`
	Servers       []KnownServer `yaml:"servers"`
}

type KnownServer struct {
	Key         string   `yaml:"key"`
	DisplayName string   `yaml:"display_name"`
	Profile     string   `yaml:"profile"`
	Family      string   `yaml:"family"`
	Site        string   `yaml:"site"`
	Match       Match    `yaml:"match"`
	ServicesOff []string `yaml:"services_off"`
	LocalMode   string   `yaml:"local_mode"` // callers.local.mode init writes; share when empty
}

type Match struct {
	Hostname        []string `yaml:"hostname"`
	GPUNameContains string   `yaml:"gpu_name_contains"`
	GPUCountMin     int      `yaml:"gpu_count_min"`
}

// Duration parses "10m", "90s" in yaml and prints as a string in JSON.
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	if n.Value == "" {
		return nil
	}
	v, err := time.ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %q is not a duration like 10m or 90s", n.Line, n.Value)
	}
	d.Duration = v
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return []byte(`"` + d.String() + `"`), nil }

// Or returns d, or def when d is zero.
func (d Duration) Or(def time.Duration) time.Duration {
	if d.Duration <= 0 {
		return def
	}
	return d.Duration
}

// ValidKey reports whether a server key is A-Z 0-9, 1..12 characters.
func ValidKey(k string) bool {
	if len(k) == 0 || len(k) > 12 {
		return false
	}
	return strings.IndexFunc(k, func(r rune) bool { return !(r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') }) < 0
}
