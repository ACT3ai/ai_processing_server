package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// ErrLegacy marks a this_server.yaml without schema_version: an earlier hand-written hardware probe.
// It is never rewritten; the server runs from a generated config and offers this_server.yaml.new to merge.
var ErrLegacy = errors.New("this_server.yaml has no schema_version (legacy hand-written probe)")

// Catalog is everything loaded from the checked-in data/ directory.
type Catalog struct {
	Dir      string
	Services map[string]ServiceDef
	Known    KnownServers
	Template string // data/templates/this_server.template.yaml
}

// IDs returns the service ids sorted.
func (c *Catalog) IDs() []string {
	ids := make([]string, 0, len(c.Services))
	for id := range c.Services {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// LoadCatalog reads data/services/*.yaml, data/servers/known_servers.yaml and the template.
func LoadCatalog(dataDir string) (*Catalog, error) {
	c := &Catalog{Dir: dataDir, Services: map[string]ServiceDef{}}
	files, err := filepath.Glob(filepath.Join(dataDir, "services", "*.yaml"))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no service catalog files in %s/services", dataDir)
	}
	for _, f := range files {
		if strings.HasPrefix(filepath.Base(f), "_") {
			continue
		}
		var s ServiceDef
		if err := readYAML(f, &s); err != nil {
			return nil, err
		}
		if s.SchemaVersion != SchemaVersion {
			return nil, fmt.Errorf("%s: schema_version %d is not supported (want %d)", f, s.SchemaVersion, SchemaVersion)
		}
		if s.ID == "" || s.Runner == "" {
			return nil, fmt.Errorf("%s: id and runner are required", f)
		}
		if want := strings.TrimSuffix(filepath.Base(f), ".yaml"); want != s.ID {
			return nil, fmt.Errorf("%s: id %q must match the file name", f, s.ID)
		}
		if s.Requires.GPU == "" {
			s.Requires.GPU = "none"
		}
		if s.ResourcesPerJob.CPUCores <= 0 {
			s.ResourcesPerJob.CPUCores = 1
		}
		c.Services[s.ID] = s
	}
	known := filepath.Join(dataDir, "servers", "known_servers.yaml")
	if _, err := os.Stat(known); err == nil {
		if err := readYAML(known, &c.Known); err != nil {
			return nil, err
		}
	}
	tpl, err := os.ReadFile(filepath.Join(dataDir, "templates", "this_server.template.yaml"))
	if err != nil {
		return nil, fmt.Errorf("read template: %w", err)
	}
	c.Template = string(tpl)
	return c, nil
}

// Load parses this_server.yaml. A file without schema_version returns ErrLegacy.
func Load(path string) (*ThisServer, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b, path)
}

// Parse parses this_server.yaml bytes; name is used in error messages.
func Parse(b []byte, name string) (*ThisServer, error) {
	var probe map[string]any
	if err := yaml.Unmarshal(b, &probe); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if _, ok := probe["schema_version"]; !ok {
		return nil, fmt.Errorf("%s: %w", name, ErrLegacy)
	}
	ts := defaults()
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&ts); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if ts.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("%s: schema_version %d is not supported by this binary (want %d)", name, ts.SchemaVersion, SchemaVersion)
	}
	return &ts, nil
}

// Validate returns fatal errors and warnings. A fatal error means `run` refuses to start (spec §6.1).
func Validate(ts *ThisServer, cat *Catalog) (errs []string, warns []string) {
	if ts.Server.Key == "CHANGE_ME" {
		errs = append(errs, "server.key is CHANGE_ME: set a unique key (A-Z, 0-9, at most 12 characters)")
	} else if !ValidKey(ts.Server.Key) {
		errs = append(errs, fmt.Sprintf("server.key %q must be A-Z and 0-9, 1 to 12 characters", ts.Server.Key))
	}
	if d := ts.Server.ServersDir; d != "" && (d != filepath.Base(d) || d == "." || d == "..") {
		errs = append(errs, fmt.Sprintf("server.servers_dir %q must be a directory name under servers/, not a path", d))
	}
	seen := map[string]bool{}
	for _, s := range ts.Services {
		if seen[s.ID] {
			errs = append(errs, fmt.Sprintf("services: %s listed twice", s.ID))
		}
		seen[s.ID] = true
		if _, ok := cat.Services[s.ID]; !ok {
			warns = append(warns, fmt.Sprintf("services: %s is not in data/services (renamed or removed); ignored", s.ID))
		}
		if s.MaxParallel < 0 {
			errs = append(errs, fmt.Sprintf("services: %s max_parallel must be >= 0", s.ID))
		}
		if s.LocalBias < -50 || s.LocalBias > 50 {
			errs = append(errs, fmt.Sprintf("services: %s local_bias must be within -50..50", s.ID))
		}
	}
	l := ts.Limits
	if l.MaxParallelJobs < 1 {
		errs = append(errs, "limits.max_parallel_jobs must be at least 1")
	}
	if l.CPUReservePct < 0 || l.CPUReservePct > 90 {
		errs = append(errs, "limits.cpu_reserve_pct must be within 0..90")
	}
	if ts.LocalAPI.Listen == "" {
		errs = append(errs, "local_api.listen is required (default 127.0.0.1:9440)")
	}
	for name, p := range ts.Platforms {
		if p.Share < 0 || p.Share > 100 {
			errs = append(errs, fmt.Sprintf("platforms.%s.share must be within 0..100", name))
		}
	}
	return errs, warns
}

func readYAML(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(b, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// ExpandHome turns a leading ~ into the user's home directory.
func ExpandHome(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") || strings.HasPrefix(p, `~\`) {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, p[1:])
	}
	return p
}
