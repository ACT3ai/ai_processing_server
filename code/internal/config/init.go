package config

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"gopkg.in/yaml.v3"
)

// CanRunFunc says whether this hardware meets a service's hard requirements, with the reason when not.
// It is passed in (from the capability package) so config stays below capability in the import graph.
type CanRunFunc func(def ServiceDef, facts hardware.Facts) (bool, string)

// InitOptions drive `ai_processing_server init`.
type InitOptions struct {
	Path     string // data/git_ignore/this_server.yaml
	Catalog  *Catalog
	Facts    hardware.Facts
	Version  string
	CanRun   CanRunFunc
	WriteNew bool // `just reinit`: write this_server.yaml.new for the owner to merge (spec §6.7)
}

// InitResult is what init did, for printing.
type InitResult struct {
	Created  bool
	Wrote    string // path written, if any
	Verdict  string
	Errors   []string
	Warnings []string
}

// InitThisServer creates this_server.yaml when it is missing and NEVER touches it when it exists (spec §6.1, P3).
// This function's create branch, and WriteNewFile, are the only writers of this_server.yaml(.new) in the binary.
func InitThisServer(o InitOptions) (InitResult, error) {
	var res InitResult
	_, statErr := os.Stat(o.Path)
	exists := statErr == nil

	if exists && !o.WriteNew {
		ts, err := Load(o.Path)
		if errors.Is(err, ErrLegacy) {
			res.Verdict = "this_server.yaml is a legacy hand-written probe (no schema_version); left untouched"
			res.Warnings = append(res.Warnings, "run `just reinit` to write this_server.yaml.new in the current schema, then merge it by hand")
			return res, nil
		}
		if err != nil {
			res.Errors = append(res.Errors, err.Error())
			res.Verdict = "this_server.yaml is invalid; left untouched"
			return res, nil
		}
		res.Errors, res.Warnings = Validate(ts, o.Catalog)
		if len(res.Errors) > 0 {
			res.Verdict = "this_server.yaml has errors; left untouched"
			return res, nil
		}
		on := 0
		for _, s := range ts.Services {
			if s.Enabled {
				on++
			}
		}
		res.Verdict = fmt.Sprintf("this_server.yaml OK: %s, %d services on (left untouched)", ts.Server.Key, on)
		return res, nil
	}

	var keep *Identity
	if exists { // WriteNew: keep the identity the owner already chose, when the file parses
		if ts, err := Load(o.Path); err == nil {
			keep = &ts.Server
		}
	}
	text, _, err := Generate(o.Catalog, o.Facts, o.Version, o.CanRun, keep)
	if err != nil {
		return res, err
	}
	if o.WriteNew || exists {
		out := o.Path + ".new"
		if err := writeAtomic(out, []byte(text), true); err != nil {
			return res, err
		}
		res.Wrote = out
		res.Verdict = "wrote " + out + " (this_server.yaml left untouched; compare and merge by hand)"
		return res, nil
	}
	if err := writeCreateOnly(o.Path, []byte(text)); err != nil {
		return res, err
	}
	res.Created, res.Wrote = true, o.Path
	res.Verdict = "created " + o.Path
	ts, _ := Parse([]byte(text), o.Path)
	if ts != nil && ts.Server.Key == "CHANGE_ME" {
		res.Warnings = append(res.Warnings, "server.key is CHANGE_ME: this machine is not in data/servers/known_servers.yaml. Set a key before `just run`.")
	}
	return res, nil
}

// Generate renders the template for this machine and parses it back, so what is written is exactly what loads.
func Generate(cat *Catalog, facts hardware.Facts, version string, canRun CanRunFunc, keep *Identity) (string, *ThisServer, error) {
	id := Identity{Key: "CHANGE_ME", DisplayName: facts.Hostname, Profile: "generic", Family: "office", Site: "unknown"}
	var off map[string]bool
	localMode := LocalShare
	if ks := MatchKnown(cat.Known, facts); ks != nil {
<<<<<<< HEAD
		id = Identity{Key: ks.Key, DisplayName: ks.DisplayName, Profile: ks.Profile, Family: ks.Family, Site: ks.Site,
			ServersDir: ks.ServersDir}
=======
		if ks.LocalMode != "" {
			localMode = ks.LocalMode
		}
		id = Identity{Key: ks.Key, DisplayName: ks.DisplayName, Profile: ks.Profile, Family: ks.Family, Site: ks.Site}
>>>>>>> origin/main
		off = map[string]bool{}
		for _, s := range ks.ServicesOff {
			off[s] = true
		}
	}
	if keep != nil {
		id = *keep
	}
	id.BoundTo = BoundTo{Hostname: facts.Hostname, MachineID: facts.MachineID}

	hw := facts
	hw.Engines = nil // engines are re-detected at every boot; overrides go under `engines:`
	hwYAML, err := yaml.Marshal(hw)
	if err != nil {
		return "", nil, err
	}

	var svc strings.Builder
	for _, sid := range cat.IDs() {
		def := cat.Services[sid]
		if ok, why := canRun(def, facts); !ok {
			fmt.Fprintf(&svc, "  # %s: %s\n", sid, why)
			continue
		}
		enabled := def.DefaultEnabled && !off[sid]
		fmt.Fprintf(&svc, "  - id: %s\n    enabled: %t\n", sid, enabled)
		if def.Title != "" {
			fmt.Fprintf(&svc, "    # %s\n", def.Title)
		}
	}

	data := map[string]any{
		"CreatedAt":    time.Now().Format(time.RFC3339),
		"CreatedBy":    "ai_processing_server init " + version,
		"ID":           id,
		"MachineID":    id.BoundTo.MachineID,
		"HardwareYAML": indent(string(hwYAML), "  "),
		"ServicesYAML": strings.TrimRight(svc.String(), "\n"),
<<<<<<< HEAD
		"KeyDir":       DefaultKeyDir(id.Key),
=======
		"LocalMode":    localMode,
>>>>>>> origin/main
	}
	tpl, err := template.New("this_server").Option("missingkey=error").Parse(cat.Template)
	if err != nil {
		return "", nil, fmt.Errorf("data/templates/this_server.template.yaml: %w", err)
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, data); err != nil {
		return "", nil, fmt.Errorf("fill template: %w", err)
	}
	ts, err := Parse(buf.Bytes(), "generated this_server.yaml")
	if err != nil {
		return "", nil, fmt.Errorf("the template produced an invalid file (bug in data/templates): %w", err)
	}
	return buf.String(), ts, nil
}

// CredentialsCheckouts are where the act3ai-credentials repo is cloned on fleet machines, in order of preference.
// Its /keys/ directory is git-ignored and never encrypted into the repo, so a private key there stays local.
var CredentialsCheckouts = []string{"~/repos/act3ai-credentials", "~/BGit/act3/act3ai-credentials"}

// DefaultKeyDir is the security.key_dir that init writes into a new this_server.yaml: under the credentials checkout
// when this machine has one, keys/ai_processing_server/{key}/, else "" (data/git_ignore/keys, security.mdx §4.2).
func DefaultKeyDir(serverKey string) string {
	if !ValidKey(serverKey) {
		return ""
	}
	for _, c := range CredentialsCheckouts {
		if st, err := os.Stat(filepath.Join(ExpandHome(c), ".git")); err == nil && st.IsDir() {
			return c + "/keys/ai_processing_server/" + serverKey
		}
	}
	return ""
}

// MatchKnown finds this machine in known_servers.yaml: hostname first, then hardware hints (spec §6.6).
func MatchKnown(k KnownServers, f hardware.Facts) *KnownServer {
	host := normHost(f.Hostname)
	for i, s := range k.Servers {
		for _, h := range s.Match.Hostname {
			if normHost(h) == host {
				return &k.Servers[i]
			}
		}
	}
	for i, s := range k.Servers {
		m := s.Match
		if m.GPUNameContains == "" && m.GPUCountMin == 0 {
			continue
		}
		if m.GPUCountMin > 0 && len(f.GPUs) < m.GPUCountMin {
			continue
		}
		if m.GPUNameContains != "" {
			found := false
			for _, g := range f.GPUs {
				if strings.Contains(strings.ToLower(g.Name), strings.ToLower(m.GPUNameContains)) {
					found = true
				}
			}
			if !found {
				continue
			}
		}
		return &k.Servers[i]
	}
	return nil
}

func normHost(h string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(h)), ".local")
}

// EnsureTokenFile creates the local API token (32 random bytes, base64url, mode 0600) when missing,
// and returns the token.
func EnsureTokenFile(path string) (string, error) {
	path = ExpandHome(path)
	if b, err := os.ReadFile(path); err == nil {
		tok := strings.TrimSpace(string(b))
		if tok == "" {
			return "", fmt.Errorf("token file %s is empty", path)
		}
		return tok, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	tok := base64.RawURLEncoding.EncodeToString(raw)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	if err := writeAtomic(path, []byte(tok+"\n"), false); err != nil {
		return "", err
	}
	return tok, nil
}

// writeCreateOnly writes path only if it does not exist, atomically, mode 0600. It can never replace a file.
func writeCreateOnly(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := writeTemp(path, b)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if err := os.Link(tmp, path); err != nil { // link fails if path exists: no overwrite, even in a race
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("%s appeared while init was running; not overwriting it", path)
		}
		if _, statErr := os.Stat(path); statErr == nil {
			return fmt.Errorf("%s exists; not overwriting it", path)
		}
		return os.Rename(tmp, path) // filesystems without hard links
	}
	return nil
}

func writeAtomic(path string, b []byte, mkdir bool) error {
	if mkdir {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
	}
	tmp, err := writeTemp(path, b)
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

func writeTemp(path string, b []byte) (string, error) {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return "", err
	}
	name := f.Name()
	if _, err := f.Write(b); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chmod(name, 0o600)
	}
	if err != nil {
		os.Remove(name)
		return "", err
	}
	return name, nil
}

func indent(s, pad string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		lines[i] = pad + l
	}
	return strings.Join(lines, "\n")
}
