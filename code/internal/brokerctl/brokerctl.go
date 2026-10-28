// Package brokerctl installs, configures and checks the nats-server this machine's controller connects to
// (pm/task_queue.mdx §8.2, §18). The broker is its own long-lived OS service (launchd, systemd --user, or a
// Windows service), never part of our binary, so it keeps every queued job and claim while our code is replaced.
//
// Its config is generated from this_server.yaml and data/queue/nats/*.conf.tmpl into ~/.act3_processing/nats/,
// outside the repo; nobody edits it by hand.
package brokerctl

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
)

// ServiceName is the OS service label.
const (
	LaunchdLabel   = "ai.act3.processing.nats"
	SystemdUnit    = "act3-processing-nats.service"
	WindowsService = "ACT3ProcessingNATS"
)

// Paths are where the broker's own files live, outside the repo.
type Paths struct {
	Home   string // ~/.act3_processing/nats
	Conf   string
	Pid    string
	Log    string
	Bin    string // a downloaded nats-server, when none suitable is on PATH
	Store  string
	Plist  string // macOS
	Unit   string // Linux
	Server string // the nats-server binary the service runs (filled by Install)
}

// DefaultPaths derives the paths from this_server.yaml.
func DefaultPaths(ts *config.ThisServer) Paths {
	home, _ := os.UserHomeDir()
	base := filepath.Join(home, ".act3_processing", "nats")
	exe := "nats-server"
	if runtime.GOOS == "windows" {
		exe += ".exe"
	}
	return Paths{
		Home: base, Conf: filepath.Join(base, "nats.conf"), Pid: filepath.Join(base, "nats.pid"), Log: filepath.Join(base, "nats.log"),
		Bin: filepath.Join(base, "bin", exe), Store: config.ExpandHome(ts.Queue.StoreDir),
		Plist: filepath.Join(home, "Library", "LaunchAgents", LaunchdLabel+".plist"),
		Unit:  filepath.Join(home, ".config", "systemd", "user", SystemdUnit),
	}
}

// hostPort splits queue.broker_url.
func hostPort(raw string) (string, int, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", 0, fmt.Errorf("queue.broker_url %q: %w", raw, err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		return "", 0, fmt.Errorf("queue.broker_url %q needs a port", raw)
	}
	return u.Hostname(), port, nil
}

// RenderConfig fills the template for the server's role.
func RenderConfig(ts *config.ThisServer, cat *config.QueueCatalog, p Paths) (string, error) {
	host, port, err := hostPort(ts.Queue.BrokerURL)
	if err != nil {
		return "", err
	}
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		return "", fmt.Errorf("queue.broker_url must point at this machine's own broker (127.0.0.1); got %s", host)
	}
	role := ts.Queue.Role
	if role == "worker" {
		role = "leaf"
	}
	tplPath := filepath.Join(cat.Dir, "nats", role+".conf.tmpl")
	b, err := os.ReadFile(tplPath)
	if err != nil {
		return "", err
	}
	tpl, err := template.New(role).Option("missingkey=error").Parse(string(b))
	if err != nil {
		return "", fmt.Errorf("%s: %w", tplPath, err)
	}
	var out bytes.Buffer
	err = tpl.Execute(&out, map[string]any{
		"ServerName": "aps_" + strings.ToLower(ts.Server.Key), "Host": "127.0.0.1", "Port": port, "MonitorPort": cat.Broker.MonitorPort,
		"PidFile": filepath.ToSlash(p.Pid), "StoreDir": filepath.ToSlash(p.Store),
		"MaxFileStore": int64(max(ts.Queue.StoreMaxGB, 1) * (1 << 30)), "HubURL": ts.Queue.HubURL,
		"Credentials": filepath.ToSlash(config.ExpandHome(ts.Queue.CredentialsFile)),
	})
	return out.String(), err
}

// WriteConfig renders and writes nats.conf. It reports whether the file changed.
func WriteConfig(ts *config.ThisServer, cat *config.QueueCatalog, p Paths) (bool, error) {
	text, err := RenderConfig(ts, cat, p)
	if err != nil {
		return false, err
	}
	for _, d := range []string{p.Home, p.Store} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return false, err
		}
	}
	if old, err := os.ReadFile(p.Conf); err == nil && string(old) == text {
		return false, nil
	}
	return true, os.WriteFile(p.Conf, []byte(text), 0o600)
}

// Version runs `<bin> --version` and returns "2.15.2".
func Version(bin string) (string, error) {
	out, err := exec.Command(bin, "--version").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s --version: %w", bin, err)
	}
	s := strings.TrimSpace(string(out))
	if i := strings.LastIndex(s, "v"); i >= 0 {
		s = s[i+1:]
	}
	return s, nil
}

// Supported reports whether a nats-server version is in data/queue/broker.yaml's range (or this_server.yaml's).
func Supported(ts *config.ThisServer, cat *config.QueueCatalog, v string) bool {
	c := cat.Broker.Supported
	if ts.Queue.BrokerVersion != "" {
		c = ts.Queue.BrokerVersion
	}
	return hardware.SatisfiesVersion(v, c)
}

// Find returns a supported nats-server: the one downloaded before, then one on PATH.
func Find(ts *config.ThisServer, cat *config.QueueCatalog, p Paths) (string, string, error) {
	cands := []string{p.Bin}
	if onPath, err := exec.LookPath("nats-server"); err == nil {
		cands = append(cands, onPath)
	}
	var tried []string
	for _, c := range cands {
		if _, err := os.Stat(c); err != nil {
			continue
		}
		v, err := Version(c)
		if err == nil && Supported(ts, cat, v) {
			return c, v, nil
		}
		tried = append(tried, fmt.Sprintf("%s (%s)", c, v))
	}
	return "", "", fmt.Errorf("no supported nats-server (%s) found; tried %v", cat.Broker.Supported, tried)
}

// Download fetches the pinned release for this OS and architecture, checks its sha256 against broker.yaml, and
// extracts nats-server into p.Bin.
func Download(ctx context.Context, cat *config.QueueCatalog, p Paths) (string, error) {
	key := runtime.GOOS + "-" + runtime.GOARCH
	d, ok := cat.Broker.Downloads[key]
	if !ok {
		return "", fmt.Errorf("data/queue/broker.yaml has no download for %s; install nats-server %s yourself", key, cat.Broker.Pinned)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", cat.Broker.BaseURL+d.File, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("download %s: HTTP %d", d.File, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 200<<20))
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	if got := hex.EncodeToString(sum[:]); got != d.SHA256 {
		return "", fmt.Errorf("download %s: sha256 %s does not match broker.yaml %s; not installed", d.File, got, d.SHA256)
	}
	bin, err := extract(d.File, body)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(p.Bin), 0o700); err != nil {
		return "", err
	}
	if err := os.WriteFile(p.Bin, bin, 0o755); err != nil {
		return "", err
	}
	return p.Bin, nil
}

func extract(name string, body []byte) ([]byte, error) {
	want := "nats-server"
	if strings.HasSuffix(name, ".zip") {
		zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
		if err != nil {
			return nil, err
		}
		for _, f := range zr.File {
			if base := filepath.Base(f.Name); base == want || base == want+".exe" {
				rc, err := f.Open()
				if err != nil {
					return nil, err
				}
				defer rc.Close()
				return io.ReadAll(rc)
			}
		}
		return nil, fmt.Errorf("%s holds no nats-server", name)
	}
	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("%s holds no nats-server", name)
		}
		if err != nil {
			return nil, err
		}
		if filepath.Base(h.Name) == want {
			return io.ReadAll(tr)
		}
	}
}

// Install makes the broker a running OS service: find or download nats-server, write its config, register the
// service and start it. Safe to run again: it replaces the service definition and restarts the service.
func Install(ctx context.Context, ts *config.ThisServer, cat *config.QueueCatalog, p Paths, logf func(string, ...any)) error {
	bin, v, err := Find(ts, cat, p)
	if err != nil {
		logf("%v; downloading nats-server %s", err, cat.Broker.Pinned)
		if bin, err = Download(ctx, cat, p); err != nil {
			return err
		}
		if v, err = Version(bin); err != nil {
			return err
		}
	}
	logf("nats-server %s at %s", v, bin)
	p.Server = bin
	if _, err := WriteConfig(ts, cat, p); err != nil {
		return err
	}
	logf("config %s", p.Conf)
	if err := installService(p); err != nil {
		return err
	}
	return waitUp(ts.Queue.BrokerURL, 15*time.Second)
}

// Reload regenerates the config and tells the running broker to re-read it without dropping connections.
func Reload(ts *config.ThisServer, cat *config.QueueCatalog, p Paths) (bool, error) {
	changed, err := WriteConfig(ts, cat, p)
	if err != nil || !changed {
		return changed, err
	}
	b, err := os.ReadFile(p.Pid)
	if err != nil {
		return true, fmt.Errorf("config written, but the broker is not running (no %s): run `just broker-install`", p.Pid)
	}
	return true, signalReload(strings.TrimSpace(string(b)))
}

// waitUp waits until the broker accepts TCP connections.
func waitUp(brokerURL string, d time.Duration) error {
	host, port, err := hostPort(brokerURL)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if Reachable(host, port) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errors.New("the broker service was installed but is not answering on " + brokerURL + "; see ~/.act3_processing/nats/nats.log")
}
