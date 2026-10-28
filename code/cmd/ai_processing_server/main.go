// Command ai_processing_server is the single binary that runs on every ACT3 processing and render-queue machine
// (pm/ai_processing_server.mdx). Subcommands:
//
//	init [--write-new]   create data/git_ignore/this_server.yaml if missing (never overwrite); --write-new writes .new
//	run                  start the server (local API, scheduler, runners) until SIGINT/SIGTERM
//	detect               print detected hardware and the offers it would make
//	validate             check this_server.yaml and data/ without starting
//	status | jobs        ask the running server (thin clients of the local API)
//	cancel <job_id> | drain | resume
//	stop [--force]       stop the controller; running jobs keep going in their task hosts (pm/task_queue.mdx §9.5)
//	runs                 list jobs running in task hosts on this machine
//	broker install|config|status|uninstall   the nats-server service this controller uses (§8.2, §18)
//	taskhost --run-dir   internal: the detached process that owns one running job (§9.3)
//	version
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/api"
	"github.com/ACT3ai/ai_processing_server/internal/brokerctl"
	"github.com/ACT3ai/ai_processing_server/internal/capability"
	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/errlog"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/logging"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
	"github.com/ACT3ai/ai_processing_server/internal/queue/natsq"
	"github.com/ACT3ai/ai_processing_server/internal/runners"
	"github.com/ACT3ai/ai_processing_server/internal/runs"
	"github.com/ACT3ai/ai_processing_server/internal/scheduler"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
	"github.com/ACT3ai/ai_processing_server/internal/taskhost"
	"gopkg.in/yaml.v3"
)

// Set by `just build` through -ldflags.
var (
	version   = "0.1.0-dev"
	gitSHA    = "unknown"
	buildTime = "unknown"
)

// paths are resolved once from flags and the binary's location.
type paths struct {
	Root     string // repo root
	Data     string // {root}/data
	State    string // {root}/data/git_ignore
	Config   string // {state}/this_server.yaml
	LogDir   string
	ErrorDir string // error.err directory (default ~/T/ai_processing_server or $APS_ERROR_DIR)
	Listen   string // overrides local_api.listen when set
	JSONOut  bool
	Force    bool
	WriteNew bool
	RunDir   string // taskhost: the run directory of the job it owns
}

// elog records errors that should not happen (pm/errors.mdx) in error.err.
var elog = errlog.For("main")

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	var p paths
	fs.StringVar(&p.Root, "root", "", "repo root (default: the directory holding the binary)")
	fs.StringVar(&p.State, "state-dir", "", "per-machine directory (default: {root}/data/git_ignore)")
	fs.StringVar(&p.Config, "config", "", "this_server.yaml path (default: {state-dir}/this_server.yaml)")
	fs.StringVar(&p.LogDir, "log-dir", "", "log directory (default: ~/_act3_logs or $ACT3_LOG_DIR)")
	fs.StringVar(&p.ErrorDir, "error-dir", "", "error.err directory (default: ~/T/ai_processing_server or $APS_ERROR_DIR)")
	fs.StringVar(&p.Listen, "listen", "", "override local_api.listen")
	fs.BoolVar(&p.JSONOut, "json", false, "print JSON (status, jobs, detect)")
	fs.BoolVar(&p.Force, "force", false, "stop: kill now instead of draining")
	fs.BoolVar(&p.WriteNew, "write-new", false, "init: write this_server.yaml.new for the owner to merge")
	fs.StringVar(&p.RunDir, "run-dir", "", "taskhost: the job's run directory")
	fs.Parse(args)
	if err := p.resolve(); err != nil && cmd != "version" {
		fail(err)
	}
	if cmd != "version" && cmd != "help" {
		closeErr, err := errlog.Init(errlog.Options{Dir: p.ErrorDir, SourceRoot: filepath.Join(p.Root, "code"), Mirror: os.Stderr})
		if err != nil {
			fmt.Fprintln(os.Stderr, "warning: error log:", err) // entries still reach stderr
		}
		defer closeErr()
		defer elog.RecoverAndRepanic("running `ai_processing_server " + cmd + "`")
	}

	var err error
	switch cmd {
	case "version", "--version", "-v":
		fmt.Printf("ai_processing_server %s (git %s, built %s)\n", version, gitSHA, buildTime)
	case "init":
		err = cmdInit(p)
	case "run":
		err = cmdRun(p)
	case "detect":
		err = cmdDetect(p)
	case "validate":
		err = cmdValidate(p)
	case "status":
		err = cmdStatus(p)
	case "jobs":
		err = cmdJobs(p, fs.Args())
	case "cancel":
		if fs.NArg() != 1 {
			fail(errors.New("usage: ai_processing_server cancel <job_id>"))
		}
		err = cmdPost(p, "/v1/jobs/"+fs.Arg(0)+"/cancel")
	case "drain":
		err = cmdPost(p, "/v1/drain")
	case "resume":
		err = cmdPost(p, "/v1/resume")
	case "stop":
		err = cmdStop(p)
	case "runs":
		err = cmdRuns(p)
	case "broker":
		err = cmdBroker(p, fs.Args())
	case "taskhost":
		if p.RunDir == "" {
			fail(errors.New("usage: ai_processing_server taskhost --run-dir <dir> (started by the controller, not by hand)"))
		}
		code := taskhost.Run(p.RunDir, slog.New(slog.NewTextHandler(os.Stderr, nil)))
		errlog.Close()
		os.Exit(code)
	case "help", "-h", "--help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fail(err)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: ai_processing_server <command> [flags]
commands: init [--write-new] · run · detect · validate · status · jobs [state] · cancel <id> · drain · resume · stop [--force]
          runs · broker install|config|status|uninstall · version
flags: --root --state-dir --config --log-dir --error-dir --listen --json
`)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "ai_processing_server:", err)
	errlog.Close() // os.Exit skips deferred calls: flush repeat counts first
	os.Exit(1)
}

func (p *paths) resolve() error {
	if p.Root == "" {
		p.Root = os.Getenv("APS_ROOT")
	}
	if p.Root == "" {
		p.Root = findRoot()
	}
	if p.Root == "" {
		return errors.New("cannot find the repo root (a directory with data/services); pass --root")
	}
	p.Root, _ = filepath.Abs(p.Root)
	p.Data = filepath.Join(p.Root, "data")
	if p.State == "" {
		p.State = filepath.Join(p.Data, "git_ignore")
	}
	if p.Config == "" {
		p.Config = filepath.Join(p.State, "this_server.yaml")
	}
	if p.LogDir == "" {
		p.LogDir = logging.DefaultDir()
	}
	return os.MkdirAll(p.State, 0o700)
}

// findRoot looks next to the binary first (the build puts it in the repo root), then up from the cwd.
func findRoot() string {
	var starts []string
	if exe, err := os.Executable(); err == nil {
		if real, err := filepath.EvalSymlinks(exe); err == nil {
			exe = real
		}
		starts = append(starts, filepath.Dir(exe))
	}
	if wd, err := os.Getwd(); err == nil {
		starts = append(starts, wd)
	}
	for _, d := range starts {
		for dir := d; ; dir = filepath.Dir(dir) {
			if st, err := os.Stat(filepath.Join(dir, "data", "services")); err == nil && st.IsDir() {
				return dir
			}
			if filepath.Dir(dir) == dir {
				break
			}
		}
	}
	return ""
}

func detect(p paths, ts *config.ThisServer) hardware.Facts {
	work := filepath.Join(p.State, "work")
	var overrides map[string]string
	if ts != nil {
		if ts.Paths.WorkDir != "" {
			work = config.ExpandHome(ts.Paths.WorkDir)
		}
		overrides = ts.Engines
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	return hardware.Detect(ctx, hardware.Options{WorkDir: work, EnginePaths: overrides})
}

// loadConfig returns the config to run with. A legacy (schema-less) this_server.yaml is never modified: the
// server runs from a generated config and this_server.yaml.new is written once for the owner to merge.
func loadConfig(p paths, facts *hardware.Facts) (*config.ThisServer, *config.Catalog, []string, error) {
	cat, err := config.LoadCatalog(p.Data)
	if err != nil {
		return nil, nil, nil, err
	}
	var warns []string
	ts, err := config.Load(p.Config)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, nil, nil, fmt.Errorf("%s does not exist: run `just build` (or `ai_processing_server init`) first", p.Config)
	case errors.Is(err, config.ErrLegacy):
		if facts == nil {
			f := detect(p, nil)
			facts = &f
		}
		_, gen, gerr := config.Generate(cat, *facts, version, capability.CanRun, nil)
		if gerr != nil {
			return nil, nil, nil, gerr
		}
		ts = gen
		warns = append(warns, fmt.Sprintf("%s is a legacy hand-written probe (no schema_version). It was NOT modified. "+
			"Running from a generated config; merge %s.new into it by hand.", p.Config, p.Config))
		if _, serr := os.Stat(p.Config + ".new"); errors.Is(serr, os.ErrNotExist) {
			_, werr := config.InitThisServer(config.InitOptions{Path: p.Config, Catalog: cat, Facts: *facts, Version: version, CanRun: capability.CanRun, WriteNew: true})
			elog.Check("writing this_server.yaml.new next to a legacy file", werr, "path", p.Config+".new")
		}
	case err != nil:
		return nil, nil, nil, err
	}
	errs, w := config.Validate(ts, cat)
	warns = append(warns, w...)
	if facts != nil && ts.Server.BoundTo.Hostname != "" {
		hostDiff := !strings.EqualFold(strings.TrimSuffix(ts.Server.BoundTo.Hostname, ".local"), strings.TrimSuffix(facts.Hostname, ".local"))
		idDiff := ts.Server.BoundTo.MachineID != "" && facts.MachineID != "" && ts.Server.BoundTo.MachineID != facts.MachineID
		if hostDiff && idDiff {
			errs = append(errs, fmt.Sprintf("this_server.yaml was created on %s; this machine is %s. Run `just reinit` or fix server.bound_to.",
				ts.Server.BoundTo.Hostname, facts.Hostname))
		} else if hostDiff {
			warns = append(warns, fmt.Sprintf("hostname changed from %s to %s (same machine id); continuing", ts.Server.BoundTo.Hostname, facts.Hostname))
		}
	}
	if len(errs) > 0 {
		return nil, nil, warns, fmt.Errorf("%s is invalid:\n  * %s", p.Config, strings.Join(errs, "\n  * "))
	}
	return ts, cat, warns, nil
}

func cmdInit(p paths) error {
	cat, err := config.LoadCatalog(p.Data)
	if err != nil {
		return err
	}
	var existing *config.ThisServer
	if ts, err := config.Load(p.Config); err == nil {
		existing = ts
	}
	facts := detect(p, existing)
	res, err := config.InitThisServer(config.InitOptions{Path: p.Config, Catalog: cat, Facts: facts, Version: version + " (git " + gitSHA + ")",
		CanRun: capability.CanRun, WriteNew: p.WriteNew})
	if err != nil {
		return err
	}
	fmt.Println(res.Verdict)
	for _, w := range res.Warnings {
		fmt.Println("  warning:", w)
	}
	for _, e := range res.Errors {
		fmt.Println("  error:", e)
	}
	tokenFile := "~/.credentials/ai_processing_server_local.token"
	if ts, err := config.Load(p.Config); err == nil {
		tokenFile = ts.LocalAPI.TokenFile
	}
	if _, err := config.EnsureTokenFile(tokenFile); err != nil {
		return fmt.Errorf("local API token: %w", err)
	}
	if res.Created {
		fmt.Println("  check server.key, the services list and limits, then `just run`")
	}
	return nil
}

func cmdValidate(p paths) error {
	facts := detect(p, nil) // needed for the server.bound_to check (a file copied from another machine)
	ts, _, warns, err := loadConfig(p, &facts)
	for _, w := range warns {
		fmt.Println("warning:", w)
	}
	if err != nil {
		return err
	}
	fmt.Printf("OK: %s (%s), %d services listed\n", ts.Server.Key, ts.Server.DisplayName, len(ts.Services))
	return nil
}

func cmdDetect(p paths) error {
	facts := detect(p, nil)
	ts, cat, warns, err := loadConfig(p, &facts)
	if err != nil {
		var gerr error
		if cat, gerr = config.LoadCatalog(p.Data); gerr != nil {
			return gerr
		}
		if _, ts, gerr = config.Generate(cat, facts, version, capability.CanRun, nil); gerr != nil {
			return elog.Check("generating a config for detect", gerr)
		}
		warns = append(warns, "using a generated config: "+err.Error())
	}
	eff, drift := hardware.Effective(facts, ts.Hardware)
	offers := capability.ComputeOffers(capability.Input{Catalog: cat, Server: ts, Facts: eff, Runners: runners.Names()})
	if p.JSONOut {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"detected": facts, "effective": eff, "drift": drift, "offers": offers, "warnings": warns})
	}
	b, _ := yaml.Marshal(facts)
	fmt.Printf("# detected on %s (machine id %s)\n%s\n", facts.Hostname, facts.MachineID, b)
	for _, e := range facts.Errors {
		fmt.Println("probe error:", e)
	}
	for _, d := range drift {
		fmt.Println("drift:", d.Message)
	}
	for _, w := range warns {
		fmt.Println("warning:", w)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "SERVICE\tPARALLEL\tREASON")
	for _, o := range offers {
		fmt.Fprintf(tw, "%s\t%d\t%s\n", o.Service, o.Parallel, o.Reason)
	}
	return tw.Flush()
}

func cmdRun(p paths) error {
	console := io.Writer(os.Stdout)
	log, closeLog, err := logging.Open(p.LogDir, console, slog.LevelInfo)
	if err != nil {
		return err
	}
	defer closeLog()

	facts := detect(p, nil)
	ts, cat, warns, err := loadConfig(p, &facts)
	if err != nil {
		log.Error("refusing to start", "error", err)
		return err
	}
	if ts.Paths.WorkDir != "" { // re-detect disks against the configured work dir
		facts = detect(p, ts)
	}
	for _, w := range warns {
		log.Warn(w)
	}
	token, err := config.EnsureTokenFile(ts.LocalAPI.TokenFile)
	if err != nil {
		return fmt.Errorf("local API token: %w", err)
	}
	listen := ts.LocalAPI.Listen
	if p.Listen != "" {
		listen = p.Listen
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("local API cannot listen on %s: %w (another server running? try `just status`)", listen, err)
	}

	work := filepath.Join(p.State, "work")
	if ts.Paths.WorkDir != "" {
		work = config.ExpandHome(ts.Paths.WorkDir)
	}
	mon := hardware.NewMonitor(hardware.Options{WorkDir: work, EnginePaths: ts.Engines}, facts)
	loader := func() (*config.ThisServer, *config.Catalog, []string, error) {
		f := mon.Detected()
		return loadConfig(p, &f)
	}
	// Freeze this binary as the code snapshot new jobs run from, before anything can rebuild it on disk (§9.2).
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	snap, err := snapshot.Ensure(filepath.Join(p.State, "snapshots"), exe, gitSHA, p.Data)
	if err != nil {
		return fmt.Errorf("code snapshot: %w", err)
	}
	thArgs := []string{"--root", p.Root, "--state-dir", p.State, "--log-dir", p.LogDir}
	if p.ErrorDir != "" {
		thArgs = append(thArgs, "--error-dir", p.ErrorDir)
	}
	srv, err := scheduler.New(scheduler.Options{Server: ts, Catalog: cat, Monitor: mon, StateDir: p.State, Loader: loader,
		OpenQueue: openQueue(log), Snapshot: snap, TaskHostArgs: thArgs, BrokerFix: "just broker-install (once), or just broker-status",
		Version: version, GitSHA: gitSHA, Warnings: warns, Logger: log})
	if err != nil {
		return err
	}
	allowDirect := func() bool {
		if p, err := config.Load(p.Config); err == nil {
			return p.LocalAPI.AllowDirectJobs
		}
		return ts.LocalAPI.AllowDirectJobs
	}
	httpSrv := &http.Server{
		Handler:           api.NewRouter(srv, api.Options{Token: token, AllowDirectJobs: allowDirect, Loopback: api.IsLoopback(listen), Logger: log}),
		ReadHeaderTimeout: 10 * time.Second,
	}

	pidFile := filepath.Join(p.State, "state", "server.pid")
	elog.Check("creating the state dir", os.MkdirAll(filepath.Dir(pidFile), 0o700))
	elog.Check("writing the pid file (just stop and just status need it)", os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600), "path", pidFile)
	defer func() { elog.Ignore("removing the pid file", os.Remove(pidFile)) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errlog.For("scheduler").Go("running the scheduler loop", func() { srv.Run(ctx) })
	errlog.For("api").Go("serving the local API", func() {
		if err := httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("local API stopped", "error", err)
			errlog.For("api").Error("serving the local API", err, "listen", listen)
		}
	})
	addr := ln.Addr().String()
	log.Info("ai_processing_server started", "version", version, "git_sha", gitSHA, "key", ts.Server.Key,
		"local_api", "http://"+addr+"/", "config", p.Config, "state_dir", p.State, "pid", os.Getpid())

	stop := make(chan os.Signal, 2)
	signal.Notify(stop, stopSignals...)
	reload := make(chan os.Signal, 1)
	if reloadSignal != nil {
		signal.Notify(reload, reloadSignal)
	}
	for {
		select {
		case <-reload:
			if _, err := srv.Reload(); err != nil {
				log.Warn("reload on signal rejected", "error", err)
			}
			continue
		case sig := <-stop:
			// A code update restarts the controller only (§9.5): stop pulling, save every claim, close the broker
			// connection without nacking, and leave every task host running for the next controller to re-adopt.
			log.Info("stopping the controller; running jobs keep going in their task hosts", "signal", sig.String())
			left := srv.Shutdown()
			sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
			elog.Ignore("shutting down the local API", httpSrv.Shutdown(sctx))
			scancel()
			cancel()
			log.Info("ai_processing_server stopped", "jobs_left_running", left)
			return nil
		}
	}
}

// openQueue connects to this machine's nats-server with the streams and buckets of data/queue/streams.yaml.
func openQueue(log *slog.Logger) scheduler.OpenQueue {
	return func(ctx context.Context, ts *config.ThisServer, cat *config.Catalog) (queue.Queue, error) {
		o := natsq.Options{URL: ts.Queue.BrokerURL, Name: "ai_processing_server " + ts.Server.Key, CredsFile: config.ExpandHome(ts.Queue.CredentialsFile),
			AckWait: ts.Continuity.AckWait.Duration, MaxDeliver: cat.Queue.Streams.Consumers.MaxDeliver, Logger: log}
		for _, st := range cat.Queue.Streams.Streams {
			o.Streams = append(o.Streams, natsq.StreamDef{Name: st.Name, Subjects: st.Subjects, Retention: st.Retention, MaxAge: st.MaxAge, DuplicateWindow: st.DuplicateWindow})
		}
		for _, kv := range cat.Queue.Streams.KV {
			o.KV = append(o.KV, natsq.KVDef{Bucket: kv.Bucket, TTL: kv.TTL, History: kv.History})
		}
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		return natsq.Open(cctx, o)
	}
}

// ---- thin clients of the local API ----

type client struct {
	base  string
	token string
}

func newClient(p paths) (*client, error) {
	ts, _, _, err := loadConfig(p, nil)
	if err != nil {
		return nil, err
	}
	listen := ts.LocalAPI.Listen
	if p.Listen != "" {
		listen = p.Listen
	}
	b, err := os.ReadFile(config.ExpandHome(ts.LocalAPI.TokenFile))
	if err != nil {
		return nil, fmt.Errorf("read token: %w", err)
	}
	return &client{base: "http://" + listen, token: strings.TrimSpace(string(b))}, nil
}

func (c *client) do(method, path string, out any) error {
	req, _ := http.NewRequest(method, c.base+path, nil)
	req.Header.Set("Authorization", "Bearer "+c.token)
	hc := &http.Client{Timeout: 10 * time.Second}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("server not reachable at %s: %w", c.base, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	os.Stdout.Write(b)
	return nil
}

func cmdStatus(p paths) error {
	c, err := newClient(p)
	if err != nil {
		return err
	}
	var info map[string]any
	if err := c.do("GET", "/v1/server", &info); err != nil {
		return err
	}
	var svc struct {
		Services []scheduler.ServiceView `json:"services"`
	}
	if err := c.do("GET", "/v1/services", &svc); err != nil {
		return err
	}
	if p.JSONOut {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{"server": info, "services": svc.Services})
	}
	id, _ := info["server"].(map[string]any)
	reason := ""
	if r, ok := info["state_reason"].(string); ok && r != "" {
		reason = " (" + r + ")"
	}
	fmt.Printf("%v (%v): %v%s  version %v  up %vs  local API %s/\n", id["key"], id["display_name"], info["state"],
		reason, info["version"], info["uptime_s"], c.base)
	if w, ok := info["warnings"].([]any); ok {
		for _, x := range w {
			fmt.Println("  warning:", x)
		}
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "SERVICE\tPARALLEL\tRUNNING\tWAITING\tREASON")
	for _, s := range svc.Services {
		reason := s.Reason
		if s.PausedBy != "" {
			reason = "paused by " + s.PausedBy
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%s\n", s.Service, s.Parallel, s.Running, s.Waiting, reason)
	}
	return tw.Flush()
}

func cmdJobs(p paths, args []string) error {
	c, err := newClient(p)
	if err != nil {
		return err
	}
	path := "/v1/jobs?limit=50"
	if len(args) > 0 {
		path += "&state=" + args[0]
	}
	if p.JSONOut {
		return c.do("GET", path, nil)
	}
	var out struct {
		Jobs []struct {
			ID       string  `json:"job_id"`
			Service  string  `json:"service"`
			State    string  `json:"state"`
			Attempt  int     `json:"attempt"`
			Progress float64 `json:"progress"`
			Note     string  `json:"note"`
			Err      string  `json:"error_message"`
		} `json:"jobs"`
	}
	if err := c.do("GET", path, &out); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "JOB\tSERVICE\tSTATE\tATTEMPT\tPROGRESS\tNOTE")
	for _, j := range out.Jobs {
		note := j.Note
		if j.Err != "" {
			note = j.Err
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%.0f%%\t%s\n", j.ID, j.Service, j.State, j.Attempt, j.Progress*100, note)
	}
	return tw.Flush()
}

func cmdPost(p paths, path string) error {
	c, err := newClient(p)
	if err != nil {
		return err
	}
	return c.do("POST", path, nil)
}

// cmdStop drains and stops the running server (`just stop`), or kills it with --force.
func cmdStop(p paths) error {
	pidFile := filepath.Join(p.State, "state", "server.pid")
	b, err := os.ReadFile(pidFile)
	if err != nil {
		fmt.Println("not running (no", pidFile+")")
		return nil
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	proc, err := os.FindProcess(pid)
	if err != nil || !alive(proc) {
		elog.Ignore("removing a stale pid file", os.Remove(pidFile))
		fmt.Println("not running (stale pid file removed)")
		return nil
	}
	if p.Force {
		if err := proc.Kill(); err != nil {
			return elog.Check("killing the server", err, "pid", pid)
		}
		elog.Ignore("removing the pid file", os.Remove(pidFile))
		fmt.Println("killed pid", pid)
		return nil
	}
	if err := terminate(p, proc); err != nil {
		return err
	}
	fmt.Printf("stopping controller pid %d; jobs running in task hosts keep going (`just runs` lists them)\n", pid)
	start := time.Now()
	for alive(proc) {
		time.Sleep(250 * time.Millisecond)
		if time.Since(start) > time.Minute {
			return errors.New("controller did not stop within a minute; use `just stop force`")
		}
	}
	fmt.Printf("stopped after %s\n", time.Since(start).Round(100*time.Millisecond))
	return nil
}

// cmdRuns lists the jobs running in task hosts on this machine, from their run directories (`just runs`).
func cmdRuns(p paths) error {
	root := filepath.Join(p.State, "runs")
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "JOB\tSERVICE\tATTEMPT\tCODE\tPID\tALIVE\tAGE\tLAST HEARTBEAT")
	n := 0
	for _, id := range runs.List(root) {
		dir := filepath.Join(root, id)
		var rec runs.Record
		if runs.Exists(dir, runs.HandledFile) || runs.ReadJSON(dir, runs.RunFile, &rec) != nil {
			continue
		}
		var hb runs.Heartbeat
		last := "-"
		if runs.ReadJSON(dir, runs.HeartbeatFile, &hb) == nil {
			last = time.Since(hb.At).Round(time.Second).String() + " ago"
		}
		state := fmt.Sprint(runs.Alive(dir, &rec))
		if runs.Exists(dir, runs.ExitFile) {
			state = "finished (waiting for the controller)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%d\t%s\t%s\t%s\n", id, rec.Service, rec.Claim.Attempt, rec.Snapshot, rec.TaskHost.PID, state,
			time.Since(rec.ClaimedAt).Round(time.Second), last)
		n++
	}
	if n == 0 {
		fmt.Println("no jobs running in task hosts")
		return nil
	}
	return tw.Flush()
}

// cmdBroker manages this machine's nats-server service (`just broker-install`, `broker-config`, `broker-status`).
func cmdBroker(p paths, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: ai_processing_server broker install|config|status|uninstall")
	}
	ts, cat, _, err := loadConfig(p, nil)
	if err != nil {
		return err
	}
	bp := brokerctl.DefaultPaths(ts)
	logf := func(f string, a ...any) { fmt.Printf(f+"\n", a...) }
	switch args[0] {
	case "install":
		if err := brokerctl.Install(context.Background(), ts, cat.Queue, bp, logf); err != nil {
			return err
		}
		fmt.Printf("broker running as an OS service at %s (%s)\n", ts.Queue.BrokerURL, brokerctl.ServiceState())
		return nil
	case "config":
		changed, err := brokerctl.Reload(ts, cat.Queue, bp)
		if err != nil {
			return err
		}
		if changed {
			fmt.Println("broker config regenerated and reloaded:", bp.Conf)
		} else {
			fmt.Println("broker config unchanged:", bp.Conf)
		}
		return nil
	case "uninstall":
		if err := brokerctl.Uninstall(bp); err != nil {
			return err
		}
		fmt.Println("broker service removed; its stored jobs stay in", bp.Store)
		return nil
	case "status":
		fmt.Println("service:", brokerctl.ServiceState())
		fmt.Println("config: ", bp.Conf)
		q, err := openQueue(slog.New(slog.NewTextHandler(io.Discard, nil)))(context.Background(), ts, cat)
		if err != nil {
			fmt.Println("broker: ", err)
			return errors.New("broker_missing: run `just broker-install`")
		}
		defer q.Close()
		info := q.Info()
		v := info.Version
		if !brokerctl.Supported(ts, cat.Queue, v) {
			v += " (NOT in the supported range " + cat.Queue.Broker.Supported + ")"
		}
		fmt.Printf("broker:  %s nats-server %s, connected %v\n", info.URL, v, info.Connected)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if depth, err := q.Depth(ctx); err == nil {
			total := 0
			for _, n := range depth {
				total += n
			}
			fmt.Printf("work:    %d jobs stored in WORK across %d consumers\n", total, len(depth))
		}
		if srvs, err := q.Servers(ctx); err == nil {
			for _, r := range srvs {
				fmt.Printf("server:  %s %s code %s, %d services, seen %s ago\n", r.Key, r.State, r.CodeVersion, len(r.Services), time.Since(r.At).Round(time.Second))
			}
		}
		return nil
	}
	return errors.New("usage: ai_processing_server broker install|config|status|uninstall")
}
