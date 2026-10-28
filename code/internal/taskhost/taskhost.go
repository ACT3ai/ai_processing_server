// Package taskhost runs one job's process tree outside the controller, so a git pull and a controller restart never
// stop a render that is underway (pm/task_queue.mdx §9.3, pm/task_authoring.mdx R1, R4, R6, R9).
//
// The controller writes task.json into the job's run directory (data/git_ignore/runs/<job_id>/) and starts
// `<snapshot>/ai_processing_server taskhost --run <dir>` detached: its own session and process group, so the
// controller can exit, be killed or be rebuilt while the task host and its children keep running. The task host:
//
//   - runs the task's argv (from the frozen snapshot) with a cleaned environment, in its own process group;
//   - appends every output line to task.log and turns progress and stage lines into events.jsonl;
//   - writes heartbeat.json every few seconds (liveness: a heartbeat older than StaleAfter means it died);
//   - stops the tree when a `cancel` file appears or the task's timeout passes;
//   - writes exit.json last. Nothing else in the run directory changes after exit.json.
//
// The controller side (Launch, Watch, Alive) only reads those files, so any controller version that reads
// RunVersion can adopt a task host started by an older one (§9.4, §9.6).
package taskhost

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/procs"
)

// RunVersion is the run directory contract this package writes and reads.
const RunVersion = 1

// File names inside a run directory (pm/task_authoring.mdx §6).
const (
	TaskFile      = "task.json"
	EventsFile    = "events.jsonl"
	HeartbeatFile = "heartbeat.json"
	ExitFile      = "exit.json"
	CancelFile    = "cancel"
	LogFile       = "task.log"
)

// Timing. Variables so tests can shorten them.
var (
	HeartbeatEvery = 5 * time.Second
	StaleAfter     = 30 * time.Second // no heartbeat for this long and no exit.json: the task host is gone
	CancelPoll     = 2 * time.Second
	WatchPoll      = 1 * time.Second
	StartGrace     = 20 * time.Second // a fresh launch gets this long to write its first heartbeat
)

// Task is task.json: everything the task host needs. Paths are absolute.
type Task struct {
	RunVersion     int      `json:"run_version"`
	JobID          string   `json:"job_id"`
	Snapshot       string   `json:"snapshot"` // snapshot key the code came from (code_version)
	Argv           []string `json:"argv"`
	Dir            string   `json:"dir,omitempty"`
	Env            []string `json:"env,omitempty"` // extra KEY=VALUE pairs on top of procs' cleaned environment
	GPU            *int     `json:"gpu,omitempty"`
	Output         string   `json:"output"` // the file the task must write
	TimeoutSeconds int64    `json:"timeout_seconds,omitempty"`
}

// Event is one line of events.jsonl.
type Event struct {
	Seq      int       `json:"seq"`
	At       time.Time `json:"at"`
	Type     string    `json:"type"` // started | stage | progress | note
	Stage    string    `json:"stage,omitempty"`
	Progress float64   `json:"progress,omitempty"` // 0..1
	Note     string    `json:"note,omitempty"`
}

// Heartbeat is heartbeat.json.
type Heartbeat struct {
	PID       int       `json:"pid"`
	StartedAt time.Time `json:"started_at"`
	At        time.Time `json:"at"`
}

// Exit is exit.json.
type Exit struct {
	Code        int       `json:"code"`
	Cancelled   bool      `json:"cancelled,omitempty"`
	TimedOut    bool      `json:"timed_out,omitempty"`
	Error       string    `json:"error,omitempty"`
	Tail        string    `json:"tail,omitempty"` // last lines of output, for the failure message
	OutputBytes int64     `json:"output_bytes"`
	FinishedAt  time.Time `json:"finished_at"`
}

// ---------------------------------------------------------------------------------------------------------------
// Task host side: `ai_processing_server taskhost --run <dir>`.

// Main is the taskhost subcommand. It returns the process exit code. It never touches the controller.
func Main(args []string) int {
	dir := ""
	for i := 0; i < len(args); i++ {
		if args[i] == "--run" && i+1 < len(args) {
			dir = args[i+1]
			i++
		}
	}
	if dir == "" {
		fmt.Fprintln(os.Stderr, "usage: ai_processing_server taskhost --run <run_dir>")
		return 2
	}
	if err := host(dir); err != nil {
		fmt.Fprintln(os.Stderr, "taskhost:", err)
		return 1
	}
	return 0
}

func host(dir string) error {
	var t Task
	if err := readJSON(filepath.Join(dir, TaskFile), &t); err != nil {
		return err
	}
	if len(t.Argv) == 0 {
		return writeExit(dir, Exit{Code: -1, Error: "task.json has no argv", FinishedAt: time.Now().UTC()})
	}
	if _, err := os.Stat(filepath.Join(dir, ExitFile)); err == nil {
		return nil // already finished; a second start is a no-op
	}
	logf, err := os.OpenFile(filepath.Join(dir, LogFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer logf.Close()
	ev, err := openEvents(dir)
	if err != nil {
		return err
	}
	defer ev.close()

	started := time.Now().UTC()
	beat := func() {
		_ = writeJSONAtomic(filepath.Join(dir, HeartbeatFile), Heartbeat{PID: os.Getpid(), StartedAt: started, At: time.Now().UTC()})
	}
	beat()
	ev.add(Event{Type: "started", Note: "snapshot " + t.Snapshot})

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	if t.TimeoutSeconds > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeoutCause(ctx, time.Duration(t.TimeoutSeconds)*time.Second, errTimedOut)
		defer stop()
	}
	errCancel := errors.New("cancelled")
	done := make(chan struct{})
	go func() {
		hb := time.NewTicker(HeartbeatEvery)
		cp := time.NewTicker(CancelPoll)
		defer hb.Stop()
		defer cp.Stop()
		for {
			select {
			case <-done:
				return
			case <-hb.C:
				beat()
			case <-cp.C:
				if _, err := os.Stat(filepath.Join(dir, CancelFile)); err == nil {
					cancel(errCancel)
				}
			}
		}
	}()

	tail := newTail(40)
	var logMu sync.Mutex
	line := func(s string) {
		logMu.Lock()
		fmt.Fprintln(logf, s)
		logMu.Unlock()
		tail.add(s)
		if st := ParseStage(s); st != "" {
			ev.add(Event{Type: "stage", Stage: st})
		} else if p, ok := ParseProgress(s); ok {
			ev.progress(p, s)
		} else if strings.HasPrefix(s, "ACT3_NOTE ") {
			ev.add(Event{Type: "note", Note: strings.TrimPrefix(s, "ACT3_NOTE ")})
		}
	}
	fmt.Fprintf(logf, "# %s taskhost pid %d, snapshot %s\n# %s\n", started.Format(time.RFC3339), os.Getpid(), t.Snapshot, strings.Join(t.Argv, " "))
	runErr := procs.Run(ctx, procs.Spec{Name: t.Argv[0], Args: t.Argv[1:], Dir: t.Dir, Env: t.Env, GPU: t.GPU,
		Stdout: line, Stderr: line, StdinNil: true})
	close(done)

	x := Exit{Code: 0, FinishedAt: time.Now().UTC(), Tail: tail.String()}
	cause := context.Cause(ctx)
	switch {
	case errors.Is(cause, errCancel):
		x.Code, x.Cancelled, x.Error = -1, true, "cancelled"
	case errors.Is(cause, errTimedOut):
		x.Code, x.TimedOut, x.Error = -1, true, fmt.Sprintf("timed out after %d s", t.TimeoutSeconds)
	case runErr != nil:
		x.Code, x.Error = exitCode(runErr), runErr.Error()
	}
	if st, err := os.Stat(t.Output); err == nil {
		x.OutputBytes = st.Size()
	}
	fmt.Fprintf(logf, "# %s exit %d %s\n", x.FinishedAt.Format(time.RFC3339), x.Code, x.Error)
	return writeExit(dir, x)
}

var errTimedOut = errors.New("timed out")

func exitCode(err error) int {
	var ee interface{ ExitCode() int }
	if errors.As(err, &ee) && ee.ExitCode() > 0 {
		return ee.ExitCode()
	}
	return 1
}

func writeExit(dir string, x Exit) error { return writeJSONAtomic(filepath.Join(dir, ExitFile), x) }

// events appends to events.jsonl, throttling progress to one line a second.
type events struct {
	mu       sync.Mutex
	f        *os.File
	seq      int
	lastProg time.Time
}

func openEvents(dir string) (*events, error) {
	p := filepath.Join(dir, EventsFile)
	seq := 0
	if old, err := ReadEvents(p, 0); err == nil && len(old) > 0 {
		seq = old[len(old)-1].Seq // a restarted task host continues the numbering
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &events{f: f, seq: seq}, nil
}

func (e *events) add(ev Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seq++
	ev.Seq, ev.At = e.seq, time.Now().UTC()
	b, _ := json.Marshal(ev)
	e.f.Write(append(b, '\n'))
}

func (e *events) progress(frac float64, note string) {
	e.mu.Lock()
	skip := time.Since(e.lastProg) < time.Second && frac < 1
	if !skip {
		e.lastProg = time.Now()
	}
	e.mu.Unlock()
	if !skip {
		e.add(Event{Type: "progress", Progress: frac, Note: strings.TrimSpace(note)})
	}
}

func (e *events) close() { e.f.Close() }

// ---------------------------------------------------------------------------------------------------------------
// Output grammar shared by our runners: "ACT3_PROGRESS 42.5", "ACT3_STAGE rendering", "ACT3_NOTE text"; and the
// third-party forms diffusers / tqdm / SeedVR2 print ("45%|", "Batch 3/12", "frames 120 of 300").

var (
	reOwn   = regexp.MustCompile(`ACT3_PROGRESS\s+(\d{1,3}(?:\.\d+)?)`)
	rePct   = regexp.MustCompile(`(\d{1,3}(?:\.\d+)?)\s*%\s*\|`)
	reRatio = regexp.MustCompile(`(?i)\b(?:step|steps|batch|frames?|chunk|segment)\b[^0-9]{0,12}(\d+)\s*(?:/|of)\s*(\d+)`)
	reStage = regexp.MustCompile(`ACT3_STAGE\s+(\S+)`)
)

// ParseProgress reads a 0..1 fraction from one output line.
func ParseProgress(line string) (float64, bool) {
	if m := reOwn.FindStringSubmatch(line); m != nil {
		v, _ := strconv.ParseFloat(m[1], 64)
		return min(v, 100) / 100, true
	}
	if m := rePct.FindStringSubmatch(line); m != nil {
		if v, _ := strconv.ParseFloat(m[1], 64); v <= 100 {
			return v / 100, true
		}
	}
	if m := reRatio.FindStringSubmatch(line); m != nil {
		a, _ := strconv.Atoi(m[1])
		b, _ := strconv.Atoi(m[2])
		if b > 0 && a <= b {
			return float64(a) / float64(b), true
		}
	}
	return 0, false
}

// ParseStage reads an ACT3_STAGE line.
func ParseStage(line string) string {
	if m := reStage.FindStringSubmatch(line); m != nil {
		return m[1]
	}
	return ""
}

// ---------------------------------------------------------------------------------------------------------------
// Controller side.

// Launch writes task.json and starts the task host detached from the calling process. hostArgv is the command that
// runs this package's Main: normally {<snapshot>/ai_processing_server, "taskhost"}; "--run <dir>" is appended.
// It returns once the process has started; the task host outlives the caller.
func Launch(hostArgv []string, dir string, t Task) error {
	if len(hostArgv) == 0 {
		return errors.New("taskhost: no host command")
	}
	t.RunVersion = RunVersion
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := writeJSONAtomic(filepath.Join(dir, TaskFile), t); err != nil {
		return err
	}
	// Mark the launch time so Alive gives a new task host StartGrace before its first heartbeat.
	_ = writeJSONAtomic(filepath.Join(dir, HeartbeatFile), Heartbeat{At: time.Now().UTC()})
	args := append(append([]string{}, hostArgv[1:]...), "--run", dir)
	return startDetached(hostArgv[0], args, filepath.Join(dir, "taskhost.out"))
}

// State is what the controller sees in a run directory.
type State int

const (
	Missing  State = iota // no task.json: never launched
	Running               // heartbeat fresh, no exit.json
	Finished              // exit.json written
	Dead                  // launched, no exit.json, heartbeat stale: the task host is gone (reboot, crash)
)

func (s State) String() string { return [...]string{"missing", "running", "finished", "dead"}[s] }

// Alive reports the run directory's state.
func Alive(dir string) State {
	if _, err := os.Stat(filepath.Join(dir, ExitFile)); err == nil {
		return Finished
	}
	if _, err := os.Stat(filepath.Join(dir, TaskFile)); err != nil {
		return Missing
	}
	var hb Heartbeat
	if err := readJSON(filepath.Join(dir, HeartbeatFile), &hb); err != nil {
		return Dead
	}
	limit := StaleAfter
	if hb.PID == 0 {
		limit = StartGrace // launched, first heartbeat not written yet
	}
	if time.Since(hb.At) > limit {
		return Dead
	}
	return Running
}

// ReadExit reads exit.json.
func ReadExit(dir string) (Exit, error) {
	var x Exit
	err := readJSON(filepath.Join(dir, ExitFile), &x)
	return x, err
}

// ReadTask reads task.json.
func ReadTask(dir string) (Task, error) {
	var t Task
	err := readJSON(filepath.Join(dir, TaskFile), &t)
	return t, err
}

// Cancel asks the task host to stop its tree; it writes exit.json within CancelPoll plus the procs grace period.
func Cancel(dir string) error {
	return os.WriteFile(filepath.Join(dir, CancelFile), []byte(time.Now().UTC().Format(time.RFC3339)), 0o600)
}

// ReadEvents returns the events in path with Seq > after.
func ReadEvents(path string, after int) ([]Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		var e Event
		if json.Unmarshal(sc.Bytes(), &e) == nil && e.Seq > after {
			out = append(out, e)
		}
	}
	return out, nil
}

// ErrDead is returned by Watch when the task host vanished without writing exit.json.
var ErrDead = errors.New("the task host stopped without finishing (machine reboot or crash)")

// Watch follows a run directory until exit.json appears, calling onEvent for every new event (in order). It returns
// the exit record, ErrDead, or ctx's error. Returning on ctx never stops the task host: that is Cancel's job.
func Watch(ctx context.Context, dir string, onEvent func(Event)) (Exit, error) {
	seen := 0
	flush := func() {
		evs, _ := ReadEvents(filepath.Join(dir, EventsFile), seen)
		for _, e := range evs {
			seen = e.Seq
			if onEvent != nil {
				onEvent(e)
			}
		}
	}
	t := time.NewTicker(WatchPoll)
	defer t.Stop()
	for {
		flush()
		switch Alive(dir) {
		case Finished:
			flush()
			return ReadExit(dir)
		case Dead, Missing:
			return Exit{}, ErrDead
		}
		select {
		case <-ctx.Done():
			return Exit{}, ctx.Err()
		case <-t.C:
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// writeJSONAtomic writes via a temp file and rename, so a reader never sees half a file.
func writeJSONAtomic(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

type tailBuf struct {
	mu    sync.Mutex
	n     int
	lines []string
}

func newTail(n int) *tailBuf { return &tailBuf{n: n} }

func (t *tailBuf) add(s string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lines = append(t.lines, s)
	if len(t.lines) > t.n {
		t.lines = t.lines[len(t.lines)-t.n:]
	}
}

func (t *tailBuf) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.Join(t.lines, "\n")
}
