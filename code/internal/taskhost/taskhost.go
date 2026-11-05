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
// A task may be a list of Steps instead of one argv (R8, checkpoints). After each step exits 0 with its output written,
// the task host records checkpoint/<n>.done. A task host started again in the same run directory (Relaunch, after a
// reboot or a crash) skips every step whose checkpoint still matches its output, so a 30-minute upscale cut into
// segments loses at most the segment it was on.
//
// The controller side (Launch, Watch, Alive) only reads those files, so any controller version that reads
// RunVersion can adopt a task host started by an older one (§9.4, §9.6).
package taskhost

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/fsx"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
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
	CheckpointDir = "checkpoint"
	DeathsFile    = "deaths.json"
)

// Timing. Variables so tests can shorten them.
var (
	HeartbeatEvery = 5 * time.Second
	StaleAfter     = 30 * time.Second // no heartbeat for this long and no exit.json: the task host is gone
	CancelPoll     = 2 * time.Second
	WatchPoll      = 1 * time.Second
	// DeadPolls: consecutive Watch polls a task host must look dead before it is taken for gone
	DeadPolls  = 3
	StartGrace = 20 * time.Second // a fresh launch gets this long to write its first heartbeat
	// StaleHard: a task host whose process is still alive (same boot, same pid) but whose heartbeat stopped is only
	// declared dead after this long. A disk stall or a swap storm can pause the heartbeat for more than StaleAfter;
	// declaring such a host dead started a second one on the same run directory while the first still rendered.
	StaleHard = 10 * time.Minute
	// MaxDeaths: after this many task hosts for one job died on this machine without finishing (a reboot or a
	// crash each time, typically a step that takes the machine down), the controller stops starting it here.
	MaxDeaths = 3
	// MemoryPoll and MemoryStrikes: the low-memory guard reads the machine's free RAM this often and stops the task
	// after this many readings in a row under the floor (one dip while weights load is not a reason).
	MemoryPoll    = time.Second
	MemoryStrikes = 2
	// freeRAM is hardware.FreeRAM; tests replace it.
	freeRAM = hardware.FreeRAM
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
	// ExpectFrames, when set, is the frame count the output video must have; the controller checks it with ffprobe
	// before the job counts as done (runners.validateVideo).
	ExpectFrames int `json:"expect_frames,omitempty"`
	// Steps, when set, replace Argv/Dir/Env: they run in order and each one is a checkpoint. The last step writes
	// Output. Older controllers that do not know Steps only ever read this file, so RunVersion stays 1.
	Steps []Step `json:"steps,omitempty"`
	// MinFreeRAMGB is the low-memory guard's floor (limits.ram_floor_gb); 0 = 5% of the machine's RAM, at least 4 GB.
	MinFreeRAMGB float64 `json:"min_free_ram_gb,omitempty"`
	// Phases, for a tool that runs several counted passes in one process (SeedVR2: encode, upscale, decode, each its
	// own "batch k/n"): a line matching a phase moves the bar into it, and the pass's own count fills only its share.
	Phases []Phase `json:"phases,omitempty"`
}

// Phase is one counted pass of a task: Match is a regular expression, Weight its share of the bar.
type Phase struct {
	Match  string  `json:"match"`
	Weight float64 `json:"weight"`
}

// phaseOf maps a pass's own fraction onto the whole bar once a line has said which phase it is in.
type phaseOf struct {
	res   []*regexp.Regexp
	start []float64 // each phase's start, as a fraction of the bar
	share []float64
	cur   int
}

func newPhaseOf(phases []Phase) *phaseOf {
	var total float64
	for _, ph := range phases {
		total += max(0, ph.Weight)
	}
	if total <= 0 {
		return nil
	}
	p := &phaseOf{}
	var at float64
	for _, ph := range phases {
		re, err := regexp.Compile(ph.Match)
		if err != nil {
			return nil
		}
		w := max(0, ph.Weight) / total
		p.res, p.start, p.share = append(p.res, re), append(p.start, at), append(p.share, w)
		at += w
	}
	return p
}

// fraction is frac of the line's phase, as a fraction of the whole bar (a line naming no phase stays in the current).
func (p *phaseOf) fraction(line string, frac float64) float64 {
	for i := len(p.res) - 1; i > p.cur; i-- {
		if p.res[i].MatchString(line) {
			p.cur = i
			break
		}
	}
	return p.start[p.cur] + p.share[p.cur]*frac
}

// ramFloor is the free RAM under which the guard stops the task.
func ramFloor(set, totalGB float64) float64 {
	if set > 0 {
		return set
	}
	return max(4, totalGB*0.05)
}

// Step is one checkpointed part of a task. Weight is its share of the progress bar (0 counts as 1).
type Step struct {
	Name   string   `json:"name"`
	Argv   []string `json:"argv"`
	Dir    string   `json:"dir,omitempty"`
	Env    []string `json:"env,omitempty"`
	Output string   `json:"output"` // the file this step must write; its size is the checkpoint
	Weight float64  `json:"weight,omitempty"`
}

// steps returns the task as a list: Steps, or one step made of Argv.
func (t Task) steps() []Step {
	if len(t.Steps) > 0 {
		return t.Steps
	}
	return []Step{{Name: "task", Argv: t.Argv, Dir: t.Dir, Env: t.Env, Output: t.Output}}
}

// checkpoint is checkpoint/<n>.done: step n finished and wrote Bytes to its output, with this SHA-256. The output is
// fsynced before the checkpoint is written, so after a power cut a checkpoint never vouches for data that did not
// reach the disk.
type checkpoint struct {
	Bytes  int64     `json:"bytes"`
	SHA256 string    `json:"sha256,omitempty"`
	At     time.Time `json:"at"`
}

func checkpointPath(dir string, n int) string {
	return filepath.Join(dir, CheckpointDir, strconv.Itoa(n)+".done")
}

// stepDone: step n has a checkpoint and its output is still there with the same size. verify also checks the
// SHA-256 (the task host does, before it skips a step; the controller's cheap counts do not).
func stepDone(dir string, n int, st Step, verify bool) bool {
	var c checkpoint
	if readJSON(checkpointPath(dir, n), &c) != nil || st.Output == "" {
		return false
	}
	fi, err := os.Stat(st.Output)
	if err != nil || fi.IsDir() || fi.Size() != c.Bytes || c.Bytes <= 0 {
		return false
	}
	if verify && c.SHA256 != "" {
		sum, err := fileSHA256(st.Output)
		return err == nil && sum == c.SHA256
	}
	return true
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Checkpoints reports how many steps of the run directory's task are done (in order, from the first) and how many
// there are. done > 0 and done < total means Relaunch resumes it instead of starting over.
func Checkpoints(dir string) (done, total int) {
	t, err := ReadTask(dir)
	if err != nil {
		return 0, 0
	}
	steps := t.steps()
	for n, st := range steps {
		if !stepDone(dir, n, st, false) {
			return n, len(steps)
		}
	}
	return len(steps), len(steps)
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

// Heartbeat is heartbeat.json. Boot is the machine's BootID when it was written: after a reboot the pid is someone
// else's (or nobody's), so a heartbeat from another boot is dead whatever its time says.
type Heartbeat struct {
	PID       int       `json:"pid"`
	Boot      string    `json:"boot,omitempty"`
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
	if len(t.Argv) == 0 && len(t.Steps) == 0 {
		return writeExit(dir, Exit{Code: -1, Error: "task.json has no argv and no steps", FinishedAt: time.Now().UTC()})
	}
	for n, st := range t.Steps {
		if len(st.Argv) == 0 {
			return writeExit(dir, Exit{Code: -1, Error: fmt.Sprintf("task.json step %d (%s) has no argv", n, st.Name), FinishedAt: time.Now().UTC()})
		}
	}
	if _, err := os.Stat(filepath.Join(dir, ExitFile)); err == nil {
		return nil // already finished; a second start is a no-op
	}
	logf, err := os.OpenFile(filepath.Join(dir, LogFile), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer logf.Close()
	ev, err := openEvents(dir, logf)
	if err != nil {
		return err
	}
	defer ev.close()

	started := time.Now().UTC()
	boot := BootID()
	var beatErr sync.Once
	beat := func() {
		// Atomic, not durable: rewritten every few seconds, and after a power cut Boot says it is stale anyway.
		if err := writeJSON(filepath.Join(dir, HeartbeatFile), Heartbeat{PID: os.Getpid(), Boot: boot, StartedAt: started, At: time.Now().UTC()}, false); err != nil {
			beatErr.Do(func() { fmt.Fprintf(logf, "# cannot write the heartbeat (disk full?): %v\n", err) })
		}
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

	steps := t.steps()
	weights, total := make([]float64, len(steps)), 0.0
	for n, st := range steps {
		weights[n] = st.Weight
		if weights[n] <= 0 {
			weights[n] = 1
		}
		total += weights[n]
	}
	var before float64 // weight of the steps already done, for the overall progress fraction
	cur := 0
	phases := newPhaseOf(t.Phases)

	tail := newTail(200) // verbose tools (SeedVR2) log a cleanup section and a banner after the error
	var logMu sync.Mutex
	// A runner that speaks our grammar (ACT3_STAGE / ACT3_PROGRESS) owns its progress: from its first such line on,
	// tqdm bars it prints (Wan's "Loading checkpoint shards: 100%|") are not job progress. Without this a Wan render
	// read 100% while still loading weights (2026-10-28, task act3_b415e3d31aac833bb9bbf031_a2).
	var ownsProgress bool
	line := func(s string) {
		logMu.Lock()
		fmt.Fprintln(logf, s)
		st := ParseStage(s)
		own := st != "" || reOwn.MatchString(s)
		ownsProgress = ownsProgress || own
		thirdPartyMuted := ownsProgress && !own
		logMu.Unlock()
		tail.add(s)
		if st != "" {
			ev.add(Event{Type: "stage", Stage: st})
		} else if p, ok := ParseProgress(s); ok {
			// a loading bar (checkpoint shards, a download) reaching 100% is not the render reaching 100%
			if !thirdPartyMuted && (own || !reLoading.MatchString(s)) {
				if phases != nil {
					p = phases.fraction(s, p)
				}
				ev.progress((before+weights[cur]*p)/total, s)
			}
		} else if strings.HasPrefix(s, "ACT3_NOTE ") {
			ev.add(Event{Type: "note", Note: strings.TrimPrefix(s, "ACT3_NOTE ")})
		}
	}
	var lowMemory string // the guard's verdict, set before it cancels
	go func() {
		// A render that eats the machine's RAM freezes it until a reboot: stop the render first.
		tick := time.NewTicker(MemoryPoll)
		defer tick.Stop()
		strikes := 0
		for {
			select {
			case <-done:
				return
			case <-tick.C:
			}
			free, total, ok := freeRAM()
			floor := ramFloor(t.MinFreeRAMGB, total)
			if !ok || free >= floor {
				strikes = 0
				continue
			}
			if strikes++; strikes < MemoryStrikes {
				continue
			}
			lowMemory = fmt.Sprintf("out of memory: the machine had %.1f GB of RAM free, under the %.1f GB floor "+
				"(limits.ram_floor_gb), so the render was stopped to keep the machine up", free, floor)
			line("ACT3_ERROR infra: " + lowMemory)
			cancel(fmt.Errorf("%w: %s", procs.ErrKillNow, lowMemory))
			return
		}
	}()
	fmt.Fprintf(logf, "# %s taskhost pid %d, snapshot %s, %d step(s)\n", started.Format(time.RFC3339), os.Getpid(), t.Snapshot, len(steps))
	var runErr error
	for n, st := range steps {
		cur = n
		logMu.Lock()
		ownsProgress = false // each step is its own process: it earns (or not) the right to mute tqdm afresh
		logMu.Unlock()
		if stepDone(dir, n, st, true) {
			fmt.Fprintf(logf, "# step %d/%d %s: already done (checkpoint), skipped\n", n+1, len(steps), st.Name)
			before += weights[n]
			continue
		}
		if ctx.Err() != nil {
			break
		}
		if len(steps) > 1 {
			ev.add(Event{Type: "note", Note: fmt.Sprintf("step %d/%d: %s", n+1, len(steps), st.Name)})
		}
		fmt.Fprintf(logf, "# step %d/%d %s\n# %s\n", n+1, len(steps), st.Name, strings.Join(st.Argv, " "))
		runErr = procs.Run(ctx, procs.Spec{Name: st.Argv[0], Args: st.Argv[1:], Dir: st.Dir, Env: st.Env, GPU: t.GPU,
			Stdout: line, Stderr: line, StdinNil: true})
		if runErr != nil || ctx.Err() != nil {
			break
		}
		fi, err := os.Stat(st.Output)
		if err != nil || fi.Size() == 0 {
			runErr = fmt.Errorf("step %d (%s) exited 0 but wrote no output %s", n+1, st.Name, st.Output)
			break
		}
		if err := writeCheckpoint(dir, n, st.Output); err != nil {
			fmt.Fprintf(logf, "# could not write checkpoint %d: %v\n", n, err) // the step still counts; only resume loses it
		}
		before += weights[n]
		if len(steps) > 1 {
			ev.progress(before/total, fmt.Sprintf("step %d/%d done", n+1, len(steps)))
		}
	}
	close(done)

	x := Exit{Code: 0, FinishedAt: time.Now().UTC(), Tail: tail.String()}
	cause := context.Cause(ctx)
	switch {
	case errors.Is(cause, errCancel):
		x.Code, x.Cancelled, x.Error = -1, true, "cancelled"
	case errors.Is(cause, errTimedOut):
		x.Code, x.TimedOut, x.Error = -1, true, fmt.Sprintf("timed out after %d s", t.TimeoutSeconds)
	case errors.Is(cause, procs.ErrKillNow):
		x.Code, x.Error = 1, lowMemory
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

func writeExit(dir string, x Exit) error { return writeJSON(filepath.Join(dir, ExitFile), x, true) }

// writeCheckpoint fsyncs the step's output, then durably records its size and hash.
func writeCheckpoint(dir string, n int, output string) error {
	if err := fsx.SyncFile(output); err != nil {
		return err
	}
	fi, err := os.Stat(output)
	if err != nil {
		return err
	}
	sum, err := fileSHA256(output)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(dir, CheckpointDir), 0o700); err != nil {
		return err
	}
	return writeJSON(checkpointPath(dir, n), checkpoint{Bytes: fi.Size(), SHA256: sum, At: time.Now().UTC()}, true)
}

// events appends to events.jsonl, throttling progress to one line a second.
type events struct {
	mu       sync.Mutex
	f        *os.File
	log      io.Writer
	failed   bool
	seq      int
	lastProg time.Time
}

func openEvents(dir string, log io.Writer) (*events, error) {
	p := filepath.Join(dir, EventsFile)
	seq := 0
	if old, err := ReadEvents(p, 0); err == nil && len(old) > 0 {
		seq = old[len(old)-1].Seq // a restarted task host continues the numbering
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	return &events{f: f, log: log, seq: seq}, nil
}

func (e *events) add(ev Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seq++
	ev.Seq, ev.At = e.seq, time.Now().UTC()
	b, _ := json.Marshal(ev)
	if _, err := e.f.Write(append(b, '\n')); err != nil && !e.failed {
		e.failed = true // say it once (a full disk fails every line); the render itself goes on
		fmt.Fprintf(e.log, "# cannot append to events.jsonl: %v\n", err)
	}
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

func (e *events) close() {
	if err := e.f.Close(); err != nil {
		fmt.Fprintf(e.log, "# closing events.jsonl: %v\n", err)
	}
}

// ---------------------------------------------------------------------------------------------------------------
// Output grammar shared by our runners: "ACT3_PROGRESS 42.5", "ACT3_STAGE rendering", "ACT3_NOTE text"; and the
// third-party forms diffusers / tqdm / SeedVR2 print ("45%|", "Batch 3/12", "frames 120 of 300").

var (
	reOwn   = regexp.MustCompile(`ACT3_PROGRESS\s+(\d{1,3}(?:\.\d+)?)`)
	rePct   = regexp.MustCompile(`(\d{1,3}(?:\.\d+)?)\s*%\s*\|`)
	reRatio = regexp.MustCompile(`(?i)\b(?:step|steps|batch|frames?|chunk|segment)\b[^0-9]{0,12}(\d+)\s*(?:/|of)\s*(\d+)`)
	reStage = regexp.MustCompile(`ACT3_STAGE\s+(\S+)`)
	// third-party bars that measure loading, not rendering
	reLoading = regexp.MustCompile(`(?i)\b(?:loading|load|shards?|fetching|downloading)\b`)
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
	if err := writeJSON(filepath.Join(dir, TaskFile), t, true); err != nil {
		return err
	}
	// Mark the launch time so Alive gives a new task host StartGrace before its first heartbeat.
	if err := writeJSON(filepath.Join(dir, HeartbeatFile), Heartbeat{Boot: BootID(), At: time.Now().UTC()}, false); err != nil {
		return err
	}
	args := append(append([]string{}, hostArgv[1:]...), "--run", dir)
	return startDetached(hostArgv[0], args, filepath.Join(dir, "taskhost.out"))
}

// Relaunch starts a task host again in a run directory whose task host died (Dead: reboot, crash, kill -9), keeping
// task.json, its snapshot, the inputs, events and checkpoints. Completed steps are skipped. The caller decides that
// resuming is worth it (Checkpoints) and that the snapshot still exists.
func Relaunch(hostArgv []string, dir string) error {
	if len(hostArgv) == 0 {
		return errors.New("taskhost: no host command")
	}
	if Alive(dir) != Dead {
		return fmt.Errorf("taskhost: relaunch needs a dead task host, %s is %s", dir, Alive(dir))
	}
	if err := os.Remove(filepath.Join(dir, CancelFile)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := writeJSON(filepath.Join(dir, HeartbeatFile), Heartbeat{Boot: BootID(), At: time.Now().UTC()}, false); err != nil {
		return err
	}
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

// Alive reports the run directory's state. Liveness is decided, in order, by:
//   - the boot: a heartbeat written before the machine last booted is dead (its pid may now be an unrelated process);
//   - the pid: no such process means dead, whatever the heartbeat's time says (a clock that jumped back after a
//     power cut made a dead host look alive forever);
//   - the heartbeat age: fresh is running; stale with the process still there is running until StaleHard.
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
	if cur := BootID(); hb.Boot != "" && cur != "" && hb.Boot != cur {
		return Dead
	}
	if hb.PID > 0 && !pidAlive(hb.PID) {
		return Dead
	}
	limit := StaleAfter
	if hb.PID == 0 {
		limit = StartGrace // launched, first heartbeat not written yet
	}
	age := time.Since(hb.At)
	switch {
	case age <= limit:
		return Running
	case hb.PID > 0 && age <= StaleHard:
		return Running // alive but its heartbeat is stalled (disk stall, swap): never start a second host beside it
	}
	return Dead
}

// RecordDeath notes that the task host whose heartbeat is in dir died without writing exit.json, and returns how
// many task hosts for this job have died here. One host's death seen twice (by Watch, then by the next Run) counts
// once. CarryDeaths keeps the record when the run directory is replaced to start over.
func RecordDeath(dir string) int {
	key := "unreadable"
	var hb Heartbeat
	if readJSON(filepath.Join(dir, HeartbeatFile), &hb) == nil {
		key = fmt.Sprintf("%d@%s", hb.PID, hb.StartedAt.Format(time.RFC3339Nano))
		if hb.PID == 0 { // a host that never wrote its first heartbeat: the launch time tells launches apart
			key = "launch@" + hb.At.Format(time.RFC3339Nano)
		}
	} else if fi, err := os.Stat(filepath.Join(dir, HeartbeatFile)); err == nil {
		key = "mtime@" + fi.ModTime().UTC().Format(time.RFC3339Nano)
	}
	seen := Deaths(dir)
	for _, k := range seen {
		if k == key {
			return len(seen)
		}
	}
	seen = append(seen, key)
	if err := writeJSON(filepath.Join(dir, DeathsFile), seen, true); err != nil {
		fmt.Fprintf(os.Stderr, "taskhost: recording a task host death in %s: %v\n", dir, err)
	}
	return len(seen)
}

// Deaths lists the recorded task host deaths of the run directory.
func Deaths(dir string) []string {
	var seen []string
	_ = readJSON(filepath.Join(dir, DeathsFile), &seen) // none recorded is the normal case
	return seen
}

// CarryDeaths writes deaths recorded in an earlier run directory into a new one for the same job.
func CarryDeaths(dir string, deaths []string) error {
	if len(deaths) == 0 {
		return nil
	}
	return writeJSON(filepath.Join(dir, DeathsFile), deaths, true)
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
// A run directory that is already gone needs no cancel.
func Cancel(dir string) error {
	err := os.WriteFile(filepath.Join(dir, CancelFile), []byte(time.Now().UTC().Format(time.RFC3339)), 0o600)
	if os.IsNotExist(err) {
		if _, serr := os.Stat(dir); os.IsNotExist(serr) {
			return nil
		}
	}
	return err
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
	dead := 0
	for {
		flush()
		switch Alive(dir) {
		case Finished:
			flush()
			return ReadExit(dir)
		case Dead, Missing:
			// one look can land on a heartbeat being replaced (Windows refuses the read mid-rename): the task host
			// is gone only when it looks gone DeadPolls times in a row
			if dead++; dead >= DeadPolls {
				return Exit{}, ErrDead
			}
		default:
			dead = 0
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

// writeJSON writes via a unique temp file and a rename, so a reader never sees half a file; durable also fsyncs
// (task.json, checkpoints, exit.json: files a power cut must not leave empty).
func writeJSON(path string, v any, durable bool) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if durable {
		return fsx.WriteDurable(path, b, 0o600)
	}
	return fsx.WriteAtomic(path, b, 0o600)
}

// writeJSONAtomic is writeJSON without fsync (tests and callers that rewrite often).
func writeJSONAtomic(path string, v any) error { return writeJSON(path, v, false) }

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
