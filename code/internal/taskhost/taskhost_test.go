//go:build !windows

package taskhost

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The test binary doubles as the task host: Launch starts it again with APS_TASKHOST_TEST=1.
func TestMain(m *testing.M) {
	if os.Getenv("APS_TASKHOST_TEST") == "1" {
		HeartbeatEvery, CancelPoll, MemoryPoll = 200*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond
		os.Exit(Main(os.Args[1:]))
	}
	os.Setenv("APS_TASKHOST_TEST", "1")
	HeartbeatEvery, CancelPoll, WatchPoll, StaleAfter = 200*time.Millisecond, 100*time.Millisecond, 50*time.Millisecond, 2*time.Second
	os.Exit(m.Run())
}

func selfHost() []string { return []string{os.Args[0]} }

// sh writes the output after printing a stage and progress lines over about secs seconds.
func shTask(dir string, secs string) Task {
	out := filepath.Join(dir, "out.mp4")
	script := `echo "ACT3_STAGE rendering"; i=0; while [ $i -lt 5 ]; do i=$((i+1)); echo "ACT3_PROGRESS $((i*20))"; sleep ` + secs + `; done; echo "ACT3_NOTE all done"; printf video > "` + out + `"`
	return Task{JobID: "j1", Snapshot: "test", Argv: []string{"/bin/sh", "-c", script}, Output: out}
}

func TestRunsToTheEndAndReportsEvents(t *testing.T) {
	dir := t.TempDir()
	if err := Launch(selfHost(), dir, shTask(dir, "0.05")); err != nil {
		t.Fatal(err)
	}
	var stages, progress, notes int
	x, err := Watch(context.Background(), dir, func(e Event) {
		switch e.Type {
		case "stage":
			stages++
		case "progress":
			progress++
		case "note":
			notes++
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if x.Code != 0 || x.OutputBytes != 5 {
		t.Fatalf("exit %+v", x)
	}
	if stages != 1 || progress == 0 || notes != 1 {
		t.Fatalf("stages %d progress %d notes %d", stages, progress, notes)
	}
}

// A runner that speaks ACT3_STAGE / ACT3_PROGRESS owns its progress: the weight-loading tqdm bars Wan prints after
// "ACT3_STAGE loading" must not read as the job being 100% done.
func TestOwnGrammarMutesThirdPartyBars(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out.mp4")
	script := `echo "ACT3_STAGE loading"; sleep 1.1; echo "Loading checkpoint shards: 100%|##########| 12/12"; sleep 1.1; ` +
		`echo "ACT3_STAGE rendering"; echo "ACT3_PROGRESS 25.0"; printf video > "` + out + `"`
	if err := Launch(selfHost(), dir, Task{JobID: "j1", Snapshot: "test", Argv: []string{"/bin/sh", "-c", script}, Output: out}); err != nil {
		t.Fatal(err)
	}
	var got []float64
	if _, err := Watch(context.Background(), dir, func(e Event) {
		if e.Type == "progress" {
			got = append(got, e.Progress)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0.25 {
		t.Fatalf("progress events %v, want only [0.25]", got)
	}
}

// The heart of continuity: the watcher (the controller) goes away mid-task, a new one attaches, and the task still
// finishes once, without being started again.
func TestSurvivesTheControllerGoingAway(t *testing.T) {
	dir := t.TempDir()
	if err := Launch(selfHost(), dir, shTask(dir, "0.3")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	_, err := Watch(ctx, dir, nil)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first watcher: %v", err)
	}
	if s := Alive(dir); s != Running {
		t.Fatalf("after the controller left the task host is %s", s)
	}
	x, err := Watch(context.Background(), dir, nil)
	if err != nil || x.Code != 0 || x.OutputBytes == 0 {
		t.Fatalf("second watcher: %+v %v", x, err)
	}
	evs, _ := ReadEvents(filepath.Join(dir, EventsFile), 0)
	started := 0
	for _, e := range evs {
		if e.Type == "started" {
			started++
		}
	}
	if started != 1 {
		t.Fatalf("task started %d times", started)
	}
}

func TestCancelStopsTheTree(t *testing.T) {
	dir := t.TempDir()
	tk := shTask(dir, "5")
	if err := Launch(selfHost(), dir, tk); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := Cancel(dir); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	x, err := Watch(ctx, dir, nil)
	if err != nil || !x.Cancelled {
		t.Fatalf("%+v %v", x, err)
	}
	if _, err := os.Stat(tk.Output); err == nil {
		t.Fatal("a cancelled task wrote its output")
	}
}

// A floor above this machine's RAM trips the real guard: the render is killed at once (no grace) and the exit says
// out of memory as infra, so the controller fails it as "limit" and production re-plans it.
func TestLowMemoryStopsTheRender(t *testing.T) {
	dir := t.TempDir()
	tk := shTask(dir, "5")
	tk.MinFreeRAMGB = 1 << 20
	start := time.Now()
	if err := Launch(selfHost(), dir, tk); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	x, err := Watch(ctx, dir, nil)
	if err != nil || x.Code != 1 || x.Cancelled || !strings.Contains(x.Error, "out of memory") || !strings.Contains(x.Tail, "ACT3_ERROR infra: out of memory") {
		t.Fatalf("%+v %v", x, err)
	}
	if d := time.Since(start); d > 8*time.Second {
		t.Fatalf("killed after %s: the grace period was not skipped", d)
	}
	if _, err := os.Stat(tk.Output); err == nil {
		t.Fatal("a stopped task wrote its output")
	}
	if f := ramFloor(0, 128); f != 6.4 || ramFloor(0, 16) != 4 || ramFloor(10, 128) != 10 {
		t.Fatalf("floors: %g", f)
	}
}

func TestTimeoutAndFailure(t *testing.T) {
	dir := t.TempDir()
	tk := shTask(dir, "5")
	tk.TimeoutSeconds = 1
	Launch(selfHost(), dir, tk)
	x, err := Watch(context.Background(), dir, nil)
	if err != nil || !x.TimedOut {
		t.Fatalf("timeout: %+v %v", x, err)
	}
	dir2 := t.TempDir()
	Launch(selfHost(), dir2, Task{JobID: "j2", Argv: []string{"/bin/sh", "-c", "echo RuntimeError: MPS backend out of memory >&2; exit 3"}, Output: filepath.Join(dir2, "o")})
	x, err = Watch(context.Background(), dir2, nil)
	if err != nil || x.Code != 3 || x.Tail == "" {
		t.Fatalf("failure: %+v %v", x, err)
	}
}

func TestDeadWhenHeartbeatStops(t *testing.T) {
	dir := t.TempDir()
	writeJSONAtomic(filepath.Join(dir, TaskFile), Task{JobID: "j"})
	writeJSONAtomic(filepath.Join(dir, HeartbeatFile), Heartbeat{PID: deadPID(t), At: time.Now().Add(-time.Minute)})
	if s := Alive(dir); s != Dead {
		t.Fatalf("got %s", s)
	}
	if _, err := Watch(context.Background(), dir, nil); !errors.Is(err, ErrDead) {
		t.Fatal(err)
	}
}

func TestParseProgress(t *testing.T) {
	for line, want := range map[string]float64{
		"ACT3_PROGRESS 42.5": 0.425, " 45%|████": 0.45, "Batch 3/12": 0.25, "frames 120 of 300": 0.4,
	} {
		if got, ok := ParseProgress(line); !ok || got != want {
			t.Errorf("%q: %v %v", line, got, ok)
		}
	}
	if _, ok := ParseProgress("loaded t2v in 40 s"); ok {
		t.Error("plain line parsed as progress")
	}
}

// R8: a task made of steps survives its task host dying (a reboot): Relaunch skips every step whose checkpoint still
// matches its output, so only the step that was running runs again.
func TestStepsResumeFromTheCheckpointAfterTheHostDies(t *testing.T) {
	dir := t.TempDir()
	runs := filepath.Join(dir, "runs.txt")
	step := func(name, out, extra string) Step {
		return Step{Name: name, Output: out, Argv: []string{"/bin/sh", "-c",
			`echo ` + name + ` >> "` + runs + `"; ` + extra + `printf data-` + name + ` > "` + out + `"`}}
	}
	pidFile := filepath.Join(dir, "two.pid")
	task := Task{JobID: "j2", Snapshot: "test", Output: filepath.Join(dir, "three"), Steps: []Step{
		step("one", filepath.Join(dir, "one"), ""),
		// The first run of step two parks itself so the test can cut the power; the rerun goes straight through.
		step("two", filepath.Join(dir, "two"), `if [ ! -f "`+pidFile+`" ]; then echo $$ > "`+pidFile+`"; sleep 30; fi; `),
		step("three", filepath.Join(dir, "three"), ""),
	}}
	if err := Launch(selfHost(), dir, task); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(pidFile); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("step two never started")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if done, total := Checkpoints(dir); done != 1 || total != 3 {
		t.Fatalf("checkpoints before the power cut: %d of %d", done, total)
	}
	// The power goes: the task host and its render die without writing exit.json.
	var hb Heartbeat
	if err := readJSON(filepath.Join(dir, HeartbeatFile), &hb); err != nil || hb.PID == 0 {
		t.Fatalf("heartbeat: %+v %v", hb, err)
	}
	b, _ := os.ReadFile(pidFile)
	var child int
	fmt.Sscan(string(b), &child)
	syscall.Kill(-hb.PID, syscall.SIGKILL)
	syscall.Kill(hb.PID, syscall.SIGKILL)
	if child > 0 {
		syscall.Kill(-child, syscall.SIGKILL)
		syscall.Kill(child, syscall.SIGKILL)
	}
	for Alive(dir) != Dead {
		if time.Now().After(deadline.Add(10 * time.Second)) {
			t.Fatalf("the killed task host never read as dead: %s", Alive(dir))
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := Relaunch(selfHost(), dir); err != nil {
		t.Fatal(err)
	}
	x, err := Watch(context.Background(), dir, nil)
	if err != nil || x.Code != 0 || x.OutputBytes == 0 {
		t.Fatalf("after the relaunch: %+v %v", x, err)
	}
	got, _ := os.ReadFile(runs)
	if string(got) != "one\ntwo\ntwo\nthree\n" {
		t.Fatalf("step runs = %q, want one once, two twice (cut, then resumed), three once", got)
	}
	if done, total := Checkpoints(dir); done != 3 || total != 3 {
		t.Fatalf("checkpoints at the end: %d of %d", done, total)
	}
}

// A checkpoint whose output changed size (a partial file, a disk problem) does not count: the step runs again.
func TestCheckpointNeedsTheSameOutput(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "a")
	os.WriteFile(filepath.Join(dir, TaskFile), []byte(`{"run_version":1,"job_id":"j","output":"`+out+`","steps":[{"name":"a","argv":["true"],"output":"`+out+`"},{"name":"b","argv":["true"],"output":"`+out+`"}]}`), 0o600)
	os.WriteFile(out, []byte("12345"), 0o600)
	os.MkdirAll(filepath.Join(dir, CheckpointDir), 0o700)
	writeJSONAtomic(checkpointPath(dir, 0), checkpoint{Bytes: 5})
	if done, total := Checkpoints(dir); done != 1 || total != 2 {
		t.Fatalf("%d of %d", done, total)
	}
	os.WriteFile(out, []byte("12"), 0o600)
	if done, _ := Checkpoints(dir); done != 0 {
		t.Fatalf("a changed output still counted as done (%d)", done)
	}
}

// deadPID is the pid of a process that has exited and been reaped.
func deadPID(t *testing.T) int {
	t.Helper()
	c := exec.Command("true")
	if err := c.Run(); err != nil {
		t.Fatal(err)
	}
	return c.Process.Pid
}

func writeHB(t *testing.T, dir string, hb Heartbeat) {
	t.Helper()
	writeJSONAtomic(filepath.Join(dir, TaskFile), Task{JobID: "j"})
	if err := writeJSONAtomic(filepath.Join(dir, HeartbeatFile), hb); err != nil {
		t.Fatal(err)
	}
}

// After a power cut the clock can come back behind (a heartbeat "from the future") and pids are reused. Liveness
// goes by boot and pid first, so a dead host is never taken for a running one, and a live but stalled one is never
// taken for dead (which started a second host on the same run directory).
func TestAliveUsesBootAndPID(t *testing.T) {
	me := os.Getpid()
	dir := t.TempDir()
	writeHB(t, dir, Heartbeat{PID: deadPID(t), StartedAt: time.Now(), At: time.Now().Add(time.Hour)})
	if s := Alive(dir); s != Dead {
		t.Fatalf("dead pid with a heartbeat from the future: %s, want dead", s)
	}
	if BootID() != "" {
		writeHB(t, dir, Heartbeat{PID: me, Boot: "an-earlier-boot", StartedAt: time.Now(), At: time.Now()})
		if s := Alive(dir); s != Dead {
			t.Fatalf("pid reused after a reboot: %s, want dead", s)
		}
	}
	writeHB(t, dir, Heartbeat{PID: me, Boot: BootID(), StartedAt: time.Now(), At: time.Now().Add(-time.Minute)})
	if s := Alive(dir); s != Running {
		t.Fatalf("live process with a stalled heartbeat: %s, want running", s)
	}
	writeHB(t, dir, Heartbeat{PID: me, Boot: BootID(), StartedAt: time.Now(), At: time.Now().Add(-StaleHard - time.Minute)})
	if s := Alive(dir); s != Dead {
		t.Fatalf("heartbeat stalled past StaleHard: %s, want dead", s)
	}
}

// A checkpoint vouches for its output's bytes, not just its size: a same-size but different file (a torn write the
// disk reported as complete) makes the task host run the step again.
func TestCheckpointHashCatchesSameSizeCorruption(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "seg.mp4")
	os.WriteFile(out, []byte("segment-data"), 0o600)
	if err := writeCheckpoint(dir, 0, out); err != nil {
		t.Fatal(err)
	}
	st := Step{Name: "a", Argv: []string{"true"}, Output: out}
	if !stepDone(dir, 0, st, true) {
		t.Fatal("an intact checkpoint was not accepted")
	}
	os.WriteFile(out, []byte("segment-DATA"), 0o600)
	if stepDone(dir, 0, st, true) {
		t.Fatal("a same-size corrupted output passed its checkpoint")
	}
}

// One host's death seen twice counts once; a new host's death counts again; the record survives a fresh run dir.
func TestRecordDeathCountsEachHostOnce(t *testing.T) {
	dir := t.TempDir()
	writeHB(t, dir, Heartbeat{PID: 11, StartedAt: time.Unix(100, 0)})
	if n := RecordDeath(dir); n != 1 {
		t.Fatalf("first death: %d", n)
	}
	if n := RecordDeath(dir); n != 1 {
		t.Fatalf("the same death counted twice: %d", n)
	}
	writeHB(t, dir, Heartbeat{PID: 12, StartedAt: time.Unix(200, 0)})
	if n := RecordDeath(dir); n != 2 {
		t.Fatalf("second host's death: %d", n)
	}
	dir2 := t.TempDir()
	if err := CarryDeaths(dir2, Deaths(dir)); err != nil || len(Deaths(dir2)) != 2 {
		t.Fatalf("deaths not carried: %v %v", Deaths(dir2), err)
	}
}

// A heartbeat caught mid-replace (Windows refuses the read during the rename) is one bad look, not a death.
func TestWatchSurvivesOneUnreadableHeartbeat(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, TaskFile), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	hb := filepath.Join(dir, HeartbeatFile)
	if err := os.WriteFile(hb, []byte("{half a fi"), 0o600); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(WatchPoll + WatchPoll/2)
		_ = writeJSONAtomic(hb, Heartbeat{PID: os.Getpid(), Boot: BootID(), StartedAt: time.Now(), At: time.Now()})
		time.Sleep(3 * WatchPoll)
		_ = writeJSONAtomic(filepath.Join(dir, ExitFile), Exit{Code: 0})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ex, err := Watch(ctx, dir, nil)
	if err != nil || ex.Code != 0 {
		t.Fatalf("Watch = %+v, %v; want the exit record", ex, err)
	}
}

// SeedVR2 counts each pass from 1: weighted phases make one bar that only rises.
func TestPhasesWeightOnePass(t *testing.T) {
	p := newPhaseOf([]Phase{{Match: `(?i)encod`, Weight: 0.05}, {Match: `(?i)upscal`, Weight: 0.45}, {Match: `(?i)decod`, Weight: 0.5}})
	var got []float64
	for _, c := range []struct {
		line string
		frac float64
	}{{"Encoding batch 41/41", 1}, {"Upscaling batch 1/41", 0}, {"Upscaling batch 41/41", 1}, {"EulerSampler: 100%|", 1}, {"Decoding batch 10/41", 10.0 / 41}, {"Decoding batch 41/41", 1}} {
		got = append(got, p.fraction(c.line, c.frac))
	}
	want := []float64{0.05, 0.05, 0.5, 0.5, 0.5 + 0.5*10/41, 1}
	for i := range want {
		if d := got[i] - want[i]; d > 1e-9 || d < -1e-9 {
			t.Fatalf("bar = %v, want %v", got, want)
		}
	}
	if newPhaseOf(nil) != nil {
		t.Fatal("no phases: the step's own fraction")
	}
}
