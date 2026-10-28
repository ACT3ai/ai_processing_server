//go:build !windows

package taskhost

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The test binary doubles as the task host: Launch starts it again with APS_TASKHOST_TEST=1.
func TestMain(m *testing.M) {
	if os.Getenv("APS_TASKHOST_TEST") == "1" {
		HeartbeatEvery, CancelPoll = 200*time.Millisecond, 100*time.Millisecond
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
	writeJSONAtomic(filepath.Join(dir, HeartbeatFile), Heartbeat{PID: 123, At: time.Now().Add(-time.Minute)})
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
