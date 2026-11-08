//go:build !windows

package taskhost

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func watchExit(t *testing.T, dir string) Exit {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	x, err := Watch(ctx, dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	return x
}

// pm/cancel.mdx §11 C-T1: every step sees ACT3_CANCEL_FILE / ACT3_RUN_DIR / ACT3_SCRATCH_DIR; procs.json records the
// step's process group; a step that ignores the interrupt is killed after the profile's grace and exit.json's stop
// section says forced, which step, and when the cancel was asked and seen.
func TestCancelRecordsTheStop(t *testing.T) {
	dir := t.TempDir()
	envOut := filepath.Join(dir, "env.txt")
	tk := Task{JobID: "j1", Snapshot: "test", Output: filepath.Join(dir, "out.mp4"),
		Argv:   []string{"/bin/sh", "-c", `echo "$ACT3_CANCEL_FILE|$ACT3_RUN_DIR|$ACT3_SCRATCH_DIR" > ` + envOut + `; trap "" INT; sleep 30`},
		Cancel: &CancelSpec{Profile: "fake", GraceSeconds: 0.5}}
	if err := Launch(selfHost(), dir, tk); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if err := Cancel(dir); err != nil {
		t.Fatal(err)
	}
	x := watchExit(t, dir)
	if !x.Cancelled || x.Stop == nil || !x.Stop.Forced || x.Stop.Step != "task" || x.Stop.AskedAt.IsZero() || x.Stop.SeenAt.IsZero() {
		t.Fatalf("exit %+v stop %+v", x, x.Stop)
	}
	if x.Stop.GraceSeconds != 0.5 {
		t.Fatalf("grace %v", x.Stop.GraceSeconds)
	}
	b, _ := os.ReadFile(envOut)
	want := filepath.Join(dir, CancelFile) + "|" + dir + "|" + filepath.Join(dir, ScratchDir)
	if strings.TrimSpace(string(b)) != want {
		t.Fatalf("step env %q, want %q", b, want)
	}
	p, err := ReadProcs(dir)
	if err != nil || len(p.Groups) != 1 || p.Groups[0].PID <= 1 || p.HostPID <= 1 {
		t.Fatalf("procs.json %+v %v", p, err)
	}
}

// C-T2: a cancel written before a step starts means the step never starts.
func TestCancelBeforeAStepNeverStartsIt(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "ran")
	os.MkdirAll(dir, 0o700)
	if err := Cancel(dir); err != nil {
		t.Fatal(err)
	}
	tk := Task{JobID: "j2", Snapshot: "test", Output: filepath.Join(dir, "out.mp4"), Argv: []string{"/bin/sh", "-c", "touch " + marker}}
	if err := Launch(selfHost(), dir, tk); err != nil {
		t.Fatal(err)
	}
	x := watchExit(t, dir)
	if !x.Cancelled || x.Stop == nil || x.Stop.StepsDone != 0 {
		t.Fatalf("%+v", x)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the step ran after the cancel")
	}
}
