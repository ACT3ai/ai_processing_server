//go:build !windows

package runners

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/procs"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
	"github.com/ACT3ai/ai_processing_server/internal/taskhost"
)

// stopRep is recRep that also takes the stop report (the scheduler's reporter does the same).
type stopRep struct {
	recRep
	stop *jobs.StopReport
}

func (r *stopRep) Stopped(s jobs.StopReport) { r.stop = &s }

// pm/cancel.mdx §11 C-R1: a cancel mid-render puts the render in cancel mode: the runner returns only once the tree
// is verified gone, the run directory is removed, and the stop report says so.
func TestCancelModeVerifiesAndReports(t *testing.T) {
	env, job := setupFake(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() { time.Sleep(700 * time.Millisecond); cancel(errors.New("cancelled by production")) }()
	rep := &stopRep{}
	if _, err := (OSSVideo{}).Run(ctx, job, env, rep); err == nil {
		t.Fatal("cancel returned no error")
	}
	r := rep.stop
	if r == nil || r.Profile != "fake" || !r.TreeVerified || r.RunDirKept || r.Escalated || r.GPU != "not_used" || r.Seconds <= 0 {
		t.Fatalf("stop report %+v", r)
	}
	if _, err := os.Stat(runDirFor(env, job.ID)); !os.IsNotExist(err) {
		t.Fatal("the run directory of a cancelled render was kept")
	}
}

// startGroup starts a process in its own process group and reaps it when it dies (as the task host reaps its steps).
func startGroup(t *testing.T) int {
	t.Helper()
	c := exec.Command("/bin/sh", "-c", `trap "" INT TERM; sleep 60`)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	go c.Wait()
	t.Cleanup(func() { syscall.Kill(-c.Process.Pid, syscall.SIGKILL) })
	return c.Process.Pid
}

// C-R2: a task host that never stops (wedged: it ignores the cancel file and never writes exit.json) is killed by
// the controller — its own group and every group in procs.json — and the stop is still verified.
func TestCancelModeEscalatesWhenTheTaskHostIsWedged(t *testing.T) {
	oldM, oldV := StopMargin, TreeVerifyWait
	StopMargin, TreeVerifyWait = 300*time.Millisecond, 3*time.Second
	t.Cleanup(func() { StopMargin, TreeVerifyWait = oldM, oldV })
	dir := t.TempDir()
	host, step := startGroup(t), startGroup(t)
	hostAt, _ := procs.StartTime(host)
	stepAt, _ := procs.StartTime(step)
	b, _ := json.Marshal(taskhost.Task{RunVersion: 1, JobID: "w1", Argv: []string{"true"}, Output: filepath.Join(dir, "out.mp4"),
		Cancel: &taskhost.CancelSpec{Profile: "fake", GraceSeconds: 0.2}})
	os.WriteFile(filepath.Join(dir, taskhost.TaskFile), b, 0o600)
	beat := func() {
		b, _ := json.Marshal(taskhost.Heartbeat{PID: host, Boot: taskhost.BootID(), StartedAt: hostAt, At: time.Now()})
		os.WriteFile(filepath.Join(dir, taskhost.HeartbeatFile), b, 0o600)
	}
	beat()
	b, _ = json.Marshal(taskhost.Procs{HostPID: host, Groups: []taskhost.ProcStep{{Step: "task", PID: step, StartedAt: stepAt}}})
	os.WriteFile(filepath.Join(dir, taskhost.ProcsFile), b, 0o600)

	rep := &stopRep{}
	r := stopTask(dir, rep)
	if !r.Escalated || !r.TreeVerified || r.RunDirKept {
		t.Fatalf("stop report %+v", r)
	}
	if syscall.Kill(-step, 0) == nil || syscall.Kill(-host, 0) == nil {
		t.Fatal("a process group survived the escalation")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("run directory kept after a verified stop")
	}
}

// C-R3: on an NVIDIA machine the stop waits until nvidia-smi no longer lists the tree's processes; one that is
// still listed at the end is reported "held" (and is a problem), and a machine without nvidia-smi is "not_checked".
func TestGPUReleaseCheck(t *testing.T) {
	old, oldW := gpuPIDs, GPUReleaseWait
	t.Cleanup(func() { gpuPIDs, GPUReleaseWait = old, oldW })
	GPUReleaseWait = 1500 * time.Millisecond
	dir := t.TempDir()
	b, _ := json.Marshal(taskhost.Procs{Groups: []taskhost.ProcStep{{Step: "upscale 1/2", PID: 4242, StartedAt: time.Now()}}})
	os.WriteFile(filepath.Join(dir, taskhost.ProcsFile), b, 0o600)
	tr := treeOf(dir)
	calls := 0
	gpuPIDs = func() ([]int, bool) {
		calls++
		if calls < 2 {
			return []int{4242}, true
		}
		return nil, true
	}
	if got := waitGPUReleased(tr, GPUReleaseWait); got != "released" {
		t.Fatalf("got %s", got)
	}
	gpuPIDs = func() ([]int, bool) { return []int{4242}, true }
	if got := waitGPUReleased(tr, 300*time.Millisecond); got != "held" {
		t.Fatalf("got %s", got)
	}
	gpuPIDs = func() ([]int, bool) { return []int{999999}, true } // someone else's render on the same card
	if got := waitGPUReleased(tr, 300*time.Millisecond); got != "released" {
		t.Fatalf("another job's process counted as ours: %s", got)
	}
	gpuPIDs = func() ([]int, bool) { return nil, false }
	if got := waitGPUReleased(tr, time.Second); got != "not_checked" {
		t.Fatalf("got %s", got)
	}
}

// C-R4: every render type's task carries its own cancel profile (pm/cancel.mdx §8).
func TestEveryRenderTypeHasACancelProfile(t *testing.T) {
	snap := snapshot.Snapshot{Key: "k", Dir: t.TempDir()}
	var vp videoParams
	vp.defaults()
	var up upscaleParams
	up.defaults()
	cases := map[string]detachedTask{
		"seedvr2":     seedvr2Task(up, "in.mp4", "out.mp4", "cuda"),
		"flashvsr":    flashvsrTask(snap, Env{}, 2, "in.mp4", "out.mp4"),
		"wan_2_2":     wanTask(snap, vp, "", "out.mp4", 832, 480, 81),
		"ltx_2_5":     ltxTask(vp, "", "", "out.mp4", 832, 480, 81),
		"kandinsky_6": kandinskyTask(snap, Env{}, vp, "", "out.mp4", 832, 480, 81),
	}
	h3, err := h3Task(snap, Env{WorkDir: t.TempDir()}, vp, map[string]string{}, filepath.Join(t.TempDir(), "out.mp4"), 1280, 720)
	if err != nil {
		t.Fatalf("h3Task: %v", err)
	}
	cases["minimax_h3"] = h3
	for want, task := range cases {
		if task.Cancel.Name != want || task.Cancel.Grace <= 0 {
			t.Errorf("%s: cancel profile %+v", want, task.Cancel)
		}
	}
	seen := map[string]bool{}
	for _, p := range Profiles() {
		if seen[p.Name] || p.Grace <= 0 {
			t.Errorf("profile %+v duplicated or without a grace", p)
		}
		seen[p.Name] = true
	}
}

// C-R5: a render that ignores the interrupt and the cancel file is killed by the task host when the fake profile's
// grace runs out; the report says forced, and the tree is still verified gone.
func TestCancelModeForcesARenderThatIgnoresTheStop(t *testing.T) {
	env, job := setupFake(t)
	t.Setenv("APS_FAKE_SECONDS", "30")
	job.Payload.Params, _ = json.Marshal(map[string]any{"prompt": "FAKE_IGNORE_STOP", "width": 320, "height": 180, "duration_seconds": 1})
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() { time.Sleep(time.Second); cancel(errors.New("cancelled by production")) }()
	rep := &stopRep{}
	if _, err := (OSSVideo{}).Run(ctx, job, env, rep); err == nil {
		t.Fatal("cancel returned no error")
	}
	r := rep.stop
	if r == nil || !r.Forced || r.Graceful || !r.TreeVerified || r.Escalated {
		t.Fatalf("stop report %+v", r)
	}
	if r.Seconds < ProfileFake.Grace.Seconds() {
		t.Fatalf("stopped in %.1f s, before its %.0f s grace", r.Seconds, ProfileFake.Grace.Seconds())
	}
}

// C-R6 (CTO review M1): an ENDED step's pid is never signalled again, and a pid that now belongs to another process
// (reused: its start time does not match ours) is never killed or counted.
func TestPidReuseIsNeverKilled(t *testing.T) {
	stranger := startGroup(t) // an unrelated process that happens to hold a pid we once recorded
	dir := t.TempDir()
	ended := time.Now().Add(-time.Hour)
	b, _ := json.Marshal(taskhost.Procs{Groups: []taskhost.ProcStep{
		{Step: "cut 1/3", PID: stranger, StartedAt: time.Now().Add(-2 * time.Hour), EndedAt: &ended}, // ended long ago
		{Step: "upscale 1/3", PID: stranger, StartedAt: time.Now().Add(-time.Hour)},                  // start time differs
	}})
	os.WriteFile(filepath.Join(dir, taskhost.ProcsFile), b, 0o600)
	tr := treeOf(dir)
	if a := tr.alive(); len(a) != 0 {
		t.Fatalf("a reused pid counted as ours: %v", a)
	}
	tr.kill()
	time.Sleep(200 * time.Millisecond)
	if syscall.Kill(stranger, 0) != nil {
		t.Fatal("cancel mode killed an unrelated process that reused a recorded pid")
	}
}

// C-R7 (CTO review M4): a child that left the step's process group (setsid: a private model server) is found and
// killed: by procs.Run's descendant snapshot taken when the cancel arrives (any Unix), and by its ACT3_RUN_DIR marker
// (Linux). The stop report counts it.
func TestEscapedChildIsKilled(t *testing.T) {
	env, job := setupFake(t)
	_ = env
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	tk := taskhost.Task{RunVersion: 1, JobID: job.ID, Snapshot: "test", Output: filepath.Join(dir, "out.mp4"),
		Cancel: &taskhost.CancelSpec{Profile: "fake", GraceSeconds: 0.5},
		Argv:   []string{"/bin/sh", "-c", `python3 -c "import os,time; os.setsid(); open('` + pidFile + `','w').write(str(os.getpid())); time.sleep(60)" & sleep 60`}}
	if err := taskhost.Launch(HostCommand(snapshot.Snapshot{}), dir, tk); err != nil {
		t.Fatal(err)
	}
	var child int
	for i := 0; i < 50 && child == 0; i++ {
		time.Sleep(100 * time.Millisecond)
		b, _ := os.ReadFile(pidFile)
		child, _ = strconv.Atoi(string(b))
	}
	if child == 0 {
		t.Fatal("the setsid child never started")
	}
	t.Cleanup(func() { syscall.Kill(child, syscall.SIGKILL) })
	rep := &stopRep{}
	r := stopTask(dir, rep)
	if syscall.Kill(child, 0) == nil {
		t.Fatalf("the escaped child %d survived the cancel: %+v", child, r)
	}
	if !r.TreeVerified || r.EscapedKilled < 1 {
		t.Fatalf("stop report %+v", r)
	}
}
