//go:build !windows

package runners

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
	"github.com/ACT3ai/ai_processing_server/internal/taskhost"
)

// The test binary doubles as the task host (HostCommand points at it with APS_RUNNERS_TASKHOST=1).
func TestMain(m *testing.M) {
	if os.Getenv("APS_RUNNERS_TASKHOST") == "1" {
		os.Exit(taskhost.Main(os.Args[1:]))
	}
	os.Exit(m.Run())
}

func argOf(argv []string, flag string) string {
	if i := slices.Index(argv, flag); i >= 0 && i+1 < len(argv) {
		return argv[i+1]
	}
	return ""
}

// The same numbers wan-local.ts produces (its spec tests): 1920×1080 → 1280×720, 5 s → 81 frames, portrait kept.
func TestWanFrameMatchesThePlugin(t *testing.T) {
	for _, c := range []struct {
		w, h        int
		secs        float64
		ww, wh, wfr int
	}{{1920, 1080, 5, 1280, 720, 81}, {1280, 720, 2, 1280, 720, 33}, {1080, 1920, 3, 720, 1280, 49}, {1000, 562, 5, 992, 560, 81}} {
		w, h, fr := wanFrame(videoParams{Width: c.w, Height: c.h, DurationSeconds: c.secs})
		if w != c.ww || h != c.wh || fr != c.wfr {
			t.Errorf("%dx%d %.0fs: got %dx%d %d frames", c.w, c.h, c.secs, w, h, fr)
		}
	}
}

func TestWanArgv(t *testing.T) {
	t.Setenv("ACT3_RENDER_HOME", "/r")
	snap := snapshot.Snapshot{Key: "k", Dir: "/snap/k"}
	p := videoParams{Kind: "i2v", Prompt: "a boat", Width: 1280, Height: 720, DurationSeconds: 5}
	tk := wanTask(snap, p, "/run/inputs/first_frame.png", "/run/out/output.mp4", 1280, 720, 81)
	a := tk.Argv
	if a[0] != "/r/wan_2_2/.venv/bin/python" || a[1] != "/snap/k/code/scripts/video_gen/wan22/render_wan.py" {
		t.Fatalf("program: %v", a[:2])
	}
	if argOf(a, "--image") != "/run/inputs/first_frame.png" || argOf(a, "--i2v-model") == "" || argOf(a, "--lightning") == "" {
		t.Fatalf("i2v flags: %v", a)
	}
	if argOf(a, "--steps") != "4" || argOf(a, "--steps-full") == "" || argOf(a, "--frames") != "81" {
		t.Fatalf("steps: %v", a)
	}
	t.Setenv("WAN_LIGHTNING", "off")
	a = wanTask(snap, videoParams{Kind: "t2v", Prompt: "x"}, "", "/o.mp4", 1280, 720, 81).Argv
	if argOf(a, "--lightning") != "" || argOf(a, "--steps-full") != "" || argOf(a, "--image") != "" {
		t.Fatalf("lightning off: %v", a)
	}
}

// seedvr2-cli.ts seedvr2Argv for the Mac Studio and the 5090.
func TestSeedVR2Argv(t *testing.T) {
	t.Setenv("ACT3_RENDER_HOME", "/r")
	a := seedvr2Task(upscaleParams{Quality: "standard", Width: 3840, Height: 2160}, "/in.mp4", "/out.mp4", "mps").Argv
	if a[0] != "/r/seedvr2/repo/.venv/bin/python" || a[2] != "/in.mp4" {
		t.Fatalf("program: %v", a[:3])
	}
	if argOf(a, "--resolution") != "2160" || argOf(a, "--max_resolution") != "3840" || argOf(a, "--batch_size") != "21" ||
		!slices.Contains(a, "--vae_encode_tiled") || !slices.Contains(a, "--vae_decode_tiled") || slices.Contains(a, "--cuda_device") {
		t.Fatalf("mac 4K: %v", a)
	}
	a = seedvr2Task(upscaleParams{Quality: "best", Width: 3840, Height: 2160}, "/in.mp4", "/out.mp4", "cuda").Argv
	if argOf(a, "--batch_size") != "5" || argOf(a, "--blocks_to_swap") != "24" || argOf(a, "--dit_model") != "seedvr2_ema_7b_fp16.safetensors" ||
		slices.Contains(a, "--vae_encode_tiled") || !slices.Contains(a, "--vae_decode_tiled") {
		t.Fatalf("5090 7B 4K: %v", a)
	}
}

type recRep struct {
	stages []string
	prog   float64
	logs   []string
}

func (r *recRep) Stage(s string)               { r.stages = append(r.stages, s) }
func (r *recRep) Progress(f float64, _ string) { r.prog = f }
func (r *recRep) Log(s string)                 { r.logs = append(r.logs, s) }

// fakeRepo is a code root holding the real fake_render.py, so the snapshot and the task are the real ones.
func fakeRepo(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not on PATH")
	}
	root := t.TempDir()
	src, err := os.ReadFile("../../scripts/fake/fake_render.py")
	if err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(root, "code", "scripts", "fake"), 0o700)
	os.WriteFile(filepath.Join(root, "code", "scripts", "fake", "fake_render.py"), src, 0o600)
	return root
}

func setupFake(t *testing.T) (Env, *jobs.Job) {
	root := fakeRepo(t)
	state := t.TempDir()
	t.Setenv("APS_CODE_ROOT", root)
	t.Setenv("APS_FAKE_MODELS", "on")
	t.Setenv("APS_FAKE_SECONDS", "2")
	t.Setenv("APS_RUNNERS_TASKHOST", "1")
	old := HostCommand
	HostCommand = func(snapshot.Snapshot) []string { return []string{os.Args[0]} }
	oldPoll := taskhost.WatchPoll
	taskhost.WatchPoll = 50 * time.Millisecond
	t.Cleanup(func() { HostCommand, taskhost.WatchPoll = old, oldPoll })
	work := filepath.Join(state, "work", "job-1")
	os.MkdirAll(work, 0o700)
	params, _ := json.Marshal(map[string]any{"prompt": "a red boat", "kind": "t2v", "width": 320, "height": 180, "duration_seconds": 2})
	job := &jobs.Job{ID: "job-1", Type: "act3.video_gen_open_source", Payload: jobs.Envelope{Params: params}}
	return Env{WorkDir: work, OutDir: filepath.Join(work, "out"), Inputs: map[string]string{}}, job
}

func TestFakeWanRendersInATaskHost(t *testing.T) {
	env, job := setupFake(t)
	r := OSSVideo{}
	if err := r.Check(context.Background(), job, env); err != nil {
		t.Fatal(err)
	}
	rep := &recRep{}
	res, err := r.Run(context.Background(), job, env, rep)
	if err != nil {
		t.Fatal(err, rep.logs)
	}
	if st, err := os.Stat(res.Outputs[0].Path); err != nil || st.Size() == 0 {
		t.Fatalf("no output: %v", err)
	}
	if rep.prog != 1 || !slices.Contains(rep.stages, "rendering") {
		t.Fatalf("progress %v stages %v", rep.prog, rep.stages)
	}
	if _, err := os.Stat(runDirFor(env, job.ID)); !os.IsNotExist(err) {
		t.Fatal("run dir not cleaned up")
	}
}

// G1 in miniature: the controller stops mid-render (its context ends with a shutdown cause), the work dir is wiped
// as the scheduler does at boot, and the next controller's Run re-attaches and collects one result.
func TestRenderSurvivesAControllerRestart(t *testing.T) {
	env, job := setupFake(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() { time.Sleep(700 * time.Millisecond); cancel(errors.New("server shutting down")) }()
	_, err := OSSVideo{}.Run(ctx, job, env, &recRep{})
	if err == nil || !strings.Contains(err.Error(), "shutting down") {
		t.Fatalf("first run: %v", err)
	}
	dir := runDirFor(env, job.ID)
	if s := taskhost.Alive(dir); s != taskhost.Running {
		t.Fatalf("after the controller stopped the task host is %s", s)
	}
	os.RemoveAll(env.WorkDir) // scheduler recover() at boot
	os.MkdirAll(env.WorkDir, 0o700)
	rep := &recRep{}
	res, err := OSSVideo{}.Run(context.Background(), job, env, rep)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(rep.logs, "\n"), "re-attached") {
		t.Fatalf("did not re-attach: %v", rep.logs)
	}
	if res.Outputs[0].Name != "video" {
		t.Fatal(res.Outputs)
	}
}

func TestCancelAndFailureKinds(t *testing.T) {
	env, job := setupFake(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() { time.Sleep(500 * time.Millisecond); cancel(errors.New("cancelled by the machine owner")) }()
	if _, err := (OSSVideo{}).Run(ctx, job, env, &recRep{}); err == nil {
		t.Fatal("cancel returned no error")
	}
	if _, err := os.Stat(runDirFor(env, job.ID)); !os.IsNotExist(err) {
		t.Fatal("cancelled run dir left behind")
	}
	job.Payload.Params, _ = json.Marshal(map[string]any{"prompt": "FAKE_FAIL", "width": 320, "height": 180, "duration_seconds": 1})
	_, err := OSSVideo{}.Run(context.Background(), job, env, &recRep{})
	var je *jobs.Error
	if !errors.As(err, &je) || je.Kind != jobs.KindInfra {
		t.Fatalf("OOM should be infra: %v", err)
	}
}

func TestUpscaleChecks(t *testing.T) {
	env, _ := setupFake(t)
	mk := func(p map[string]any, inputs ...string) *jobs.Job {
		b, _ := json.Marshal(p)
		j := &jobs.Job{ID: "u", Payload: jobs.Envelope{Params: b}}
		for _, n := range inputs {
			j.Payload.Inputs = append(j.Payload.Inputs, jobs.Input{Name: n, Path: "/x"})
		}
		return j
	}
	if err := (OSSUpscale{}).Check(context.Background(), mk(map[string]any{"width": 3840, "height": 2160}, "source_video"), env); err != nil {
		t.Fatal(err)
	}
	// Exactly what the internal web app sends (processing/tasks.ts envelopeFor): model seedvr2-<quality>, camelCase.
	app := map[string]any{"render_type": "seedvr2", "model": "seedvr2-sharp", "quality": "sharp", "kind": "upscale",
		"width": 3840, "height": 2160, "fps": 24, "durationSeconds": 31, "factor": 2, "target": "4k"}
	if err := (OSSUpscale{}).Check(context.Background(), mk(app, "source_video"), env); err == nil || !strings.Contains(err.Error(), "30 s") {
		t.Fatalf("app params not read (durationSeconds 31 must be refused on a Mac): %v", err)
	}
	var vp videoParams
	json.Unmarshal([]byte(`{"model":"wan2.2-a14b","durationSeconds":3,"negativePrompt":"blur","prompt":"x"}`), &vp)
	vp.defaults()
	if vp.DurationSeconds != 3 || vp.NegativePrompt != "blur" {
		t.Fatalf("wan camelCase: %+v", vp)
	}
	for _, bad := range []*jobs.Job{
		mk(map[string]any{"width": 3840, "height": 2160}),
		mk(map[string]any{"width": 3840, "height": 2160, "quality": "ultra"}, "source_video"),
		mk(map[string]any{"width": 7680, "height": 4320}, "source_video"),
	} {
		if err := (OSSUpscale{}).Check(context.Background(), bad, env); err == nil {
			t.Errorf("accepted %s", bad.Payload.Params)
		}
	}
}
