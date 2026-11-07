//go:build !windows

package runners

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
	"github.com/ACT3ai/ai_processing_server/internal/taskhost"
)

// The test binary doubles as the task host (HostCommand points at it with APS_RUNNERS_TASKHOST=1).
func TestMain(m *testing.M) {
	if os.Getenv("APS_RUNNERS_TASKHOST") == "1" {
		taskhost.HeartbeatEvery, taskhost.CancelPoll = 200*time.Millisecond, 100*time.Millisecond
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

// TI2V-5B: its own folder (image-to-video from the same weights), 24 fps, sides of 32, no Lightning, full steps.
func TestWan5BArgv(t *testing.T) {
	t.Setenv("ACT3_RENDER_HOME", "/r")
	p := videoParams{Model: "wan2.2-ti2v-5b", Kind: "i2v", Prompt: "a boat", Width: 1920, Height: 1080, DurationSeconds: 5}
	w, h, fr := wanFrame(p)
	if w != 1248 || h != 704 || fr != 121 {
		t.Fatalf("frame: %dx%d %d", w, h, fr)
	}
	a := wanTask(snapshot.Snapshot{Key: "k", Dir: "/snap/k"}, p, "/in.png", "/o.mp4", w, h, fr).Argv
	if argOf(a, "--model") != "/r/wan_2_2/models/Wan2.2-TI2V-5B-Diffusers" || argOf(a, "--i2v-model") != argOf(a, "--model") {
		t.Fatalf("model dirs: %v", a)
	}
	if argOf(a, "--fps") != "24" || argOf(a, "--lightning") != "" || argOf(a, "--steps-full") != "" || argOf(a, "--steps") == "4" {
		t.Fatalf("5B flags: %v", a)
	}
	n := 81 // production counts 5 s at 16 fps; the 5B keeps the 5 s at its 24
	if _, _, fr := wanFrame(videoParams{Model: "wan2.2-ti2v-5b", Width: 1280, Height: 720, Frames: &n, FPS: 16}); fr != 121 {
		t.Fatalf("frames at 24 fps: %d", fr)
	}
	if _, _, fr := wanFrame(videoParams{Width: 1280, Height: 720, Frames: &n, FPS: 16}); fr != 81 {
		t.Fatalf("A14B frames: %d", fr)
	}
	t.Setenv("ACT3_WAN_MODEL", "wan2.2-ti2v-5b")
	q := videoParams{Prompt: "x"}
	q.defaults()
	if q.Model != "wan2.2-ti2v-5b" {
		t.Fatalf("machine model not the default: %q", q.Model)
	}
	if why := wanInstallProblem("wan2.2-nope"); !strings.Contains(why, "wan2.2-ti2v-5b") {
		t.Fatalf("unknown model: %q", why)
	}
	// A14B is installed only with its I2V weights; the 5B renders image-to-video from its own folder
	home := t.TempDir()
	t.Setenv("ACT3_RENDER_HOME", home)
	touch(t, venvPython(filepath.Join(home, "wan_2_2")), 1)
	touch(t, filepath.Join(home, "wan_2_2", "models", "Wan2.2-T2V-A14B-Diffusers", "model_index.json"), 1)
	touch(t, filepath.Join(home, "wan_2_2", "models", "Wan2.2-TI2V-5B-Diffusers", "model_index.json"), 1)
	if why := wanInstallProblem(WanModel); !strings.Contains(why, "Wan2.2-I2V-A14B-Diffusers") {
		t.Fatalf("A14B without I2V: %q", why)
	}
	if why := wanInstallProblem("wan2.2-ti2v-5b"); why != "" {
		t.Fatalf("5B: %q", why)
	}
	touch(t, filepath.Join(home, "wan_2_2", "models", "Wan2.2-I2V-A14B-Diffusers", "model_index.json"), 1)
	if why := wanInstallProblem(WanModel); why != "" {
		t.Fatalf("A14B with I2V: %q", why)
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
		!slices.Contains(a, "--vae_encode_tiled") || !slices.Contains(a, "--vae_decode_tiled") {
		t.Fatalf("5090 7B 4K: %v", a)
	}
	// 3B at 4K: 21 frames per batch runs a 32 GB card out of memory; 9 fits. 1080p keeps 21.
	a = seedvr2Task(upscaleParams{Quality: "standard", Width: 3840, Height: 2160}, "/in.mp4", "/out.mp4", "cuda").Argv
	if argOf(a, "--batch_size") != "9" || slices.Contains(a, "--blocks_to_swap") || !slices.Contains(a, "--vae_encode_tiled") {
		t.Fatalf("5090 3B 4K: %v", a)
	}
	// 3B at 1080p keeps 21 frames, but an untiled decode of them outgrows the card
	a = seedvr2Task(upscaleParams{Quality: "standard", Width: 1920, Height: 1080}, "/in.mp4", "/out.mp4", "cuda").Argv
	if argOf(a, "--batch_size") != "21" || !slices.Contains(a, "--vae_decode_tiled") || slices.Contains(a, "--vae_encode_tiled") {
		t.Fatalf("5090 3B 1080p: %v", a)
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
	err := (OSSUpscale{}).Check(context.Background(), mk(app, "source_video"), env)
	if accelerator() == "mps" && (err == nil || !strings.Contains(err.Error(), "30 s")) {
		t.Fatalf("app params not read (durationSeconds 31 must be refused on a Mac): %v", err)
	}
	if accelerator() != "mps" && err != nil {
		t.Fatalf("a 31 s upscale is fine on CUDA: %v", err)
	}
	var up upscaleParams
	b, _ := json.Marshal(app)
	json.Unmarshal(b, &up)
	up.defaults()
	if up.DurationSeconds != 31 {
		t.Fatalf("upscale camelCase durationSeconds not read: %+v", up)
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

func TestPlanSegments(t *testing.T) {
	for _, c := range []struct {
		frames, seg, batch int
		want               [][2]int
	}{
		{100, 80, 1, [][2]int{{0, 100}}},                   // under 1.5 segments: one piece
		{60, 20, 5, [][2]int{{0, 20}, {20, 40}, {40, 60}}}, // whole batches
		{65, 20, 5, [][2]int{{0, 20}, {20, 40}, {40, 65}}}, // a 5-frame tail joins the last segment
		{70, 18, 5, [][2]int{{0, 20}, {20, 40}, {40, 60}, {60, 70}}},
	} {
		if got := planSegments(c.frames, c.seg, c.batch); !slices.Equal(flat(got), flat(c.want)) {
			t.Errorf("planSegments(%d, %d, %d) = %v, want %v", c.frames, c.seg, c.batch, got, c.want)
		}
	}
}

func TestSegmentSeconds(t *testing.T) {
	t.Setenv("APS_UPSCALE_SEGMENT_SECONDS", "")
	ten := 10.0
	for _, c := range []struct {
		param *float64
		w, h  int
		want  float64
	}{
		{nil, 1920, 1080, 30},
		{nil, 1280, 720, 30},
		{nil, 3840, 2160, 7.5}, // a 4K segment holds as many pixels as 30 s of 1080p
		{&ten, 3840, 2160, 10}, // the job's own segment_seconds is used as given
	} {
		if got := segmentSeconds(c.param, c.w, c.h); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("segmentSeconds(%v, %d, %d) = %v, want %v", c.param, c.w, c.h, got, c.want)
		}
	}
	t.Setenv("APS_UPSCALE_SEGMENT_SECONDS", "60")
	if got := segmentSeconds(nil, 3840, 2160); got != 15 {
		t.Errorf("$APS_UPSCALE_SEGMENT_SECONDS=60 at 4K = %v, want 15", got)
	}
}

func flat(r [][2]int) []int {
	var out []int
	for _, x := range r {
		out = append(out, x[0], x[1])
	}
	return out
}

// G4 with a checkpoint (R8): a segmented upscale loses its task host to a power cut after the first segment; the
// next Run resumes at the second segment on the same snapshot, and the joined result has every source frame.
func TestSegmentedUpscaleResumesAfterAPowerCut(t *testing.T) {
	ff, err1 := exec.LookPath("ffmpeg")
	fp, err2 := exec.LookPath("ffprobe")
	if err1 != nil || err2 != nil {
		t.Skip("ffmpeg / ffprobe not on PATH")
	}
	env, job := setupFake(t)
	t.Setenv("APS_FAKE_SECONDS", "1")
	t.Setenv("SEEDVR2_BATCH_SIZE", "5")
	oldStale := taskhost.StaleAfter
	taskhost.StaleAfter = 2 * time.Second
	t.Cleanup(func() { taskhost.StaleAfter = oldStale })
	src := filepath.Join(t.TempDir(), "src.mp4")
	if out, err := exec.Command(ff, "-y", "-v", "error", "-f", "lavfi", "-i", "testsrc=size=64x64:rate=10:duration=6",
		"-c:v", "libx264", "-pix_fmt", "yuv420p", src).CombinedOutput(); err != nil {
		t.Fatalf("make source: %v %s", err, out)
	}
	env.Engines = map[string]*hardware.Engine{"ffmpeg": {Path: ff}, "ffprobe": {Path: fp}}
	env.Inputs = map[string]string{"source_video": src}
	job.Type = "act3.upscale_open_source"
	job.Payload.Params, _ = json.Marshal(map[string]any{"width": 128, "height": 128, "segment_seconds": 2})

	// First controller: it stops as soon as the first segment is upscaled (step 2 of 7 checkpointed).
	dir := runDirFor(env, job.ID)
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() {
		for i := 0; i < 600; i++ {
			if _, err := os.Stat(filepath.Join(dir, taskhost.CheckpointDir, "1.done")); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		cancel(errors.New("server shutting down"))
	}()
	if _, err := (OSSUpscale{}).Run(ctx, job, env, &recRep{}); err == nil {
		t.Fatal("the first run should have stopped with the controller")
	}
	if done, total := taskhost.Checkpoints(dir); done < 2 || total != 7 {
		t.Fatalf("checkpoints at the power cut: %d of %d", done, total)
	}
	// The power goes: the task host and its render die.
	b, err := os.ReadFile(filepath.Join(dir, taskhost.HeartbeatFile))
	var hb taskhost.Heartbeat
	if err != nil || json.Unmarshal(b, &hb) != nil || hb.PID == 0 {
		t.Fatalf("heartbeat: %s %v", b, err)
	}
	syscall.Kill(-hb.PID, syscall.SIGKILL)
	exec.Command("pkill", "-9", "-f", dir).Run() // the fake render's python, in its own process group
	for i := 0; taskhost.Alive(dir) != taskhost.Dead; i++ {
		if i > 200 {
			t.Fatalf("the killed task host never read as dead: %s", taskhost.Alive(dir))
		}
		time.Sleep(50 * time.Millisecond)
	}
	done, _ := taskhost.Checkpoints(dir)

	// The machine is back: the next controller resumes from the checkpoint instead of starting over.
	rep := &recRep{}
	res, err := (OSSUpscale{}).Run(context.Background(), job, env, rep)
	if err != nil {
		t.Fatal(err, rep.logs)
	}
	logs := strings.Join(rep.logs, "\n")
	if !strings.Contains(logs, "resuming at step") {
		t.Fatalf("did not resume from the checkpoint (%d steps done): %s", done, logs)
	}
	vf, err := probeFrames(fp, res.Outputs[0].Path)
	if err != nil || vf.Frames != 60 {
		t.Fatalf("joined result has %d frames (want all 60): %v", vf.Frames, err)
	}
}

// A task host that dies again and again (each time taking the machine down, so the job is re-adopted at boot under
// the same attempt) is not started forever: after MaxDeaths it fails as infra and the queue's retry policy decides.
func TestTaskHostThatKeepsDyingGivesUp(t *testing.T) {
	env, job := setupFake(t)
	dir := runDirFor(env, job.ID)
	os.MkdirAll(dir, 0o700)
	dead := exec.Command("true")
	dead.Run()
	b, _ := json.Marshal(taskhost.Task{RunVersion: 1, JobID: job.ID, Snapshot: "s", Argv: []string{"true"}, Output: filepath.Join(dir, "out", "output.mp4")})
	os.WriteFile(filepath.Join(dir, taskhost.TaskFile), b, 0o600)
	b, _ = json.Marshal(taskhost.Heartbeat{PID: dead.Process.Pid, StartedAt: time.Now().Add(-time.Hour), At: time.Now().Add(-time.Hour)})
	os.WriteFile(filepath.Join(dir, taskhost.HeartbeatFile), b, 0o600)
	if err := taskhost.CarryDeaths(dir, []string{"earlier-1", "earlier-2"}); err != nil {
		t.Fatal(err)
	}
	_, err := OSSVideo{}.Run(context.Background(), job, env, &recRep{})
	var je *jobs.Error
	if !errors.As(err, &je) || je.Kind != jobs.KindInfra || !strings.Contains(je.Message, "died 3 times") {
		t.Fatalf("third death: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("the run directory of a task host that keeps dying was kept")
	}
	// One death only: it starts again, and the count goes with the new run directory.
	os.MkdirAll(dir, 0o700)
	os.WriteFile(filepath.Join(dir, taskhost.TaskFile), []byte(`{"run_version":1,"job_id":"job-1","argv":["true"],"output":"x"}`), 0o600)
	b, _ = json.Marshal(taskhost.Heartbeat{PID: dead.Process.Pid, StartedAt: time.Now().Add(-time.Hour), At: time.Now().Add(-time.Hour)})
	os.WriteFile(filepath.Join(dir, taskhost.HeartbeatFile), b, 0o600)
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() { time.Sleep(300 * time.Millisecond); cancel(errors.New("server shutting down")) }()
	OSSVideo{}.Run(ctx, job, env, &recRep{})
	if n := len(taskhost.Deaths(dir)); n != 1 {
		t.Fatalf("death count in the new run directory: %d, want 1", n)
	}
	taskhost.Cancel(dir)
}

// A render that exits 0 is only done when its output is a real video, with every frame when the count is known.
func TestValidateVideo(t *testing.T) {
	ff, err1 := exec.LookPath("ffmpeg")
	fp, err2 := exec.LookPath("ffprobe")
	if err1 != nil || err2 != nil {
		t.Skip("ffmpeg / ffprobe not on PATH")
	}
	dir := t.TempDir()
	good := filepath.Join(dir, "good.mp4")
	if out, err := exec.Command(ff, "-y", "-v", "error", "-f", "lavfi", "-i", "testsrc=size=64x64:rate=10:duration=3",
		"-c:v", "libx264", "-pix_fmt", "yuv420p", good).CombinedOutput(); err != nil {
		t.Fatalf("make video: %v %s", err, out)
	}
	bad := filepath.Join(dir, "bad.mp4")
	os.WriteFile(bad, []byte("not a video at all"), 0o600)
	if err := validateVideo(fp, good, 30); err != nil {
		t.Fatalf("a good 30-frame video failed: %v", err)
	}
	if err := validateVideo(fp, good, 60); err == nil {
		t.Fatal("a video missing half its frames passed")
	}
	if err := validateVideo(fp, bad, 0); err == nil {
		t.Fatal("a file that is not a video passed")
	}
	if err := validateVideo("", bad, 0); err != nil {
		t.Fatal("no ffprobe must mean not checked")
	}
}

// Running out of disk mid-render is the machine's fault (infra, retried), not the job's.
func TestDiskFullIsInfra(t *testing.T) {
	for _, tail := range []string{"OSError: [Errno 28] No space left on device", "av_interleaved_write_frame(): No space left on device"} {
		if k := failureKind(tail); k != jobs.KindInfra {
			t.Errorf("%q: %s, want infra", tail, k)
		}
	}
	if k := failureKind("ValueError: bad prompt"); k != jobs.KindInternal {
		t.Errorf("plain failure: %s", k)
	}
}

// The error a script prints on stderr can be read before its last stdout progress line.
func TestLastLineSkipsOurGrammar(t *testing.T) {
	tail := "ACT3_STAGE rendering\nRuntimeError: CUDA out of memory\nACT3_PROGRESS 100.0\n"
	if l := lastLine(tail); l != "RuntimeError: CUDA out of memory" {
		t.Errorf("got %q", l)
	}
	if l := lastLine("ACT3_PROGRESS 10.0\n"); l != "no output" {
		t.Errorf("only progress: got %q", l)
	}
	// SeedVR2's output: the error, then a cleanup section and its sponsor banner
	seedvr2 := "torch.OutOfMemoryError: Allocation on device 0 would exceed allowed memory. (out of memory)\n" +
		"[08:39:12.928] Process 2816552 terminating - VRAM will be automatically freed\n" +
		"[08:39:12.928] Questions? Updates? Watch, star & sponsor if you can!\n" +
		"[08:39:12.928] https://github.com/numz/ComfyUI-SeedVR2_VideoUpscaler\n"
	if l := lastLine(seedvr2); !strings.HasPrefix(l, "torch.OutOfMemoryError") {
		t.Errorf("banner after the error: got %q", l)
	}
	if l := lastLine("loading\nall done, but no file\n"); l != "all done, but no file" {
		t.Errorf("no error line: got %q", l)
	}
}
