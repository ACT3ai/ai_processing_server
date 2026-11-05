//go:build !windows

package runners

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
)

func familyJob(p map[string]any, inputs ...string) *jobs.Job {
	b, _ := json.Marshal(p)
	j := &jobs.Job{ID: "f", Payload: jobs.Envelope{Params: b}}
	for _, n := range inputs {
		j.Payload.Inputs = append(j.Payload.Inputs, jobs.Input{Name: n, Path: "/x"})
	}
	return j
}

func kindOf(err error) jobs.ErrorKind {
	var je *jobs.Error
	if errors.As(err, &je) {
		return je.Kind
	}
	return ""
}

func touch(t *testing.T, path string, size int64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

// LTX-2.5 as production sends it (1280x704, 5 s at 24 fps): the official pipeline from the repo's venv, 8n+1 frames,
// and on CUDA the flags that fit the 22B transformer into 32 GB.
func TestLTXArgv(t *testing.T) {
	t.Setenv("ACT3_RENDER_HOME", "/r")
	p := videoParams{Model: LTXModel, Kind: "t2v", Prompt: "a boat", Width: 1920, Height: 1080, DurationSeconds: 5}
	w, h, fr := ltxFrame(p)
	if w != 1280 || h != 704 || fr != 121 {
		t.Fatalf("frame: %dx%d %d", w, h, fr)
	}
	a := ltxTask(p, "/in/first.png", "", "/o.mp4", w, h, fr)
	if a.Argv[0] != venvPython("/r/ltx_2_5/LTX-2") || a.Argv[1] != "-m" || a.Argv[2] != "ltx_pipelines.distilled" || a.Dir != "/r/ltx_2_5/LTX-2" {
		t.Fatalf("program: %v in %s", a.Argv[:3], a.Dir)
	}
	if argOf(a.Argv, "--num-frames") != "121" || argOf(a.Argv, "--frame-rate") != "24" || argOf(a.Argv, "--output-path") != "/o.mp4" {
		t.Fatalf("flags: %v", a.Argv)
	}
	if slices.Contains(a.Argv, "--image") {
		t.Fatalf("t2v must not pass an image: %v", a.Argv)
	}
	if want := runtime.GOOS != "darwin"; slices.Contains(a.Argv, "fp8-cast") != want {
		t.Fatalf("CUDA memory flags (want %v): %v", want, a.Argv)
	}
	t.Setenv("LTX_ARGS", "--offload disk")
	t.Setenv("LTX_TRANSFORMER_PATH", "/fp8.safetensors")
	a = ltxTask(p, "/in/first.png", "", "/o.mp4", w, h, fr)
	if slices.Contains(a.Argv, "fp8-cast") || argOf(a.Argv, "--offload") != "disk" || argOf(a.Argv, "--transformer-path") != "/fp8.safetensors" {
		t.Fatalf("overrides: %v", a.Argv)
	}
	n := 81 // counted at 16 fps: keeps its 5 s
	if _, _, fr := ltxFrame(videoParams{Width: 1280, Height: 704, Frames: &n, FPS: 16}); fr != 121 {
		t.Fatalf("frames from 16 fps: %d", fr)
	}
	p.Kind = "i2v"
	if a = ltxTask(p, "/in/first.png", "", "/o.mp4", w, h, fr); argOf(a.Argv, "--image") != "/in/first.png" || a.Argv[slices.Index(a.Argv, "--image")+2] != "0" {
		t.Fatalf("i2v must condition frame 0 on the first frame: %v", a.Argv)
	}
	if err := ltxCheck(familyJob(nil), videoParams{Kind: "i2v", DurationSeconds: 5}); kindOf(err) != jobs.KindInvalidInput {
		t.Fatalf("i2v without a first frame must be refused: %v", err)
	}
	if err := ltxCheck(familyJob(nil, "first_frame"), videoParams{Kind: "i2v", DurationSeconds: 5}); err != nil {
		t.Fatalf("i2v with a first frame: %v", err)
	}
	if err := ltxCheck(familyJob(nil), videoParams{Kind: "t2v", DurationSeconds: 11}); kindOf(err) != jobs.KindInvalidInput {
		t.Fatalf("11 s must be refused: %v", err)
	}
}

// Kandinsky: Pro-distill and Lite-distill both take 10 steps at guidance 1; i2v passes the first frame; frames 4n+1 up to 121.
func TestKandinskyArgv(t *testing.T) {
	t.Setenv("ACT3_RENDER_HOME", "/r")
	snap := snapshot.Snapshot{Key: "k", Dir: "/snap/k"}
	p := videoParams{Model: "kandinsky-6.0-pro", Kind: "i2v", Prompt: "a boat", Width: 1920, Height: 1080, DurationSeconds: 5}
	w, h, fr := kandinskyFrame(p)
	if w != 1280 || h != 720 || fr != 121 {
		t.Fatalf("frame: %dx%d %d", w, h, fr)
	}
	a := kandinskyTask(snap, Env{}, p, "/in/first.png", "/o.mp4", w, h, fr).Argv
	if a[1] != "/snap/k/code/scripts/video_gen/kandinsky6/render_kandinsky.py" || argOf(a, "--model") != "/r/kandinsky_6/models/Kandinsky-6.0-Pro-distill-5s-Diffusers" {
		t.Fatalf("script/model: %v", a)
	}
	if argOf(a, "--steps") != "10" || argOf(a, "--guidance") != "1.0" || argOf(a, "--image") != "/in/first.png" || argOf(a, "--fps") != "24" {
		t.Fatalf("pro flags: %v", a)
	}
	p.Model, p.Kind = "kandinsky-6.0-lite", "t2v"
	a = kandinskyTask(snap, Env{}, p, "/in/first.png", "/o.mp4", w, h, fr).Argv
	if argOf(a, "--steps") != "10" || argOf(a, "--guidance") != "1.0" || argOf(a, "--image") != "" {
		t.Fatalf("lite flags: %v", a)
	}
	if kandinskyQuality("kandinsky-6.0-ultra") != "" || kandinskyQuality("wan2.2-a14b") != "" {
		t.Fatal("only lite and pro are Kandinsky models")
	}
	if err := kandinskyCheck(familyJob(nil), videoParams{Model: "kandinsky-6.0-lite", Kind: "i2v", DurationSeconds: 5}); kindOf(err) != jobs.KindInvalidInput {
		t.Fatalf("i2v without a first frame must be refused: %v", err)
	}
}

func TestH3CanvasArgvAndChecks(t *testing.T) {
	for _, c := range []struct{ w, h, ww, wh int }{
		{1920, 1080, 1344, 768}, {1080, 1920, 768, 1344}, {1024, 1024, 768, 768},
		// a smaller frame (production's 480p / 720p tiers) is kept, rounded down to the 32 grid and at least 256
		{832, 480, 832, 480}, {1248, 704, 1248, 704}, {1280, 720, 1280, 704}, {400, 224, 384, 256},
	} {
		if w, h := h3Frame(videoParams{Width: c.w, Height: c.h}); w != c.ww || h != c.wh || w*h > h3MaxPixels+32*768 {
			t.Errorf("%dx%d → %dx%d, want %dx%d (768 short edge, area cap, /32; smaller kept)", c.w, c.h, w, h, c.ww, c.wh)
		}
	}
	t.Setenv("ACT3_RENDER_HOME", t.TempDir())
	snap := snapshot.Snapshot{Key: "k", Dir: "/snap/k"}
	run := t.TempDir()
	out := filepath.Join(run, "out", "output.mp4")
	sixteen := 81
	p := videoParams{Model: H3Model, Kind: "i2v", Prompt: "integrated_multimodal_description: [Shot 1] a boat", Width: 1280, Height: 720, Frames: &sixteen, FPS: 16}
	tk, err := h3Task(snap, Env{}, p, map[string]string{"first_frame": "/in/first.png", "prompt_0": "/in/p.txt"}, out, 1344, 768)
	if err != nil {
		t.Fatal(err)
	}
	a := tk.Argv
	if a[1] != "/snap/k/code/scripts/video_gen/h3_regenerate/render_h3_generate.py" || argOf(a, "--image") != "/in/first.png" ||
		argOf(a, "--seconds") != "5.062" || argOf(a, "--steps") != "20" || argOf(a, "--width") != "1344" {
		t.Fatalf("argv (81 frames counted at 16 fps keep their 5.06 s): %v", a)
	}
	if b, _ := os.ReadFile(argOf(a, "--prompt-file")); !strings.HasPrefix(string(b), "integrated_multimodal_description:") || slices.Contains(a, "--prompt") {
		t.Fatalf("the prompt travels in a file: %q", b)
	}
	p.Kind = "t2v"
	if tk, _ = h3Task(snap, Env{}, p, map[string]string{"first_frame": "/in/first.png"}, out, 1344, 768); argOf(tk.Argv, "--image") != "" {
		t.Fatalf("t2v sends no first frame: %v", tk.Argv)
	}
	internal := func(v videoParams) videoParams { v.Model, v.Delivery = H3Model, "internal"; return v }
	refused := func(err error, kind jobs.ErrorKind, says string) bool {
		return kindOf(err) == kind && strings.Contains(fmt.Sprint(err), says)
	}
	if err := h3Check(familyJob(nil), Env{}, internal(videoParams{Kind: "i2v", DurationSeconds: 5})); !refused(err, jobs.KindInvalidInput, "first_frame") {
		t.Fatalf("i2v without a first frame must be refused: %v", err)
	}
	if err := h3Check(familyJob(nil), Env{}, internal(videoParams{Kind: "t2v", DurationSeconds: 20})); !refused(err, jobs.KindInvalidInput, "at most") {
		t.Fatalf("20 s must be refused: %v", err)
	}
	many := 150
	if err := h3Check(familyJob(nil), Env{}, internal(videoParams{Kind: "t2v", DurationSeconds: 5, Steps: &many})); !refused(err, jobs.KindInvalidInput, "1..100") {
		t.Fatalf("150 steps must be refused: %v", err)
	}
	for _, d := range []any{nil, "customer", "prod"} {
		if err := h3Check(familyJob(nil), Env{}, videoParams{Model: H3Model, Kind: "t2v", DurationSeconds: 5, Delivery: d}); !refused(err, jobs.KindInvalidInput, "license_not_cleared") {
			t.Fatalf("delivery %v must fail closed: %v", d, err)
		}
	}
	cust := familyJob(nil)
	cust.Payload.DataRules.CustomerData = true
	if err := h3Check(cust, Env{}, internal(videoParams{Kind: "t2v", DurationSeconds: 5})); !refused(err, jobs.KindInvalidInput, "license_not_cleared") {
		t.Fatalf("customer data must be refused: %v", err)
	}
	t.Setenv("APS_FAKE_MODELS", "1")
	if err := h3Check(familyJob(nil), Env{}, internal(videoParams{Kind: "t2v", DurationSeconds: 5})); err != nil {
		t.Fatalf("an internal t2v job passes: %v", err)
	}
}

func TestLTXAnchoredLastFrame(t *testing.T) {
	p := videoParams{Kind: "i2v", Width: 1280, Height: 704, DurationSeconds: 5, Prompt: "a boat"}
	w, h, fr := ltxFrame(p)
	a := ltxTask(p, "/in/first.png", "/in/last.png", "/o.mp4", w, h, fr).Argv
	i := slices.Index(a, "/in/last.png")
	if i < 0 || a[i-1] != "--image" || a[i+1] != fmt.Sprint(fr-1) || a[i+2] != "1.0" {
		t.Fatalf("the last frame guides the clip's final frame: %v", a)
	}
	if lastFrameOf(map[string]string{"first_frame": "/f", "last_frame_2": "/l"}) != "/l" || lastFrameOf(map[string]string{}) != "" {
		t.Fatal("lastFrameOf")
	}
}

func TestH3SeedanceReferences(t *testing.T) {
	in := map[string]string{"prompt_0": "/w/p.txt", "reference_image_10": "/w/c.png", "reference_image_2": "/w/a.png",
		"reference_image_3": "/w/b.png", "audio_11": "/w/v.mp3", "first_frame": "/w/f.png"}
	got := strings.Join(h3InputArgs("t2v", in), " ")
	if got != "--ref-image /w/a.png --ref-image /w/b.png --ref-image /w/c.png --ref-audio /w/v.mp3" {
		t.Fatalf("references in manifest order (by index, not by name): %s", got)
	}
	frames := map[string]string{"first_frame": "/w/f.png", "last_frame_2": "/w/l.png"}
	if got := strings.Join(h3InputArgs("i2v", frames), " "); got != "--image /w/f.png --last-frame /w/l.png" {
		t.Fatalf("FL2VA: %s", got)
	}
	job := func(names ...string) *jobs.Job { return familyJob(nil, names...) }
	internal := videoParams{Model: H3Model, Kind: "t2v", DurationSeconds: 5, Delivery: "internal"}
	if err := h3Check(job("audio_1"), Env{}, internal); kindOf(err) != jobs.KindInvalidInput || !strings.Contains(fmt.Sprint(err), "only reference") {
		t.Fatalf("audio alone: %v", err)
	}
	var many []string
	for i := range 10 {
		many = append(many, fmt.Sprintf("reference_image_%d", i+1))
	}
	if err := h3Check(job(many...), Env{}, internal); kindOf(err) != jobs.KindInvalidInput {
		t.Fatalf("10 images: %v", err)
	}
	t.Setenv("APS_FAKE_MODELS", "1")
	i2v := internal
	i2v.Kind = "i2v"
	if err := h3Check(job("reference_image_1", "audio_2"), Env{}, i2v); err != nil {
		t.Fatalf("a Ref2VA request needs no first_frame input: %v", err)
	}
}

func TestH3InstallNeedsGenerationFilesOnly(t *testing.T) {
	if accelerator() != "cuda" {
		if why := h3InstallProblem(); !strings.Contains(why, "CUDA") {
			t.Fatalf("no CUDA: %q", why)
		}
		t.Skip("the file checks run on CUDA hosts")
	}
	home := t.TempDir()
	t.Setenv("ACT3_RENDER_HOME", home)
	if h3InstallProblem() == "" {
		t.Fatal("an empty home is not an install")
	}
	touch := func(p string, n int64) {
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		f, _ := os.Create(p)
		_ = f.Truncate(n)
		_ = f.Close()
	}
	touch(venvPython(regenHome()), 1)
	for _, f := range h3rComfyFiles {
		if !f.refinerOnly {
			touch(comfyPath(regenHome(), f.rel), f.min-1)
		}
	}
	if why := h3InstallProblem(); !strings.Contains(why, "partial") {
		t.Fatalf("files under their floor are a partial download: %q", why)
	}
	for _, f := range h3rComfyFiles {
		if !f.refinerOnly {
			touch(comfyPath(regenHome(), f.rel), f.min)
		}
	}
	if why := h3InstallProblem(); why != "" {
		t.Fatalf("generation needs no refiner files: %q", why)
	}
	// a Seedance-fed job (references) needs the Ref2VA base too; a first-frame job does not
	if why := h3InstallProblemFor(familyJob(nil, "first_frame")); why != "" {
		t.Fatalf("FL2VA job: %q", why)
	}
	if why := h3InstallProblemFor(familyJob(nil, "reference_image_1")); !strings.Contains(why, "ref2va") {
		t.Fatalf("Ref2VA job without its base: %q", why)
	}
	touch(comfyPath(regenHome(), h3Ref2VABase), 19_000_000_000)
	if why := h3InstallProblemFor(familyJob(nil, "reference_image_1", "audio_2")); why != "" {
		t.Fatalf("Ref2VA installed: %q", why)
	}
}

func TestFlashVSRScaleAndChecks(t *testing.T) {
	if s := flashvsrScale(upscaleParams{Factor: 2.5}, "", "/none"); s != 2.5 {
		t.Fatalf("factor: %g", s)
	}
	if s := flashvsrScale(upscaleParams{Width: 1920}, "/no/ffprobe", "/none"); s != 4 {
		t.Fatalf("unreadable source falls back to x4: %g", s)
	}
	if err := flashvsrCheck(familyJob(nil), upscaleParams{Factor: 4}); kindOf(err) != jobs.KindInvalidInput {
		t.Fatalf("no source must be refused: %v", err)
	}
	if err := flashvsrCheck(familyJob(nil, "source_video"), upscaleParams{Factor: 12}); kindOf(err) != jobs.KindInvalidInput {
		t.Fatalf("x12 must be refused: %v", err)
	}
	if err := flashvsrCheck(familyJob(nil, "source_video"), upscaleParams{Factor: 4}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ACT3_RENDER_HOME", "/r")
	a := flashvsrTask(snapshot.Snapshot{Key: "k", Dir: "/snap/k"}, Env{}, 4, "/in.mp4", "/o.mp4")
	if a.Argv[1] != "/snap/k/code/scripts/upscale/flashvsr/run_flashvsr.py" || argOf(a.Argv, "--scale") != "4" ||
		!slices.Contains(a.Env, "ACT3_OSS_HOME=/r/flashvsr") {
		t.Fatalf("task: %+v", a)
	}
}

// The upscale runner routes flashvsr-1.1 to FlashVSR and still refuses a model nobody built.
func TestUpscaleRoutesFlashVSR(t *testing.T) {
	t.Setenv("APS_FAKE_MODELS", "on")
	ctx := context.Background()
	if err := (OSSUpscale{}).Check(ctx, familyJob(map[string]any{"model": "flashvsr-1.1", "factor": 4}, "source_video"), Env{}); err != nil {
		t.Fatal(err)
	}
	if err := (OSSUpscale{}).Check(ctx, familyJob(map[string]any{"model": "topaz", "width": 3840, "height": 2160}, "source_video"), Env{}); kindOf(err) != jobs.KindInvalidInput ||
		!strings.Contains(err.Error(), FlashVSRModel) {
		t.Fatalf("unknown upscaler: %v", err)
	}
	if err := (OSSVideo{}).Check(ctx, familyJob(map[string]any{"model": "sora", "prompt": "x"}), Env{}); kindOf(err) != jobs.KindInvalidInput ||
		!strings.Contains(err.Error(), "kandinsky-6.0-pro") {
		t.Fatalf("unknown video model: %v", err)
	}
	if err := (OSSVideo{}).Check(ctx, familyJob(map[string]any{"model": "ltx-2.5", "prompt": "x", "kind": "t2v"}), Env{}); err != nil {
		t.Fatal(err)
	}
}

// A runner with several models is offered when any one is installed; a job for a missing one is "not installed".
func TestInstallProblemsAcrossModels(t *testing.T) {
	home := t.TempDir()
	t.Setenv("ACT3_RENDER_HOME", home)
	t.Setenv("APS_FAKE_MODELS", "")
	if InstallProblems()[OSSVideo{}.Name()] == "" || InstallProblems()[OSSUpscale{}.Name()] == "" {
		t.Fatal("nothing installed must be a problem for both runners")
	}
	touch(t, venvPython(filepath.Join(home, "kandinsky_6")), 1)
	touch(t, filepath.Join(home, "kandinsky_6", "models", "Kandinsky-6.0-Lite-distill-5s-Diffusers", "model_index.json"), 2)
	touch(t, venvPython(filepath.Join(home, "flashvsr")), 1)
	touch(t, filepath.Join(home, "flashvsr", "FlashVSR", "examples", "WanVSR", ".keep"), 1)
	touch(t, filepath.Join(home, "flashvsr", "models", "FlashVSR-v1.1", flashvsrWeight), 101<<20)
	got := InstallProblems()
	if got[OSSVideo{}.Name()] != "" || got[OSSUpscale{}.Name()] != "" {
		t.Fatalf("Kandinsky Lite and FlashVSR installed: %v", got)
	}
	err := (OSSVideo{}).Check(context.Background(), familyJob(map[string]any{"model": "ltx-2.5", "prompt": "x", "kind": "t2v"}), Env{})
	if kindOf(err) != jobs.KindInfra || !strings.Contains(err.Error(), "not installed") {
		t.Fatalf("LTX job on a Kandinsky-only server: %v", err)
	}
	if err := (OSSVideo{}).Check(context.Background(), familyJob(map[string]any{"model": "kandinsky-6.0-lite", "prompt": "x"}), Env{}); err != nil {
		t.Fatal(err)
	}
}
