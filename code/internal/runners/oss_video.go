package runners

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
)

// OSSVideo is the runner behind act3.video_gen_open_source: open-source video generation on our own GPUs.
// Model "wan2.2-a14b" (the default): Wan 2.2 A14B text-to-video, or image-to-video from input "first_frame" when the
// I2V weights are installed, on CUDA (B5090) or Metal (the Mac Studio). It runs code/scripts/video_gen/wan22/
// render_wan.py from the job's snapshot with the install at ~/act3_render/wan_2_2/ (models are not code, R5).
//
// The argv rules are a port of the internal web app's plugin, which stays the reference for limits and flags:
// ~/BGit/all/film/code/subp/video_gen_ai/wan/v2_2_a14b/local/wan-local.ts (16 fps, frames 4n+1 up to 81, sides
// multiple of 16 up to 1280×720, Lightning 4 steps when its LoRAs are installed). Model spec:
// ~/BGit/all/film/pm/subp/video_gen_ai/wan/v2_2_a14b/wan_v2_2_a14b.mdx.
//
// params: {model, kind: t2v|i2v, width, height, duration_seconds, frames, steps, seed, prompt, negative_prompt}; the
// internal web app's camelCase durationSeconds and negativePrompt are accepted too (its processing/tasks.ts).
type OSSVideo struct{}

// WanModel is the model id this runner renders (task_queue.mdx §11.2 models.allow patterns match it).
const WanModel = "wan2.2-a14b"

const (
	wanFPS       = 16
	wanMaxFrames = 81
	wanMaxW      = 1280
	wanMaxH      = 720
)

type videoParams struct {
	Model           string  `json:"model"`
	Kind            string  `json:"kind"`
	Width           int     `json:"width"`
	Height          int     `json:"height"`
	DurationSeconds float64 `json:"duration_seconds"`
	Frames          *int    `json:"frames"`
	Steps           *int    `json:"steps"`
	Seed            *int64  `json:"seed"`
	Prompt          string  `json:"prompt"`
	NegativePrompt  string  `json:"negative_prompt"`

	// The internal web app spreads its LocalJob params as they are (camelCase): accepted as aliases.
	DurationSecondsAlt float64 `json:"durationSeconds"`
	NegativePromptAlt  string  `json:"negativePrompt"`
}

func (OSSVideo) Name() string       { return "oss_video" }
func (OSSVideo) Reattachable() bool { return true }

func (p *videoParams) defaults() {
	if p.DurationSeconds == 0 {
		p.DurationSeconds = p.DurationSecondsAlt
	}
	if p.NegativePrompt == "" {
		p.NegativePrompt = p.NegativePromptAlt
	}
	if p.Model == "" {
		p.Model = WanModel
	}
	if p.Kind == "" {
		p.Kind = "t2v"
	}
	if p.Width == 0 {
		p.Width = 1280
	}
	if p.Height == 0 {
		p.Height = 720
	}
	if p.DurationSeconds == 0 {
		p.DurationSeconds = 5
	}
}

func wanHome() string { return renderHome("wan_2_2") }

func (OSSVideo) Check(_ context.Context, job *jobs.Job, env Env) error {
	var p videoParams
	if err := decodeParams(job, &p); err != nil {
		return err
	}
	p.defaults()
	if p.Model != WanModel {
		return jobs.Errf(jobs.KindInvalidInput, "model %q is not built on this server yet (built: %s)", p.Model, WanModel)
	}
	if p.Kind != "t2v" && p.Kind != "i2v" {
		return jobs.Errf(jobs.KindInvalidInput, "Wan 2.2 makes video from a prompt (t2v) or a first frame (i2v), not %q", p.Kind)
	}
	if p.DurationSeconds <= 0 || p.DurationSeconds > float64(wanMaxFrames)/wanFPS+0.01 {
		return jobs.Errf(jobs.KindInvalidInput, "Wan 2.2 makes at most %.2f s per clip", float64(wanMaxFrames)/wanFPS)
	}
	if p.Width < 16 || p.Height < 16 || p.Width > 4096 || p.Height > 4096 {
		return jobs.Errf(jobs.KindInvalidInput, "width and height must be 16..4096")
	}
	if strings.TrimSpace(p.Prompt) == "" {
		return jobs.Errf(jobs.KindInvalidInput, "prompt is required")
	}
	if p.Kind == "i2v" && !hasInput(job, "first_frame") {
		return jobs.Errf(jobs.KindInvalidInput, "an i2v job needs the input \"first_frame\"")
	}
	if FakeModels() {
		return nil
	}
	home := wanHome()
	for _, f := range []string{venvPython(home), filepath.Join(home, "models", "Wan2.2-T2V-A14B-Diffusers", "model_index.json")} {
		if !fileAtLeast(f, 1) {
			return jobs.Errf(jobs.KindInfra, "Wan 2.2 is not installed on this server: missing %s (install guide: ~/BGit/all/app/smartc/film/servers/bms/oss_video/wan_2_2/)", f)
		}
	}
	return nil
}

func (OSSVideo) Run(ctx context.Context, job *jobs.Job, env Env, rep Reporter) (*Result, error) {
	var p videoParams
	if err := decodeParams(job, &p); err != nil {
		return nil, err
	}
	p.defaults()
	return runDetached(ctx, job, env, rep, "video", func(snap snapshot.Snapshot, inputs map[string]string, out string) (detachedTask, error) {
		w, h, frames := wanFrame(p)
		if FakeModels() {
			return fakeTask(snap, out, "", p.Prompt, w, h)
		}
		return wanTask(snap, p, inputs["first_frame"], out, w, h, frames), nil
	})
}

// wanFrame applies the model's limits: scale to fit 1280×720 (or 720×1280), sides down to a multiple of 16, frames
// 4n+1 up to 81 at 16 fps.
func wanFrame(p videoParams) (w, h, frames int) {
	landscape := p.Width >= p.Height
	maxW, maxH := float64(wanMaxW), float64(wanMaxH)
	if !landscape {
		maxW, maxH = wanMaxH, wanMaxW
	}
	scale := math.Min(1, math.Min(maxW/float64(max(1, p.Width)), maxH/float64(max(1, p.Height))))
	w = floorTo(int(float64(p.Width)*scale), 16)
	h = floorTo(int(float64(p.Height)*scale), 16)
	if p.Frames != nil && *p.Frames > 0 {
		frames = framesKn1(*p.Frames, 4, wanMaxFrames)
	} else {
		frames = framesKn1(int(math.Round(p.DurationSeconds*wanFPS))+1, 4, wanMaxFrames)
	}
	return
}

func wanTask(snap snapshot.Snapshot, p videoParams, image, out string, w, h, frames int) detachedTask {
	home := wanHome()
	accel := accelerator()
	lightning := os.Getenv("WAN_LIGHTNING_DIR")
	if lightning == "" {
		lightning = filepath.Join(home, "models", "Wan2.2-Lightning", "Wan2.2-T2V-A14B-4steps-lora-250928")
	}
	lightningI2V := os.Getenv("WAN_LIGHTNING_I2V_DIR")
	if lightningI2V == "" {
		lightningI2V = filepath.Join(home, "models", "Wan2.2-Lightning", "Wan2.2-I2V-A14B-4steps-lora-rank64-Seko-V1")
	}
	off := os.Getenv("WAN_LIGHTNING") == "off"
	seed := int64(42)
	if p.Seed != nil {
		seed = *p.Seed
	}
	argv := []string{
		venvPython(home), snap.Path("code/scripts/video_gen/wan22/render_wan.py"),
		"--model", filepath.Join(home, "models", "Wan2.2-T2V-A14B-Diffusers"),
		"--out", out,
		"--prompt", truncate(strings.TrimSpace(p.Prompt), 500),
		"--width", fmt.Sprint(w), "--height", fmt.Sprint(h),
		"--frames", fmt.Sprint(frames), "--fps", fmt.Sprint(wanFPS),
		"--seed", fmt.Sprint(seed), "--device", accel,
	}
	if p.NegativePrompt != "" {
		argv = append(argv, "--negative", truncate(p.NegativePrompt, 500))
	}
	if p.Kind == "i2v" && image != "" {
		argv = append(argv, "--image", image, "--i2v-model", filepath.Join(home, "models", "Wan2.2-I2V-A14B-Diffusers"))
		if !off {
			argv = append(argv, "--lightning-i2v", lightningI2V)
		}
	}
	if !off {
		argv = append(argv, "--lightning", lightning)
	}
	full := 20
	if accel == "cuda" {
		full = 30
	}
	steps := full
	if !off {
		steps = 4
	}
	if p.Steps != nil && *p.Steps > 0 {
		steps = *p.Steps
	}
	argv = append(argv, "--steps", fmt.Sprint(steps))
	if p.Steps == nil && !off {
		argv = append(argv, "--steps-full", fmt.Sprint(full)) // no LoRA loaded: never 4 steps
	}
	var env []string
	if accel == "mps" {
		env = append(env, "PYTORCH_ENABLE_MPS_FALLBACK=1")
	}
	return detachedTask{Argv: argv, Dir: filepath.Dir(out), Env: append(env, "PYTHONUNBUFFERED=1"), Output: out}
}

func hasInput(job *jobs.Job, name string) bool {
	for _, in := range job.Payload.Inputs {
		if in.Name == name {
			return true
		}
	}
	return false
}

// floorTo rounds n down to a multiple of m, at least m.
func floorTo(n, m int) int { return max(m, n/m*m) }

// framesKn1 is the nearest k·n+1 frame count at or under maxF.
func framesKn1(wanted, k, maxF int) int {
	n := max(1, min(int(math.Round(float64(wanted-1)/float64(k))), (maxF-1)/k))
	return k*n + 1
}
