package runners

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
)

// OSSVideo is the runner behind act3.video_gen_open_source: open-source video generation on our own GPUs. The job's
// model picks the family: Wan 2.2 (below), LTX-2.5 (oss_ltx.go), Kandinsky 6.0 Lite/Pro (oss_kandinsky.go).
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

// WanModel is the default model id this runner renders (task_queue.mdx §11.2 models.allow patterns match it).
// $ACT3_WAN_MODEL picks another of wanModels for this machine (the service catalog sizes its offer from the same
// variable); a job naming a model this machine has not installed fails with "not installed".
const WanModel = "wan2.2-a14b"

// wanSpec is one Wan 2.2 checkpoint's limits, a port of the plugin's WAN_MODELS (wan-local.ts).
type wanSpec struct {
	dir                    string // under <home>/models; image-to-video uses the same folder when i2vDir is ""
	i2vDir                 string
	fps, maxFrames         int
	maxW, maxH, multipleOf int
	lightning              bool // the 4-step Lightning LoRAs exist for it
}

var wanModels = map[string]wanSpec{
	WanModel:         {dir: "Wan2.2-T2V-A14B-Diffusers", i2vDir: "Wan2.2-I2V-A14B-Diffusers", fps: 16, maxFrames: 81, maxW: 1280, maxH: 720, multipleOf: 16, lightning: true},
	"wan2.2-ti2v-5b": {dir: "Wan2.2-TI2V-5B-Diffusers", fps: 24, maxFrames: 121, maxW: 1280, maxH: 704, multipleOf: 32},
}

// wanMachineModel is the model this machine renders: $ACT3_WAN_MODEL, else WanModel.
func wanMachineModel() string {
	if m := os.Getenv("ACT3_WAN_MODEL"); m != "" {
		return m
	}
	return WanModel
}

func wanSpecFor(model string) wanSpec {
	if m, ok := wanModels[model]; ok {
		return m
	}
	return wanModels[WanModel]
}

type videoParams struct {
	Model           string  `json:"model"`
	Kind            string  `json:"kind"`
	Width           int     `json:"width"`
	Height          int     `json:"height"`
	DurationSeconds float64 `json:"duration_seconds"`
	Frames          *int    `json:"frames"`
	FPS             int     `json:"fps"` // the rate Frames was counted at; another model's fps keeps the duration
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
		p.Model = wanMachineModel()
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
	if p.Width < 16 || p.Height < 16 || p.Width > 4096 || p.Height > 4096 {
		return jobs.Errf(jobs.KindInvalidInput, "width and height must be 16..4096")
	}
	if strings.TrimSpace(p.Prompt) == "" {
		return jobs.Errf(jobs.KindInvalidInput, "prompt is required")
	}
	var installProblem func() string
	switch q := kandinskyQuality(p.Model); {
	case p.Model == LTXModel:
		if err := ltxCheck(p); err != nil {
			return err
		}
		installProblem = ltxInstallProblem
	case q != "":
		if err := kandinskyCheck(job, p); err != nil {
			return err
		}
		installProblem = func() string { return kandinskyInstallProblem(q) }
	default:
		if err := wanCheck(job, p); err != nil {
			return err
		}
		installProblem = func() string { return wanInstallProblem(p.Model) }
	}
	if FakeModels() {
		return nil
	}
	if why := installProblem(); why != "" {
		return jobs.Errf(jobs.KindInfra, "%s", why)
	}
	return nil
}

func wanCheck(job *jobs.Job, p videoParams) error {
	m, ok := wanModels[p.Model]
	if !ok {
		return jobs.Errf(jobs.KindInvalidInput, "model %q is not built on this server (built: %s)", p.Model, strings.Join(videoModelIDs(), ", "))
	}
	if p.Kind != "t2v" && p.Kind != "i2v" {
		return jobs.Errf(jobs.KindInvalidInput, "Wan 2.2 makes video from a prompt (t2v) or a first frame (i2v), not %q", p.Kind)
	}
	if p.DurationSeconds <= 0 || p.DurationSeconds > float64(m.maxFrames)/float64(m.fps)+0.01 {
		return jobs.Errf(jobs.KindInvalidInput, "Wan 2.2 makes at most %.2f s per clip", float64(m.maxFrames)/float64(m.fps))
	}
	if p.Kind == "i2v" && !hasInput(job, "first_frame") {
		return jobs.Errf(jobs.KindInvalidInput, "an i2v job needs the input \"first_frame\"")
	}
	return nil
}

// videoModelIDs: every model this runner renders, for "not built" answers.
func videoModelIDs() []string {
	return append(wanModelIDs(), LTXModel, "kandinsky-6.0-lite", "kandinsky-6.0-pro")
}

func (OSSVideo) Run(ctx context.Context, job *jobs.Job, env Env, rep Reporter) (*Result, error) {
	var p videoParams
	if err := decodeParams(job, &p); err != nil {
		return nil, err
	}
	p.defaults()
	return runDetached(ctx, job, env, rep, "video", func(snap snapshot.Snapshot, inputs map[string]string, out string) (detachedTask, error) {
		var w, h, frames int
		switch {
		case p.Model == LTXModel:
			w, h, frames = ltxFrame(p)
		case kandinskyQuality(p.Model) != "":
			w, h, frames = kandinskyFrame(p)
		default:
			w, h, frames = wanFrame(p)
		}
		if FakeModels() {
			return fakeTask(snap, out, "", p.Prompt, w, h)
		}
		switch {
		case p.Model == LTXModel:
			return ltxTask(p, out, w, h, frames), nil
		case kandinskyQuality(p.Model) != "":
			return kandinskyTask(snap, env, p, inputs["first_frame"], out, w, h, frames), nil
		}
		return wanTask(snap, p, inputs["first_frame"], out, w, h, frames), nil
	})
}

func wanModelIDs() []string {
	ids := make([]string, 0, len(wanModels))
	for id := range wanModels {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// wanFrame applies the model's limits: scale to fit its largest frame (A14B 1280×720, TI2V-5B 1280×704, either way
// round), sides down to its multiple (16, 32), frames 4n+1 up to its maximum at its fps (81 at 16, 121 at 24).
func wanFrame(p videoParams) (w, h, frames int) {
	m := wanSpecFor(p.Model)
	landscape := p.Width >= p.Height
	maxW, maxH := float64(m.maxW), float64(m.maxH)
	if !landscape {
		maxW, maxH = maxH, maxW
	}
	scale := math.Min(1, math.Min(maxW/float64(max(1, p.Width)), maxH/float64(max(1, p.Height))))
	w = floorTo(int(float64(p.Width)*scale), m.multipleOf)
	h = floorTo(int(float64(p.Height)*scale), m.multipleOf)
	frames = framesKn1(framesWanted(p, m.fps), 4, m.maxFrames)
	return
}

func wanTask(snap snapshot.Snapshot, p videoParams, image, out string, w, h, frames int) detachedTask {
	home := wanHome()
	accel := accelerator()
	m := wanSpecFor(p.Model)
	lightning := os.Getenv("WAN_LIGHTNING_DIR")
	if lightning == "" {
		lightning = filepath.Join(home, "models", "Wan2.2-Lightning", "Wan2.2-T2V-A14B-4steps-lora-250928")
	}
	lightningI2V := os.Getenv("WAN_LIGHTNING_I2V_DIR")
	if lightningI2V == "" {
		lightningI2V = filepath.Join(home, "models", "Wan2.2-Lightning", "Wan2.2-I2V-A14B-4steps-lora-rank64-Seko-V1")
	}
	off := os.Getenv("WAN_LIGHTNING") == "off" || !m.lightning
	seed := int64(42)
	if p.Seed != nil {
		seed = *p.Seed
	}
	argv := []string{
		venvPython(home), snap.Path("code/scripts/video_gen/wan22/render_wan.py"),
		"--model", filepath.Join(home, "models", m.dir),
		"--out", out,
		"--prompt", truncate(strings.TrimSpace(p.Prompt), 500),
		"--width", fmt.Sprint(w), "--height", fmt.Sprint(h),
		"--frames", fmt.Sprint(frames), "--fps", fmt.Sprint(m.fps),
		"--seed", fmt.Sprint(seed), "--device", accel,
	}
	if p.NegativePrompt != "" {
		argv = append(argv, "--negative", truncate(p.NegativePrompt, 500))
	}
	if p.Kind == "i2v" && image != "" {
		i2v := m.i2vDir
		if i2v == "" {
			i2v = m.dir
		}
		argv = append(argv, "--image", image, "--i2v-model", filepath.Join(home, "models", i2v))
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
