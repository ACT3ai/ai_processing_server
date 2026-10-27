package runners

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
)

// OSSUpscale is the runner behind act3.upscale_open_source: video upscaling with SeedVR2 on our own GPUs (the Mac
// Studio on Metal, B5090 on CUDA). Input "source_video"; params name the TARGET frame. It runs the numz
// inference_cli.py installed at ~/act3_render/seedvr2/repo/ (an engine, outside this repo: R5) with the weights in
// ~/act3_render/seedvr2/models/SEEDVR2/.
//
// The argv is a port of the product's builder, which stays the reference:
// ~/BGit/all/film/code/subp/upscaling/open_source/seedvr2/seedvr2-cli.ts seedvr2Argv, with the machine profiles
// ../seed_vr2_mac_studio/mac-studio.ts (MPS: batch 21, no BlockSwap, VAE tiled from 1080p because MPSGraph refuses
// tensors over INT_MAX) and ../seed_vr2_nvidia/nvidia.ts (CUDA). Spec:
// ~/BGit/all/film/pm/subp/upscaling/open_source/seedvr2.mdx §3.
//
// params: {model: "seedvr2" (default) or "seedvr2-<quality>", quality: standard|best|sharp, width, height (the target),
// duration_seconds}.
type OSSUpscale struct{}

type upscaleParams struct {
	Model           string  `json:"model"`
	Quality         string  `json:"quality"`
	Width           int     `json:"width"`
	Height          int     `json:"height"`
	DurationSeconds float64 `json:"duration_seconds"`

	DurationSecondsAlt float64 `json:"durationSeconds"` // the internal web app's camelCase
}

// SeedVR2 qualities (seedvr2-models.ts): the --dit_model file and its size.
var seedvr2Qualities = map[string]struct{ dit, size string }{
	"standard": {"seedvr2_ema_3b_fp16.safetensors", "3b"},
	"best":     {"seedvr2_ema_7b_fp16.safetensors", "7b"},
	"sharp":    {"seedvr2_ema_7b_sharp_fp16.safetensors", "7b"},
}

const seedvr2VAE = "ema_vae_fp16.safetensors"

func (OSSUpscale) Name() string       { return "oss_upscale" }
func (OSSUpscale) Reattachable() bool { return true }

func (p *upscaleParams) defaults() {
	if p.DurationSeconds == 0 {
		p.DurationSeconds = p.DurationSecondsAlt
	}
	if p.Model == "" {
		p.Model = "seedvr2"
	}
	// "seedvr2-best" names the quality in the model id (the internal web app's processing/tasks.ts TASK_FOR).
	if q, ok := strings.CutPrefix(p.Model, "seedvr2-"); ok {
		p.Model = "seedvr2"
		if p.Quality == "" || p.Quality == q {
			p.Quality = q
		}
	}
	if p.Quality == "" {
		p.Quality = "standard"
	}
}

func seedvr2Home() string { return renderHome("seedvr2") }

func (OSSUpscale) Check(_ context.Context, job *jobs.Job, env Env) error {
	var p upscaleParams
	if err := decodeParams(job, &p); err != nil {
		return err
	}
	p.defaults()
	if p.Model != "seedvr2" {
		return jobs.Errf(jobs.KindInvalidInput, "upscaler %q is not built on this server yet (built: seedvr2)", p.Model)
	}
	q, ok := seedvr2Qualities[p.Quality]
	if !ok {
		return jobs.Errf(jobs.KindInvalidInput, "SeedVR2 has no quality %q (standard, best, sharp)", p.Quality)
	}
	if p.Width < 16 || p.Height < 16 || p.Width > 7680 || p.Height > 7680 {
		return jobs.Errf(jobs.KindInvalidInput, "width and height (the target frame) must be 16..7680")
	}
	if float64(p.Width)*float64(p.Height) > 8.3e6+1 {
		return jobs.Errf(jobs.KindInvalidInput, "SeedVR2 outputs at most 8.3 megapixels (4K UHD)")
	}
	if accelerator() == "mps" && p.DurationSeconds > 30 {
		return jobs.Errf(jobs.KindInvalidInput, "SeedVR2 on this Mac takes clips up to 30 s")
	}
	if !hasInput(job, "source_video") {
		return jobs.Errf(jobs.KindInvalidInput, "an upscale needs the input \"source_video\"")
	}
	if FakeModels() {
		return nil
	}
	home := seedvr2Home()
	repo := filepath.Join(home, "repo")
	models := filepath.Join(home, "models", "SEEDVR2")
	for _, f := range []string{venvPython(repo), filepath.Join(repo, "inference_cli.py")} {
		if !fileAtLeast(f, 1) {
			return jobs.Errf(jobs.KindInfra, "SeedVR2 is not installed on this server: missing %s (install: ~/BGit/all/film/code/subp/upscaling/open_source/seed_vr2_mac_studio/install_mac.sh)", f)
		}
	}
	for _, w := range []string{seedvr2VAE, q.dit} {
		if !fileAtLeast(filepath.Join(models, w), 100<<20) {
			return jobs.Errf(jobs.KindInfra, "SeedVR2 weight %s is missing or partial in %s", w, models)
		}
	}
	return nil
}

func (OSSUpscale) Run(ctx context.Context, job *jobs.Job, env Env, rep Reporter) (*Result, error) {
	var p upscaleParams
	if err := decodeParams(job, &p); err != nil {
		return nil, err
	}
	p.defaults()
	return runDetached(ctx, job, env, rep, "video", func(snap snapshot.Snapshot, inputs map[string]string, out string) (detachedTask, error) {
		src := inputs["source_video"]
		if FakeModels() {
			return fakeTask(snap, out, src, "", p.Width, p.Height)
		}
		return seedvr2Task(p, src, out, accelerator()), nil
	})
}

// seedvr2Task builds the inference_cli.py command (seedvr2-cli.ts seedvr2Argv).
func seedvr2Task(p upscaleParams, src, out, accel string) detachedTask {
	home := seedvr2Home()
	repo := filepath.Join(home, "repo")
	q := seedvr2Qualities[p.Quality]
	short, long := min(p.Width, p.Height), max(p.Width, p.Height)
	batch, swap := 21, 0
	if accel == "cuda" {
		batch, swap = nvidiaBatch(q.size, short), nvidiaSwap(q.size, short)
	}
	if v, err := strconv.Atoi(os.Getenv("SEEDVR2_BATCH_SIZE")); err == nil && v > 0 {
		batch = v
	}
	if v, err := strconv.Atoi(os.Getenv("SEEDVR2_BLOCKS_TO_SWAP")); err == nil && v >= 0 && accel == "cuda" {
		swap = v
	}
	batch = 4*max(0, (max(1, batch)-1+2)/4) + 1 // nearest 4n+1
	argv := []string{
		venvPython(repo), filepath.Join(repo, "inference_cli.py"), src,
		"--output", out, "--output_format", "mp4", "--video_backend", "ffmpeg",
		"--model_dir", filepath.Join(home, "models", "SEEDVR2"), "--dit_model", q.dit,
		"--resolution", fmt.Sprint(short), "--max_resolution", fmt.Sprint(long),
		"--batch_size", fmt.Sprint(batch), "--color_correction", "lab", "--seed", "42",
	}
	if accel == "cuda" {
		argv = append(argv, "--cuda_device", "0")
		if swap > 0 {
			argv = append(argv, "--dit_offload_device", "cpu", "--blocks_to_swap", fmt.Sprint(swap))
		}
	}
	if accel == "mps" && short >= 1080 {
		argv = append(argv, "--vae_encode_tiled")
	}
	if (accel == "mps" && short >= 1080) || short >= nvidiaHighRes {
		argv = append(argv, "--vae_decode_tiled")
	}
	argv = append(argv, strings.Fields(os.Getenv("SEEDVR2_EXTRA_ARGS"))...)
	env := []string{"PYTHONUNBUFFERED=1"}
	if accel == "mps" {
		env = append(env, "PYTORCH_ENABLE_MPS_FALLBACK=1")
	}
	return detachedTask{Argv: argv, Dir: repo, Env: env, Output: out}
}

// The RTX 5090 profile (seed_vr2_nvidia/nvidia.ts): 7B needs BlockSwap and small batches from 1440p up.
const nvidiaHighRes = 1440

func nvidiaBatch(size string, short int) int {
	switch {
	case size == "3b":
		return 21
	case short >= nvidiaHighRes:
		return 5
	}
	return 9
}

func nvidiaSwap(size string, short int) int {
	switch {
	case size == "3b":
		return 0
	case short >= 2160:
		return 24
	case short >= nvidiaHighRes:
		return 16
	}
	return 0
}
