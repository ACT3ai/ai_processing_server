package runners

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
)

// LTX-2.5 (model "ltx-2.5") for act3.video_gen_open_source: text- or image-to-video WITH audio through the official pipeline,
// `python -m ltx_pipelines.distilled` in the pinned Lightricks/LTX-2 checkout at ~/act3_render/ltx_2_5/LTX-2 (the
// install is the BMS child's recipes: ~/BGit/all/app/smartc/film/servers/bms/oss_video/ltx_2_5/). The repo is the
// model's code, like weights: it is not frozen into the snapshot.
//
// On CUDA the 22B BF16 transformer does not fit 32 GB, so the runner passes --quantization fp8-cast --offload cpu
// (weights in pinned RAM, streamed per layer); MPS runs BF16 as is. LTX_ARGS replaces those flags; LTX_TRANSFORMER_PATH
// points at another transformer file. Port of ~/BGit/all/film/code/subp/video_gen_ai/ltx/ltx_2_5/local/ltx-local.ts
// (24 fps, frames 8n+1 up to 257, sides multiple of 32 up to 1280). Spec: ltx_2_5.mdx §4, §10, §12.
const LTXModel = "ltx-2.5"

const (
	ltxFPS       = 24
	ltxMaxFrames = 257
	ltxMaxSide   = 1280
)

var ltxWeights = []struct {
	label string
	path  []string
	min   int64
}{
	{"distilled transformer 22B", []string{"diffusion_models", "ltx-2.5-22b-distilled-transformer-bf16.safetensors"}, 1 << 30},
	{"Gemma 4 12B text encoder", []string{"text_encoders", "gemma4-12b-with-proj-ltx-2.5-bf16.safetensors"}, 1 << 30},
	{"video VAE", []string{"vae", "ltx-2.5-video-vae-bf16.safetensors"}, 100 << 20},
	{"audio VAE", []string{"vae", "ltx-2.5-audio-vae-bf16.safetensors"}, 10 << 20},
	{"latent upscaler x2", []string{"latent_upscale_models", "ltx-2.5-latent-spatial-upscaler-x2-bf16-1.0.safetensors"}, 10 << 20},
}

func ltxHome() string { return renderHome("ltx_2_5") }

func ltxWeight(home string, i int) string {
	if i == 0 {
		if p := os.Getenv("LTX_TRANSFORMER_PATH"); p != "" {
			return p
		}
	}
	return filepath.Join(append([]string{home, "models", "ltx-2.5"}, ltxWeights[i].path...)...)
}

// ltxInstallProblem checks the repo's venv, the checkout and the five weight files a job loads.
func ltxInstallProblem() string {
	home := ltxHome()
	repo := filepath.Join(home, "LTX-2")
	if !fileAtLeast(venvPython(repo), 1) {
		return fmt.Sprintf("LTX-2.5 is not installed on this server: missing %s (install guide: ~/BGit/all/app/smartc/film/servers/bms/oss_video/ltx_2_5/)", venvPython(repo))
	}
	if st, err := os.Stat(filepath.Join(repo, "packages")); err != nil || !st.IsDir() {
		return fmt.Sprintf("LTX-2.5 is not installed on this server: no Lightricks/LTX-2 checkout at %s", repo)
	}
	for i, w := range ltxWeights {
		if p := ltxWeight(home, i); !fileAtLeast(p, w.min) {
			return fmt.Sprintf("LTX-2.5 is not installed on this server: the %s is missing or partial at %s", w.label, p)
		}
	}
	return ""
}

func ltxCheck(job *jobs.Job, p videoParams) error {
	if p.Kind != "t2v" && p.Kind != "i2v" {
		return jobs.Errf(jobs.KindInvalidInput, "LTX-2.5 makes video from text or a first frame, not %q", p.Kind)
	}
	if p.Kind == "i2v" && !hasInput(job, "first_frame") {
		return jobs.Errf(jobs.KindInvalidInput, "an i2v job needs the input \"first_frame\"")
	}
	if p.DurationSeconds <= 0 || p.DurationSeconds > float64(ltxMaxFrames)/ltxFPS+0.01 {
		return jobs.Errf(jobs.KindInvalidInput, "LTX-2.5 makes at most %.1f s per clip here", float64(ltxMaxFrames)/ltxFPS)
	}
	return nil
}

// ltxFrame: sides down to a multiple of 32 within 1280 (aspect kept), frames 8n+1 up to 257 at 24 fps.
func ltxFrame(p videoParams) (w, h, frames int) {
	scale := math.Min(1, float64(ltxMaxSide)/float64(max(1, p.Width, p.Height)))
	w = floorTo(int(float64(p.Width)*scale), 32)
	h = floorTo(int(float64(p.Height)*scale), 32)
	frames = framesKn1(framesWanted(p, ltxFPS), 8, ltxMaxFrames)
	return
}

func ltxTask(p videoParams, image, out string, w, h, frames int) detachedTask {
	home := ltxHome()
	repo := filepath.Join(home, "LTX-2")
	seed := int64(42)
	if p.Seed != nil {
		seed = *p.Seed
	}
	argv := []string{
		venvPython(repo), "-m", "ltx_pipelines.distilled",
		"--transformer-path", ltxWeight(home, 0),
		"--text-encoder-path", ltxWeight(home, 1),
		"--video-vae-path", ltxWeight(home, 2),
		"--audio-vae-path", ltxWeight(home, 3),
		"--spatial-upsampler-path", ltxWeight(home, 4),
		"--width", fmt.Sprint(w), "--height", fmt.Sprint(h),
		"--num-frames", fmt.Sprint(frames), "--frame-rate", fmt.Sprint(ltxFPS),
		"--seed", fmt.Sprint(seed),
		"--prompt", truncate(strings.TrimSpace(p.Prompt), 4000),
		"--output-path", out,
	}
	if p.Kind == "i2v" && image != "" {
		argv = append(argv, "--image", image, "0", "1.0") // frame 0 at full strength: the clip starts on the shot's frame
	}
	extra := os.Getenv("LTX_ARGS")
	if extra == "" && accelerator() == "cuda" {
		extra = "--quantization fp8-cast --offload cpu"
	}
	argv = append(argv, strings.Fields(extra)...)
	env := []string{"PYTHONUNBUFFERED=1", "LTX2_COMPUTE_DTYPE=bfloat16"}
	if accelerator() == "mps" {
		env = append(env, "PYTORCH_ENABLE_MPS_FALLBACK=1")
	}
	return detachedTask{Argv: argv, Dir: repo, Env: env, Output: out}
}

// framesWanted is the job's frame count at fps: its own count (rescaled when it was counted at another rate), else
// its duration.
func framesWanted(p videoParams, fps int) int {
	if p.Frames != nil && *p.Frames > 0 {
		if p.FPS > 0 && p.FPS != fps {
			return int(math.Round(float64(*p.Frames-1)*float64(fps)/float64(p.FPS))) + 1
		}
		return *p.Frames
	}
	return int(math.Round(p.DurationSeconds*float64(fps))) + 1
}
