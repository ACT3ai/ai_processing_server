package runners

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
)

// Kandinsky 6.0 Video (models "kandinsky-6.0-lite" and "kandinsky-6.0-pro") for act3.video_gen_open_source, INTERNAL
// EVALUATION ONLY: code/scripts/video_gen/kandinsky6/render_kandinsky.py from the job's snapshot, a synced copy of
// ~/BGit/all/film/code/subp/video_gen_ai/kandinsky/video_pro_6_0/local/render_kandinsky.py, with the install at
// ~/act3_render/kandinsky_6/ (the BMS child's recipes). Lite-distill (3B) runs on CUDA and MPS; Pro-distill (what
// production sends) is CUDA only, its 56 GB transformer streamed to the GPU in block groups. Both are the 10-step
// distilled checkpoints at guidance 1 (the 50-step Pro takes minutes a step when streamed on a 32 GB card). The clip carries
// the pipeline's audio. Port of kandinsky-local.ts: 24 fps, at most 121 frames (5 s), sides multiple of 16.
// Spec: kandinsky_video_pro_6_0.mdx §10.6, §12.3.
const (
	kandinskyFPS       = 24
	kandinskyMaxFrames = 121
	kandinskyMaxSide   = 1280
)

var kandinskyCheckpoints = map[string]struct{ dir, env string }{
	"lite": {"Kandinsky-6.0-Lite-distill-5s-Diffusers", "KANDINSKY_LITE_DIR"},
	"pro":  {"Kandinsky-6.0-Pro-distill-5s-Diffusers", "KANDINSKY_PRO_DIR"},
}

func kandinskyHome() string { return renderHome("kandinsky_6") }

// kandinskyQuality is "lite" or "pro" for a model id, "" when it is not a Kandinsky model.
func kandinskyQuality(model string) string {
	q, ok := strings.CutPrefix(model, "kandinsky-6.0-")
	if _, known := kandinskyCheckpoints[q]; ok && known {
		return q
	}
	return ""
}

func kandinskyCheckpoint(q string) string {
	c := kandinskyCheckpoints[q]
	if p := os.Getenv(c.env); p != "" {
		return p
	}
	return filepath.Join(kandinskyHome(), "models", c.dir)
}

// kandinskyInstallProblem checks the venv and the checkpoint one quality loads.
func kandinskyInstallProblem(q string) string {
	home := kandinskyHome()
	for _, f := range []string{venvPython(home), filepath.Join(kandinskyCheckpoint(q), "model_index.json")} {
		if !fileAtLeast(f, 1) {
			return fmt.Sprintf("Kandinsky 6.0 %s is not installed on this server: missing %s (install guide: ~/BGit/all/app/smartc/film/servers/bms/oss_video/kandinsky_6/)", q, f)
		}
	}
	return ""
}

func kandinskyCheck(job *jobs.Job, p videoParams) error {
	if p.Kind != "t2v" && p.Kind != "i2v" {
		return jobs.Errf(jobs.KindInvalidInput, "Kandinsky 6.0 makes video from text or a first frame, not %q", p.Kind)
	}
	if kandinskyQuality(p.Model) == "pro" && accelerator() != "cuda" {
		return jobs.Errf(jobs.KindInvalidInput, "Kandinsky Pro is CUDA only; this server runs Lite")
	}
	if p.DurationSeconds <= 0 || p.DurationSeconds > float64(kandinskyMaxFrames)/kandinskyFPS+0.01 {
		return jobs.Errf(jobs.KindInvalidInput, "the 5 s Kandinsky checkpoints make at most 5 s")
	}
	if p.Kind == "i2v" && !hasInput(job, "first_frame") {
		return jobs.Errf(jobs.KindInvalidInput, "an i2v job needs the input \"first_frame\"")
	}
	return nil
}

// kandinskyFrame: sides down to a multiple of 16 within 1280 (aspect kept), frames 4n+1 up to 121 at 24 fps.
func kandinskyFrame(p videoParams) (w, h, frames int) {
	scale := math.Min(1, float64(kandinskyMaxSide)/float64(max(1, p.Width, p.Height)))
	w = floorTo(int(float64(p.Width)*scale), 16)
	h = floorTo(int(float64(p.Height)*scale), 16)
	frames = framesKn1(framesWanted(p, kandinskyFPS), 4, kandinskyMaxFrames)
	return
}

func kandinskyTask(snap snapshot.Snapshot, env Env, p videoParams, image, out string, w, h, frames int) detachedTask {
	q := kandinskyQuality(p.Model)
	steps, guidance := 10, "1.0" // both checkpoints are distilled: 10 steps, no CFG
	if p.Steps != nil && *p.Steps > 0 {
		steps = *p.Steps
	}
	seed := int64(42)
	if p.Seed != nil {
		seed = *p.Seed
	}
	accel := accelerator()
	argv := []string{
		venvPython(kandinskyHome()), snap.Path("code/scripts/video_gen/kandinsky6/render_kandinsky.py"),
		"--model", kandinskyCheckpoint(q),
		"--out", out,
		"--prompt", truncate(strings.TrimSpace(p.Prompt), 2000),
		"--width", fmt.Sprint(w), "--height", fmt.Sprint(h),
		"--frames", fmt.Sprint(frames), "--fps", fmt.Sprint(kandinskyFPS),
		"--steps", fmt.Sprint(steps), "--guidance", guidance,
		"--seed", fmt.Sprint(seed), "--device", accel,
	}
	if p.Kind == "i2v" && image != "" {
		argv = append(argv, "--image", image)
	}
	vars := []string{"PYTHONUNBUFFERED=1"}
	if ff := enginePath(env, "ffmpeg"); ff != "" {
		vars = append(vars, "ACT3_FFMPEG="+ff)
	}
	if accel == "mps" {
		vars = append(vars, "PYTORCH_ENABLE_MPS_FALLBACK=1")
	}
	return detachedTask{Argv: argv, Dir: filepath.Dir(out), Env: vars, Output: out, Cancel: ProfileKandinsky}
}
