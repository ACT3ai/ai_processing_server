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

// MiniMax H3 (model "minimax-h3") for act3.video_gen_open_source: text or first frame to video WITH audio, internal
// testing only (MiniMax H3 Community License). code/scripts/video_gen/h3_regenerate/render_h3_generate.py from the job's
// snapshot, a synced copy of ~/BGit/all/film/code/subp/video_gen_ai/nvidia/h3_regenerate_2k/render_h3_generate.py, runs
// ComfyUI's native MiniMax-H3 nodes in a private ComfyUI started for the job. It shares the Open-H3-Regenerate-2K
// install (~/act3_render/open_h3_regenerate_2k: ComfyUI, the int8 base, the text encoder and the VAEs), so an H3 draft
// and its 2K regenerate run on the same files. CUDA only. Port of h3-generate-local.ts: 24 fps, H3's 768-short-edge
// canvas (area about 768×1344, sides multiple of 32), up to 15 s; the script snaps frames to H3's 17k+5 grid and cuts
// the clip back to the asked length. Spec: ~/BGit/all/film/pm/subp/video_gen_ai/minimax/fast_h3_omni/minimax_omni.mdx.
const (
	H3Model        = "minimax-h3"
	h3FPS          = 24
	h3MaxSeconds   = 362.0 / h3FPS // 17·21+5 frames, the trained range's top
	h3ShortEdge    = 768
	h3MaxPixels    = 768 * 1344
	h3Steps        = 20
	h3MaxSteps     = 100
	h3MaxRefImages = 9 // Ref2VA (spec_minimax.mdx §3)
	h3MaxRefAudio  = 3
	h3MinRAMGB     = 96.0 // the int8 base and the 32B text encoder offload to system RAM between stages
)

// h3InstallProblem: CUDA, the venv, ComfyUI and the files generation loads (not the refiner's).
func h3InstallProblem() string {
	if accelerator() != "cuda" {
		return "MiniMax H3 runs on CUDA only"
	}
	home := regenHome()
	if !fileAtLeast(venvPython(home), 1) {
		return h3Missing(venvPython(home))
	}
	for _, f := range h3rComfyFiles {
		if p := comfyPath(home, f.rel); !f.refinerOnly && !fileAtLeast(p, f.min) {
			return h3Missing(p)
		}
	}
	return ""
}

// h3InstallProblemFor: what this job needs, the Ref2VA base too when it carries references (the FL2VA install alone
// renders text and first-frame jobs, so the server still offers H3 without it).
func h3InstallProblemFor(job *jobs.Job) string {
	if why := h3InstallProblem(); why != "" {
		return why
	}
	names := inputNames(job)
	if len(roleInputs(names, "reference_image"))+len(roleInputs(names, "audio")) > 0 {
		if p := comfyPath(regenHome(), h3Ref2VABase); !fileAtLeast(p, 19_000_000_000) {
			return h3Missing(p)
		}
	}
	return ""
}

var h3Ref2VABase = []string{"models", "diffusion_models", "minimax_h3_ref2va_pruned_int8_convrot.safetensors"}

func h3Missing(p string) string {
	return fmt.Sprintf("MiniMax H3 is not installed on this server: missing or partial %s (install: ~/BGit/all/app/smartc/film/servers/bms/oss_video/open_h3_regenerate_2k/)", p)
}

func h3Check(job *jobs.Job, env Env, p videoParams) error {
	if p.Kind != "t2v" && p.Kind != "i2v" {
		return jobs.Errf(jobs.KindInvalidInput, "MiniMax H3 makes video from text or a first frame, not %q", p.Kind)
	}
	// the same MiniMax H3 Community License as the regenerate: internal delivery only until Legal clears it
	if !H3RegenerateLicenseCleared && (deliveryOf(p.Delivery) == "customer" || job.Payload.DataRules.CustomerData) {
		return jobs.Errf(jobs.KindInvalidInput, "license_not_cleared: MiniMax H3 output may not reach a customer until Legal "+
			"clears the MiniMax H3 Community License; send delivery \"internal\" for internal tests")
	}
	if s := h3Seconds(p); s <= 0 || s > h3MaxSeconds+0.01 {
		return jobs.Errf(jobs.KindInvalidInput, "one MiniMax H3 clip is at most %.1f s", h3MaxSeconds)
	}
	if p.Steps != nil && (*p.Steps < 1 || *p.Steps > h3MaxSteps) {
		return jobs.Errf(jobs.KindInvalidInput, "%d steps is outside 1..%d", *p.Steps, h3MaxSteps)
	}
	refs, audio := len(roleInputs(inputNames(job), "reference_image")), len(roleInputs(inputNames(job), "audio"))
	switch {
	case refs > h3MaxRefImages || audio > h3MaxRefAudio:
		return jobs.Errf(jobs.KindInvalidInput, "H3 takes at most %d reference images and %d reference audio clips", h3MaxRefImages, h3MaxRefAudio)
	case audio > 0 && refs == 0:
		return jobs.Errf(jobs.KindInvalidInput, "H3 never takes audio as the only reference: add an image")
	case refs == 0 && p.Kind == "i2v" && !hasInput(job, "first_frame"):
		return jobs.Errf(jobs.KindInvalidInput, "an i2v job needs the input \"first_frame\"")
	}
	if FakeModels() {
		return nil
	}
	if accelerator() != "cuda" {
		return jobs.Errf(jobs.KindInfra, "MiniMax H3 runs on CUDA only")
	}
	if v := gpuVRAMGB(env); v > 0 && v < h3rCardClassGB {
		return jobs.Errf(jobs.KindInfra, "vram_too_small: this GPU has %s; MiniMax H3 needs a 32 GB class card", gb(v))
	}
	if r := systemRAMGB(); r > 0 && r < h3MinRAMGB {
		return jobs.Errf(jobs.KindInfra, "ram_too_small: %s of system RAM; MiniMax H3 wants %.0f GB+", gb(r), h3MinRAMGB)
	}
	return nil
}

// h3SecondsArg: the length to three decimals, rounded down so the cut never keeps one frame more than asked.
func h3SecondsArg(p videoParams) string {
	return fmt.Sprintf("%.3f", math.Floor(h3Seconds(p)*1000)/1000)
}

// h3Seconds: the asked length, from frames at the job's own rate when it counted them, else its duration.
func h3Seconds(p videoParams) float64 {
	if p.Frames != nil && *p.Frames > 0 {
		fps := p.FPS
		if fps <= 0 {
			fps = h3FPS
		}
		return float64(*p.Frames) / float64(fps)
	}
	return p.DurationSeconds
}

// h3Frame: H3's canvas for the job's aspect (ComfyUI nodes_minimax_h3.py adapt_canvas): a 768 short edge, the area
// capped at 768×1344 before each side is rounded to 32. A smaller frame the job asks for (production's 480p / 720p
// tiers) is kept, rounded down to the 32 grid, the way the node keeps a reference video smaller than its canvas.
func h3Frame(p videoParams) (w, h int) {
	cw, ch := h3Canvas(p)
	if p.Width > 0 && p.Height > 0 && p.Width*p.Height < cw*ch {
		round := func(v int) int { return max(256, v/32*32) } // down to the grid, never under the script's floor
		return round(p.Width), round(p.Height)
	}
	return cw, ch
}

func h3Canvas(p videoParams) (w, h int) {
	ratio := float64(max(1, p.Width)) / float64(max(1, p.Height))
	nw, nh := h3ShortEdge*ratio, float64(h3ShortEdge)
	if ratio < 1 {
		nw, nh = float64(h3ShortEdge), h3ShortEdge/ratio
	}
	if nw*nh > h3MaxPixels {
		s := math.Sqrt(h3MaxPixels / (nw * nh))
		nw, nh = nw*s, nh*s
	}
	round := func(v float64) int { return max(32, int(math.Round(v/32))*32) }
	return round(nw), round(nh)
}

func h3Task(snap snapshot.Snapshot, env Env, p videoParams, inputs map[string]string, out string, w, h int) (detachedTask, error) {
	home := regenHome()
	work := filepath.Join(filepath.Dir(filepath.Dir(out)), "work")
	promptFile := filepath.Join(work, "prompt.txt")
	if err := os.MkdirAll(work, 0o700); err != nil {
		return detachedTask{}, jobs.Wrap(jobs.KindInfra, err, "create the work dir")
	}
	if err := os.WriteFile(promptFile, []byte(strings.TrimSpace(p.Prompt)), 0o600); err != nil {
		return detachedTask{}, jobs.Wrap(jobs.KindInfra, err, "write the prompt file")
	}
	steps := h3Steps
	if p.Steps != nil && *p.Steps > 0 {
		steps = *p.Steps
	}
	seed := uint32(42)
	if p.Seed != nil {
		seed = uint32(*p.Seed) // as the regenerate does: RandomNoise refuses a negative seed
	}
	argv := []string{
		venvPython(home), snap.Path("code/scripts/video_gen/h3_regenerate/render_h3_generate.py"),
		"--home", home, "--out", out, "--prompt-file", promptFile,
		"--width", fmt.Sprint(w), "--height", fmt.Sprint(h),
		"--seconds", h3SecondsArg(p), "--steps", fmt.Sprint(steps), "--seed", fmt.Sprint(seed),
	}
	argv = append(argv, h3InputArgs(p.Kind, inputs)...)
	return detachedTask{Argv: argv, Dir: work, Env: h3Env(env), Output: out, ExpectFrames: int(math.Round(h3Seconds(p) * h3FPS))}, nil
}

// h3InputArgs: reference images and audio (a Seedance-fed request: Ref2VA) in their <Picture N> / <Audio N> order,
// else the first frame and an anchored last frame.
func h3InputArgs(kind string, inputs map[string]string) []string {
	names := make([]string, 0, len(inputs))
	for n := range inputs {
		names = append(names, n)
	}
	args := []string{}
	for _, n := range roleInputs(names, "reference_image") {
		args = append(args, "--ref-image", inputs[n])
	}
	for _, n := range roleInputs(names, "audio") {
		args = append(args, "--ref-audio", inputs[n])
	}
	if len(args) > 0 || kind != "i2v" {
		return args
	}
	if f := inputs["first_frame"]; f != "" {
		args = append(args, "--image", f)
	}
	if last := lastFrameOf(inputs); last != "" {
		args = append(args, "--last-frame", last)
	}
	return args
}
