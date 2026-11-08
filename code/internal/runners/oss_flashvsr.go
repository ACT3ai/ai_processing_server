package runners

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
)

// FlashVSR v1.1 (model "flashvsr-1.1") for act3.upscale_open_source, INTERNAL: code/scripts/upscale/flashvsr/
// run_flashvsr.py from the job's snapshot, a synced copy of ~/BGit/all/app/smartc/film/servers/bms/oss_video/flashvsr/
// code/run_flashvsr.py. That port replaces upstream's CUDA-only Block-Sparse-Attention with the same block mask in
// plain torch, so it runs on MPS and CUDA; it reads the upstream checkout and weights from ACT3_OSS_HOME
// (~/act3_render/flashvsr, installed by the BMS child's recipes). The output is exactly --scale x the source, original
// fps and audio. Spec: open_source_upscaling.mdx §19-23.
const FlashVSRModel = "flashvsr-1.1"

const flashvsrWeight = "diffusion_pytorch_model_streaming_dmd.safetensors"

func flashvsrHome() string { return renderHome("flashvsr") }

func isFlashVSR(model string) bool { return model == FlashVSRModel || model == "flashvsr" }

// flashvsrInstallProblem checks the venv, the upstream checkout the port imports and the streaming weights.
func flashvsrInstallProblem() string {
	home := flashvsrHome()
	if !fileAtLeast(venvPython(home), 1) {
		return fmt.Sprintf("FlashVSR is not installed on this server: missing %s (install guide: ~/BGit/all/app/smartc/film/servers/bms/oss_video/flashvsr/)", venvPython(home))
	}
	if st, err := os.Stat(filepath.Join(home, "FlashVSR", "examples", "WanVSR")); err != nil || !st.IsDir() {
		return fmt.Sprintf("FlashVSR is not installed on this server: no OpenImagingLab/FlashVSR checkout at %s", filepath.Join(home, "FlashVSR"))
	}
	if w := filepath.Join(home, "models", "FlashVSR-v1.1", flashvsrWeight); !fileAtLeast(w, 100<<20) {
		return fmt.Sprintf("FlashVSR is not installed on this server: weight %s is missing or partial", w)
	}
	return ""
}

func flashvsrCheck(job *jobs.Job, p upscaleParams) error {
	if p.Factor < 0 || p.Factor > 8 {
		return jobs.Errf(jobs.KindInvalidInput, "FlashVSR scales 1..8x, not %g", p.Factor)
	}
	if p.Factor == 0 && (p.Width < 16 || p.Height < 16 || p.Width > 7680 || p.Height > 7680) {
		return jobs.Errf(jobs.KindInvalidInput, "an upscale needs factor, or a target width and height of 16..7680")
	}
	if !hasInput(job, "source_video") {
		return jobs.Errf(jobs.KindInvalidInput, "an upscale needs the input \"source_video\"")
	}
	return nil
}

// flashvsrScale: the job's factor, else its target width over the source's (read with ffprobe), else 4.
func flashvsrScale(p upscaleParams, ffprobe, src string) float64 {
	if p.Factor > 0 {
		return p.Factor
	}
	if ffprobe == "" {
		ffprobe = "ffprobe"
	}
	out, err := exec.Command(ffprobe, "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width",
		"-of", "csv=p=0", src).Output()
	if w, perr := strconv.Atoi(strings.TrimSpace(string(out))); err == nil && perr == nil && w > 0 && p.Width > 0 {
		return float64(p.Width) / float64(w)
	}
	return 4
}

func flashvsrTask(snap snapshot.Snapshot, env Env, scale float64, src, out string) detachedTask {
	home := flashvsrHome()
	argv := []string{
		venvPython(home), snap.Path("code/scripts/upscale/flashvsr/run_flashvsr.py"),
		"--input", src, "--output", out, "--scale", strconv.FormatFloat(scale, 'f', -1, 64),
	}
	vars := []string{"PYTHONUNBUFFERED=1", "ACT3_OSS_HOME=" + home}
	if ff := enginePath(env, "ffmpeg"); ff != "" {
		vars = append(vars, "ACT3_FFMPEG="+ff)
	}
	if fp := enginePath(env, "ffprobe"); fp != "" {
		vars = append(vars, "ACT3_FFPROBE="+fp)
	}
	return detachedTask{Argv: argv, Dir: home, Env: vars, Output: out, Cancel: ProfileFlashVSR}
}
