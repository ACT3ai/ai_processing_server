package runners

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/procs"
	"github.com/ACT3ai/ai_processing_server/internal/taskhost"
)

// Segmented upscale (pm/task_authoring.mdx R8, checkpoints): a long source is cut into segments, each segment is
// upscaled as its own task-host step, and the parts are joined at the end. Every finished step is a checkpoint, so a
// reboot or a crashed task host 25 minutes into a 30-minute upscale resumes at the segment it was on instead of
// starting over (taskhost.Relaunch, runDetached → resume).
//
// Segment length is a whole number of SeedVR2 batches, so the model sees exactly the frame groups it would see on
// the uncut video and the joins add no seam of their own. The cut is frame-accurate (input seek, re-encode at
// near-lossless CRF); the join is a stream copy.

// DefaultSegmentSeconds: about this much source video per checkpoint at a 1080p output. A segment re-loads the model
// (tens of seconds), so segments are kept to several minutes of render work each. $APS_UPSCALE_SEGMENT_SECONDS
// overrides. Larger outputs get proportionally shorter segments (segmentSeconds).
const DefaultSegmentSeconds = 30

// segmentPixels: a segment gets the output-pixel budget of DefaultSegmentSeconds at 1080p. The whole segment is
// assembled in RAM: a 30 s clip upscaled to 4K as one segment ran the 5090 out of memory (2026-11-07).
const segmentPixels = 1920 * 1080

// videoFrames is what segmenting needs from the source.
type videoFrames struct {
	FPS    float64
	Frames int
}

// probeFrames reads the frame rate and frame count of the first video stream.
func probeFrames(ffprobe, path string) (videoFrames, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var out, errOut strings.Builder
	err := procs.Run(ctx, procs.Spec{Name: ffprobe,
		Args: []string{"-v", "error", "-select_streams", "v:0", "-show_entries",
			"stream=r_frame_rate,avg_frame_rate,nb_frames,duration:format=duration", "-of", "json", path},
		Stdout: func(l string) { out.WriteString(l) }, Stderr: func(l string) { errOut.WriteString(l + " ") }})
	if err != nil {
		return videoFrames{}, fmt.Errorf("ffprobe: %v %s", err, errOut.String())
	}
	var doc struct {
		Streams []struct {
			RFrameRate   string `json:"r_frame_rate"`
			AvgFrameRate string `json:"avg_frame_rate"`
			NbFrames     string `json:"nb_frames"`
			Duration     string `json:"duration"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal([]byte(out.String()), &doc); err != nil || len(doc.Streams) == 0 {
		return videoFrames{}, fmt.Errorf("ffprobe found no video stream in %s", path)
	}
	st := doc.Streams[0]
	fps := ratio(st.AvgFrameRate)
	if fps <= 0 {
		fps = ratio(st.RFrameRate)
	}
	n, _ := strconv.Atoi(st.NbFrames)
	if n <= 0 {
		d, _ := strconv.ParseFloat(st.Duration, 64)
		if d <= 0 {
			d, _ = strconv.ParseFloat(doc.Format.Duration, 64)
		}
		n = int(math.Round(d * fps))
	}
	if fps <= 0 || n <= 0 {
		return videoFrames{}, fmt.Errorf("ffprobe could not read the frame rate and frame count of %s", path)
	}
	return videoFrames{FPS: fps, Frames: n}, nil
}

func ratio(s string) float64 {
	a, b, ok := strings.Cut(s, "/")
	x, _ := strconv.ParseFloat(a, 64)
	if !ok {
		return x
	}
	y, _ := strconv.ParseFloat(b, 64)
	if y == 0 {
		return 0
	}
	return x / y
}

// segmentSeconds is the job's segment_seconds, else $APS_UPSCALE_SEGMENT_SECONDS, else the default; the last two are
// scaled down for an output (width x height) above 1080p. <= 0 turns segmenting off.
func segmentSeconds(param *float64, width, height int) float64 {
	if param != nil {
		return *param
	}
	sec := float64(DefaultSegmentSeconds)
	if v, err := strconv.ParseFloat(os.Getenv("APS_UPSCALE_SEGMENT_SECONDS"), 64); err == nil {
		sec = v
	}
	if px := float64(width) * float64(height); px > segmentPixels {
		sec *= segmentPixels / px
	}
	return sec
}

// planSegments splits frames into [start, end) ranges of segFrames (rounded up to whole batches). A tail shorter
// than half a segment joins the one before it. One range means "do not segment".
func planSegments(frames, segFrames, batch int) [][2]int {
	batch = max(batch, 1)
	segFrames = (max(segFrames, 1) + batch - 1) / batch * batch
	if frames <= segFrames*3/2 {
		return [][2]int{{0, frames}}
	}
	var out [][2]int
	for s := 0; s < frames; s += segFrames {
		out = append(out, [2]int{s, min(s+segFrames, frames)})
	}
	if last := out[len(out)-1]; len(out) > 1 && last[1]-last[0] < segFrames/2 {
		out = out[:len(out)-1]
		out[len(out)-1][1] = frames
	}
	return out
}

// upscaleOne builds the model step for one source file: it writes out.
type upscaleOne func(src, out string) detachedTask

// segmentedSteps turns one upscale into cut → upscale per segment, then a join that writes out. It returns nil
// steps when the source is short enough to run as one step (or cannot be probed): the caller then runs it whole.
// frames is the source's frame count when it was probed (the joined output must have as many).
func segmentedSteps(ffmpeg, ffprobe, src, out string, segSeconds float64, batch int, one upscaleOne) (steps []taskhost.Step, frames int, err error) {
	if ffmpeg == "" || ffprobe == "" || segSeconds <= 0 {
		return nil, 0, nil
	}
	vf, err := probeFrames(ffprobe, src)
	if err != nil {
		return nil, 0, err
	}
	plan := planSegments(vf.Frames, int(math.Round(segSeconds*vf.FPS)), batch)
	if len(plan) < 2 {
		return nil, vf.Frames, nil
	}
	dir := filepath.Join(filepath.Dir(out), "segments")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, 0, err
	}
	var list strings.Builder
	for i, r := range plan {
		cut := filepath.Join(dir, fmt.Sprintf("src_%03d.mp4", i))
		up := filepath.Join(dir, fmt.Sprintf("up_%03d.mp4", i))
		// Seek a quarter frame early so rounding never drops the segment's first frame; -frames:v stops it exactly.
		ss := max(0, (float64(r[0])-0.25)/vf.FPS)
		steps = append(steps, taskhost.Step{Name: fmt.Sprintf("cut %d/%d (frames %d-%d)", i+1, len(plan), r[0], r[1]-1),
			Argv: []string{ffmpeg, "-y", "-v", "error", "-ss", strconv.FormatFloat(ss, 'f', 6, 64), "-i", src,
				"-frames:v", strconv.Itoa(r[1] - r[0]), "-an", "-c:v", "libx264", "-crf", "8", "-preset", "veryfast",
				"-pix_fmt", "yuv420p", cut},
			Output: cut, Weight: 0.03, GraceSeconds: ProfileFFmpeg.Grace.Seconds()})
		t := one(cut, up)
		steps = append(steps, taskhost.Step{Name: fmt.Sprintf("upscale %d/%d", i+1, len(plan)), Argv: t.Argv, Dir: t.Dir,
			Env: t.Env, Phases: t.Phases, Output: up, Weight: float64(r[1]-r[0]) / float64(vf.Frames) * float64(len(plan))})
		fmt.Fprintf(&list, "file '%s'\n", strings.ReplaceAll(up, "'", `'\''`))
	}
	listPath := filepath.Join(dir, "join.txt")
	if err := os.WriteFile(listPath, []byte(list.String()), 0o600); err != nil {
		return nil, 0, err
	}
	steps = append(steps, taskhost.Step{Name: "join", Output: out, Weight: 0.03, GraceSeconds: ProfileFFmpeg.Grace.Seconds(),
		Argv: []string{ffmpeg, "-y", "-v", "error", "-f", "concat", "-safe", "0", "-i", listPath, "-c", "copy",
			"-movflags", "+faststart", out}})
	return steps, vf.Frames, nil
}
