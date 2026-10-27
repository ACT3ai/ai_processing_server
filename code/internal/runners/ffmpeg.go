package runners

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/procs"
)

// FFmpeg is the runner behind act3.transcode, wtc.transcode (one input → H.264 + AAC + yuv420p MP4, the
// combination X and every social platform accept; never AV1) and act3.video_merge (inputs concatenated in order).
//
// params: {height, crf, preset, audio_bitrate}. Progress comes from `-progress pipe:1` against the ffprobe duration.
type FFmpeg struct{}

type ffmpegParams struct {
	Height       int    `json:"height"`
	CRF          int    `json:"crf"`
	Preset       string `json:"preset"`
	AudioBitrate string `json:"audio_bitrate"`
}

func (FFmpeg) Name() string { return "ffmpeg" }

func isMerge(job *jobs.Job) bool { return strings.HasSuffix(job.Type, ".video_merge") }

func (FFmpeg) Check(_ context.Context, job *jobs.Job, env Env) error {
	var p ffmpegParams
	if err := decodeParams(job, &p); err != nil {
		return err
	}
	if env.Engines["ffmpeg"] == nil || env.Engines["ffprobe"] == nil {
		return jobs.Errf(jobs.KindInfra, "ffmpeg and ffprobe are required and not installed on this server")
	}
	if p.Height != 0 && (p.Height < 16 || p.Height > 4320 || p.Height%2 != 0) {
		return jobs.Errf(jobs.KindInvalidInput, "height must be even, 16..4320")
	}
	if p.CRF != 0 && (p.CRF < 1 || p.CRF > 51) {
		return jobs.Errf(jobs.KindInvalidInput, "crf must be within 1..51")
	}
	if p.Preset != "" && !slices.Contains(strings.Fields("ultrafast superfast veryfast faster fast medium slow slower veryslow"), p.Preset) {
		return jobs.Errf(jobs.KindInvalidInput, "unknown x264 preset %q", p.Preset)
	}
	n := len(job.Payload.Inputs)
	if isMerge(job) && n < 2 {
		return jobs.Errf(jobs.KindInvalidInput, "video_merge needs at least two inputs, got %d", n)
	}
	if !isMerge(job) && n != 1 {
		return jobs.Errf(jobs.KindInvalidInput, "transcode needs exactly one input, got %d", n)
	}
	return nil
}

func (FFmpeg) Run(ctx context.Context, job *jobs.Job, env Env, rep Reporter) (*Result, error) {
	var p ffmpegParams
	if err := decodeParams(job, &p); err != nil {
		return nil, err
	}
	if p.CRF == 0 {
		p.CRF = 23
	}
	if p.Preset == "" {
		p.Preset = "veryfast"
	}
	if p.AudioBitrate == "" {
		p.AudioBitrate = "128k"
	}
	ff, fp := env.Engines["ffmpeg"].Path, env.Engines["ffprobe"].Path

	rep.Stage("probe")
	var total float64
	var inputs []string
	var probes []map[string]any
	for _, in := range job.Payload.Inputs {
		path := env.Inputs[in.Name]
		info, err := Probe(ctx, fp, path)
		if err != nil {
			return nil, jobs.Wrap(jobs.KindInvalidInput, err, "input "+in.Name+" is not a readable video")
		}
		total += info.Duration
		inputs = append(inputs, path)
		probes = append(probes, map[string]any{"name": in.Name, "duration_s": info.Duration, "width": info.Width, "height": info.Height, "codec": info.VideoCodec})
	}

	out := filepath.Join(env.OutDir, "video.mp4")
	var args []string
	if isMerge(job) {
		list := filepath.Join(env.WorkDir, "concat.txt")
		var b strings.Builder
		for _, in := range inputs {
			fmt.Fprintf(&b, "file '%s'\n", strings.ReplaceAll(in, "'", `'\''`))
		}
		if err := os.WriteFile(list, []byte(b.String()), 0o600); err != nil {
			return nil, jobs.Wrap(jobs.KindInfra, err, "write concat list")
		}
		args = []string{"-hide_banner", "-y", "-f", "concat", "-safe", "0", "-i", list}
	} else {
		args = []string{"-hide_banner", "-y", "-i", inputs[0]}
	}
	args = append(args, "-map", "0:v:0", "-map", "0:a?", "-c:v", "libx264", "-preset", p.Preset, "-crf", strconv.Itoa(p.CRF),
		"-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", p.AudioBitrate, "-movflags", "+faststart")
	if p.Height > 0 {
		args = append(args, "-vf", fmt.Sprintf("scale=-2:%d", p.Height))
	}
	args = append(args, "-progress", "pipe:1", "-nostats", out)

	rep.Stage("encode")
	var tail []string
	err := procs.Run(ctx, procs.Spec{
		Name: ff, Args: args, Dir: env.WorkDir, GPU: env.Slot.GPU,
		Stdout: func(line string) {
			if v, ok := strings.CutPrefix(line, "out_time_us="); ok && total > 0 {
				if us, err := strconv.ParseFloat(v, 64); err == nil && us > 0 {
					rep.Progress(min(0.99, us/1e6/total), fmt.Sprintf("encoded %.1fs of %.1fs", us/1e6, total))
				}
			}
		},
		Stderr: func(line string) { rep.Log("ffmpeg: " + line); tail = appendTail(tail, line, 10) },
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, jobs.Wrap(jobs.KindInfra, err, "ffmpeg failed: "+strings.Join(tail, " | "))
	}

	info, err := Probe(ctx, fp, out)
	if err != nil {
		return nil, jobs.Wrap(jobs.KindInternal, err, "ffmpeg wrote an unreadable output")
	}
	meta := map[string]any{"codec": info.VideoCodec, "pix_fmt": info.PixFmt, "width": info.Width, "height": info.Height,
		"duration_s": info.Duration, "audio_codec": info.AudioCodec}
	data := map[string]any{"op": map[bool]string{true: "merge", false: "transcode"}[isMerge(job)],
		"inputs": probes, "input_duration_s": total, "output": meta}
	res, err := writeJSONOutput(env, "result", data)
	if err != nil {
		return nil, err
	}
	return &Result{Outputs: []jobs.Output{{Name: "video", Path: out, ContentType: "video/mp4", Meta: meta}, res}, Data: data}, nil
}

// MediaInfo is what Probe reads with ffprobe.
type MediaInfo struct {
	Duration   float64
	Width      int
	Height     int
	VideoCodec string
	AudioCodec string
	PixFmt     string
}

// Probe runs ffprobe on a file.
func Probe(ctx context.Context, ffprobe, path string) (MediaInfo, error) {
	var out strings.Builder
	var errOut strings.Builder
	err := procs.Run(ctx, procs.Spec{
		Name:   ffprobe,
		Args:   []string{"-v", "error", "-show_entries", "format=duration:stream=codec_type,codec_name,width,height,pix_fmt", "-of", "json", path},
		Stdout: func(l string) { out.WriteString(l) },
		Stderr: func(l string) { errOut.WriteString(l + " ") },
	})
	if err != nil {
		return MediaInfo{}, fmt.Errorf("ffprobe: %v %s", err, errOut.String())
	}
	var doc struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
		Streams []struct {
			CodecType string `json:"codec_type"`
			CodecName string `json:"codec_name"`
			Width     int    `json:"width"`
			Height    int    `json:"height"`
			PixFmt    string `json:"pix_fmt"`
		} `json:"streams"`
	}
	if err := json.Unmarshal([]byte(out.String()), &doc); err != nil {
		return MediaInfo{}, fmt.Errorf("ffprobe output: %w", err)
	}
	var mi MediaInfo
	mi.Duration, _ = strconv.ParseFloat(doc.Format.Duration, 64)
	for _, s := range doc.Streams {
		switch s.CodecType {
		case "video":
			if mi.VideoCodec == "" {
				mi.VideoCodec, mi.Width, mi.Height, mi.PixFmt = s.CodecName, s.Width, s.Height, s.PixFmt
			}
		case "audio":
			if mi.AudioCodec == "" {
				mi.AudioCodec = s.CodecName
			}
		}
	}
	if mi.VideoCodec == "" {
		return mi, fmt.Errorf("no video stream")
	}
	return mi, nil
}
