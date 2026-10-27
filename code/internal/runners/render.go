package runners

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/procs"
)

// Render is the test render behind sys.render_test: a Mandelbrot zoom rendered frame by frame to PNG, then
// encoded to an H.264 / yuv420p MP4 when ffmpeg is present. It exercises the same path a real render takes
// (frames on disk, progress per frame, an external encoder, media outputs) with results the tests can recompute.
//
// Pixel (px, py) of frame f maps to
//
//	scale = 3.0 divided by zoom, f times
//	x0 = cx + (px − w/2)·scale/w,  y0 = cy + (py − h/2)·scale/w
//
// and iterates z = z² + c until |z|² > 4 or max_iter. Multiplications are wrapped in float64() so the compiler
// cannot fuse them into FMA instructions; the result is bit-identical to a plain IEEE-754 reimplementation.
type Render struct{}

type renderParams struct {
	Width       int     `json:"width"`
	Height      int     `json:"height"`
	Frames      int     `json:"frames"`
	MaxIter     int     `json:"max_iter"`
	CenterX     float64 `json:"center_x"`
	CenterY     float64 `json:"center_y"`
	Zoom        float64 `json:"zoom"`
	FPS         int     `json:"fps"`
	EncodeVideo *bool   `json:"encode_video"`
	ThrottleMs  int     `json:"throttle_ms"`
}

func (p *renderParams) defaults() {
	if p.Width == 0 {
		p.Width = 160
	}
	if p.Height == 0 {
		p.Height = 90
	}
	if p.Frames == 0 {
		p.Frames = 8
	}
	if p.MaxIter == 0 {
		p.MaxIter = 200
	}
	if p.CenterX == 0 && p.CenterY == 0 {
		p.CenterX, p.CenterY = -0.743643887037151, 0.131825904205330
	}
	if p.Zoom == 0 {
		p.Zoom = 1.5
	}
	if p.FPS == 0 {
		p.FPS = 12
	}
}

func (Render) Name() string { return "render" }

func (Render) Check(_ context.Context, job *jobs.Job, env Env) error {
	var p renderParams
	if err := decodeParams(job, &p); err != nil {
		return err
	}
	p.defaults()
	switch {
	case p.Width < 16 || p.Width > 4096 || p.Width%2 != 0:
		return jobs.Errf(jobs.KindInvalidInput, "width must be even, 16..4096")
	case p.Height < 16 || p.Height > 4096 || p.Height%2 != 0:
		return jobs.Errf(jobs.KindInvalidInput, "height must be even, 16..4096")
	case p.Frames < 1 || p.Frames > 2000:
		return jobs.Errf(jobs.KindInvalidInput, "frames must be within 1..2000")
	case p.MaxIter < 1 || p.MaxIter > 100000:
		return jobs.Errf(jobs.KindInvalidInput, "max_iter must be within 1..100000")
	case p.Zoom <= 0:
		return jobs.Errf(jobs.KindInvalidInput, "zoom must be positive")
	case p.EncodeVideo != nil && *p.EncodeVideo && env.Engines["ffmpeg"] == nil:
		return jobs.Errf(jobs.KindInfra, "encode_video needs ffmpeg, which is not installed on this server")
	}
	return nil
}

type frameStat struct {
	Index     int    `json:"index"`
	IterSum   int64  `json:"iter_sum"`
	Inside    int    `json:"inside"`
	PNGSHA256 string `json:"png_sha256"`
}

func (r Render) Run(ctx context.Context, job *jobs.Job, env Env, rep Reporter) (*Result, error) {
	var p renderParams
	if err := decodeParams(job, &p); err != nil {
		return nil, err
	}
	p.defaults()
	encode := env.Engines["ffmpeg"] != nil
	if p.EncodeVideo != nil {
		encode = *p.EncodeVideo
	}
	framesDir := filepath.Join(env.WorkDir, "frames")
	if err := os.MkdirAll(framesDir, 0o700); err != nil {
		return nil, jobs.Wrap(jobs.KindInfra, err, "create frames dir")
	}
	renderShare := 1.0
	if encode {
		renderShare = 0.8
	}

	rep.Stage("render")
	var stats []frameStat
	var total int64
	scale := 3.0
	throttle := time.Duration(p.ThrottleMs) * time.Millisecond
	for f := 0; f < p.Frames; f++ {
		if f > 0 {
			scale = scale / p.Zoom
		}
		img, st := mandelbrotFrame(p.Width, p.Height, p.MaxIter, p.CenterX, p.CenterY, scale)
		st.Index = f
		path := filepath.Join(framesDir, fmt.Sprintf("frame_%04d.png", f))
		sum, err := writePNG(path, img)
		if err != nil {
			return nil, jobs.Wrap(jobs.KindInfra, err, "write frame")
		}
		st.PNGSHA256 = sum
		stats = append(stats, st)
		total += st.IterSum
		if err := step(ctx, rep, renderShare*float64(f+1)/float64(p.Frames), fmt.Sprintf("frame %d/%d rendered", f+1, p.Frames), throttle); err != nil {
			return nil, err
		}
	}

	first := filepath.Join(env.OutDir, "first_frame.png")
	last := filepath.Join(env.OutDir, "last_frame.png")
	copyBytes(filepath.Join(framesDir, "frame_0000.png"), first)
	copyBytes(filepath.Join(framesDir, fmt.Sprintf("frame_%04d.png", p.Frames-1)), last)
	outputs := []jobs.Output{
		{Name: "first_frame", Path: first, ContentType: "image/png"},
		{Name: "last_frame", Path: last, ContentType: "image/png"},
	}

	data := map[string]any{
		"op": "mandelbrot", "width": p.Width, "height": p.Height, "frames": p.Frames, "max_iter": p.MaxIter,
		"center_x": p.CenterX, "center_y": p.CenterY, "zoom": p.Zoom, "total_iterations": total, "frame_stats": stats,
		"video_encoded": false,
	}
	if encode {
		rep.Stage("encode")
		video := filepath.Join(env.OutDir, "render.mp4")
		ff := env.Engines["ffmpeg"].Path
		var lastLines []string
		err := procs.Run(ctx, procs.Spec{
			Name: ff,
			Args: []string{"-hide_banner", "-y", "-framerate", strconv.Itoa(p.FPS), "-i", filepath.Join(framesDir, "frame_%04d.png"),
				"-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p", "-movflags", "+faststart",
				"-progress", "pipe:1", "-nostats", video},
			GPU: env.Slot.GPU,
			Stdout: func(line string) {
				if v, ok := strings.CutPrefix(line, "frame="); ok {
					if n, err := strconv.Atoi(v); err == nil {
						rep.Progress(renderShare+(1-renderShare)*float64(n)/float64(p.Frames), fmt.Sprintf("encoded %d/%d frames", n, p.Frames))
					}
				}
			},
			Stderr: func(line string) { rep.Log("ffmpeg: " + line); lastLines = appendTail(lastLines, line, 10) },
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil, context.Cause(ctx)
			}
			return nil, jobs.Wrap(jobs.KindInfra, err, "ffmpeg encode failed: "+strings.Join(lastLines, " | "))
		}
		outputs = append(outputs, jobs.Output{Name: "video", Path: video, ContentType: "video/mp4",
			Meta: map[string]any{"codec": "h264", "pix_fmt": "yuv420p", "fps": p.FPS, "frames": p.Frames}})
		data["video_encoded"] = true
		data["fps"] = p.FPS
	}
	res, err := writeJSONOutput(env, "result", data)
	if err != nil {
		return nil, err
	}
	return &Result{Outputs: append(outputs, res), Data: data}, nil
}

// mandelbrotFrame renders one frame and returns its statistics. See the Render comment for the exact formula.
func mandelbrotFrame(w, h, maxIter int, cx, cy, scale float64) (*image.RGBA, frameStat) {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	var st frameStat
	fw, fh := float64(w), float64(h)
	for py := 0; py < h; py++ {
		y0 := cy + float64(float64(float64(py)-fh/2)*scale)/fw
		for px := 0; px < w; px++ {
			x0 := cx + float64(float64(float64(px)-fw/2)*scale)/fw
			zr, zi := 0.0, 0.0
			i := 0
			for ; i < maxIter; i++ {
				zr2 := float64(zr * zr)
				zi2 := float64(zi * zi)
				if zr2+zi2 > 4 {
					break
				}
				zi = float64(2*zr*zi) + y0
				zr = zr2 - zi2 + x0
			}
			st.IterSum += int64(i)
			if i == maxIter {
				st.Inside++
				img.Set(px, py, color.RGBA{0, 0, 0, 255})
				continue
			}
			t := float64(i) / float64(maxIter)
			img.Set(px, py, color.RGBA{
				uint8(9 * (1 - t) * t * t * t * 255),
				uint8(15 * (1 - t) * (1 - t) * t * t * 255),
				uint8(8.5 * (1 - t) * (1 - t) * (1 - t) * t * 255), 255})
		}
	}
	return img, st
}

func writePNG(path string, img image.Image) (string, error) {
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	if err := png.Encode(&teeWriter{f, h}, img); err != nil {
		f.Close()
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), f.Close()
}

type teeWriter struct {
	a, b interface{ Write([]byte) (int, error) }
}

func (t *teeWriter) Write(p []byte) (int, error) {
	t.b.Write(p)
	return t.a.Write(p)
}

func copyBytes(src, dst string) error {
	b, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, b, 0o600)
}

func appendTail(lines []string, l string, n int) []string {
	lines = append(lines, l)
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines
}
