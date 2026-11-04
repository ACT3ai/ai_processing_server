package runners

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/procs"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
	"github.com/ACT3ai/ai_processing_server/internal/taskhost"
)

// OSSRegenerate is the runner behind act3.video_regenerate_open_source: NVIDIA Open-H3-Regenerate-2K, the open 2K / 4K
// refiner on the MiniMax H3 transformer. Input "draft_video" (a low-resolution take from any model); it REGENERATES the
// take at 768p / 1080p / 2k / 4k and re-muxes the draft's own audio. CUDA only (B5090): there is no Metal port.
//
// The task runs code/scripts/video_gen/h3_regenerate/render_h3_regenerate.py from the job's frozen snapshot (R1, R2) as
// TWO checkpointed steps (R8): "refine" runs the ComfyUI port (the 5090's first path, §0 verdict 2, §9.2) or NVIDIA's
// infer.py, and writes a silent refined video; "finish" trims the frame-grid pad and re-muxes the draft audio. A reboot
// after the 3–10 minute refine resumes at "finish" instead of refining again. The script also skips a refine whose
// output and settings stamp are already there, so a retry on the same machine is idempotent (R11).
//
// The planning rules are a Go port of the ACT3 builder, which stays the reference:
// ~/BGit/all/film/code/subp/video_gen_ai/nvidia/h3_regenerate_2k/h3-regenerate-job.ts buildRegenerateJob. The two are
// held together by one fixture table both test suites read (__tests__/fixtures/regenerate_parity.json there).
// Model spec: ~/BGit/all/film/pm/subp/video_gen_ai/nvidia/h3_regenerate_2k/h3_regenerate_2k.mdx (§ numbers below).
//
// params (every field optional, any JSON type — a drifted type is normalized or warned about, never a rejected job):
// {model, target|resolution, preset: fidelity|detail, start_sigma, steps, seed, input_size, backend: auto|cli|comfyui,
// lora, prompt (already converted to H3 T2VA by ACT3), draft_source | draft: {source}, shot: {has_cast, has_dialogue,
// has_animal, close_up}, allow_unmeasured, allow_truncate, allow_crop, allow_silence, fallback: downgrade|refuse,
// delivery: internal|customer, ladder}. The internal web app's camelCase spellings are accepted too.
type OSSRegenerate struct{}

// RegenerateModel is the model id this runner renders.
const RegenerateModel = "open-h3-regenerate-2k"

// H3RegenerateLicenseCleared is Legal's switch (h3_regenerate_2k.mdx §12): the MiniMax H3 Community License excludes the
// United States from its free grant, so output is internal testing only until Legal clears it. It is a constant, not an
// env var or a param, on purpose: opening it is a reviewed commit, never a setting someone flips on one machine.
const H3RegenerateLicenseCleared = false

const (
	h3rFPS             = 24         // §3.1 the refiner resamples to 24 fps
	h3rCLIWindow       = 121        // §3.1 the CLI cuts or pads --input-video to 121 frames (5.04 s)
	h3rMaxFrames       = 360        // §3.2 15 s in one pass
	h3rMinFrames       = 12         // spec gap: no documented minimum; half a second
	h3rMinShortSide    = 256        // spec gap: no floor; 384P is the documented low end (§3.1)
	h3rMaxSilentCrop   = 0.25       // ACT3 rule: refuse a cover-crop that removes more than this, unless allow_crop
	h3rMaxStartSigma   = 0.909375   // §2.4 trained from σ 0.91 down; the default grid start
	h3rMinStartSigma   = 0.3        // spec gap: no floor
	h3rDefaultSeed     = 1766580932 // §9.2 defaults
	h3rSolEngineVRAMGB = 140.0      // §5.3 sol_engine_fwd + CUDA graphs ≈ 150 GB
	h3rMinVRAMGB       = 24.0       // spec gap: the smallest card shown refining is 32 GB (§7.4)
	// h3rCardClassGB: the "32 GB card" class (§7.4). nvidia-smi reports 32607 MiB = 31.8 GB on an RTX 5090, so a strict
	// 32 refused every job on the card the spec measured. 30 = the service's min_vram_gb.
	h3rCardClassGB  = 30.0
	h3rRecommendRAM = 128.0 // §7.3 item 7: 128 GB+ system RAM for block swap
	h3rMinRAM       = 64.0  // ACT3 rule (same as the TS builder): under this the block-swapped base cannot be held
	h3rPromptMax    = 40000 // MiniMax's API cap (§3.3); no documented cap for the open refiner
)

// h3rTargets: §3.2 output rungs; renderH is the rows the model renders before the 1088→1080 / 2176→2160 crop.
var h3rTargets = map[string]struct{ w, h, renderH, order int }{
	"768p": {1344, 768, 768, 0}, "1080p": {1920, 1080, 1088, 1}, "2k": {2560, 1440, 1440, 2}, "4k": {3840, 2160, 2176, 3},
}
var h3rTargetOrder = []string{"768p", "1080p", "2k", "4k"}

// h3rPresets: §2.5. Fidelity = 2 steps from 0.7, draft audio held; Detail = NVIDIA default, 3 steps from 0.909375.
var h3rPresets = map[string]struct {
	steps int
	sigma float64
	audio string
}{"fidelity": {2, 0.7, "draft"}, "detail": {3, 0.909375, "silence"}}

// regenShot: tri-state shot facts, any JSON type (true, "yes", 1 …); nil = unknown.
type regenShot struct {
	HasCast, HasDialogue, HasAnimal, CloseUp any
}

// regenParams: every field is `any` so one drifted type ("input_size": 576, "lora": 128, "has_cast": "yes") is
// normalized instead of failing json.Unmarshal for the whole job.
type regenParams struct {
	Model, Target, Preset, StartSigma, Steps, Seed, InputSize, Backend, Lora, Prompt, DraftSource any
	Shot                                                                                          regenShot
	AllowUnmeasured, AllowTruncate, AllowCrop, AllowSilence, Fallback, Delivery, Ladder           any
	LicenseCleared                                                                                any // read only to warn: §12
}

// pickKey: the first present, non-null value among the spellings of one field.
func pickKey(m map[string]any, names ...string) any {
	for _, n := range names {
		if v, ok := m[n]; ok && v != nil {
			return v
		}
	}
	return nil
}

// parseRegenParams decodes the job's params tolerantly. Only a body that is not a JSON object is an error.
func parseRegenParams(raw []byte) (regenParams, error) {
	var p regenParams
	if len(strings.TrimSpace(string(raw))) == 0 {
		return p, nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return p, jobs.Wrap(jobs.KindInvalidInput, err, "params are not a JSON object")
	}
	if m == nil {
		return p, nil
	}
	p.Model = pickKey(m, "model")
	p.Target = pickKey(m, "target", "resolution")
	p.Preset = pickKey(m, "preset")
	p.StartSigma = pickKey(m, "start_sigma", "startSigma", "sigma")
	p.Steps = pickKey(m, "steps")
	p.Seed = pickKey(m, "seed")
	p.InputSize = pickKey(m, "input_size", "inputSize")
	p.Backend = pickKey(m, "backend")
	p.Lora = pickKey(m, "lora")
	p.Prompt = pickKey(m, "prompt")
	p.DraftSource = pickKey(m, "draft_source", "draftSource")
	if d, ok := m["draft"].(map[string]any); ok && p.DraftSource == nil { // the TS request nests it: draft.source
		p.DraftSource = pickKey(d, "source", "model", "renderer")
	}
	if p.DraftSource == nil {
		p.DraftSource = pickKey(m, "source")
	}
	if s, ok := m["shot"].(map[string]any); ok {
		p.Shot = regenShot{
			HasCast: pickKey(s, "has_cast", "hasCast", "cast"), HasDialogue: pickKey(s, "has_dialogue", "hasDialogue", "dialogue"),
			HasAnimal: pickKey(s, "has_animal", "hasAnimal", "animal"), CloseUp: pickKey(s, "close_up", "closeUp"),
		}
	}
	p.AllowUnmeasured = pickKey(m, "allow_unmeasured", "allowUnmeasured")
	p.AllowTruncate = pickKey(m, "allow_truncate", "allowTruncate")
	p.AllowCrop = pickKey(m, "allow_crop", "allowCrop")
	p.AllowSilence = pickKey(m, "allow_silence", "allowSilence")
	p.Fallback = pickKey(m, "fallback")
	p.Delivery = pickKey(m, "delivery")
	p.Ladder = pickKey(m, "ladder")
	p.LicenseCleared = pickKey(m, "license_cleared", "licenseCleared")
	return p, nil
}

func (OSSRegenerate) Name() string       { return "oss_regenerate" }
func (OSSRegenerate) Reattachable() bool { return true }

func regenHome() string { return renderHome("open_h3_regenerate_2k") }

/* ── small normalizers: never fail on drift, return ok=false on garbage ───────────────────────────────────────────── */

var reSquash = regexp.MustCompile(`[\s_\-.]+`)

func squash(v any) string {
	switch t := v.(type) {
	case string:
		return reSquash.ReplaceAllString(strings.ToLower(strings.TrimSpace(t)), "")
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case json.Number:
		return t.String()
	case bool:
		return strconv.FormatBool(t)
	}
	return ""
}

func textOf(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64, json.Number, bool:
		return squash(t)
	}
	return ""
}

func numberOf(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, !math.IsNaN(t) && !math.IsInf(t, 0)
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case int:
		return float64(t), true
	case string:
		s := strings.ReplaceAll(strings.TrimSpace(t), ",", "")
		f, err := strconv.ParseFloat(s, 64)
		return f, err == nil && !math.IsNaN(f) && !math.IsInf(f, 0)
	}
	return 0, false
}

// boolTri: a boolean from any spelling; known=false when the value says nothing (nil, "", "maybe", an object).
func boolTri(v any) (val, known bool) {
	switch t := v.(type) {
	case bool:
		return t, true
	case float64:
		return t != 0, true
	case int:
		return t != 0, true
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "true", "yes", "y", "1", "on":
			return true, true
		case "false", "no", "n", "0", "off":
			return false, true
		}
	}
	return false, false
}

func boolOf(v any) bool { b, ok := boolTri(v); return ok && b }

// normalizeTarget: "2K", " 1440p ", "2560x1440", "UHD", 768 … → a rung id; "" when unknown.
func normalizeTarget(v any) string {
	s := strings.NewReplacer("×", "x", "*", "x", "２", "2", "Ｋ", "k", "ｋ", "k").Replace(squash(v))
	switch s {
	case "768p", "768", "1344x768", "768x1344":
		return "768p"
	case "1080p", "1080", "fhd", "fullhd", "1920x1080", "1080x1920":
		return "1080p"
	case "2k", "1440p", "1440", "qhd", "2560x1440", "1440x2560", "2kqhd":
		return "2k"
	case "4k", "2160p", "2160", "uhd", "3840x2160", "2160x3840", "4kuhd":
		return "4k"
	}
	return ""
}

func normalizePreset(v any) string {
	switch squash(v) {
	case "fidelity", "tuned", "faithful", "identity", "cast":
		return "fidelity"
	case "detail", "default", "nvidia", "nvidiadefault", "texture":
		return "detail"
	}
	return ""
}

var reH3 = regexp.MustCompile(`(^|minimax|fast|open)h3|h3(omni|base|hyperflow|fl2va|ref2va|t2va|i2va|l2va)|^h3`)
var reHailuoKey = regexp.MustCompile(`^minimax(video01|video|i2v|t2v|01)`)

// isH3Source: the MiniMax H3 transformer drew the draft (H3, FastH3 Omni, HyperFlow). Hailuo is not H3. A bare
// "minimax" is ambiguous — production's renderer key MINIMAX is Hailuo Video-01 — so it is unknown (§3.1 heuristic).
func isH3Source(v any) (h3, known bool) {
	q := squash(v)
	switch {
	case q == "" || q == "minimax" || q == "minimaxai" || q == "unknown" || q == "auto":
		return false, false
	case strings.Contains(q, "hailuo") || reHailuoKey.MatchString(q):
		return false, true
	}
	return reH3.MatchString(q), true
}

var reLabel = regexp.MustCompile(`(?i)<\s*(Subject|Picture|Video|Audio)\s*\d+\s*>`)
var reDialogue = regexp.MustCompile(`(?is)<d>.*?</d>|\(S\d+\)`)

/* ── the draft as the server sees it ──────────────────────────────────────────────────────────────────────────────── */

type draftInfo struct {
	Width, Height int
	FPS, Seconds  float64
	HasAudio      bool
}

// probeDraft reads size, rate, duration and audio from the file itself (R13: inputs are untrusted).
func probeDraft(ffprobe, path string) (draftInfo, error) {
	if ffprobe == "" {
		ffprobe = "ffprobe"
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var out, errOut strings.Builder
	err := procs.Run(ctx, procs.Spec{Name: ffprobe,
		Args: []string{"-v", "error", "-show_entries",
			"stream=codec_type,width,height,avg_frame_rate,r_frame_rate,duration:format=duration", "-of", "json", path},
		Stdout: func(l string) { out.WriteString(l) }, Stderr: func(l string) { errOut.WriteString(l + " ") }})
	if err != nil {
		return draftInfo{}, fmt.Errorf("the draft is not a readable video: %v %s", err, strings.TrimSpace(errOut.String()))
	}
	var doc struct {
		Streams []struct {
			CodecType    string `json:"codec_type"`
			Width        int    `json:"width"`
			Height       int    `json:"height"`
			AvgFrameRate string `json:"avg_frame_rate"`
			RFrameRate   string `json:"r_frame_rate"`
			Duration     string `json:"duration"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal([]byte(out.String()), &doc); err != nil {
		return draftInfo{}, fmt.Errorf("ffprobe output unreadable: %v", err)
	}
	var d draftInfo
	video := false
	for _, s := range doc.Streams {
		switch s.CodecType {
		case "video":
			if video {
				continue
			}
			video = true
			d.Width, d.Height = s.Width, s.Height
			d.FPS = ratio(s.AvgFrameRate)
			if d.FPS <= 0 {
				d.FPS = ratio(s.RFrameRate)
			}
			d.Seconds, _ = strconv.ParseFloat(s.Duration, 64)
		case "audio":
			d.HasAudio = true
		}
	}
	if !video {
		return draftInfo{}, fmt.Errorf("the draft has no video stream")
	}
	if d.Seconds <= 0 {
		d.Seconds, _ = strconv.ParseFloat(doc.Format.Duration, 64)
	}
	return d, nil
}

/* ── the plan (port of h3-regenerate-job.ts buildRegenerateJob) ───────────────────────────────────────────────────── */

// regenHW is what the plan knows about this machine: card memory, system RAM (0 = unknown) and which backends are
// installed (nil = not checked).
type regenHW struct {
	VRAMGB, RAMGB float64
	CLI, Comfy    *bool
}

type regenPlan struct {
	Backend       string   `json:"backend"`
	Target        string   `json:"target"`
	Preset        string   `json:"preset"`
	Steps         int      `json:"steps"`
	StartSigma    float64  `json:"start_sigma"`
	InputSize     string   `json:"input_size"`
	WindowFrames  int      `json:"window_frames"`
	OutputSeconds float64  `json:"output_seconds"`
	Fwd           string   `json:"fwd"`
	VAE           string   `json:"vae"`
	ModelAudio    string   `json:"model_audio"`
	Remux         string   `json:"remux"`
	Lora          string   `json:"lora"`
	Seed          uint32   `json:"seed"`
	Ladder        bool     `json:"ladder"`
	Delivery      string   `json:"delivery"`
	AllowCrop     bool     `json:"allow_crop,omitempty"`
	AllowTruncate bool     `json:"allow_truncate,omitempty"`
	Envelope      string   `json:"envelope"`
	VRAMGB        float64  `json:"vram_gb,omitempty"`
	Warnings      []string `json:"warnings,omitempty"`
}

// h3GridFrames: H3's local frame grid, num_frames snapped UP to 17n+5, at least 124 (spec_minimax.mdx §3).
func h3GridFrames(frames int) int {
	n := max(7, int(math.Ceil(float64(max(1, frames)-5)/17)))
	return 17*n + 5
}

// latentTokens: (W/16)·(renderH/16)·latent frames, 37 latent frames per 124-frame grid (§5.3: 2K/5 s ≈ 533k).
func latentTokens(target string, grid int) float64 {
	t := h3rTargets[target]
	lf := math.Ceil(37 * float64(grid) / 124)
	return float64(t.w/16) * float64(t.renderH/16) * lf
}

// fastDecoderPeakGB: §5.3 ≈ 6.5 GB + 26 KB per latent token.
func fastDecoderPeakGB(tokens float64) float64 { return 6.5 + tokens*26000/1e9 }

// measured: the envelope keyed by backend (§7.4, §7.5). Every measurement under 140 GB is a ComfyUI run, for the 2K/5 s
// working set and smaller; the CLI's dense forward wants ~71 GB at 2K with no offload flags (§5.3, §9.1), so the CLI
// under 140 GB stays unmeasured until §7.5 says otherwise. The 32 GB card is a class (h3rCardClassGB).
func measured(vram, tokens float64, backend string) bool {
	switch {
	case vram <= 0 || math.IsNaN(vram):
		return false
	case vram >= h3rSolEngineVRAMGB:
		return true
	case backend == "cli":
		return false
	case vram < h3rCardClassGB:
		return false
	}
	return tokens <= latentTokens("2k", 124)
}

// planErr: invalid_input with the same error code the TS builder uses ("code: sentence").
func planErr(code, format string, a ...any) error {
	return jobs.Errf(jobs.KindInvalidInput, "%s: %s", code, fmt.Sprintf(format, a...))
}

func gb(v float64) string { return strconv.FormatFloat(math.Round(v*10)/10, 'f', -1, 64) + " GB" }

// planRegenerate turns params + the probed draft + this machine into one plan, or an error that says why no honest
// plan exists. Warnings are carried in the plan (the job log shows them).
func planRegenerate(p regenParams, d draftInfo, hw regenHW) (regenPlan, error) {
	vramGB := hw.VRAMGB
	pl := regenPlan{Seed: h3rDefaultSeed, Lora: "r576", VRAMGB: vramGB}
	warn := func(f string, a ...any) { pl.Warnings = append(pl.Warnings, fmt.Sprintf(f, a...)) }

	// §3.1 draft checks, on the probed file
	if d.Width <= 0 || d.Height <= 0 {
		return pl, planErr("draft_size_unknown", "the draft's frame size could not be read")
	}
	short := min(d.Width, d.Height)
	h3, known := isH3Source(p.DraftSource)
	if !known {
		h3 = d.Width == 1344 && d.Height == 768 && (d.FPS <= 0 || math.Abs(d.FPS-24) < 0.05)
		warn("the draft's model is not named (or ambiguous: %q); treated as %s", textOf(p.DraftSource), map[bool]string{true: "an H3 draft (1344x768 @ 24 fps)", false: "a non-H3 draft"}[h3])
	}
	switch squash(p.InputSize) {
	case "768p", "768", "1344x768":
		pl.InputSize = "768p"
	case "576p", "576", "1024x576":
		pl.InputSize = "576p"
	default:
		if squash(p.InputSize) != "" {
			warn("input size %v is not 768p or 576p; using the rule", p.InputSize)
		}
		pl.InputSize = map[bool]string{true: "768p", false: "576p"}[h3] // §3.1 576p for non-H3 drafts
	}
	if pl.InputSize == "768p" && known && !h3 {
		warn("the spec says to encode non-H3 drafts at 576p")
	}
	gw, gh := 1344.0, 768.0
	if pl.InputSize == "576p" {
		gw, gh = 1024, 576
	}
	pl.AllowCrop = boolOf(p.AllowCrop)
	da, ga := float64(d.Width)/float64(d.Height), gw/gh
	crop := 1 - math.Min(da, ga)/math.Max(da, ga)
	if d.Height > d.Width {
		// spec gap: no portrait rule — refused unless the caller accepts the crop; the script gets --allow-crop then
		if !pl.AllowCrop {
			return pl, planErr("draft_portrait", "the draft is portrait (%dx%d); the refiner's grid is landscape (%.0fx%.0f) and a cover-crop removes %.0f%% of every frame (set allow_crop to accept)", d.Width, d.Height, gw, gh, crop*100)
		}
		warn("the draft is portrait (%dx%d): a landscape band is refined, %.0f%% of each frame is cropped (allow_crop)", d.Width, d.Height, crop*100)
	} else if crop > h3rMaxSilentCrop+1e-9 && !pl.AllowCrop {
		return pl, planErr("draft_aspect", "the draft's aspect %.3f differs from the refine grid %.3f: a cover-crop would remove %.0f%% of the frame (set allow_crop to accept)", da, ga, crop*100)
	} else if crop > 0.02 {
		warn("cover-crop to %.0fx%.0f removes %.0f%% of the frame", gw, gh, crop*100)
	}
	if short < h3rMinShortSide {
		return pl, planErr("draft_too_small", "the draft's short side is %d px; the refiner takes 384P-768P drafts (floor %d px)", short, h3rMinShortSide)
	} else if short < 384 {
		warn("the draft's short side (%d px) is under 384P; it is upscaled to %s before encoding", short, pl.InputSize)
	} else if short > 768 {
		warn("the draft (%dx%d) is above 768P; it is downscaled to the %s input", d.Width, d.Height, pl.InputSize)
	}
	if d.FPS > 0 && math.Abs(d.FPS-h3rFPS) > 0.05 {
		warn("the draft runs at %.3f fps; the refiner resamples it to 24 fps", d.FPS)
	}
	if d.Seconds <= 0 || math.IsNaN(d.Seconds) || math.IsInf(d.Seconds, 0) {
		return pl, planErr("draft_duration_unknown", "the draft's duration could not be read")
	}
	f24 := int(math.Round(d.Seconds * h3rFPS))
	if f24 < h3rMinFrames {
		return pl, planErr("draft_too_short", "the draft is %.2f s; at least %.1f s is needed", d.Seconds, float64(h3rMinFrames)/h3rFPS)
	}

	// §2.5 preset — decided BEFORE the backend: Fidelity on a cast / dialogue shot holds the draft's audio, which only
	// the ComfyUI port can do (§2.5, §9.1)
	is := func(v any) bool { return boolOf(v) }
	has := func(v any) bool { _, k := boolTri(v); return k }
	sh := p.Shot
	prompt, _ := p.Prompt.(string)
	dialogueInPrompt := reDialogue.MatchString(prompt)
	locked := is(sh.HasCast) || is(sh.HasDialogue) || is(sh.HasAnimal) || is(sh.CloseUp) || dialogueInPrompt
	factsKnown := has(sh.HasCast) || has(sh.HasDialogue) || has(sh.HasAnimal) || has(sh.CloseUp) || dialogueInPrompt
	if pr := normalizePreset(p.Preset); pr != "" {
		pl.Preset = pr
		if pr == "detail" && locked {
			warn("Detail preset on a shot with cast, dialogue or an animal: faces can be recast and lip sync is lost")
		}
	} else {
		if squash(p.Preset) != "" {
			warn("preset %v is unknown; choosing from the shot", p.Preset)
		}
		pl.Preset = map[bool]string{true: "fidelity", false: "detail"}[locked || !factsKnown]
	}
	holdAudio := pl.Preset == "fidelity" && locked && d.HasAudio

	// §0 verdict 2, §7.1, §9.1: below 140 GB ComfyUI is the first path; the CLI only when asked, on 140 GB+, or alone
	if vramGB > 0 && vramGB < h3rMinVRAMGB {
		return pl, jobs.Errf(jobs.KindInfra, "vram_too_small: this GPU has %s; the refiner needs a 32 GB class card (floor %.0f GB)", gb(vramGB), h3rMinVRAMGB)
	}
	installed := func(b string) (val, known bool) {
		ptr := hw.CLI
		if b == "comfyui" {
			ptr = hw.Comfy
		}
		if ptr == nil {
			return false, false
		}
		return *ptr, true
	}
	switch squash(p.Backend) {
	case "cli", "infer", "inferpy", "official":
		pl.Backend = "cli"
	case "comfyui", "comfy":
		pl.Backend = "comfyui"
	default:
		if b := squash(p.Backend); b != "" && b != "auto" {
			warn("backend %v is unknown; choosing automatically", p.Backend)
		}
		preferred := "comfyui"
		switch {
		case f24 > h3rCLIWindow, holdAudio:
			preferred = "comfyui"
		case vramGB >= h3rSolEngineVRAMGB:
			preferred = "cli"
		}
		other := map[string]string{"cli": "comfyui", "comfyui": "cli"}[preferred]
		pv, pk := installed(preferred)
		ov, ok := installed(other)
		if pk && !pv && (!ok || ov) {
			warn("%s would be the first choice here, but only %s is installed on this server", preferred, other)
			preferred = other
		}
		pl.Backend = preferred
	}
	if v, k := installed(pl.Backend); k && !v {
		return pl, jobs.Errf(jobs.KindInfra, "backend_not_installed: the %s backend is not installed on this server", pl.Backend)
	}
	if pl.Backend == "cli" && holdAudio {
		const why = "the Fidelity preset holds the draft's audio to keep lip sync on a cast / dialogue shot, and the official CLI can only hold encoded silence"
		if !boolOf(p.AllowSilence) {
			return pl, planErr("fidelity_needs_draft_audio", "%s: use backend comfyui, or set allow_silence to accept the drift", why)
		}
		warn("%s: refining with silence (allow_silence)", why)
	}

	// §3.1/§3.2 the refined window
	capF := h3rMaxFrames
	if pl.Backend == "cli" {
		capF = h3rCLIWindow
	}
	pl.WindowFrames = min(f24, capF)
	pl.AllowTruncate = boolOf(p.AllowTruncate)
	if f24 > capF {
		lost := float64(f24-capF) / h3rFPS
		if !pl.AllowTruncate {
			if pl.Backend == "cli" {
				return pl, planErr("draft_too_long", "the official CLI refines only the first 5.04 s; this draft is %.2f s (%.2f s would be lost): use backend comfyui or set allow_truncate", d.Seconds, lost)
			}
			return pl, planErr("draft_too_long", "one refine pass takes at most 15 s; this draft is %.2f s (%.2f s would be lost): cut it first or set allow_truncate", d.Seconds, lost)
		}
		warn("only the first %.2f s of the %.2f s draft is refined; %.2f s are dropped", float64(capF)/h3rFPS, d.Seconds, lost)
	}
	// The CLI pads to 124. The ComfyUI nodes refine 121-frame windows, each padded to 124, so memory is one window's
	// whatever the length (§9.2).
	grid := h3rCLIWindow + 3
	if pl.Backend == "comfyui" {
		grid = h3GridFrames(min(pl.WindowFrames, h3rCLIWindow))
		if pl.WindowFrames > h3rCLIWindow {
			warn("the %.2f s draft is refined in %d windows of 5 s; a seam can show where two meet", float64(pl.WindowFrames)/h3rFPS, (pl.WindowFrames+h3rCLIWindow-1)/h3rCLIWindow)
		}
	}
	pl.OutputSeconds = math.Min(math.Round(d.Seconds*1000)/1000, math.Round(float64(pl.WindowFrames)/h3rFPS*1000)/1000)

	pre := h3rPresets[pl.Preset]
	pl.Steps, pl.StartSigma = pre.steps, pre.sigma
	if n, ok := numberOf(p.Steps); ok && (n == 2 || n == 3) {
		pl.Steps = int(n)
	} else if squash(p.Steps) != "" {
		warn("steps %v: the refiner runs 2 or 3 steps; keeping %d", p.Steps, pl.Steps)
	}
	if pl.Steps != pre.steps { // §9.1 --steps 2 starts at 0.7; Fidelity at 3 steps starts at the 0.8 in-between (§7.5 row 3)
		pl.StartSigma = map[int]float64{2: 0.7, 3: 0.8}[pl.Steps]
	}
	if squash(p.StartSigma) != "" {
		if s, ok := numberOf(p.StartSigma); !ok {
			warn("start sigma %v is not a number; keeping %g", p.StartSigma, pl.StartSigma)
		} else if s > h3rMaxStartSigma+1e-9 {
			pl.StartSigma = h3rMaxStartSigma
			warn("start sigma %g is above the trained range; clamped to %g", s, h3rMaxStartSigma)
		} else if s < h3rMinStartSigma {
			pl.StartSigma = h3rMinStartSigma
			warn("start sigma %g would barely change the draft; clamped to %g", s, h3rMinStartSigma)
		} else {
			pl.StartSigma = math.Round(s*1e6) / 1e6
		}
	}

	// §3.2 target + §7 memory guards
	pl.Target = normalizeTarget(p.Target)
	if pl.Target == "" {
		if squash(p.Target) != "" {
			warn("target %v is not 768p / 1080p / 2k / 4k; using 2k", p.Target)
		}
		pl.Target = "2k"
	}
	fits := func(t string) bool { return measured(vramGB, latentTokens(t, grid), pl.Backend) }
	if vramGB > 0 && !fits(pl.Target) {
		where := fmt.Sprintf("%s with %s", gb(vramGB), pl.Backend)
		if pl.Backend == "cli" && vramGB < h3rSolEngineVRAMGB {
			where = fmt.Sprintf("the official CLI on %s (nothing measured below %.0f GB: dense forward ~71 GB at 2K, no offload flags)", gb(vramGB), h3rSolEngineVRAMGB)
		}
		why := fmt.Sprintf("%s for %.2f s on %s is outside what has been measured (h3_regenerate_2k.mdx §7.5)", pl.Target, float64(pl.WindowFrames)/h3rFPS, where)
		if boolOf(p.AllowUnmeasured) {
			warn("%s; trying it (allow_unmeasured)", why)
		} else {
			lower := ""
			for _, t := range h3rTargetOrder {
				if h3rTargets[t].order < h3rTargets[pl.Target].order && fits(t) && h3rTargets[t].h > short {
					lower = t
				}
			}
			refuse := squash(p.Fallback) == "refuse"
			switch {
			case lower == "":
				return pl, planErr("target_unmeasured", "%s; no lower rung is both measured and above the draft (set allow_unmeasured to try it%s)", why, map[bool]string{true: ", or use backend comfyui", false: ""}[pl.Backend == "cli"])
			case refuse:
				return pl, planErr("target_unmeasured", "%s; fallback is \"refuse\" (%s would fit; set allow_unmeasured to try %s)", why, lower, pl.Target)
			}
			warn("%s; rendering %s instead", why, lower)
			pl.Target = lower
		}
	} else if vramGB <= 0 {
		warn("this GPU's VRAM is unknown: the memory envelope was not checked")
	}
	if h3rTargets[pl.Target].h < short {
		warn("the target (%s) is smaller than the draft (%dp): the refine downsizes it", pl.Target, short)
	}
	pl.Envelope = map[bool]string{true: "measured", false: "unmeasured"}[measured(vramGB, latentTokens(pl.Target, grid), pl.Backend)]
	pl.Fwd = "standard"
	if vramGB >= h3rSolEngineVRAMGB {
		pl.Fwd = "sol_engine"
	}
	pl.VAE = "fast"
	if pl.Backend == "comfyui" {
		pl.VAE = "standard" // the Olm nodes decode with the released H3 VAE, tiled
	} else if peak := fastDecoderPeakGB(latentTokens(pl.Target, grid)); vramGB > 0 && peak > vramGB*0.9 {
		pl.VAE = "standard"
		warn("the fast decoder needs ~%.1f GB here; using the released decoder", peak)
	}
	// §7.3 item 7: block swap needs system RAM (same floor as the TS builder)
	if hw.RAMGB > 0 && pl.Fwd == "standard" && h3rTargets[pl.Target].order >= h3rTargets["2k"].order {
		if hw.RAMGB < h3rMinRAM {
			return pl, jobs.Errf(jobs.KindInfra, "ram_too_small: %s of system RAM cannot hold the block-swapped H3 base for a %s refine (want %.0f GB+)", gb(hw.RAMGB), pl.Target, h3rRecommendRAM)
		} else if hw.RAMGB < h3rRecommendRAM {
			warn("%s of system RAM: %.0f GB+ is recommended for block swap", gb(hw.RAMGB), h3rRecommendRAM)
		}
	}
	// §9.1 resolution ladder (a CLI recipe): auto for 4K under 140 GB; an explicit yes for any rung above 768p
	ladderAsk := squash(p.Ladder)
	if lv, lk := boolTri(p.Ladder); lk && ladderAsk != "auto" {
		pl.Ladder = lv && pl.Target != "768p" && pl.Backend == "cli"
	} else {
		pl.Ladder = pl.Target == "4k" && pl.Fwd == "standard" && pl.Backend == "cli"
	}

	// §2.5 audio: never regenerated; the draft's track is re-muxed
	pl.ModelAudio = pre.audio
	if pl.ModelAudio == "draft" && (pl.Backend == "cli" || !d.HasAudio) {
		pl.ModelAudio = "silence"
	}
	pl.Remux = map[bool]string{true: "on", false: "off"}[d.HasAudio]
	if !d.HasAudio && (is(sh.HasDialogue) || dialogueInPrompt) {
		warn("the shot has dialogue but the draft has no audio track: the master will be silent")
	}

	// §9.2 LoRA: both backends load NVIDIA's merged r576 (the CLI from --weights, the Olm nodes from the released bundle)
	if l := squash(p.Lora); l != "" {
		switch {
		case strings.Contains(l, "draken"):
			return pl, planErr("lora_unverified", "the DrakenZA conversion is unverified (no Sharp conditioning) and must not be used")
		case !strings.Contains(l, "576") && !strings.Contains(l, "nvidia") && l != "default":
			warn("LoRA %v: the installed refiners load NVIDIA's merged r576 only", p.Lora)
		}
	}

	if squash(p.Seed) != "" {
		if s, ok := numberOf(p.Seed); ok && math.Abs(s) < 1e18 {
			i := int64(math.Floor(s))
			pl.Seed = uint32(((i % (1 << 32)) + (1 << 32)) % (1 << 32))
		} else {
			warn("seed %v is not a usable number; using %d", p.Seed, h3rDefaultSeed)
		}
	}
	pl.Delivery = deliveryOf(p.Delivery)
	if p.LicenseCleared != nil {
		warn("a license clearance in the params is ignored: only Legal opens the gate, in code (H3RegenerateLicenseCleared, §12)")
	}
	return pl, nil
}

func deliveryOf(v any) string {
	switch squash(v) {
	case "internal", "test", "internaltest":
		return "internal"
	}
	return "customer" // §12: the gate fails closed
}

/* ── Check / Run ──────────────────────────────────────────────────────────────────────────────────────────────────── */

func (OSSRegenerate) Check(_ context.Context, job *jobs.Job, env Env) error {
	p, err := parseRegenParams(job.Payload.Params)
	if err != nil {
		return err
	}
	if m := squash(p.Model); m != "" && m != squash(RegenerateModel) && m != "openh3regenerate2k" && m != "h3regenerate2k" {
		return jobs.Errf(jobs.KindInvalidInput, "model %v is not built for this service (built: %s)", p.Model, RegenerateModel)
	}
	// §12 license gate: customer delivery (or customer data, or an unstated delivery) is refused until Legal clears it
	if !H3RegenerateLicenseCleared && (deliveryOf(p.Delivery) == "customer" || job.Payload.DataRules.CustomerData) {
		return jobs.Errf(jobs.KindInvalidInput, "license_not_cleared: Open-H3-Regenerate-2K output may not reach a customer until Legal "+
			"clears the MiniMax H3 Community License (h3_regenerate_2k.mdx §12); send delivery \"internal\" for internal tests")
	}
	if !hasInput(job, "draft_video") {
		return jobs.Errf(jobs.KindInvalidInput, "a regenerate needs the input \"draft_video\"")
	}
	if p.Prompt != nil {
		if _, ok := p.Prompt.(string); !ok {
			return jobs.Errf(jobs.KindInvalidInput, "prompt must be text (got %T): the draft's own prompt converted to H3 T2VA", p.Prompt)
		}
	}
	prompt, _ := p.Prompt.(string)
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return jobs.Errf(jobs.KindInvalidInput, "prompt is required: the draft's own prompt converted to H3 T2VA (spec_open_h3_regenerate_2k.mdx §1)")
	}
	if reLabel.MatchString(prompt) {
		return jobs.Errf(jobs.KindInvalidInput, "the prompt still has reference labels (<Picture N> …): convert it first (spec_open_h3_regenerate_2k.mdx §4)")
	}
	if n := len([]rune(prompt)); n > h3rPromptMax {
		return jobs.Errf(jobs.KindInvalidInput, "the prompt is %d characters; past %d it is longer than any H3 prompt", n, h3rPromptMax)
	}
	if t := normalizeTarget(p.Target); t == "" && squash(p.Target) != "" {
		return jobs.Errf(jobs.KindInvalidInput, "target %v is not 768p, 1080p, 2k or 4k", p.Target)
	}
	if FakeModels() {
		return nil
	}
	if accelerator() != "cuda" {
		return jobs.Errf(jobs.KindInfra, "Open-H3-Regenerate-2K runs on CUDA only; this machine has no port (h3_regenerate_2k.mdx §8)")
	}
	if st := regenInstall(); !st.cli && !st.comfy {
		return jobs.Errf(jobs.KindInfra, "%s", st.why)
	}
	return nil
}

func (OSSRegenerate) Run(ctx context.Context, job *jobs.Job, env Env, rep Reporter) (*Result, error) {
	p, err := parseRegenParams(job.Payload.Params)
	if err != nil {
		return nil, err
	}
	prompt, _ := p.Prompt.(string)
	prompt = strings.TrimSpace(prompt)
	var plan *regenPlan
	res, err := runDetached(ctx, job, env, rep, "video", func(snap snapshot.Snapshot, inputs map[string]string, out string) (detachedTask, error) {
		draft := inputs["draft_video"]
		d, err := probeDraft(enginePath(env, "ffprobe"), draft)
		if err != nil {
			return detachedTask{}, jobs.Errf(jobs.KindInvalidInput, "%v", err)
		}
		hw := regenHW{VRAMGB: gpuVRAMGB(env), RAMGB: systemRAMGB()}
		if !FakeModels() { // the plan picks among what is really installed here
			st := regenInstall()
			hw.CLI, hw.Comfy = &st.cli, &st.comfy
		}
		pl, err := planRegenerate(p, d, hw)
		if err != nil {
			return detachedTask{}, err
		}
		plan = &pl
		for _, w := range pl.Warnings {
			rep.Log("plan: " + w)
		}
		b, _ := json.Marshal(pl)
		rep.Log("plan: " + string(b))
		if FakeModels() {
			t, _ := fakeTask(snap, out, draft, prompt, h3rTargets[pl.Target].w/8, h3rTargets[pl.Target].h/8)
			return t, nil
		}
		return regenTask(snap, env, pl, draft, prompt, out)
	})
	if err != nil || res == nil {
		return res, err
	}
	data := map[string]any{"model": RegenerateModel,
		"license": map[string]any{"cleared": H3RegenerateLicenseCleared, "delivery": deliveryOf(p.Delivery),
			"notice": "MiniMax H3 Community License: internal use only until Legal clears it (h3_regenerate_2k.mdx §12)"}}
	if m, ok := res.Data.(map[string]any); ok {
		for k, v := range m {
			data[k] = v
		}
	}
	if plan != nil {
		data["plan"] = plan
	}
	res.Data = data
	for i := range res.Outputs {
		res.Outputs[i].Meta = map[string]any{"license_cleared": H3RegenerateLicenseCleared, "delivery": deliveryOf(p.Delivery)}
	}
	return res, nil
}

// gpuVRAMGB is the total memory of the card this job holds: $H3R_GPU_VRAM_GB, else nvidia-smi for the slot's GPU,
// else 0 (unknown: the memory envelope is not checked and the plan says so). A 5090 reads 31.8 here (h3rCardClassGB).
var gpuVRAMGB = func(env Env) float64 {
	if v, err := strconv.ParseFloat(os.Getenv("H3R_GPU_VRAM_GB"), 64); err == nil && v > 0 {
		return v
	}
	smi, err := exec.LookPath("nvidia-smi")
	if err != nil {
		return 0
	}
	idx := 0
	if env.Slot.GPU != nil {
		idx = *env.Slot.GPU
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out strings.Builder
	if procs.Run(ctx, procs.Spec{Name: smi, Args: []string{"--query-gpu=memory.total", "--format=csv,noheader,nounits", "-i", strconv.Itoa(idx)},
		Stdout: func(l string) { out.WriteString(l) }}) != nil {
		return 0
	}
	mib, err := strconv.ParseFloat(strings.TrimSpace(out.String()), 64)
	if err != nil {
		return 0
	}
	return math.Round(mib/1024*10) / 10
}

// systemRAMGB: $H3R_SYSTEM_RAM_GB, else the machine's total RAM, else 0 (unknown).
var systemRAMGB = func() float64 {
	if v, err := strconv.ParseFloat(os.Getenv("H3R_SYSTEM_RAM_GB"), 64); err == nil && v > 0 {
		return v
	}
	if _, total, ok := hardware.FreeRAM(); ok {
		return math.Round(total*10) / 10
	}
	return 0
}

// regenTask: the two checkpointed steps (refine → finish), from the snapshot (R1), scratch under the run dir (R10).
// The prompt goes in a file (--prompt-file): one argv word of up to 40,000 characters is over Windows' 32,767-character
// command line, and spec R8 keeps a Windows 5090 in play.
func regenTask(snap snapshot.Snapshot, env Env, pl regenPlan, draft, prompt, out string) (detachedTask, error) {
	home := regenHome()
	work := filepath.Join(filepath.Dir(filepath.Dir(out)), "work")
	refined := filepath.Join(work, "refined.mp4")
	promptFile := filepath.Join(work, "prompt.txt")
	if err := os.MkdirAll(work, 0o700); err != nil {
		return detachedTask{}, jobs.Wrap(jobs.KindInfra, err, "create the work dir")
	}
	if err := os.WriteFile(promptFile, []byte(prompt), 0o600); err != nil {
		return detachedTask{}, jobs.Wrap(jobs.KindInfra, err, "write the prompt file")
	}
	// the venv that runs ComfyUI also runs the stdlib-only script, which starts ComfyUI with its own interpreter
	base := []string{venvPython(home), snap.Path("code/scripts/video_gen/h3_regenerate/render_h3_regenerate.py")}
	common := []string{
		"--backend", pl.Backend, "--home", home, "--weights", filepath.Join(home, "weights"),
		"--draft", draft, "--prompt-file", promptFile, "--resolution", pl.Target, "--input-size", pl.InputSize,
		"--steps", strconv.Itoa(pl.Steps), "--start-sigma", strconv.FormatFloat(pl.StartSigma, 'f', -1, 64),
		"--fwd", pl.Fwd, "--vae", pl.VAE, "--seed", strconv.FormatUint(uint64(pl.Seed), 10),
		"--window-frames", strconv.Itoa(pl.WindowFrames), "--model-audio", pl.ModelAudio, "--lora", pl.Lora,
		"--refined", refined, "--delivery", pl.Delivery,
	}
	if H3RegenerateLicenseCleared {
		common = append(common, "--license-cleared")
	}
	if pl.Ladder {
		common = append(common, "--ladder")
	}
	// what the caller accepted: the script re-checks the probed file and refuses anything else (§3.1, §3.2)
	if pl.AllowCrop {
		common = append(common, "--allow-crop")
	}
	if pl.AllowTruncate {
		common = append(common, "--allow-truncate")
	}
	envv := h3Env(env)
	refine := append(append(append([]string{}, base...), "--stage", "refine"), common...)
	finish := append(append(append([]string{}, base...), "--stage", "finish"), common...)
	finish = append(finish, "--out", out, "--remux", pl.Remux, "--output-seconds", strconv.FormatFloat(pl.OutputSeconds, 'f', 3, 64))
	return detachedTask{
		Steps: []taskhost.Step{
			{Name: "refine", Argv: refine, Dir: work, Env: envv, Output: refined, Weight: 0.95},
			{Name: "finish", Argv: finish, Dir: work, Env: envv, Output: out, Weight: 0.05},
		},
		Output:       out,
		ExpectFrames: int(math.Round(pl.OutputSeconds * h3rFPS)),
	}, nil
}

// regenInstallState: which backends this server can run. cli = venv + Sana checkout with infer.py + full-size
// weights; comfy = the venv, ComfyUI with the Olm Open-H3 nodes and every model file (bms/oss_video/open_h3_regenerate_2k,
// laid out like h3-regenerate-local.ts comfyChecks). why names the first missing piece of each.
type regenInstallState struct {
	cli, comfy bool
	why        string
}

// h3rComfyFiles: the ComfyUI install, relative to <home>/ComfyUI, with size floors that catch a partial download.
// refinerOnly marks what MiniMax H3 generation does not load (h3InstallProblem).
var h3rComfyFiles = []struct {
	rel         []string
	min         int64
	refinerOnly bool
}{
	{[]string{"main.py"}, 1, false},
	{[]string{"custom_nodes", "ComfyUI-Olm-Open-H3-Regenerate-2K", "nodes.py"}, 1, true},
	{[]string{"models", "diffusion_models", "minimax_h3_fl2va_pruned_int8_convrot.safetensors"}, 20_000_000_000, false},
	{[]string{"models", "text_encoders", "qwen3vl_32b_minimax_h3_nvfp4_awq.safetensors"}, 15_000_000_000, false},
	{[]string{"models", "vae", "minimax_h3_video_vae_fp16.safetensors"}, 5_000_000_000, false},
	{[]string{"models", "vae", "minimax_h3_audio_vae_fp32.safetensors"}, 500_000_000, false},
	{[]string{"models", "latent_upscale_models", "minimax_h3_latent_upscaler_3d_conv_v1", "minimax_h3_latent_upscaler_3d_conv_v1_bf16.safetensors"}, 600_000_000, true},
	{[]string{"models", "open_h3_refiner", "Open-H3-Regenerate-2K", "lora", "open_h3_regenerate_2k_lora.safetensors"}, 12_000_000_000, true},
	{[]string{"models", "open_h3_refiner", "Open-H3-Regenerate-2K", "lora", "sharp_curve_branches.safetensors"}, 1_000_000_000, true},
	{[]string{"models", "open_h3_refiner", "Open-H3-Regenerate-2K", "lora", "adaln_t_table.bin"}, 32800, true},
}

func comfyPath(home string, rel []string) string {
	return filepath.Join(append([]string{home, "ComfyUI"}, rel...)...)
}

// h3Env: both H3 scripts resolve ffmpeg / ffprobe through H3R_<TOOL>, then PATH.
func h3Env(env Env) []string {
	vars := []string{"PYTHONUNBUFFERED=1"}
	for _, t := range []string{"ffmpeg", "ffprobe"} {
		if p := enginePath(env, t); p != "" {
			vars = append(vars, "H3R_"+strings.ToUpper(t)+"="+p)
		}
	}
	return vars
}

var regenInstall = func() regenInstallState {
	if accelerator() != "cuda" {
		return regenInstallState{why: "Open-H3-Regenerate-2K runs on CUDA only (no Metal / MLX port yet, h3_regenerate_2k.mdx §8)"}
	}
	home := regenHome()
	cliWhy := ""
	infer := ""
	for _, d := range [][]string{{"sana", "models", "Open-H3-Regenerate-2K"}, {"sana", "models", "minimax_h3", "Open-H3-Regenerate-2K"}} {
		if p := filepath.Join(append([]string{home}, append(d, "infer.py")...)...); fileAtLeast(p, 1) {
			infer = p
		}
	}
	w := filepath.Join(home, "weights")
	checks := []struct {
		path string
		min  int64
	}{
		{venvPython(home), 1},
		{filepath.Join(w, "lora", "open_h3_regenerate_2k_lora.safetensors"), 10_000_000_000},
		{filepath.Join(w, "lora", "sharp_curve_branches.safetensors"), 1_000_000_000},
		{filepath.Join(w, "lora", "adaln_t_table.bin"), 1},
		{filepath.Join(w, "vae_fast", "fast_h3_vae_noncausal_24l.safetensors"), 5_000_000_000},
		{filepath.Join(w, "base_model", "model_index.json"), 1},
	}
	if infer == "" {
		cliWhy = "missing " + filepath.Join(home, "sana", "models", "Open-H3-Regenerate-2K", "infer.py")
	}
	for _, c := range checks {
		if cliWhy == "" && !fileAtLeast(c.path, c.min) {
			cliWhy = "missing or partial " + c.path
		}
	}
	comfyWhy := ""
	if !fileAtLeast(venvPython(home), 1) {
		comfyWhy = "missing " + venvPython(home)
	}
	for _, f := range h3rComfyFiles {
		if p := comfyPath(home, f.rel); comfyWhy == "" && !fileAtLeast(p, f.min) {
			comfyWhy = "missing or partial " + p
		}
	}
	st := regenInstallState{cli: cliWhy == "", comfy: comfyWhy == ""}
	if !st.cli && !st.comfy {
		st.why = "Open-H3-Regenerate-2K is not installed on this server: ComfyUI " + comfyWhy + "; CLI " + cliWhy +
			" (install: ~/BGit/all/app/smartc/film/servers/bms/oss_video/open_h3_regenerate_2k, `just build` in app/code/film/servers)"
	}
	return st
}

// regenInstallProblem: "" when at least one backend is ready, else why not (InstallProblems in install.go reads it).
func regenInstallProblem() string { return regenInstall().why }
