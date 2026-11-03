//go:build !windows

package runners

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/snapshot"
)

// Expectations come from h3_regenerate_2k.mdx (§2.5 presets, §3 inputs, §5.3 memory, §7 the 32 GB card, §9.1 flags,
// §10.2 task design, §12 license). The cross-port cases live in ONE fixture the TS reference builder reads too.

var h3Draft = draftInfo{Width: 1344, Height: 768, FPS: 24, Seconds: 5, HasAudio: true}

// A real RTX 5090: nvidia-smi says 32607 MiB, which gpuVRAMGB reports as 31.8 (never exactly 32).
const rtx5090 = 31.8

func params(t *testing.T, m map[string]any) regenParams {
	t.Helper()
	b, _ := json.Marshal(m)
	p, err := parseRegenParams(b)
	if err != nil {
		t.Fatalf("params %s: %v", b, err)
	}
	return p
}

func plan(t *testing.T, m map[string]any, d draftInfo, hw regenHW) (regenPlan, error) {
	t.Helper()
	return planRegenerate(params(t, m), d, hw)
}

func vram(v float64) regenHW { return regenHW{VRAMGB: v} }
func ptr(b bool) *bool       { return &b }

func errCode(err error) string {
	var je *jobs.Error
	if !errors.As(err, &je) {
		return ""
	}
	code, _, _ := strings.Cut(je.Message, ":")
	return code
}

// The shared TS<->Go table: ~/BGit/all/film/code/subp/video_gen_ai/nvidia/h3_regenerate_2k/__tests__/fixtures/.
func TestRegenerateParityFixture(t *testing.T) {
	path := os.Getenv("H3R_PARITY_FIXTURE")
	if path == "" {
		home, _ := os.UserHomeDir()
		path = filepath.Join(home, "BGit", "all", "film", "code", "subp", "video_gen_ai", "nvidia", "h3_regenerate_2k", "__tests__", "fixtures", "regenerate_parity.json")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("parity fixture not on this machine (%v): set H3R_PARITY_FIXTURE", err)
	}
	var doc struct {
		Cases []struct {
			Name   string         `json:"name"`
			Params map[string]any `json:"params"`
			Draft  struct {
				Width    int     `json:"width"`
				Height   int     `json:"height"`
				FPS      float64 `json:"fps"`
				Seconds  float64 `json:"seconds"`
				HasAudio bool    `json:"has_audio"`
			} `json:"draft"`
			Machine struct {
				VRAM      float64          `json:"vram_gb"`
				RAM       float64          `json:"ram_gb"`
				Installed *map[string]bool `json:"installed"`
			} `json:"machine"`
			Expect map[string]any `json:"expect"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Cases) < 10 {
		t.Fatalf("fixture has only %d cases", len(doc.Cases))
	}
	for _, c := range doc.Cases {
		hw := regenHW{VRAMGB: c.Machine.VRAM, RAMGB: c.Machine.RAM}
		if c.Machine.Installed != nil {
			in := *c.Machine.Installed
			hw.CLI, hw.Comfy = ptr(in["cli"]), ptr(in["comfyui"])
		}
		d := draftInfo{Width: c.Draft.Width, Height: c.Draft.Height, FPS: c.Draft.FPS, Seconds: c.Draft.Seconds, HasAudio: c.Draft.HasAudio}
		pl, err := plan(t, c.Params, d, hw)
		ok := c.Expect["ok"].(bool)
		if (err == nil) != ok {
			t.Errorf("%s: ok=%v, err %v", c.Name, err == nil, err)
			continue
		}
		if !ok {
			if want := c.Expect["error"].(string); errCode(err) != want {
				t.Errorf("%s: error code %q, want %q (%v)", c.Name, errCode(err), want, err)
			}
			continue
		}
		got := map[string]any{"backend": pl.Backend, "target": pl.Target, "preset": pl.Preset, "steps": float64(pl.Steps),
			"start_sigma": pl.StartSigma, "input_size": pl.InputSize, "model_audio": pl.ModelAudio, "window_frames": float64(pl.WindowFrames),
			"vae": pl.VAE, "fwd": pl.Fwd, "ladder": pl.Ladder, "envelope": pl.Envelope, "lora": pl.Lora,
			"allow_crop": pl.AllowCrop, "allow_truncate": pl.AllowTruncate}
		for k, want := range c.Expect {
			if k == "ok" || k == "error" {
				continue
			}
			g, known := got[k]
			if !known {
				t.Fatalf("%s: the fixture expects %q, which this test does not read", c.Name, k)
			}
			if wf, isF := want.(float64); isF {
				if math.Abs(g.(float64)-wf) > 1e-6 {
					t.Errorf("%s: %s = %v, want %v", c.Name, k, g, want)
				}
			} else if g != want {
				t.Errorf("%s: %s = %v, want %v", c.Name, k, g, want)
			}
		}
	}
}

// The reviewer's finding: on the real card every job was refused because 31.8 < 32.
func TestRegenerateRealCardReadings(t *testing.T) {
	for _, v := range []float64{31.8, 31.84, 31.5, 32, 32.6} {
		pl, err := plan(t, map[string]any{"shot": map[string]any{"has_cast": true}}, h3Draft, vram(v))
		if err != nil || pl.Target != "2k" || pl.Backend != "comfyui" || pl.Envelope != "measured" {
			t.Errorf("%.2f GB: %+v %v", v, pl, err)
		}
	}
	_, err := plan(t, nil, h3Draft, vram(23.9))
	if errCode(err) != "vram_too_small" || !strings.Contains(err.Error(), "23.9 GB") {
		t.Errorf("23.9 GB must be refused with the real reading in the message: %v", err)
	}
	_, err = plan(t, map[string]any{"backend": "cli", "shot": map[string]any{"has_cast": false}}, h3Draft, vram(31.8))
	if errCode(err) != "target_unmeasured" || !strings.Contains(err.Error(), "31.8 GB") || strings.Contains(err.Error(), "32 GB is") {
		t.Errorf("the CLI on a 5090 is unmeasured, said with one decimal: %v", err)
	}
	if measured(31.8, latentTokens("2k", 124), "cli") || !measured(31.8, latentTokens("2k", 124), "comfyui") || measured(29.9, 1, "comfyui") {
		t.Error("measured() is keyed by backend and the 30 GB card class")
	}
}

func TestRegeneratePresets(t *testing.T) {
	for _, c := range []struct {
		name  string
		p     map[string]any
		want  string
		steps int
		sigma float64
	}{
		{"dialogue", map[string]any{"shot": map[string]any{"has_dialogue": true}}, "fidelity", 2, 0.7},
		{"animal, string yes", map[string]any{"shot": map[string]any{"has_animal": "yes"}}, "fidelity", 2, 0.7},
		{"wide, no cast", map[string]any{"shot": map[string]any{"has_cast": false, "has_dialogue": "no"}}, "detail", 3, 0.909375},
		{"dialogue in the prompt", map[string]any{"prompt": "[Shot 1] He (S1) says: <d>[English] Hi.</d>"}, "fidelity", 2, 0.7},
		{"unknown facts", nil, "fidelity", 2, 0.7},
		{"garbage facts count as unknown", map[string]any{"shot": map[string]any{"has_cast": "maybe", "close_up": map[string]any{}}}, "fidelity", 2, 0.7},
		{"explicit detail wins", map[string]any{"preset": " Detail ", "shot": map[string]any{"hasCast": true}}, "detail", 3, 0.909375},
		{"camelCase shot", map[string]any{"shot": map[string]any{"hasCast": false, "hasDialogue": false}}, "detail", 3, 0.909375},
		{"fidelity at 3 steps = the 0.8 in-between", map[string]any{"steps": 3}, "fidelity", 3, 0.8},
		{"custom sigma", map[string]any{"start_sigma": "0.65"}, "fidelity", 2, 0.65},
		{"camelCase sigma", map[string]any{"startSigma": 0.75}, "fidelity", 2, 0.75},
		{"sigma above the trained range", map[string]any{"start_sigma": 1.5}, "fidelity", 2, 0.909375},
		{"sigma too low", map[string]any{"start_sigma": 0.01}, "fidelity", 2, 0.3},
		{"junk sigma and steps", map[string]any{"start_sigma": "loud", "steps": "seven"}, "fidelity", 2, 0.7},
		{"preset of the wrong type", map[string]any{"preset": 2, "shot": map[string]any{"has_cast": false}}, "detail", 3, 0.909375},
	} {
		pl, err := plan(t, c.p, h3Draft, vram(rtx5090))
		if err != nil || pl.Preset != c.want || pl.Steps != c.steps || math.Abs(pl.StartSigma-c.sigma) > 1e-9 {
			t.Errorf("%s: %+v err %v", c.name, pl, err)
		}
	}
}

func TestRegenerateParamsNeverRejectADriftedType(t *testing.T) {
	// each of these failed json.Unmarshal into the old string/*bool struct and rejected the whole job
	for _, body := range []string{`{"input_size":576}`, `{"shot":{"has_cast":"yes"}}`, `{"lora":128}`, `{"preset":2}`, `{"backend":true}`,
		`{"delivery":1}`, `{"fallback":["refuse"]}`, `{"shot":"cast"}`, `{"draft":{"source":"ltx"}}`, `{"draft":"x"}`, `{}`, `null`, ``} {
		p, err := parseRegenParams([]byte(body))
		if err != nil {
			t.Errorf("%s: %v", body, err)
			continue
		}
		if _, err := planRegenerate(p, h3Draft, vram(rtx5090)); err != nil && errCode(err) == "" {
			t.Errorf("%s: an unclassified plan error %v", body, err)
		}
	}
	if _, err := parseRegenParams([]byte(`[1,2]`)); err == nil {
		t.Error("an array is not params")
	}
	p := params(t, map[string]any{"draft": map[string]any{"source": "ltx_2_5"}})
	if pl, _ := planRegenerate(p, h3Draft, vram(rtx5090)); pl.InputSize != "576p" {
		t.Errorf("draft.source (the TS request shape) must reach the plan: %s", pl.InputSize)
	}
}

func TestRegenerateInputSizeAndDraftRules(t *testing.T) {
	for src, want := range map[string]string{"minimax_h3": "768p", "FastH3 Omni": "768p", "h3-hyperflow": "768p", "MiniMax-H3 L2VA": "768p",
		"ltx_2_5": "576p", "Wan 2.2": "576p", "hailuo-02": "576p", "veo-3.1": "576p", "grok imagine": "576p", "minimax-video-01": "576p"} {
		pl, err := plan(t, map[string]any{"draft_source": src}, h3Draft, vram(rtx5090))
		if err != nil || pl.InputSize != want {
			t.Errorf("%s: %s %v", src, pl.InputSize, err)
		}
	}
	// a bare "minimax" is ambiguous (production's MINIMAX key is Hailuo): the 1344x768@24 heuristic decides
	for _, c := range []struct {
		d    draftInfo
		want string
	}{{h3Draft, "768p"}, {draftInfo{Width: 1280, Height: 720, FPS: 30, Seconds: 5}, "576p"}} {
		if pl, _ := plan(t, map[string]any{"draft_source": "MINIMAX"}, c.d, vram(rtx5090)); pl.InputSize != c.want {
			t.Errorf("bare minimax %dx%d: %s", c.d.Width, c.d.Height, pl.InputSize)
		}
	}
	for _, c := range []struct {
		w, h   int
		crop   bool
		ok     bool
		reason string
	}{
		{672, 384, false, true, ""}, {1024, 576, false, true, ""}, {480, 270, false, true, ""},
		{320, 180, false, false, "draft_too_small"}, {768, 1344, false, false, "draft_portrait"}, {768, 1344, true, true, ""},
		{768, 768, false, false, "draft_aspect"}, {768, 768, true, true, ""}, {1920, 804, false, false, "draft_aspect"}, {1920, 1080, false, true, ""},
	} {
		pl, err := plan(t, map[string]any{"draft_source": "ltx", "allow_crop": c.crop}, draftInfo{Width: c.w, Height: c.h, FPS: 24, Seconds: 3}, vram(rtx5090))
		if (err == nil) != c.ok || (err != nil && errCode(err) != c.reason) {
			t.Errorf("%dx%d crop=%v: %v", c.w, c.h, c.crop, err)
		}
		if err == nil && pl.AllowCrop != c.crop {
			t.Errorf("%dx%d: allow_crop must ride in the plan for the script", c.w, c.h)
		}
		var je *jobs.Error
		if err != nil && (!errors.As(err, &je) || je.Kind != jobs.KindInvalidInput) {
			t.Errorf("%dx%d: a bad draft is invalid_input, got %v", c.w, c.h, err)
		}
	}
}

func TestRegenerateWindowsNeverTruncateSilently(t *testing.T) {
	for _, c := range []struct {
		secs     float64
		backend  string
		truncate any
		ok       bool
		be       string
		frames   int
	}{
		{3, "", nil, true, "cli", 72}, {5.04, "", nil, true, "cli", 121}, {8, "", nil, true, "comfyui", 192},
		{15, "", nil, true, "comfyui", 360}, {8, "CLI", nil, false, "", 0}, {8, "cli", true, true, "cli", 121},
		{20, "", nil, false, "", 0}, {20, "", "yes", true, "comfyui", 360}, {0.2, "", nil, false, "", 0},
	} {
		// a 180 GB card with no cast: the auto choice is the CLI for short drafts (sol_engine_fwd fits)
		p := map[string]any{"backend": c.backend, "allow_truncate": c.truncate, "shot": map[string]any{"has_cast": false}}
		pl, err := plan(t, p, draftInfo{Width: 1344, Height: 768, FPS: 24, Seconds: c.secs, HasAudio: true}, vram(180))
		if (err == nil) != c.ok {
			t.Errorf("%.2fs %s: %v", c.secs, c.backend, err)
			continue
		}
		if c.ok && (pl.Backend != c.be || pl.WindowFrames != c.frames || pl.OutputSeconds > math.Min(c.secs, float64(pl.WindowFrames)/24)+1e-3) {
			t.Errorf("%.2fs: %+v", c.secs, pl)
		}
		if c.ok && c.frames < int(math.Round(c.secs*24)) && !pl.AllowTruncate {
			t.Errorf("%.2fs: a cut plan must carry allow_truncate to the script", c.secs)
		}
	}
}

func TestRegenerateMemoryGuards(t *testing.T) {
	wide := map[string]any{"shot": map[string]any{"has_cast": false}}
	with := func(k string, v any) map[string]any {
		m := map[string]any{"shot": map[string]any{"has_cast": false}}
		m[k] = v
		return m
	}
	pl, err := plan(t, wide, h3Draft, vram(rtx5090))
	if err != nil || pl.Target != "2k" || pl.Fwd != "standard" || pl.VAE != "fast" || pl.Ladder || pl.Backend != "comfyui" {
		t.Fatalf("2K/5 s on a 5090: %+v %v", pl, err)
	}
	if pl, _ = plan(t, with("target", "4K"), h3Draft, vram(rtx5090)); pl.Target != "2k" {
		t.Errorf("4K on 32 GB steps down to 2K: %s", pl.Target)
	}
	_, err = plan(t, with("fallback", "refuse"), h3Draft, vram(rtx5090))
	if err != nil {
		t.Errorf("2K fits: refuse changes nothing: %v", err)
	}
	m := with("target", "4k")
	m["fallback"] = "refuse"
	if _, err = plan(t, m, h3Draft, vram(rtx5090)); errCode(err) != "target_unmeasured" || !strings.Contains(err.Error(), "2k would fit") {
		t.Errorf("fallback refuse names the rung that would fit: %v", err)
	}
	m = with("target", "uhd")
	m["allow_unmeasured"], m["backend"] = "yes", "cli"
	if pl, _ = plan(t, m, h3Draft, vram(rtx5090)); pl.Target != "4k" || !pl.Ladder || pl.VAE != "standard" {
		t.Errorf("4K allowed on 32 GB via the CLI: ladder + released VAE: %+v", pl)
	}
	if pl, _ = plan(t, with("target", "4k"), h3Draft, vram(180)); pl.Target != "4k" || pl.Fwd != "sol_engine" || pl.Ladder || pl.Backend != "cli" {
		t.Errorf("B200 class: %+v", pl)
	}
	var je *jobs.Error
	if _, err = plan(t, wide, h3Draft, vram(16)); !errors.As(err, &je) || je.Kind != jobs.KindInfra {
		t.Errorf("16 GB is infra (another server may take it): %v", err)
	}
	if pl, _ = plan(t, wide, h3Draft, vram(0)); !slices.ContainsFunc(pl.Warnings, func(w string) bool { return strings.Contains(w, "VRAM is unknown") }) {
		t.Errorf("unknown VRAM must be said: %v", pl.Warnings)
	}
	if _, err = plan(t, wide, h3Draft, regenHW{VRAMGB: rtx5090, RAMGB: 48}); errCode(err) != "ram_too_small" {
		t.Errorf("48 GB RAM: %v", err)
	}
	if pl, _ = plan(t, wide, h3Draft, regenHW{VRAMGB: rtx5090, RAMGB: 96}); !slices.ContainsFunc(pl.Warnings, func(w string) bool { return strings.Contains(w, "128 GB+") }) {
		t.Errorf("96 GB RAM warns: %v", pl.Warnings)
	}
	// §5.3 numbers: 533k tokens and ~21 GB at 2K/5 s; ~45 GB at 2K/15 s
	if tk := latentTokens("2k", 124); math.Abs(tk-533000)/533000 > 0.01 || math.Abs(fastDecoderPeakGB(tk)-21) > 1 {
		t.Errorf("2K/5 s: %v tokens, %.1f GB", tk, fastDecoderPeakGB(tk))
	}
	if g := fastDecoderPeakGB(latentTokens("2k", h3GridFrames(360))); math.Abs(g-45) > 3 {
		t.Errorf("2K/15 s: %.1f GB", g)
	}
	for n := 1; n <= 400; n++ {
		g := h3GridFrames(n)
		if g < n || g < 124 || (g-5)%17 != 0 || g-17 >= max(n, 124) {
			t.Fatalf("grid(%d) = %d", n, g)
		}
	}
}

func TestRegenerateBackendFollowsTheSpecAndTheInstall(t *testing.T) {
	dlg := map[string]any{"shot": map[string]any{"has_dialogue": true}}
	wide := map[string]any{"shot": map[string]any{"has_cast": false}}
	for _, c := range []struct {
		name string
		p    map[string]any
		hw   regenHW
		be   string
		code string
	}{
		{"5090 dialogue, install unknown → ComfyUI (§9.1)", dlg, vram(rtx5090), "comfyui", ""},
		{"5090 wide, install unknown → ComfyUI", wide, vram(rtx5090), "comfyui", ""},
		{"VRAM unknown → ComfyUI", wide, vram(0), "comfyui", ""},
		{"ComfyUI-only", wide, regenHW{VRAMGB: rtx5090, CLI: ptr(false), Comfy: ptr(true)}, "comfyui", ""},
		{"CLI-only wide → the CLI, unmeasured", wide, regenHW{VRAMGB: rtx5090, CLI: ptr(true), Comfy: ptr(false)}, "", "target_unmeasured"},
		{"nothing installed", wide, regenHW{VRAMGB: rtx5090, CLI: ptr(false), Comfy: ptr(false)}, "", "backend_not_installed"},
		{"asked for an uninstalled CLI", map[string]any{"backend": "cli"}, regenHW{VRAMGB: 180, CLI: ptr(false), Comfy: ptr(true)}, "", "backend_not_installed"},
		{"B200 wide → CLI", wide, vram(180), "cli", ""},
		{"B200 dialogue → ComfyUI (holds the draft audio)", dlg, vram(180), "comfyui", ""},
		{"B200 dialogue, ComfyUI not installed → refused, not silently silent", dlg, regenHW{VRAMGB: 180, CLI: ptr(true), Comfy: ptr(false)}, "", "fidelity_needs_draft_audio"},
	} {
		pl, err := plan(t, c.p, h3Draft, c.hw)
		if c.code != "" {
			if errCode(err) != c.code {
				t.Errorf("%s: %v", c.name, err)
			}
			continue
		}
		if err != nil || pl.Backend != c.be {
			t.Errorf("%s: %s %v", c.name, pl.Backend, err)
		}
	}
	if pl, _ := plan(t, map[string]any{"backend": "comfy"}, h3Draft, vram(rtx5090)); pl.ModelAudio != "draft" {
		t.Errorf("ComfyUI Fidelity holds the draft audio: %s", pl.ModelAudio)
	}
	if pl, _ := plan(t, nil, draftInfo{Width: 1344, Height: 768, FPS: 24, Seconds: 4}, vram(rtx5090)); pl.Remux != "off" || pl.ModelAudio != "silence" {
		t.Errorf("no audio track: %+v", pl)
	}
}

func TestRegenerateLoraSeedLicense(t *testing.T) {
	if _, err := plan(t, map[string]any{"lora": "DrakenZA conversion"}, h3Draft, vram(rtx5090)); errCode(err) != "lora_unverified" {
		t.Errorf("DrakenZA must be refused: %v", err)
	}
	b200cli := map[string]any{"lora": "r128", "backend": "cli", "shot": map[string]any{"has_cast": false}}
	if pl, _ := plan(t, b200cli, h3Draft, vram(180)); pl.Lora != "r576" {
		t.Errorf("the CLI only takes r576: %s", pl.Lora)
	}
	if pl, _ := plan(t, map[string]any{"lora": 128}, h3Draft, vram(rtx5090)); pl.Lora != "r128" {
		t.Errorf("a numeric 128 is the father558 LoRA on ComfyUI: %s", pl.Lora)
	}
	if pl, _ := plan(t, map[string]any{"lora": "mystery-lora"}, h3Draft, vram(rtx5090)); pl.Lora != "r576" ||
		!slices.ContainsFunc(pl.Warnings, func(w string) bool { return strings.Contains(w, "unknown") }) {
		t.Errorf("an unknown LoRA falls back to r576 with a warning (TS does too): %+v", pl)
	}
	for in, want := range map[any]uint32{"42": 42, 42.9: 42, -1.0: 4294967295, "abc": h3rDefaultSeed, 1e20: h3rDefaultSeed, "1,766,580,932": 1766580932} {
		if pl, _ := plan(t, map[string]any{"seed": in}, h3Draft, vram(rtx5090)); pl.Seed != want {
			t.Errorf("seed %v → %d", in, pl.Seed)
		}
	}
	if pl, _ := plan(t, map[string]any{}, h3Draft, vram(rtx5090)); pl.Seed != h3rDefaultSeed {
		t.Errorf("no seed → the §9.2 default")
	}
	for in, want := range map[any]string{"": "customer", "Internal": "internal", " test ": "internal", "customer": "customer", "x": "customer", nil: "customer", 1.0: "customer"} {
		if deliveryOf(in) != want {
			t.Errorf("delivery %v → %s", in, deliveryOf(in))
		}
	}
	pl, _ := plan(t, map[string]any{"license_cleared": true}, h3Draft, vram(rtx5090))
	if !slices.ContainsFunc(pl.Warnings, func(w string) bool { return strings.Contains(w, "ignored") }) {
		t.Errorf("a license field in params is warned about and ignored: %v", pl.Warnings)
	}
}

// Property: random, hostile params and drafts never panic, and every accepted plan stays inside the model's limits.
func TestRegeneratePlanFuzz(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	pick := func(xs ...any) any { return xs[r.Intn(len(xs))] }
	accepted := 0
	for i := 0; i < 4000; i++ {
		m := map[string]any{"target": pick("2k", "4K", "1080", "uhd", "", nil, "8k", 1440.0, true), "preset": pick("fidelity", "detail", "", "x", 3, nil),
			"start_sigma": pick(nil, 0.7, 0.8, 2.0, -1.0, "a", "0.5"), "steps": pick(nil, 2.0, 3.0, 4.0, "2"), "seed": pick(nil, 0.0, -5.0, 1.5, "z", 1e20),
			"backend": pick("", "auto", "cli", "comfyui", "zzz", 7), "draft_source": pick("", "h3", "ltx", "FastH3 Omni", "minimax", 5),
			"allow_unmeasured": pick(nil, true, "yes", false), "allow_truncate": pick(nil, true), "allow_crop": pick(nil, true, "on"), "allow_silence": pick(nil, true),
			"fallback": pick("", "refuse", nil), "lora": pick("", "r128", "kijai", "nope", 128), "prompt": pick("", "<d>x</d>", 5),
			"shot": pick(nil, "x", map[string]any{"has_cast": pick(true, false, "yes", nil)}, map[string]any{"hasDialogue": true})}
		d := draftInfo{Width: pick(0, 320, 672, 1024, 1344, 1920, 768).(int), Height: pick(0, 180, 384, 576, 768, 1080, 1344).(int),
			FPS: pick(0.0, 16.0, 24.0, 29.97, 60.0).(float64), Seconds: pick(0.0, 0.3, 2.0, 5.0, 8.0, 15.0, 40.0, math.NaN()).(float64), HasAudio: r.Intn(2) == 0}
		hw := regenHW{VRAMGB: pick(0.0, 16.0, 23.9, 24.0, 29.5, 31.8, 32.0, 48.0, 80.0, 180.0).(float64), RAMGB: pick(0.0, 48.0, 96.0, 128.0).(float64)}
		if r.Intn(3) == 0 {
			hw.CLI, hw.Comfy = ptr(r.Intn(2) == 0), ptr(r.Intn(2) == 0)
		}
		pl, err := plan(t, m, d, hw)
		if err != nil {
			if errCode(err) == "" {
				t.Fatalf("an error without a code for %+v: %v", m, err)
			}
			continue
		}
		accepted++
		if h3rTargets[pl.Target].w == 0 || (pl.Steps != 2 && pl.Steps != 3) || pl.StartSigma < 0.3 || pl.StartSigma > 0.909375 ||
			pl.WindowFrames < h3rMinFrames || pl.WindowFrames > h3rMaxFrames || (pl.Backend == "cli" && pl.WindowFrames > h3rCLIWindow) ||
			pl.OutputSeconds <= 0 || pl.OutputSeconds > float64(pl.WindowFrames)/24+1e-3 || (pl.InputSize != "768p" && pl.InputSize != "576p") {
			t.Fatalf("plan out of limits for %+v %+v %+v: %+v", m, d, hw, pl)
		}
		if hw.VRAMGB > 0 && hw.VRAMGB < 140 && !boolOf(m["allow_unmeasured"]) && pl.Envelope != "measured" {
			t.Fatalf("an unmeasured plan was accepted without allow_unmeasured: %+v %+v", m, pl)
		}
		if (hw.CLI != nil && pl.Backend == "cli" && !*hw.CLI) || (hw.Comfy != nil && pl.Backend == "comfyui" && !*hw.Comfy) {
			t.Fatalf("planned an uninstalled backend: %+v %+v", hw, pl)
		}
		if pl.Backend == "cli" && pl.Preset == "fidelity" && d.HasAudio && !boolOf(m["allow_silence"]) {
			if sh, ok := m["shot"].(map[string]any); (ok && (boolOf(sh["has_cast"]) || boolOf(sh["hasDialogue"]))) || m["prompt"] == "<d>x</d>" {
				t.Fatalf("Fidelity on a cast shot went to the silent CLI: %+v %+v", m, pl)
			}
		}
	}
	if accepted < 150 {
		t.Fatalf("only %d of 4000 random requests planned: the property was barely exercised", accepted)
	}
}

func TestRegenerateTaskSteps(t *testing.T) {
	t.Setenv("ACT3_RENDER_HOME", "/r")
	snap := snapshot.Snapshot{Key: "k", Dir: "/snap/k"}
	run := t.TempDir()
	out := filepath.Join(run, "out", "output.mp4")
	pl, err := plan(t, map[string]any{"delivery": "internal", "backend": "cli", "allow_unmeasured": true, "shot": map[string]any{"has_cast": false}}, h3Draft, vram(rtx5090))
	if err != nil {
		t.Fatal(err)
	}
	prompt := "integrated_multimodal_description: [Shot 1] a man says \"finish\" ; $(rm -rf /) — the man was killed\n\noverall_soundscape: N/A"
	tk, err := regenTask(snap, pl, "/run/inputs/draft_video.mp4", prompt, out, comfyCfg{})
	if err != nil {
		t.Fatal(err)
	}
	if len(tk.Steps) != 2 || tk.Steps[0].Name != "refine" || tk.Steps[1].Name != "finish" {
		t.Fatalf("steps: %+v", tk.Steps)
	}
	a := tk.Steps[0].Argv
	if a[0] != "/r/open_h3_regenerate_2k/.venv/bin/python" || a[1] != "/snap/k/code/scripts/video_gen/h3_regenerate/render_h3_regenerate.py" {
		t.Fatalf("program: %v", a[:2])
	}
	// the prompt travels in a file, never in argv (Windows' 32,767-character command line)
	pf := argOf(a, "--prompt-file")
	if b, _ := os.ReadFile(pf); string(b) != prompt || slices.Contains(a, "--prompt") {
		t.Fatalf("prompt file %q holds %q; argv %v", pf, b, a)
	}
	if argOf(a, "--stage") != "refine" || argOf(a, "--steps") != "3" || argOf(a, "--fwd") != "standard" ||
		argOf(a, "--delivery") != "internal" || slices.Contains(a, "--license-cleared") || slices.Contains(a, "--comfy-url") || slices.Contains(a, "--allow-crop") {
		t.Fatalf("refine argv: %v", a)
	}
	work := filepath.Join(run, "work")
	if tk.Steps[0].Output != filepath.Join(work, "refined.mp4") || argOf(a, "--refined") != filepath.Join(work, "refined.mp4") {
		t.Fatalf("the refine checkpoint lives in the run dir's work/: %v", tk.Steps[0].Output)
	}
	b := tk.Steps[1].Argv
	if argOf(b, "--out") != out || argOf(b, "--remux") != "on" || argOf(b, "--output-seconds") != "5.000" || tk.ExpectFrames != 120 {
		t.Fatalf("finish argv: %v expect %d", b, tk.ExpectFrames)
	}
	// ComfyUI: no model venv needed (the script is stdlib only); crop and truncate consent travel to the script
	pl2, err := plan(t, map[string]any{"delivery": "internal", "allow_crop": true, "allow_truncate": true}, draftInfo{Width: 768, Height: 1344, FPS: 24, Seconds: 20, HasAudio: true}, vram(180))
	if err != nil {
		t.Fatal(err)
	}
	tk, err = regenTask(snap, pl2, "/d.mp4", "p", out, comfyCfg{"http://127.0.0.1:8188", "/wf.json", "/comfy/input"})
	if err != nil {
		t.Fatal(err)
	}
	a = tk.Steps[0].Argv
	if argOf(a, "--comfy-url") != "http://127.0.0.1:8188" || argOf(a, "--window-frames") != "360" || !slices.Contains(a, "--allow-crop") ||
		!slices.Contains(a, "--allow-truncate") || a[0] != "python3" {
		t.Fatalf("comfy argv: %v", a)
	}
}

func TestScriptErrorKindDecidesBeforeTheRegexes(t *testing.T) {
	for tail, want := range map[string]jobs.ErrorKind{
		"prompt: the man ran out of memory of his childhood\nACT3_ERROR invalid_input: the draft is portrait": jobs.KindInvalidInput,
		"Killed: 9 (echoed)\nACT3_ERROR license_not_cleared: …":                                               jobs.KindInvalidInput,
		"ACT3_ERROR not_installed: infer.py not found":                                                        jobs.KindInfra,
		"ACT3_ERROR internal: infer.py exited 1: ValueError":                                                  jobs.KindInternal,
		"ACT3_NOTE x\nACT3_ERROR infra: out of memory (the refine needs a bigger card)":                       jobs.KindInfra,
		"torch.OutOfMemoryError: CUDA out of memory":                                                          jobs.KindInfra, // no verdict: the regex
		"ValueError: shape mismatch":                                                                          jobs.KindInternal,
		"ACT3_ERROR infra: first\nACT3_ERROR invalid_input: the LAST line is the verdict":                     jobs.KindInvalidInput,
	} {
		if got := failureKind(tail); got != want {
			t.Errorf("%q → %s, want %s", tail, got, want)
		}
	}
}

func TestRegenerateCheck(t *testing.T) {
	env, _ := setupFake(t)
	mk := func(p map[string]any, customerData bool, inputs ...string) *jobs.Job {
		b, _ := json.Marshal(p)
		j := &jobs.Job{ID: "r", Payload: jobs.Envelope{Params: b, DataRules: jobs.DataRules{CustomerData: customerData}}}
		for _, n := range inputs {
			j.Payload.Inputs = append(j.Payload.Inputs, jobs.Input{Name: n, Path: "/x"})
		}
		return j
	}
	ok := map[string]any{"prompt": "integrated_multimodal_description: [Shot 1] x", "delivery": "internal", "target": "2K"}
	if err := (OSSRegenerate{}).Check(context.Background(), mk(ok, false, "draft_video"), env); err != nil {
		t.Fatal(err)
	}
	with := func(k string, v any) map[string]any {
		m := map[string]any{}
		for a, b := range ok {
			m[a] = b
		}
		if v == nil {
			delete(m, k)
		} else {
			m[k] = v
		}
		return m
	}
	for name, c := range map[string]struct {
		job  *jobs.Job
		want string
	}{
		"delivery unstated":        {mk(with("delivery", nil), false, "draft_video"), "license_not_cleared"},
		"customer delivery":        {mk(with("delivery", "customer"), false, "draft_video"), "license_not_cleared"},
		"customer + clearance ask": {mk(with("license_cleared", true), true, "draft_video"), "license_not_cleared"},
		"customer data":            {mk(ok, true, "draft_video"), "license_not_cleared"},
		"no draft":                 {mk(ok, false), "draft_video"},
		"empty prompt":             {mk(with("prompt", "  \n "), false, "draft_video"), "prompt is required"},
		"prompt not text":          {mk(with("prompt", 5), false, "draft_video"), "prompt must be text"},
		"labels left":              {mk(with("prompt", "[Shot 1] the man in <Picture 1>"), false, "draft_video"), "reference labels"},
		"unknown model":            {mk(with("model", "wan2.2-a14b"), false, "draft_video"), "not built"},
		"unknown target":           {mk(with("target", "8K"), false, "draft_video"), "target"},
		"huge prompt":              {mk(with("prompt", strings.Repeat("a", 40001)), false, "draft_video"), "characters"},
		"params not an object":     {&jobs.Job{ID: "r", Payload: jobs.Envelope{Params: []byte(`[1]`)}}, "params"},
	} {
		err := (OSSRegenerate{}).Check(context.Background(), c.job, env)
		var je *jobs.Error
		if err == nil || !strings.Contains(err.Error(), c.want) || !errors.As(err, &je) || je.Kind != jobs.KindInvalidInput {
			t.Errorf("%s: %v", name, err)
		}
	}
	// the internal web app's camelCase, drifted types and model spelling: never "params do not match"
	app := with("model", "Open-H3-Regenerate-2K")
	app["startSigma"], app["allowUnmeasured"], app["input_size"], app["shot"] = 0.8, true, 576, map[string]any{"hasCast": "yes"}
	if err := (OSSRegenerate{}).Check(context.Background(), mk(app, false, "draft_video"), env); err != nil {
		t.Errorf("app params: %v", err)
	}
	// a 40,000-character prompt of multi-byte characters is 40,000 characters, not 120,000 bytes
	if err := (OSSRegenerate{}).Check(context.Background(), mk(with("prompt", strings.Repeat("é", 40000)), false, "draft_video"), env); err != nil {
		t.Errorf("rune count, not bytes: %v", err)
	}
}

// The whole path with fake models: a real draft (ffmpeg), probed, planned, rendered in a task host from the snapshot.
func TestFakeRegenerateInATaskHost(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not on PATH")
	}
	env, job := setupFake(t)
	draft := filepath.Join(t.TempDir(), "draft.mp4")
	if out, err := exec.Command("ffmpeg", "-y", "-v", "error", "-f", "lavfi", "-i", "testsrc=size=672x384:rate=24:duration=2",
		"-f", "lavfi", "-i", "sine=duration=2", "-shortest", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", draft).CombinedOutput(); err != nil {
		t.Fatalf("ffmpeg: %v %s", err, out)
	}
	job.Type = "act3.video_regenerate_open_source"
	job.Payload.Params, _ = json.Marshal(map[string]any{"prompt": "integrated_multimodal_description: [Shot 1] a test card", "delivery": "internal", "draft": map[string]any{"source": "ltx"}})
	job.Payload.Inputs = []jobs.Input{{Name: "draft_video", Path: draft}}
	env.Inputs = map[string]string{"draft_video": draft}
	r := OSSRegenerate{}
	if err := r.Check(context.Background(), job, env); err != nil {
		t.Fatal(err)
	}
	rep := &recRep{}
	res, err := r.Run(context.Background(), job, env, rep)
	if err != nil {
		t.Fatal(err, rep.logs)
	}
	d, _ := res.Data.(map[string]any)
	if d["model"] != RegenerateModel || d["plan"] == nil || res.Outputs[0].Meta["license_cleared"] != false {
		t.Fatalf("result data: %+v meta %+v", d, res.Outputs[0].Meta)
	}
	if !slices.ContainsFunc(rep.logs, func(l string) bool { return strings.Contains(l, `"input_size":"576p"`) }) {
		t.Fatalf("the plan is logged (and draft.source reached it): %v", rep.logs)
	}
	// a draft the server refuses on sight: portrait, probed from the file even if the params said nothing
	portrait := filepath.Join(t.TempDir(), "p.mp4")
	exec.Command("ffmpeg", "-y", "-v", "error", "-f", "lavfi", "-i", "testsrc=size=384x672:rate=24:duration=1", "-c:v", "libx264", "-pix_fmt", "yuv420p", portrait).Run()
	job.ID = "job-2"
	env.Inputs = map[string]string{"draft_video": portrait}
	_, err = r.Run(context.Background(), job, env, &recRep{})
	var je *jobs.Error
	if !errors.As(err, &je) || je.Kind != jobs.KindInvalidInput || !strings.Contains(err.Error(), "portrait") {
		t.Fatalf("portrait draft: %v", err)
	}
}
