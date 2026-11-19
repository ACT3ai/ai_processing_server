package runners

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
)

// a plan with an asset reference in every place the contract names, plus prose that only looks like one
func layersTestPlan() map[string]any {
	var plan map[string]any
	_ = json.Unmarshal([]byte(`{
	  "version": 1, "shotId": "s1",
	  "output": { "width": 64, "height": 36, "fps": "30/1", "lengthFlicks": 1411200000 },
	  "steps": [
	    { "id": "lt", "kind": "html_render", "fields": { "text": "asset: the old map" },
	      "bundle": { "entry": "index.html", "files": [
	        { "path": "index.html", "url": "asset:a3" }, { "path": "img/logo.png", "url": "asset:a4" } ] } },
	    { "id": "combine", "kind": "combine", "layers": [
	      { "z": 40, "clipId": "lt", "source": { "step": "lt" } },
	      { "z": 20, "clipId": "broll", "source": { "url": "asset:a1" } },
	      { "z": 10, "clipId": "base", "source": { "path": "asset:a2" },
	        "nested": { "deeper": [ { "deepest": "asset:a1" } ] } } ] },
	    { "id": "audio", "kind": "audio_mix", "plan": { "version": 2, "inputs": [
	      { "id": "base", "source": { "url": "asset:a2" } }, { "id": "tone", "source": { "url": "asset:a5" } } ] } },
	    { "id": "join", "kind": "join", "after": ["combine", "audio"], "picture": { "url": "asset:a1" } }
	  ]
	}`), &plan)
	return plan
}

func TestRewriteAssetRefsEveryLocation(t *testing.T) {
	plan := layersTestPlan()
	if got, want := AssetRefIDs(plan), []string{"a1", "a2", "a3", "a4", "a5"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("AssetRefIDs = %v, want %v", got, want)
	}
	paths := map[string]string{"a1": "/j/assets/broll.mp4", "a2": "/j/assets/base.mp4", "a3": "/j/assets/index.html",
		"a4": "/j/assets/logo.png", "a5": "/j/assets/tone.wav"}
	if err := RewriteAssetRefs(plan, paths); err != nil {
		t.Fatal(err)
	}
	steps := plan["steps"].([]any)
	get := func(v any, keys ...any) any {
		for _, k := range keys {
			switch kk := k.(type) {
			case string:
				v = v.(map[string]any)[kk]
			case int:
				v = v.([]any)[kk]
			}
		}
		return v
	}
	checks := map[string][2]any{
		"bundle file 0 url":   {get(steps[0], "bundle", "files", 0, "url"), "/j/assets/index.html"},
		"bundle file 0 path":  {get(steps[0], "bundle", "files", 0, "path"), "index.html"},
		"bundle file 1 url":   {get(steps[0], "bundle", "files", 1, "url"), "/j/assets/logo.png"},
		"bundle file 1 path":  {get(steps[0], "bundle", "files", 1, "path"), "img/logo.png"},
		"prose kept":          {get(steps[0], "fields", "text"), "asset: the old map"},
		"step source kept":    {get(steps[1], "layers", 0, "source", "step"), "lt"},
		"layer source.url":    {get(steps[1], "layers", 1, "source", "url"), "/j/assets/broll.mp4"},
		"layer source.path":   {get(steps[1], "layers", 2, "source", "path"), "/j/assets/base.mp4"},
		"nested":              {get(steps[1], "layers", 2, "nested", "deeper", 0, "deepest"), "/j/assets/broll.mp4"},
		"audio input 0":       {get(steps[2], "plan", "inputs", 0, "source", "url"), "/j/assets/base.mp4"},
		"audio input 1":       {get(steps[2], "plan", "inputs", 1, "source", "url"), "/j/assets/tone.wav"},
		"join picture":        {get(steps[3], "picture", "url"), "/j/assets/broll.mp4"},
		"join after (no ref)": {get(steps[3], "after", 0), "combine"},
	}
	for name, c := range checks {
		if c[0] != c[1] {
			t.Errorf("%s = %v, want %v", name, c[0], c[1])
		}
	}
	if left := AssetRefIDs(plan); len(left) != 0 {
		t.Fatalf("references left after the rewrite: %v", left)
	}
}

func TestRewriteAssetRefsUnknownIDs(t *testing.T) {
	plan := layersTestPlan()
	err := RewriteAssetRefs(plan, map[string]string{"a1": "/x/1", "a2": "/x/2", "a3": "/x/3"})
	var je *jobs.Error
	if !errors.As(err, &je) || je.Kind != jobs.KindInvalidInput {
		t.Fatalf("want invalid_input, got %v", err)
	}
	if !strings.Contains(err.Error(), "a4, a5") {
		t.Fatalf("error should name every missing id once: %v", err)
	}
}

func layersJobWith(t *testing.T, params map[string]any, inputs ...string) *jobs.Job {
	t.Helper()
	b, _ := json.Marshal(params)
	j := &jobs.Job{ID: "lr1", Type: "act3.layers_render", Payload: jobs.Envelope{Service: "act3.layers_render", Params: b}}
	for _, n := range inputs {
		j.Payload.Inputs = append(j.Payload.Inputs, jobs.Input{Name: n})
	}
	return j
}

func TestParseLayersParams(t *testing.T) {
	plan := layersTestPlan()
	assets := []map[string]any{{"id": "a1", "role": "video", "name": "broll.mp4"}}
	// the job's renderTask, passed through
	lj, err := parseLayersParams(layersJobWith(t, map[string]any{"render_type": "layers_render",
		"renderTask": map[string]any{"renderType": "layers_render", "renderId": "r1", "plan": plan, "assets": assets, "htmlLayers": 3, "durationSeconds": 7.5}}))
	if err != nil {
		t.Fatal(err)
	}
	if lj.RenderID != "r1" || lj.HTMLLayers != 3 || lj.DurationSeconds != 7.5 || len(lj.Assets) != 1 || lj.Assets[0].Name != "broll.mp4" {
		t.Fatalf("renderTask form: %+v", lj)
	}
	// flat layersPlan: htmlLayers counted from the plan, duration from lengthFlicks (2 s)
	lj, err = parseLayersParams(layersJobWith(t, map[string]any{"layersPlan": plan}))
	if err != nil {
		t.Fatal(err)
	}
	if lj.HTMLLayers != 1 || lj.DurationSeconds != 2 {
		t.Fatalf("flat form: html %d, duration %v", lj.HTMLLayers, lj.DurationSeconds)
	}
	for name, p := range map[string]map[string]any{
		"no plan":      {"renderTask": map[string]any{"assets": assets}},
		"plan not obj": {"plan": []int{1}},
		"no steps":     {"plan": map[string]any{"version": 1}},
		"bad asset id": {"plan": plan, "assets": []map[string]any{{"id": "../a1"}}},
		"dup asset id": {"plan": plan, "assets": []map[string]any{{"id": "a1"}, {"id": "a1"}}},
	} {
		if _, err := parseLayersParams(layersJobWith(t, p)); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func layersEngines() map[string]*hardware.Engine {
	return map[string]*hardware.Engine{"act3_motion_video": {Path: "/x/v"}, "act3_motion_audio": {Path: "/x/a"},
		"ffmpeg": {Path: "/x/ffmpeg"}, "ffprobe": {Path: "/x/ffprobe"}, "node": {Path: "/x/node"}, "chromium": {Path: "/x/chrome"}}
}

func TestLayersCheck(t *testing.T) {
	ctx := context.Background()
	plan := layersTestPlan()
	all := []map[string]any{{"id": "a1"}, {"id": "a2"}, {"id": "a3"}, {"id": "a4"}, {"id": "a5"}}
	five := []string{"a1", "a2", "a3", "a4", "a5"}
	env := Env{Engines: layersEngines()}
	if err := (LayersRender{}).Check(ctx, layersJobWith(t, map[string]any{"plan": plan, "assets": all}, five...), env); err != nil {
		t.Fatal(err)
	}
	if err := (LayersRender{}).Check(ctx, layersJobWith(t, map[string]any{"plan": plan, "assets": all[:4]}, five[:4]...), env); err == nil || !strings.Contains(err.Error(), "a5") {
		t.Fatalf("a plan naming an unlisted asset must fail, got %v", err)
	}
	if err := (LayersRender{}).Check(ctx, layersJobWith(t, map[string]any{"plan": plan, "assets": all}, "a1"), env); err == nil {
		t.Fatal("fewer inputs than assets must fail")
	}
	// Chromium only matters when the plan has HTML layers
	noChrome := layersEngines()
	delete(noChrome, "chromium")
	err := (LayersRender{}).Check(ctx, layersJobWith(t, map[string]any{"plan": plan, "assets": all}, five...), Env{Engines: noChrome})
	var je *jobs.Error
	if !errors.As(err, &je) || je.Kind != jobs.KindInfra || !strings.Contains(err.Error(), "chromium") {
		t.Fatalf("missing chromium with an HTML layer: want infra, got %v", err)
	}
	if err := (LayersRender{}).Check(ctx, layersJobWith(t, map[string]any{"plan": plan, "assets": all, "htmlLayers": 0}, five...), Env{Engines: noChrome}); err != nil {
		t.Fatalf("htmlLayers 0 needs no Chromium: %v", err)
	}
}

func TestPlaceLayersAssets(t *testing.T) {
	in := t.TempDir()
	mk := func(name, body string) string {
		p := filepath.Join(in, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	n5 := int64(5)
	assets := []LayersAsset{
		{ID: "a1", Role: "video", Name: "broll.mp4", Bytes: &n5},  // staged as video_0 (the render engine's naming)
		{ID: "a2", Role: "audio", Name: "tone.wav"},               // staged by id
		{ID: "a3", Role: "html_file", Name: "../evil/index.html"}, // staged by name; cleaned to its base
		{ID: "a4", Role: "image", Name: "broll.mp4"},              // same name as a1
	}
	inputs := map[string]string{"video_0": mk("video_0.mp4", "12345"), "a2": mk("a2.wav", "w"), "index.html": mk("index.html", "<p>"),
		"a4": mk("a4.mp4", "img")}
	dir := filepath.Join(t.TempDir(), "assets")
	paths, err := PlaceLayersAssets(assets, inputs, dir)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"a1": "broll.mp4", "a2": "tone.wav", "a3": "index.html", "a4": "a4_broll.mp4"}
	for id, base := range want {
		if !filepath.IsAbs(paths[id]) || filepath.Base(paths[id]) != base || filepath.Dir(paths[id]) != dir {
			t.Errorf("%s placed at %q, want %s/%s", id, paths[id], dir, base)
		}
		if _, err := os.Stat(paths[id]); err != nil {
			t.Errorf("%s: %v", id, err)
		}
	}
	// a wrong byte count is refused
	n9 := int64(9)
	if _, err := PlaceLayersAssets([]LayersAsset{{ID: "a1", Role: "video", Name: "x.mp4", Bytes: &n9}}, inputs, t.TempDir()); err == nil {
		t.Fatal("want a size mismatch")
	}
	// an asset that was not staged
	if _, err := PlaceLayersAssets([]LayersAsset{{ID: "a9", Role: "take", Name: "t.mp4"}}, inputs, t.TempDir()); err == nil {
		t.Fatal("want an unstaged asset error")
	}
	// without an asset list every input is an asset named by its input name
	paths, err = PlaceLayersAssets(nil, map[string]string{"a1": inputs["video_0"]}, t.TempDir())
	if err != nil || filepath.Base(paths["a1"]) != "video_0.mp4" {
		t.Fatalf("no asset list: %v %v", paths, err)
	}
}

func TestLayersCommand(t *testing.T) {
	b := LayersBins{Video: "/j/bin/act3-motion-video", Audio: "/j/bin/act3-motion-audio", FFmpeg: "/usr/bin/ffmpeg", FFprobe: "/usr/bin/ffprobe",
		Node: "/opt/homebrew/bin/node", Chrome: "/c/headless_shell", Templates: "/j/motion_video/templates",
		Runner: "/j/motion_video/html_runner/render.mjs", Profile: "/j/hardware_profile.json"}
	got := LayersCommand(b, "/j", "/w/out/final.mp4", "/cache")
	want := []string{"run", "--plan", "/j/plan.json", "--out", "/w/out/final.mp4", "--work", "/j/work", "--cache", "/cache",
		"--audio-bin", "/j/bin/act3-motion-audio", "--profile", "/j/hardware_profile.json", "--ffmpeg", "/usr/bin/ffmpeg",
		"--ffprobe", "/usr/bin/ffprobe", "--node", "/opt/homebrew/bin/node", "--chrome", "/c/headless_shell",
		"--templates", "/j/motion_video/templates", "--runner", "/j/motion_video/html_runner/render.mjs"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command\n got %q\nwant %q", got, want)
	}
	// optional flags are left out when empty
	b.Chrome, b.Templates, b.Runner, b.Profile = "", "", "", ""
	got = LayersCommand(b, "/j", "/w/out/final.mp4", "/cache")
	for _, f := range []string{"--chrome", "--templates", "--runner", "--profile"} {
		for _, a := range got {
			if a == f {
				t.Errorf("%s present although empty", f)
			}
		}
	}
}

func TestParseMotionEventAndProgress(t *testing.T) {
	for _, l := range []string{"act3-motion-video: oops", "", "{not json", `{"step":"combine"}`} {
		if _, ok := ParseMotionEvent(l); ok {
			t.Errorf("%q is not an event", l)
		}
	}
	e, ok := ParseMotionEvent(`  {"event":"progress","step":"combine","done":45,"total":90}`)
	if !ok || e.Event != "progress" || e.Step != "combine" || e.Done != 45 || e.Total != 90 {
		t.Fatalf("parsed %+v %v", e, ok)
	}

	clock := time.Unix(0, 0)
	p := newLayersProgress(2)
	p.now = func() time.Time { return clock }
	type step struct {
		line   string
		adv    time.Duration
		report bool
	}
	var fr []float64
	for _, s := range []step{
		{`{"event":"progress","step":"html_render","done":5,"total":10}`, 0, true}, // first one always goes
		{`{"event":"progress","step":"html_render","done":9,"total":10}`, 200 * time.Millisecond, false},
		{`{"event":"step","step":"html_render","id":"lt","cached":true}`, 100 * time.Millisecond, true}, // a finished step always goes
		{`{"event":"step","step":"html_render","id":"lt2"}`, 0, true},
		{`{"event":"progress","step":"combine","done":1,"total":100}`, 300 * time.Millisecond, false},
		{`{"event":"progress","step":"combine","done":50,"total":100}`, 1100 * time.Millisecond, true},
		{`{"event":"progress","step":"html_render","done":0,"total":10}`, 1100 * time.Millisecond, true}, // late html line: never backwards
		{`{"event":"progress","step":"combine","done":100,"total":100}`, 1100 * time.Millisecond, true},
		{`{"event":"step","step":"join"}`, 0, true},
	} {
		clock = clock.Add(s.adv)
		ev, _ := ParseMotionEvent(s.line)
		f, _, rep := p.Update(ev)
		if rep != s.report {
			t.Errorf("%s: report %v, want %v", s.line, rep, s.report)
		}
		fr = append(fr, f)
	}
	for i := 1; i < len(fr); i++ {
		if fr[i] < fr[i-1] {
			t.Fatalf("progress went backwards: %v", fr)
		}
	}
	if fr[3] != 0.35 || fr[5] < 0.6 || fr[5] > 0.65 || fr[7] != 0.90 || fr[8] != 0.98 {
		t.Fatalf("fractions %v", fr)
	}
}

func TestLayersTimeLimit(t *testing.T) {
	cases := []struct {
		d    float64
		html int
		want time.Duration
	}{
		{0, 0, 5 * time.Minute},                       // floor
		{10, 0, 8 * time.Minute},                      // 180 + 10×30 = 480 s
		{10, 3, 18 * time.Minute},                     // 180 + 10×(30+60) = 1080 s
		{60, 6, 180*time.Second + 60*150*time.Second}, // 9 180 s
		{3600, 20, 4 * time.Hour},                     // ceiling
	}
	for _, c := range cases {
		if got := LayersTimeLimit(c.d, c.html); got != c.want {
			t.Errorf("LayersTimeLimit(%v, %d) = %s, want %s", c.d, c.html, got, c.want)
		}
	}
	if LayersTimeLimit(10, 3) <= LayersTimeLimit(10, 0) || LayersTimeLimit(20, 1) <= LayersTimeLimit(10, 1) {
		t.Fatal("the limit must grow with HTML layers and output seconds")
	}
}

type layersRep struct {
	mu    sync.Mutex
	prog  []float64
	logs  []string
	stage []string
}

func (r *layersRep) Stage(s string) { r.mu.Lock(); r.stage = append(r.stage, s); r.mu.Unlock() }
func (r *layersRep) Progress(f float64, _ string) {
	r.mu.Lock()
	r.prog = append(r.prog, f)
	r.mu.Unlock()
}
func (r *layersRep) Log(l string) { r.mu.Lock(); r.logs = append(r.logs, l); r.mu.Unlock() }

// fakeMotion writes a fake act3-motion-video (body is the shell after argument parsing) and act3-motion-audio.
func fakeMotion(t *testing.T, body string) map[string]*hardware.Engine {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell fakes")
	}
	dir := t.TempDir()
	video := filepath.Join(dir, "act3-motion-video")
	script := `#!/bin/sh
cmd=$1; shift
while [ $# -gt 0 ]; do
  case $1 in --plan) plan=$2;; --out) out=$2;; --audio-bin) audio=$2;; --cache) cache=$2;; esac; shift 2
done
echo "$0" > "$(dirname "$plan")/ran_from"
` + body
	if err := os.WriteFile(video, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	audio := filepath.Join(dir, "act3-motion-audio")
	if err := os.WriteFile(audio, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	e := layersEngines()
	e["act3_motion_video"], e["act3_motion_audio"] = &hardware.Engine{Path: video}, &hardware.Engine{Path: audio}
	e["ffmpeg"], e["ffprobe"], e["node"] = &hardware.Engine{Path: "/bin/sh"}, &hardware.Engine{Path: "/bin/sh"}, &hardware.Engine{Path: "/bin/sh"}
	return e
}

func layersFakeEnv(t *testing.T, engines map[string]*hardware.Engine) (Env, *jobs.Job) {
	t.Helper()
	work := t.TempDir()
	out := filepath.Join(work, "out")
	in := filepath.Join(work, "in")
	_ = os.MkdirAll(out, 0o700)
	_ = os.MkdirAll(in, 0o700)
	inputs := map[string]string{}
	var names []string
	for i, a := range []string{"a1.mp4", "a2.mp4", "a3.html", "a4.png", "a5.wav"} {
		id := strings.TrimSuffix(a, filepath.Ext(a))
		p := filepath.Join(in, a)
		_ = os.WriteFile(p, []byte{byte(i)}, 0o600)
		inputs[id] = p
		names = append(names, id)
	}
	t.Setenv("ACT3_MOTION_CACHE_DIR", filepath.Join(work, "cache"))
	t.Setenv("ACT3_MOTION_PROFILE", "")
	j := layersJobWith(t, map[string]any{"renderTask": map[string]any{"plan": layersTestPlan(), "htmlLayers": 1, "durationSeconds": 2,
		"assets": []map[string]any{{"id": "a1", "role": "video", "name": "broll.mp4"}, {"id": "a2", "role": "video", "name": "base.mp4"},
			{"id": "a3", "role": "html_file", "name": "index.html"}, {"id": "a4", "role": "image", "name": "logo.png"},
			{"id": "a5", "role": "audio", "name": "tone.wav"}}}}, names...)
	return Env{WorkDir: work, OutDir: out, Inputs: inputs, Engines: engines}, j
}

func TestLayersRunWithFakeBinary(t *testing.T) {
	engines := fakeMotion(t, `
cp "$plan" "$(dirname "$plan")/../plan_seen.json"
echo '{"event":"progress","step":"combine","done":30,"total":60}' >&2
echo 'a plain stderr line' >&2
echo '{"event":"step","step":"join"}' >&2
printf 'MP4' > "$out"
echo '{"ok":true,"frames":60,"audio":{"ok":true}}'
`)
	env, job := layersFakeEnv(t, engines)
	rep := &layersRep{}
	res, err := (LayersRender{}).Run(context.Background(), job, env, rep)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Outputs) != 1 || res.Outputs[0].Name != "video" || res.Outputs[0].ContentType != "video/mp4" ||
		res.Outputs[0].Path != filepath.Join(env.OutDir, "final.mp4") {
		t.Fatalf("outputs %+v", res.Outputs)
	}
	sum := res.Data.(map[string]any)["summary"].(map[string]any)
	if sum["ok"] != true || sum["frames"] != float64(60) {
		t.Fatalf("summary %v", sum)
	}
	// the plan the binary saw has local absolute paths, never asset refs
	b, err := os.ReadFile(filepath.Join(env.WorkDir, "plan_seen.json"))
	if err != nil {
		t.Fatal(err)
	}
	var seen map[string]any
	_ = json.Unmarshal(b, &seen)
	if left := AssetRefIDs(seen); len(left) != 0 {
		t.Fatalf("plan still has refs %v", left)
	}
	if !strings.Contains(string(b), filepath.Join(env.WorkDir, "layers", "assets", "broll.mp4")) {
		t.Fatalf("plan does not point at the job's assets: %s", b)
	}
	// the job folder (binaries, assets, plan, work) is gone; the output stays
	if _, err := os.Stat(filepath.Join(env.WorkDir, "layers")); !os.IsNotExist(err) {
		t.Fatalf("job folder left behind: %v", err)
	}
	if len(rep.prog) < 2 || rep.prog[len(rep.prog)-1] != 1 {
		t.Fatalf("progress %v", rep.prog)
	}
	joined := strings.Join(rep.logs, "\n")
	if !strings.Contains(joined, "/layers/bin/act3-motion-video run --plan") || !strings.Contains(joined, "a plain stderr line") {
		t.Fatalf("logs:\n%s", joined)
	}
}

// The binary runs from the job's own copy: rebuilding or deleting the source binary mid-job changes nothing.
func TestLayersRunUsesFrozenCopy(t *testing.T) {
	engines := fakeMotion(t, `
cp "$(dirname "$plan")/ran_from" "$(dirname "$plan")/../ran_from"
printf 'MP4' > "$out"
echo '{"ok":true}'
`)
	env, job := layersFakeEnv(t, engines)
	if _, err := (LayersRender{}).Run(context.Background(), job, env, &layersRep{}); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(env.WorkDir, "ran_from"))
	if got := strings.TrimSpace(string(b)); got != filepath.Join(env.WorkDir, "layers", "bin", "act3-motion-video") {
		t.Fatalf("ran from %q, not the job's copy", got)
	}
}

func TestLayersRunFailureCarriesErrorAndStderr(t *testing.T) {
	for _, c := range []struct {
		code int
		kind jobs.ErrorKind
	}{{2, jobs.KindInvalidInput}, {3, jobs.KindTransient}} {
		engines := fakeMotion(t, `
echo 'act3-motion-video: combine: ffmpeg died' >&2
echo '{"ok":false,"error":"combine: ffmpeg died"}'
exit `+strconv.Itoa(c.code)+`
`)
		env, job := layersFakeEnv(t, engines)
		_, err := (LayersRender{}).Run(context.Background(), job, env, &layersRep{})
		var je *jobs.Error
		if !errors.As(err, &je) || je.Kind != c.kind {
			t.Fatalf("exit %d: want %s, got %v", c.code, c.kind, err)
		}
		if !strings.Contains(err.Error(), "combine: ffmpeg died | stderr: act3-motion-video: combine: ffmpeg died") {
			t.Fatalf("exit %d: message %v", c.code, err)
		}
	}
}

// pidAlive reports whether a process exists (signal 0).
func pidAlive(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return exec.Command("kill", "-0", itoa(pid)).Run() == nil && p != nil
}

func itoa(n int) string { return strconv.Itoa(n) }

// a child that would outlive its parent (Chromium, FFmpeg) is killed with the group on cancel and at the time limit
func TestLayersRunCancelAndTimeLimitKillTheGroup(t *testing.T) {
	// the fake's shell ignores the interrupt for its background child, so the group kill after the grace does the work
	oldGrace := ProfileLayers.Grace
	ProfileLayers.Grace = time.Second
	defer func() { ProfileLayers.Grace = oldGrace }()
	body := `
sleep 300 &
echo $! > "$(dirname "$plan")/../child.pid"
echo '{"event":"progress","step":"combine","done":1,"total":60}' >&2
wait
`
	waitPid := func(env Env) int {
		for i := 0; i < 100; i++ {
			if b, err := os.ReadFile(filepath.Join(env.WorkDir, "child.pid")); err == nil && len(strings.TrimSpace(string(b))) > 0 {
				var pid int
				_ = json.Unmarshal([]byte(strings.TrimSpace(string(b))), &pid)
				return pid
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("the fake never started its child")
		return 0
	}
	assertGone := func(pid int) {
		for i := 0; i < 100 && pidAlive(pid); i++ {
			time.Sleep(50 * time.Millisecond)
		}
		if pidAlive(pid) {
			_ = exec.Command("kill", "-9", itoa(pid)).Run()
			t.Fatalf("child %d survived", pid)
		}
	}

	t.Run("cancel", func(t *testing.T) {
		env, job := layersFakeEnv(t, fakeMotion(t, body))
		ctx, cancel := context.WithCancelCause(context.Background())
		done := make(chan error, 1)
		go func() { _, err := (LayersRender{}).Run(ctx, job, env, &layersRep{}); done <- err }()
		pid := waitPid(env)
		cancel(errors.New("cancelled by test"))
		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "cancelled by test") {
				t.Fatalf("want the cancel cause, got %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("cancel did not stop the render")
		}
		assertGone(pid)
	})

	t.Run("time limit", func(t *testing.T) {
		old := layersLimitFor
		layersLimitFor = func(float64, int) time.Duration { return 700 * time.Millisecond }
		defer func() { layersLimitFor = old }()
		env, job := layersFakeEnv(t, fakeMotion(t, body))
		t0 := time.Now()
		_, err := (LayersRender{}).Run(context.Background(), job, env, &layersRep{})
		var je *jobs.Error
		if !errors.As(err, &je) || je.Kind != jobs.KindTimeout {
			t.Fatalf("want timeout, got %v", err)
		}
		if time.Since(t0) > 30*time.Second {
			t.Fatal("the time limit did not stop the render in time")
		}
		if _, err := os.Stat(filepath.Join(env.WorkDir, "layers")); !os.IsNotExist(err) {
			t.Fatal("job folder left behind after the time limit")
		}
		b, _ := os.ReadFile(filepath.Join(env.WorkDir, "child.pid"))
		var pid int
		_ = json.Unmarshal([]byte(strings.TrimSpace(string(b))), &pid)
		assertGone(pid)
	})
}

// realMotionEngines finds the real render tasks ($ACT3_MOTION_BIN_DIR or render_tasks/target/release in ~/BGit/all or
// ~/repos/all), FFmpeg, FFprobe, Node and a Chromium; ok is false when a must-have is missing.
func realMotionEngines(t *testing.T) (map[string]*hardware.Engine, bool) {
	t.Helper()
	e := map[string]*hardware.Engine{}
	for _, n := range []string{"act3-motion-video", "act3-motion-audio"} {
		for _, c := range hardware.MotionBinCandidates(n) {
			if p := expandTilde(c); fileAtLeast(p, 1) {
				e[strings.ReplaceAll(n, "-", "_")] = &hardware.Engine{Path: p}
				break
			}
		}
	}
	for _, n := range []string{"ffmpeg", "ffprobe", "node"} {
		if p, err := exec.LookPath(n); err == nil {
			e[n] = &hardware.Engine{Path: p}
		}
	}
	for _, c := range hardware.ChromiumCandidates() {
		if fileAtLeast(c, 1) {
			e["chromium"] = &hardware.Engine{Path: c}
			break
		}
	}
	for _, n := range []string{"act3_motion_video", "act3_motion_audio", "ffmpeg", "ffprobe", "node"} {
		if e[n] == nil {
			t.Logf("skipped: %s not found (build ~/BGit/all/film/code/render_tasks or set ACT3_MOTION_BIN_DIR)", n)
			return nil, false
		}
	}
	return e, true
}

// The real act3-motion-video + act3-motion-audio on a tiny plan: a 1 s picture with a tone, through the asset
// rewrite, the frozen binaries and the progress lines, to an MP4 with video and audio. With a Chromium, a lower
// third HTML layer too (its template and the HTML runner come from the job's frozen copy).
func TestLayersRunRealBinaries(t *testing.T) {
	if testing.Short() {
		t.Skip("real render")
	}
	engines, ok := realMotionEngines(t)
	if !ok {
		t.Skip("act3-motion-video / act3-motion-audio / ffmpeg / node not installed")
	}
	ff := engines["ffmpeg"].Path
	in := t.TempDir()
	base, tone := filepath.Join(in, "a1.mp4"), filepath.Join(in, "a2.wav")
	for _, args := range [][]string{
		{"-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "testsrc2=s=160x90:r=30:d=2", "-pix_fmt", "yuv420p", "-c:v", "libx264", base},
		{"-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000:duration=2", "-ac", "2", tone},
	} {
		if out, err := exec.Command(ff, args...).CombinedOutput(); err != nil {
			t.Fatalf("ffmpeg %v: %v %s", args, err, out)
		}
	}
	const f = 705_600
	steps := []any{
		map[string]any{"id": "combine", "kind": "combine", "layers": []any{
			map[string]any{"z": 10, "clipId": "base", "source": map[string]any{"url": "asset:a1"}, "alpha": "opaque"}}},
		map[string]any{"id": "audio", "kind": "audio_mix", "plan": map[string]any{"version": 2, "inputs": []any{
			map[string]any{"id": "tone", "source": map[string]any{"url": "asset:a2"}, "outFlicks": 1000 * f, "gainDb": -6}}}},
		map[string]any{"id": "join", "kind": "join", "after": []any{"combine", "audio"}},
	}
	html := 0
	if engines["chromium"] != nil {
		html = 1
		steps = append([]any{map[string]any{"id": "lt", "kind": "html_render", "template": "lower_third@1",
			"fields": map[string]any{"text": "asset: not a reference"}}}, steps...)
		layers := steps[1].(map[string]any)["layers"].([]any)
		steps[1].(map[string]any)["layers"] = append([]any{map[string]any{"z": 40, "clipId": "lt", "source": map[string]any{"step": "lt"}}}, layers...)
	} else {
		t.Log("no Chromium: the HTML layer part is skipped")
	}
	plan := map[string]any{"version": 1, "shotId": "aps_layers_test",
		"output": map[string]any{"width": 160, "height": 90, "fps": "30/1", "lengthFlicks": 1000 * f, "codec": "h264", "encoder": "software"},
		"steps":  steps}
	work := t.TempDir()
	out := filepath.Join(work, "out")
	_ = os.MkdirAll(out, 0o700)
	t.Setenv("ACT3_MOTION_CACHE_DIR", filepath.Join(work, "cache"))
	t.Setenv("ACT3_MOTION_PROFILE", "")
	job := layersJobWith(t, map[string]any{"render_type": "layers_render", "renderTask": map[string]any{
		"renderType": "layers_render", "renderId": "r_test", "shotId": "s_test", "durationSeconds": 1, "htmlLayers": html, "plan": plan,
		"assets": []map[string]any{{"id": "a1", "role": "video", "name": "base.mp4"}, {"id": "a2", "role": "audio", "name": "tone.wav"}}}},
		"video_0", "audio_1")
	env := Env{WorkDir: work, OutDir: out, Inputs: map[string]string{"video_0": base, "audio_1": tone}, Engines: engines}
	if err := (LayersRender{}).Check(context.Background(), job, env); err != nil {
		t.Fatal(err)
	}
	rep := &layersRep{}
	res, err := (LayersRender{}).Run(context.Background(), job, env, rep)
	if err != nil {
		t.Fatalf("%v\nlog:\n%s", err, strings.Join(rep.logs, "\n"))
	}
	sum := res.Data.(map[string]any)["summary"].(map[string]any)
	if sum["ok"] != true {
		t.Fatalf("summary %v", sum)
	}
	probe, err := exec.Command(engines["ffprobe"].Path, "-v", "error", "-show_entries", "stream=codec_type:format=duration", "-of", "json", res.Outputs[0].Path).Output()
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Streams []struct {
			CodecType string `json:"codec_type"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	_ = json.Unmarshal(probe, &doc)
	kinds := map[string]bool{}
	for _, s := range doc.Streams {
		kinds[s.CodecType] = true
	}
	d, _ := strconv.ParseFloat(doc.Format.Duration, 64)
	if !kinds["video"] || !kinds["audio"] || d < 0.9 || d > 1.2 {
		t.Fatalf("final.mp4: streams %v, duration %v (%s)", kinds, d, probe)
	}
	if len(rep.prog) < 2 {
		t.Fatalf("no progress forwarded: %v", rep.prog)
	}
	t.Logf("rendered with %d HTML layer(s): summary %v", html, sum)
}

// An asset id from the plan never steers where a file is written: duplicate names and nameless assets use the id in
// the file name, and an id like "../../x" must stay inside the assets folder.
func TestPlaceLayersAssetsHostileIDs(t *testing.T) {
	in := t.TempDir()
	mk := func(name string) string {
		p := filepath.Join(in, name)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	assets := []LayersAsset{
		{ID: "ok", Role: "image", Name: "pic.png"},
		{ID: "../../escape", Role: "image", Name: "pic.png"}, // duplicate name -> id prefix
		{ID: "/abs/../../escape2", Role: "image", Name: ""},  // nameless -> <id>.bin
	}
	inputs := map[string]string{"ok": mk("ok.png"), "../../escape": mk("e.png"), "/abs/../../escape2": mk("e2.png")}
	dir := filepath.Join(t.TempDir(), "assets")
	paths, err := PlaceLayersAssets(assets, inputs, dir)
	if err != nil {
		t.Fatal(err)
	}
	for id, p := range paths {
		if filepath.Dir(p) != dir {
			t.Errorf("asset %q placed at %q, outside %s", id, p, dir)
		}
	}
}
