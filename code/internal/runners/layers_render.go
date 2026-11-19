package runners

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/procs"
)

// LayersRender is the runner behind act3.layers_render: one ACT3 "AI Video Layers" shot render (motion.mdx §39, §54,
// §64.5). It runs the two Rust render tasks built from ~/BGit/all/film/code/render_tasks (act3-motion-video, which
// starts act3-motion-audio itself) on a render plan whose every media reference is `asset:<id>`.
//
// The contract is appsrv's ~/BGit/all/film/code/p/act3ai-appsrv/src/modules/layers-render/layers-render.contract.ts
// (LayersRenderTask). The params carry the plan and the asset list, as `params.renderTask.{plan, assets, htmlLayers,
// durationSeconds}` (the job's own renderTask, passed through), or flat as `params.{plan | layersPlan, assets,
// htmlLayers, durationSeconds}`. Every asset arrives as one staged input: named by the asset id (`a1`), by
// `<role>_<index>` (the internal web app's render engine, film/servers processing/tasks.ts envelopeFor), or by the
// asset's file name.
//
// Steps: copy the binaries (and the HTML runner + templates) into the job directory, so a git pull or a cargo build
// mid-job never changes what this job runs; place every asset in the job directory under its own name (extensions
// matter to FFmpeg and Chromium); rewrite each `asset:<id>` to that absolute path; write plan.json; run
//
//	act3-motion-video run --plan <job>/plan.json --out <out>/final.mp4 --work <job>/work --cache <cache>
//	  --audio-bin <job>/bin/act3-motion-audio [--profile hardware_profile.json] --ffmpeg … --ffprobe … --node …
//	  [--chrome …] [--templates <job>/motion_video/templates --runner <job>/motion_video/html_runner/render.mjs]
//
// and forward its JSON progress lines (stderr) as progress, at most about once a second. One output `video`
// (video/mp4); the job's result is `{summary: <act3-motion-video's stdout JSON>}`, which the engine sends as
// `complete.body.summary`. Exit 2 (bad plan) → invalid_input, exit 3 → transient, with the error and the last stderr
// lines. A time limit scaled by output seconds and HTML layer count (LayersTimeLimit) stops a wedged render; cancel
// and the limit kill the whole process group (Node, Chromium and FFmpeg children too). Not detached: like FFmpeg, a
// controller restart renders again (the HTML layer cache makes a re-run cheap).
type LayersRender struct{}

// ProfileLayers: act3-motion-video stops its children on the interrupt; Chromium needs a moment to close.
var ProfileLayers = CancelProfile{Name: "layers_render", Grace: 10 * time.Second}

// AssetRef is the prefix of every media reference in a layers plan (layers-render.contract.ts ASSET_REF).
const AssetRef = "asset:"

// FlicksPerSecond is the plan's time unit (motion_plan timing.rs).
const FlicksPerSecond = 705_600_000

func (LayersRender) Name() string { return "layers_render" }

// LayersAsset is one input the plan names (layers-render.contract.ts LayersRenderAsset; `key` is never sent).
type LayersAsset struct {
	ID          string  `json:"id"`
	Role        string  `json:"role"`
	Name        string  `json:"name"`
	ContentType *string `json:"contentType"`
	Bytes       *int64  `json:"bytes"`
}

type layersTask struct {
	RenderID        string          `json:"renderId"`
	ShotID          string          `json:"shotId"`
	DurationSeconds float64         `json:"durationSeconds"`
	Plan            json.RawMessage `json:"plan"`
	LayersPlan      json.RawMessage `json:"layersPlan"`
	Assets          []LayersAsset   `json:"assets"`
	HTMLLayers      *int            `json:"htmlLayers"`
}

type layersParams struct {
	layersTask
	RenderTask *layersTask `json:"renderTask"`
}

// layersJob is the job's params, resolved.
type layersJob struct {
	RenderID        string
	ShotID          string
	Plan            map[string]any
	Assets          []LayersAsset
	HTMLLayers      int
	DurationSeconds float64
}

var assetIDRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

func parseLayersParams(job *jobs.Job) (layersJob, error) {
	var p layersParams
	if err := decodeParams(job, &p); err != nil {
		return layersJob{}, err
	}
	t := p.layersTask
	if rt := p.RenderTask; rt != nil {
		// the job's own renderTask wins; flat fields fill its gaps
		if len(rt.Plan) == 0 {
			rt.Plan = firstRaw(rt.LayersPlan, t.Plan, t.LayersPlan)
		}
		if rt.Assets == nil {
			rt.Assets = t.Assets
		}
		if rt.HTMLLayers == nil {
			rt.HTMLLayers = t.HTMLLayers
		}
		if rt.DurationSeconds == 0 {
			rt.DurationSeconds = t.DurationSeconds
		}
		if rt.RenderID == "" {
			rt.RenderID = t.RenderID
		}
		if rt.ShotID == "" {
			rt.ShotID = t.ShotID
		}
		t = *rt
	}
	raw := firstRaw(t.Plan, t.LayersPlan)
	if len(raw) == 0 || string(raw) == "null" {
		return layersJob{}, jobs.Errf(jobs.KindInvalidInput, "a layers render needs its plan (params.renderTask.plan)")
	}
	var plan map[string]any
	if err := json.Unmarshal(raw, &plan); err != nil {
		return layersJob{}, jobs.Wrap(jobs.KindInvalidInput, err, "the layers plan is not a JSON object")
	}
	if _, ok := plan["steps"].([]any); !ok {
		return layersJob{}, jobs.Errf(jobs.KindInvalidInput, "the layers plan has no steps")
	}
	seen := map[string]bool{}
	for i, a := range t.Assets {
		if !assetIDRe.MatchString(a.ID) {
			return layersJob{}, jobs.Errf(jobs.KindInvalidInput, "asset %d has a bad id %q", i, a.ID)
		}
		if seen[a.ID] {
			return layersJob{}, jobs.Errf(jobs.KindInvalidInput, "asset id %q is listed twice", a.ID)
		}
		seen[a.ID] = true
	}
	lj := layersJob{RenderID: t.RenderID, ShotID: t.ShotID, Plan: plan, Assets: t.Assets, DurationSeconds: t.DurationSeconds}
	if t.HTMLLayers != nil {
		lj.HTMLLayers = max(*t.HTMLLayers, 0)
	} else {
		lj.HTMLLayers = countHTMLSteps(plan)
	}
	if lj.DurationSeconds <= 0 {
		lj.DurationSeconds = planSeconds(plan)
	}
	return lj, nil
}

func firstRaw(rs ...json.RawMessage) json.RawMessage {
	for _, r := range rs {
		if len(r) > 0 && string(r) != "null" {
			return r
		}
	}
	return nil
}

func countHTMLSteps(plan map[string]any) int {
	n := 0
	steps, _ := plan["steps"].([]any)
	for _, s := range steps {
		if m, ok := s.(map[string]any); ok && m["kind"] == "html_render" {
			n++
		}
	}
	return n
}

// planSeconds is output.lengthFlicks in seconds (0 when the plan does not say).
func planSeconds(plan map[string]any) float64 {
	out, _ := plan["output"].(map[string]any)
	if f, ok := out["lengthFlicks"].(float64); ok && f > 0 {
		return f / FlicksPerSecond
	}
	return 0
}

// AssetRefIDs lists every asset id the plan references (`asset:<id>` anywhere), sorted, without duplicates.
func AssetRefIDs(plan any) []string {
	set := map[string]bool{}
	walkStrings(plan, func(s string) string {
		if id, ok := assetRefID(s); ok {
			set[id] = true
		}
		return s
	})
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// assetRefID is the id of an `asset:<id>` reference; a string that only starts with "asset:" (prose in a field such
// as "asset: the map") is not one.
func assetRefID(s string) (string, bool) {
	id, ok := strings.CutPrefix(s, AssetRef)
	if !ok || !assetIDRe.MatchString(id) {
		return "", false
	}
	return id, true
}

// RewriteAssetRefs replaces every `asset:<id>` string in the plan (layers[].source.url / .path, bundle.files[].url,
// audio_mix plan.inputs[].source.url, join.picture.url, and anywhere else) with paths[id], in place. Other strings,
// bundle files' relative `path` included, are kept. An id with no path is an error naming every such id.
func RewriteAssetRefs(plan any, paths map[string]string) error {
	var missing []string
	walkStrings(plan, func(s string) string {
		id, ok := assetRefID(s)
		if !ok {
			return s
		}
		if p, ok := paths[id]; ok {
			return p
		}
		missing = append(missing, id)
		return s
	})
	if len(missing) > 0 {
		sort.Strings(missing)
		return jobs.Errf(jobs.KindInvalidInput, "the plan names asset(s) the job did not send: %s", strings.Join(dedupe(missing), ", "))
	}
	return nil
}

func dedupe(s []string) []string {
	var out []string
	for i, v := range s {
		if i == 0 || v != s[i-1] {
			out = append(out, v)
		}
	}
	return out
}

// walkStrings calls fn on every string value (not key) of a decoded JSON value and stores what it returns.
func walkStrings(v any, fn func(string) string) {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			if s, ok := x.(string); ok {
				t[k] = fn(s)
			} else {
				walkStrings(x, fn)
			}
		}
	case []any:
		for i, x := range t {
			if s, ok := x.(string); ok {
				t[i] = fn(s)
			} else {
				walkStrings(x, fn)
			}
		}
	}
}

var unsafeNameRe = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// SafeAssetName is the file name an asset is saved as: its own name's base, cleaned; `<id>.bin` when it has none.
func SafeAssetName(a LayersAsset) string {
	n := unsafeNameRe.ReplaceAllString(filepath.Base(strings.ReplaceAll(a.Name, `\`, "/")), "_")
	n = strings.TrimLeft(n, ".")
	if n == "" || n == "_" {
		return safeIDPart(a.ID) + ".bin"
	}
	return n
}

// safeIDPart is an asset id made safe to use inside a file name: an id from the plan is not trusted to be a bare
// token, so `../x` or `/abs` can never steer where an asset is written (security pass 2026-11-19, WEB-N2-1 follow-up).
func safeIDPart(id string) string {
	n := strings.TrimLeft(unsafeNameRe.ReplaceAllString(strings.ReplaceAll(id, `\`, "/"), "_"), "._")
	if n == "" {
		return "asset"
	}
	return n
}

// StagedInputFor finds the staged input of asset i: by its id, by `<role>_<i>` (the render engine's naming), by its
// name (with or without the extension), then by the staged file's base name.
func StagedInputFor(a LayersAsset, i int, inputs map[string]string) string {
	name := SafeAssetName(a)
	for _, k := range []string{a.ID, fmt.Sprintf("%s_%d", a.Role, i), a.Name, name, strings.TrimSuffix(name, filepath.Ext(name))} {
		if k == "" {
			continue
		}
		if p, ok := inputs[k]; ok {
			return p
		}
	}
	for _, p := range inputs {
		if filepath.Base(p) == name {
			return p
		}
	}
	return ""
}

// PlaceLayersAssets puts every asset into dir under its own name (a hard link, else a copy) and returns id → absolute
// path. Without an asset list, every staged input is an asset whose id is the input's name. Two assets with one name
// get `<id>_<name>`.
func PlaceLayersAssets(assets []LayersAsset, inputs map[string]string, dir string) (map[string]string, error) {
	if len(assets) == 0 {
		for name := range inputs {
			assets = append(assets, LayersAsset{ID: name, Name: filepath.Base(inputs[name])})
		}
		sort.Slice(assets, func(i, j int) bool { return assets[i].ID < assets[j].ID })
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, jobs.Wrap(jobs.KindInfra, err, "create the assets folder")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, jobs.Wrap(jobs.KindInternal, err, "assets folder path")
	}
	out := map[string]string{}
	used := map[string]bool{}
	for i, a := range assets {
		src := StagedInputFor(a, i, inputs)
		if src == "" {
			return nil, jobs.Errf(jobs.KindInvalidInput, "asset %s (%s, %s) was not staged: no input named %s or %s_%d", a.ID, a.Role, a.Name, a.ID, a.Role, i)
		}
		name := SafeAssetName(a)
		if used[name] {
			name = safeIDPart(a.ID) + "_" + name
		}
		used[name] = true
		dst := filepath.Join(abs, name)
		if filepath.Dir(dst) != abs {
			return nil, jobs.Errf(jobs.KindInvalidInput, "asset %q would be written outside the assets folder", a.ID)
		}
		if err := linkOrCopy(src, dst); err != nil {
			return nil, jobs.Wrap(jobs.KindInfra, err, "place asset "+a.ID)
		}
		if a.Bytes != nil && *a.Bytes > 0 {
			if st, err := os.Stat(dst); err == nil && st.Size() != *a.Bytes {
				return nil, jobs.Errf(jobs.KindInvalidInput, "asset %s arrived with %d bytes, production said %d", a.ID, st.Size(), *a.Bytes)
			}
		}
		out[a.ID] = dst
	}
	return out, nil
}

// LayersTimeLimit is how long one layers render may run: 3 minutes of start-up and join, plus per output second
// 30 s of combine/encode and 20 s per HTML layer (Chromium screenshots every frame), clamped to 5 min .. 4 h. Generous
// on purpose: it exists to stop a wedged render, not to race a slow one (a 10 s shot with 3 HTML layers gets 18 min).
func LayersTimeLimit(durationSeconds float64, htmlLayers int) time.Duration {
	d := math.Max(durationSeconds, 1)
	secs := 180 + d*(30+20*float64(max(htmlLayers, 0)))
	return time.Duration(math.Min(math.Max(secs, 300), 4*3600)) * time.Second
}

// LayersBins is what one layers render runs, all copied into the job directory.
type LayersBins struct {
	Video, Audio string // act3-motion-video, act3-motion-audio
	FFmpeg       string
	FFprobe      string
	Node         string
	Chrome       string // "" = act3-motion-video looks itself
	Templates    string // "" = the binary's built-in default
	Runner       string // html_runner/render.mjs; "" = default
	Profile      string // hardware_profile.json; "" = none
}

// LayersCommand is act3-motion-video's argument list (without the program) for one job directory.
func LayersCommand(b LayersBins, jobDir, outPath, cacheDir string) []string {
	args := []string{"run", "--plan", filepath.Join(jobDir, "plan.json"), "--out", outPath, "--work", filepath.Join(jobDir, "work"),
		"--cache", cacheDir, "--audio-bin", b.Audio}
	for _, kv := range [][2]string{{"--profile", b.Profile}, {"--ffmpeg", b.FFmpeg}, {"--ffprobe", b.FFprobe}, {"--node", b.Node},
		{"--chrome", b.Chrome}, {"--templates", b.Templates}, {"--runner", b.Runner}} {
		if kv[1] != "" {
			args = append(args, kv[0], kv[1])
		}
	}
	return args
}

// MotionEvent is one JSON line act3-motion-video prints on stderr.
type MotionEvent struct {
	Event  string  `json:"event"` // progress | step
	Step   string  `json:"step"`  // html_render | combine | audio | join | …
	ID     string  `json:"id"`
	Done   float64 `json:"done"`
	Total  float64 `json:"total"`
	Cached bool    `json:"cached"`
	MS     float64 `json:"ms"`
}

// ParseMotionEvent reads one stderr line; ok is false for anything that is not a JSON event.
func ParseMotionEvent(line string) (MotionEvent, bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "{") {
		return MotionEvent{}, false
	}
	var e MotionEvent
	if json.Unmarshal([]byte(line), &e) != nil || e.Event == "" {
		return MotionEvent{}, false
	}
	return e, true
}

// layersProgress turns events into one rising fraction: html_render steps fill 0.05-0.35 (when the plan has any),
// combine the rest up to 0.90, join 0.90-0.98. The audio job runs beside the combine and only adds a note.
type layersProgress struct {
	html      int // html_render steps in the plan
	htmlDone  int
	frac      float64
	last      time.Time
	every     time.Duration
	now       func() time.Time
	lastNote  string
	lastShown float64
}

func newLayersProgress(htmlLayers int) *layersProgress {
	return &layersProgress{html: htmlLayers, frac: 0.05, every: time.Second, now: time.Now}
}

// Update folds one event in; it returns the fraction, a note and whether to report now (throttled to `every`, but a
// finished step is always reported).
func (p *layersProgress) Update(e MotionEvent) (float64, string, bool) {
	combineFrom := 0.05
	if p.html > 0 {
		combineFrom = 0.35
	}
	f, note, force := p.frac, "", false
	ratio := 0.0
	if e.Total > 0 {
		ratio = math.Min(math.Max(e.Done/e.Total, 0), 1)
	}
	switch {
	case e.Event == "step" && strings.HasPrefix(e.Step, "html"):
		p.htmlDone++
		if p.html > 0 {
			f = 0.05 + 0.30*math.Min(float64(p.htmlDone)/float64(p.html), 1)
		}
		note, force = fmt.Sprintf("HTML layer %d/%d rendered", p.htmlDone, max(p.html, p.htmlDone)), true
		if e.Cached {
			note += " (cached)"
		}
	case e.Event == "progress" && strings.HasPrefix(e.Step, "html"):
		if p.html > 0 {
			f = 0.05 + 0.30*math.Min((float64(p.htmlDone)+ratio)/float64(p.html), 1)
		}
		note = fmt.Sprintf("HTML layer %d/%d: %.0f/%.0f frames", p.htmlDone+1, max(p.html, p.htmlDone+1), e.Done, e.Total)
	case e.Event == "progress" && e.Step == "combine":
		f = combineFrom + (0.90-combineFrom)*ratio
		note = fmt.Sprintf("combining layers: %.0f/%.0f frames", e.Done, e.Total)
	case e.Event == "progress" && e.Step == "join", e.Event == "step" && e.Step == "join":
		f = 0.90 + 0.08*ratio
		if e.Event == "step" {
			f, force = 0.98, true
		}
		note = "joining picture and sound"
	case e.Event == "step":
		note, force = e.Step+" done", true
	default:
		note = e.Step
		if e.Total > 0 {
			note = fmt.Sprintf("%s: %.0f/%.0f", e.Step, e.Done, e.Total)
		}
	}
	p.frac = math.Max(p.frac, math.Min(f, 0.99)) // never goes backwards
	p.lastNote = note
	now := p.now()
	if !force && !p.last.IsZero() && now.Sub(p.last) < p.every {
		return p.frac, note, false
	}
	p.last, p.lastShown = now, p.frac
	return p.frac, note, true
}

// layersCacheDir: $ACT3_MOTION_CACHE_DIR, else <paths.cache_dir>/layers_render, else the user cache folder. The HTML
// layer cache is keyed by content hash, so jobs share it safely.
func layersCacheDir(env Env) string {
	if d := strings.TrimSpace(os.Getenv("ACT3_MOTION_CACHE_DIR")); d != "" {
		return expandTilde(d)
	}
	if env.CacheDir != "" {
		return filepath.Join(env.CacheDir, "layers_render")
	}
	if d, err := os.UserCacheDir(); err == nil {
		return filepath.Join(d, "act3_ai_processing_server", "layers_render")
	}
	return filepath.Join(os.TempDir(), "act3_layers_render_cache")
}

// layersProfile: $ACT3_MOTION_PROFILE, else hardware_profile.json in the cache dir ("" when neither exists).
func layersProfile(cache string) string {
	for _, p := range []string{expandTilde(strings.TrimSpace(os.Getenv("ACT3_MOTION_PROFILE"))), filepath.Join(cache, "hardware_profile.json")} {
		if p != "" && fileAtLeast(p, 2) {
			return p
		}
	}
	return ""
}

// motionSourceDir is render_tasks/motion_video next to the binaries (target/release → ../../motion_video), or
// $ACT3_MOTION_SRC_DIR; "" when it has no templates/ and html_runner/render.mjs.
func motionSourceDir(videoBin string) string {
	cands := []string{}
	if d := strings.TrimSpace(os.Getenv("ACT3_MOTION_SRC_DIR")); d != "" {
		cands = append(cands, expandTilde(d))
	}
	if real, err := filepath.EvalSymlinks(videoBin); err == nil {
		videoBin = real
	}
	cands = append(cands, filepath.Join(filepath.Dir(videoBin), "..", "..", "motion_video"))
	for _, c := range cands {
		if fileAtLeast(filepath.Join(c, "html_runner", "render.mjs"), 1) {
			if st, err := os.Stat(filepath.Join(c, "templates")); err == nil && st.IsDir() {
				return filepath.Clean(c)
			}
		}
	}
	return ""
}

func expandTilde(p string) string {
	if strings.HasPrefix(p, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, p[2:])
	}
	return p
}

func engineMissing(env Env, html int) []string {
	var miss []string
	for _, n := range []string{"act3_motion_video", "act3_motion_audio", "ffmpeg", "ffprobe", "node"} {
		if env.Engines[n] == nil || env.Engines[n].Path == "" {
			miss = append(miss, n)
		}
	}
	if html > 0 && (env.Engines["chromium"] == nil || env.Engines["chromium"].Path == "") {
		miss = append(miss, "chromium")
	}
	return miss
}

func (LayersRender) Check(_ context.Context, job *jobs.Job, env Env) error {
	lj, err := parseLayersParams(job)
	if err != nil {
		return err
	}
	if len(lj.Assets) > 0 {
		known := map[string]string{}
		for _, a := range lj.Assets {
			known[a.ID] = "x"
		}
		var unknown []string
		for _, id := range AssetRefIDs(lj.Plan) {
			if known[id] == "" {
				unknown = append(unknown, id)
			}
		}
		if len(unknown) > 0 {
			return jobs.Errf(jobs.KindInvalidInput, "the plan names asset(s) missing from the asset list: %s", strings.Join(unknown, ", "))
		}
		if n := len(job.Payload.Inputs); n < len(lj.Assets) {
			return jobs.Errf(jobs.KindInvalidInput, "the plan lists %d assets but the job sends %d inputs", len(lj.Assets), n)
		}
	}
	if miss := engineMissing(env, lj.HTMLLayers); len(miss) > 0 {
		return jobs.Errf(jobs.KindInfra, "layers_render is not installed on this server: missing %s (build ~/BGit/all/film/code/render_tasks with `cargo build --release --workspace`, set ACT3_MOTION_BIN_DIR, install node and Playwright's chromium-headless-shell)", strings.Join(miss, ", "))
	}
	return nil
}

// copyExecutable copies a binary (never a link: a rebuild must not reach a running job) and makes it executable.
func copyExecutable(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	if err := copyAll(src, dst); err != nil {
		return err
	}
	return os.Chmod(dst, 0o755)
}

// copyTree copies a directory of regular files (templates, the HTML runner).
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		to := filepath.Join(dst, rel)
		if d.IsDir() {
			if d.Name() == "node_modules" || d.Name() == ".git" {
				return filepath.SkipDir
			}
			return os.MkdirAll(to, 0o700)
		}
		if !d.Type().IsRegular() {
			return nil
		}
		return copyAll(p, to)
	})
}

// freezeLayersTools copies what the job runs into jobDir/bin and jobDir/motion_video.
func freezeLayersTools(env Env, jobDir, cache string) (LayersBins, []string, error) {
	var notes []string
	exe := ""
	if runtime.GOOS == "windows" {
		exe = ".exe"
	}
	b := LayersBins{
		Video: filepath.Join(jobDir, "bin", "act3-motion-video"+exe), Audio: filepath.Join(jobDir, "bin", "act3-motion-audio"+exe),
		FFmpeg: env.Engines["ffmpeg"].Path, FFprobe: env.Engines["ffprobe"].Path, Node: env.Engines["node"].Path,
	}
	if c := env.Engines["chromium"]; c != nil {
		b.Chrome = c.Path
	}
	srcVideo := env.Engines["act3_motion_video"].Path
	for _, c := range [][2]string{{srcVideo, b.Video}, {env.Engines["act3_motion_audio"].Path, b.Audio}} {
		if err := copyExecutable(c[0], c[1]); err != nil {
			return b, notes, jobs.Wrap(jobs.KindInfra, err, "copy "+filepath.Base(c[0])+" into the job")
		}
	}
	notes = append(notes, "binaries frozen from "+filepath.Dir(srcVideo))
	if src := motionSourceDir(srcVideo); src != "" {
		mv := filepath.Join(jobDir, "motion_video")
		if err := copyTree(filepath.Join(src, "templates"), filepath.Join(mv, "templates")); err != nil {
			return b, notes, jobs.Wrap(jobs.KindInfra, err, "copy the motion-graphics templates into the job")
		}
		if err := copyTree(filepath.Join(src, "html_runner"), filepath.Join(mv, "html_runner")); err != nil {
			return b, notes, jobs.Wrap(jobs.KindInfra, err, "copy the HTML runner into the job")
		}
		b.Templates, b.Runner = filepath.Join(mv, "templates"), filepath.Join(mv, "html_runner", "render.mjs")
		notes = append(notes, "templates and HTML runner frozen from "+src)
	} else {
		notes = append(notes, "templates/html_runner not found beside the binaries (set ACT3_MOTION_SRC_DIR): act3-motion-video uses its built-in paths")
	}
	if p := layersProfile(cache); p != "" {
		b.Profile = filepath.Join(jobDir, "hardware_profile.json")
		if err := copyBytes(p, b.Profile); err != nil {
			return b, notes, jobs.Wrap(jobs.KindInfra, err, "copy the hardware profile")
		}
		notes = append(notes, "hardware profile "+p)
	}
	return b, notes, nil
}

var errLayersLimit = errors.New("layers render over its time limit")

// layersLimitFor is LayersTimeLimit; tests shorten it.
var layersLimitFor = LayersTimeLimit

func (LayersRender) Run(ctx context.Context, job *jobs.Job, env Env, rep Reporter) (*Result, error) {
	lj, err := parseLayersParams(job)
	if err != nil {
		return nil, err
	}
	if miss := engineMissing(env, lj.HTMLLayers); len(miss) > 0 {
		return nil, jobs.Errf(jobs.KindInfra, "layers_render is not installed on this server: missing %s", strings.Join(miss, ", "))
	}
	jobDir, err := filepath.Abs(filepath.Join(env.WorkDir, "layers"))
	if err != nil {
		return nil, jobs.Wrap(jobs.KindInternal, err, "job folder path")
	}
	// temp files go on every exit (the lifecycle also removes the whole work dir); the output lives in env.OutDir
	defer func() { elog.Ignore("removing the layers job folder", os.RemoveAll(jobDir), "job_id", job.ID) }()
	cache := layersCacheDir(env)
	if err := os.MkdirAll(cache, 0o700); err != nil {
		return nil, jobs.Wrap(jobs.KindInfra, err, "create the layers cache "+cache)
	}

	rep.Stage("prepare")
	bins, notes, err := freezeLayersTools(env, jobDir, cache)
	for _, n := range notes {
		rep.Log("layers_render: " + n)
	}
	if err != nil {
		return nil, err
	}
	paths, err := PlaceLayersAssets(lj.Assets, env.Inputs, filepath.Join(jobDir, "assets"))
	if err != nil {
		return nil, err
	}
	if err := RewriteAssetRefs(lj.Plan, paths); err != nil {
		return nil, err
	}
	planBytes, err := json.MarshalIndent(lj.Plan, "", "  ")
	if err != nil {
		return nil, jobs.Wrap(jobs.KindInternal, err, "encode plan.json")
	}
	if err := os.WriteFile(filepath.Join(jobDir, "plan.json"), planBytes, 0o600); err != nil {
		return nil, jobs.Wrap(jobs.KindInfra, err, "write plan.json")
	}
	if err := os.MkdirAll(filepath.Join(jobDir, "work"), 0o700); err != nil {
		return nil, jobs.Wrap(jobs.KindInfra, err, "create the render work folder")
	}
	outPath, err := filepath.Abs(filepath.Join(env.OutDir, "final.mp4"))
	if err != nil {
		return nil, jobs.Wrap(jobs.KindInternal, err, "output path")
	}
	args := LayersCommand(bins, jobDir, outPath, cache)
	limit := layersLimitFor(lj.DurationSeconds, lj.HTMLLayers)
	rep.Log(fmt.Sprintf("layers_render: %d assets, %d HTML layers, %.2f s of output, time limit %s", len(paths), lj.HTMLLayers, lj.DurationSeconds, limit))
	rep.Log("layers_render: " + bins.Video + " " + strings.Join(args, " "))

	runCtx, stop := context.WithTimeoutCause(ctx, limit, errLayersLimit)
	defer stop()
	extra := []string{"ACT3_RENDER_CACHE=" + cache}
	if bins.Chrome != "" {
		extra = append(extra, "ACT3_CHROMIUM="+bins.Chrome)
	}
	// Node and FFmpeg on PATH too, for anything the runner starts by name
	pathDirs := []string{filepath.Dir(bins.Node), filepath.Dir(bins.FFmpeg)}
	extra = append(extra, "PATH="+strings.Join(pathDirs, string(os.PathListSeparator))+string(os.PathListSeparator)+os.Getenv("PATH"))

	rep.Stage("render")
	rep.Progress(0.05, "starting act3-motion-video")
	prog := newLayersProgress(lj.HTMLLayers)
	var stdout, tail []string
	t0 := time.Now()
	err = procs.Run(runCtx, procs.Spec{
		Name: bins.Video, Args: args, Dir: jobDir, Grace: ProfileLayers.Grace, Env: extra, StdinNil: true,
		Stdout: func(line string) {
			if strings.TrimSpace(line) != "" {
				stdout = appendTail(stdout, line, 5)
			}
		},
		Stderr: func(line string) {
			if e, ok := ParseMotionEvent(line); ok {
				if f, note, now := prog.Update(e); now {
					rep.Progress(f, note)
				}
				if e.Event == "step" {
					rep.Log("act3-motion-video: " + line)
				}
				return
			}
			rep.Log("act3-motion-video: " + line)
			tail = appendTail(tail, line, 12)
		},
	})
	runtimeMS := time.Since(t0).Milliseconds()
	summary := lastJSONObject(stdout)
	if err != nil {
		if errors.Is(context.Cause(runCtx), errLayersLimit) && ctx.Err() == nil {
			return nil, jobs.Errf(jobs.KindTimeout, "layers render stopped at its time limit (%s for %.1f s with %d HTML layers); last output: %s", limit, lj.DurationSeconds, lj.HTMLLayers, strings.Join(tail, " | "))
		}
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, layersFailure(err, summary, tail, job.ID)
	}
	if ok, _ := summary["ok"].(bool); !ok {
		return nil, layersFailure(errors.New("act3-motion-video exited 0 without an ok summary"), summary, tail, job.ID)
	}
	if !fileAtLeast(outPath, 1) {
		return nil, jobs.Errf(jobs.KindInternal, "act3-motion-video reported ok but wrote no %s", outPath)
	}
	meta := map[string]any{"runtime_ms": runtimeMS, "html_layers": lj.HTMLLayers, "duration_s": lj.DurationSeconds}
	rep.Progress(1, fmt.Sprintf("rendered %.1f s in %.1f s", lj.DurationSeconds, float64(runtimeMS)/1000))
	data := map[string]any{"summary": summary, "runtime_ms": runtimeMS, "render_id": lj.RenderID, "shot_id": lj.ShotID}
	return &Result{Outputs: []jobs.Output{{Name: "video", Path: outPath, ContentType: "video/mp4", Meta: meta}}, Data: data}, nil
}

// lastJSONObject is the last stdout line that parses as a JSON object (nil when none does).
func lastJSONObject(lines []string) map[string]any {
	for i := len(lines) - 1; i >= 0; i-- {
		var m map[string]any
		if json.Unmarshal([]byte(strings.TrimSpace(lines[i])), &m) == nil {
			return m
		}
	}
	return nil
}

// LayersExitCode is the child's exit status from procs.Run's error (-1 when it did not exit normally).
func LayersExitCode(err error) int {
	var ee interface{ ExitCode() int }
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	if m := regexp.MustCompile(`exit status (\d+)`).FindStringSubmatch(err.Error()); m != nil {
		n, _ := strconv.Atoi(m[1])
		return n
	}
	return -1
}

// layersFailure classifies a failed render: exit 2 (bad command line or plan) → invalid_input; a tool that could not
// start → infra; anything else (exit 3) → transient. The message carries the summary's error and the last stderr lines.
func layersFailure(err error, summary map[string]any, tail []string, jobID string) error {
	msg, _ := summary["error"].(string)
	if msg == "" {
		msg = err.Error()
	}
	if len(tail) > 0 {
		msg += " | stderr: " + strings.Join(tail, " | ")
	}
	code := LayersExitCode(err)
	kind := jobs.KindTransient
	switch {
	case code == 2:
		kind = jobs.KindInvalidInput
	case strings.HasPrefix(err.Error(), "start "), strings.Contains(msg, "could not start") || strings.Contains(msg, "No such file or directory") || strings.Contains(msg, "not installed"):
		kind = jobs.KindInfra
	}
	elog.Warn("running act3-motion-video", fmt.Errorf("exit %d: %s", code, msg), "job_id", jobID, "kind", string(kind))
	return jobs.Wrap(kind, err, "act3-motion-video failed: "+msg)
}
