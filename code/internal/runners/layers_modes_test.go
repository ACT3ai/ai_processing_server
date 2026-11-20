package runners

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
)

// The four layers task modes (motion.mdx §39, §49): shot, picture, audio and join, sent as separate render tasks.

func TestParseLayersModes(t *testing.T) {
	plan := layersTestPlan()
	for _, c := range []struct {
		params map[string]any
		mode   string
		err    string
	}{
		{map[string]any{"plan": plan}, ModeShot, ""},
		{map[string]any{"mode": "PICTURE", "plan": plan}, ModePicture, ""},
		{map[string]any{"renderTask": map[string]any{"mode": "audio", "plan": plan}}, ModeAudio, ""},
		{map[string]any{"mode": "join"}, ModeJoin, ""}, // no plan needed
		{map[string]any{"mode": "remix", "plan": plan}, "", "not one of shot, picture, audio, join"},
		{map[string]any{"mode": "audio", "plan": map[string]any{"version": 1, "steps": []any{map[string]any{"id": "c", "kind": "combine"}}}}, "", "needs an audio_mix step"},
		{map[string]any{"mode": "picture", "plan": map[string]any{"version": 1, "steps": []any{map[string]any{"id": "a", "kind": "audio_mix"}}}}, "", "needs a combine step"},
		{map[string]any{"plan": map[string]any{"version": 1, "output": map[string]any{"codec": "png"}, "steps": []any{}}}, "", "png"},
		{map[string]any{"mode": "join", "join": map[string]any{"lengthFlicks": -5}}, "", "lengthFlicks"},
		{map[string]any{"mode": "join", "join": map[string]any{"audioBitrate": "lots"}}, "", "audioBitrate"},
	} {
		lj, err := parseLayersParams(layersJobWith(t, c.params))
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Fatalf("%v: want error %q, got %v", c.params, c.err, err)
			}
			continue
		}
		if err != nil || lj.Mode != c.mode {
			t.Fatalf("%v: mode %q err %v, want %q", c.params, lj.Mode, err, c.mode)
		}
	}
	// the audio job never starts Chromium, whatever the plan's HTML steps
	lj, _ := parseLayersParams(layersJobWith(t, map[string]any{"mode": "audio", "plan": plan, "htmlLayers": 4}))
	if lj.HTMLLayers != 0 {
		t.Fatalf("audio mode counted %d HTML layers", lj.HTMLLayers)
	}
	// a join takes the shot's length from the plan when one rides along
	lj, _ = parseLayersParams(layersJobWith(t, map[string]any{"mode": "join", "plan": plan}))
	if lj.Join.LengthFlicks == nil || *lj.Join.LengthFlicks != 1411200000 || lj.DurationSeconds != 2 {
		t.Fatalf("join length from the plan: %+v", lj.Join)
	}
}

func TestLayersOutputPerMode(t *testing.T) {
	prores := map[string]any{"output": map[string]any{"codec": "prores4444"}}
	for _, c := range []struct {
		lj               layersJob
		name, file, kind string
	}{
		{layersJob{Mode: ModeShot, Plan: map[string]any{}}, "video", "final.mp4", "video/mp4"},
		{layersJob{Mode: ModePicture, Plan: map[string]any{}}, "video", "picture.mp4", "video/mp4"},
		{layersJob{Mode: ModePicture, Plan: prores}, "video", "picture.mov", "video/quicktime"},
		{layersJob{Mode: ModeAudio, Plan: map[string]any{}}, "audio", "audio.wav", "audio/wav"},
		{layersJob{Mode: ModeJoin}, "video", "final.mp4", "video/mp4"},
	} {
		n, f, k := layersOutput(c.lj)
		if n != c.name || f != c.file || k != c.kind {
			t.Fatalf("%s: got %s %s %s", c.lj.Mode, n, f, k)
		}
	}
}

func TestLayersEnginesPerMode(t *testing.T) {
	only := map[string]*hardware.Engine{"act3_motion_audio": {Path: "/x/a"}, "ffmpeg": {Path: "/x/f"}, "ffprobe": {Path: "/x/p"}}
	env := Env{Engines: only}
	if m := engineMissing(env, 0, ModeAudio); len(m) != 0 {
		t.Fatalf("audio needs only act3-motion-audio + FFmpeg, missing %v", m)
	}
	if m := engineMissing(env, 0, ModeJoin); len(m) != 1 || m[0] != "act3_motion_join" {
		t.Fatalf("join needs act3-motion-join, missing %v", m)
	}
	if m := engineMissing(env, 1, ModeShot); !strings.Contains(strings.Join(m, ","), "act3_motion_video") || !strings.Contains(strings.Join(m, ","), "chromium") {
		t.Fatalf("shot missing %v", m)
	}
	j := layersJobWith(t, map[string]any{"mode": "join"}, "audio")
	if err := (LayersRender{}).Check(context.Background(), j, Env{Engines: layersEngines()}); err == nil || !strings.Contains(err.Error(), "picture") {
		t.Fatalf("a join without a picture input must fail, got %v", err)
	}
}

// fakeTool writes a shell script that records its arguments and writes "$out".
func fakeTool(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	script := `#!/bin/sh
echo "$@" > "` + dir + `/` + name + `.args"
cmd=$1; shift
while [ $# -gt 0 ]; do
  case $1 in --out) out=$2;; --plan) plan=$2;; --picture) picture=$2;; --audio) audio=$2;; esac; shift 2
done
` + body
	if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLayersRunAudioMode(t *testing.T) {
	engines := fakeMotion(t, "exit 9\n") // the video binary must not run
	dir := t.TempDir()
	engines["act3_motion_audio"] = &hardware.Engine{Path: fakeTool(t, dir, "act3-motion-audio", `printf 'RIFF' > "$out"
echo '{"inputs":5,"peakDb":-3.1}'
`)}
	env, job := layersFakeEnv(t, engines)
	job.Payload.Params = []byte(strings.Replace(string(job.Payload.Params), `"renderTask":{`, `"renderTask":{"mode":"audio",`, 1))
	res, err := (LayersRender{}).Run(context.Background(), job, env, &layersRep{})
	if err != nil {
		t.Fatal(err)
	}
	o := res.Outputs[0]
	if o.Name != "audio" || o.ContentType != "audio/wav" || filepath.Base(o.Path) != "audio.wav" {
		t.Fatalf("output %+v", o)
	}
	args, _ := os.ReadFile(filepath.Join(dir, "act3-motion-audio.args"))
	if !strings.HasPrefix(string(args), "mix --plan ") || !strings.Contains(string(args), "/layers/plan.json") {
		t.Fatalf("audio args %s", args)
	}
	if res.Data.(map[string]any)["summary"].(map[string]any)["inputs"] != float64(5) {
		t.Fatalf("summary %v", res.Data)
	}
}

func TestLayersRunPictureMode(t *testing.T) {
	engines := fakeMotion(t, `echo "$cmd" > "$(dirname "$plan")/../cmd_seen"
printf 'MP4' > "$out"
echo '{"ok":true}'
`)
	env, job := layersFakeEnv(t, engines)
	job.Payload.Params = []byte(strings.Replace(string(job.Payload.Params), `"renderTask":{`, `"renderTask":{"mode":"picture",`, 1))
	res, err := (LayersRender{}).Run(context.Background(), job, env, &layersRep{})
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(res.Outputs[0].Path) != "picture.mp4" {
		t.Fatalf("output %+v", res.Outputs[0])
	}
	b, _ := os.ReadFile(filepath.Join(env.WorkDir, "cmd_seen"))
	if strings.TrimSpace(string(b)) != "combine" {
		t.Fatalf("picture mode ran %q, want combine", b)
	}
}

func TestLayersRunJoinMode(t *testing.T) {
	if testingShort() {
		t.Skip()
	}
	dir := t.TempDir()
	engines := map[string]*hardware.Engine{"ffmpeg": {Path: "/bin/sh"}, "ffprobe": {Path: "/bin/sh"},
		"act3_motion_join": {Path: fakeTool(t, dir, "act3-motion-join", `test -f "$picture" || exit 7
test -f "$audio" || exit 8
printf 'MP4' > "$out"
echo '{"ok":true,"kind":"join","audioFit":"exact"}'
`)}}
	work := t.TempDir()
	out := filepath.Join(work, "out")
	_ = os.MkdirAll(out, 0o700)
	pic, wav := filepath.Join(work, "p.mp4"), filepath.Join(work, "a.wav")
	_ = os.WriteFile(pic, []byte("p"), 0o600)
	_ = os.WriteFile(wav, []byte("a"), 0o600)
	t.Setenv("ACT3_MOTION_CACHE_DIR", filepath.Join(work, "cache"))
	job := layersJobWith(t, map[string]any{"mode": "join", "join": map[string]any{"lengthFlicks": 21168000000, "audioBitrate": "192k"}}, "picture", "audio")
	env := Env{WorkDir: work, OutDir: out, Inputs: map[string]string{"picture": pic, "audio": wav}, Engines: engines}
	res, err := (LayersRender{}).Run(context.Background(), job, env, &layersRep{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Outputs[0].Name != "video" || filepath.Base(res.Outputs[0].Path) != "final.mp4" {
		t.Fatalf("output %+v", res.Outputs[0])
	}
	args, _ := os.ReadFile(filepath.Join(dir, "act3-motion-join.args"))
	for _, want := range []string{"join --picture " + pic, "--audio " + wav, "--length-flicks 21168000000", "--audio-bitrate 192k"} {
		if !strings.Contains(string(args), want) {
			t.Fatalf("join args %q lack %q", args, want)
		}
	}
	// a join that fails (no ok summary) is a failure with the tool's name
	engines["act3_motion_join"] = &hardware.Engine{Path: fakeTool(t, dir, "act3-motion-join", `echo 'the picture is 2.0 s, shorter than the shot' >&2
echo '{"ok":false,"error":"the picture is 2.0 s, shorter than the shot"}'
exit 3
`)}
	_, err = (LayersRender{}).Run(context.Background(), job, env, &layersRep{})
	if err == nil || !strings.Contains(err.Error(), "act3-motion-join failed: the picture is 2.0 s") || kindOf(err) != jobs.KindTransient {
		t.Fatalf("join failure: %v (%s)", err, kindOf(err))
	}
}

func testingShort() bool { return testing.Short() }
