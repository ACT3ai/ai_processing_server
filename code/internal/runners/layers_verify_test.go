package runners

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/hardware"
)

// Mode verify (motion.mdx §74): act3-motion-verify check on the input `video`, params.expect → output `verify`.

func TestParseLayersParamsVerify(t *testing.T) {
	lj, err := parseLayersParams(layersJobWith(t, map[string]any{"mode": "verify", "expect": map[string]any{"version": 1, "seconds": 4.5, "audio": true}}))
	if err != nil {
		t.Fatal(err)
	}
	if lj.Mode != ModeVerify || lj.DurationSeconds != 4.5 || lj.Expect["audio"] != true || lj.Plan != nil {
		t.Fatalf("verify: %+v", lj)
	}
	// renderTask form, no expect at all
	lj, err = parseLayersParams(layersJobWith(t, map[string]any{"renderTask": map[string]any{"mode": "verify", "renderId": "r9"}}))
	if err != nil || lj.Expect != nil || lj.RenderID != "r9" {
		t.Fatalf("no expect: %+v %v", lj, err)
	}
	if _, err := parseLayersParams(layersJobWith(t, map[string]any{"mode": "verify", "expect": []int{1}})); err == nil {
		t.Fatal("an expect that is not an object must be refused")
	}
	name, file, ct := layersOutput(layersJob{Mode: ModeVerify})
	if name != "verify" || file != "verify.json" || ct != "application/json" {
		t.Fatalf("output %s %s %s", name, file, ct)
	}
	if got := layersEnginesFor(ModeVerify); !reflect.DeepEqual(got, []string{"act3_motion_verify", "ffmpeg", "ffprobe"}) {
		t.Fatalf("engines %v", got)
	}
}

func TestLayersVerifyCheckAndCommand(t *testing.T) {
	e := map[string]*hardware.Engine{"act3_motion_verify": {Path: "/x/verify"}, "ffmpeg": {Path: "/x/ffmpeg"}, "ffprobe": {Path: "/x/ffprobe"}}
	env := Env{Engines: e}
	if err := (LayersRender{}).Check(context.Background(), layersJobWith(t, map[string]any{"mode": "verify"}, "video"), env); err != nil {
		t.Fatal(err)
	}
	if err := (LayersRender{}).Check(context.Background(), layersJobWith(t, map[string]any{"mode": "verify"}, "picture"), env); err == nil || !strings.Contains(err.Error(), "input named video") {
		t.Fatalf("no video input: %v", err)
	}
	delete(e, "act3_motion_verify")
	if err := (LayersRender{}).Check(context.Background(), layersJobWith(t, map[string]any{"mode": "verify"}, "video"), env); err == nil || !strings.Contains(err.Error(), "act3_motion_verify") {
		t.Fatalf("no binary: %v", err)
	}
	b := LayersBins{FFmpeg: "/f", FFprobe: "/p"}
	got := LayersVerifyCommand(b, "/in/v.mp4", "/in/r.mp4", "/j/expect.json", "/o/verify.json")
	want := []string{"check", "--in", "/in/v.mp4", "--out", "/o/verify.json", "--ffmpeg", "/f", "--ffprobe", "/p", "--expect", "/j/expect.json", "--reference", "/in/r.mp4"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command %v", got)
	}
	if got := LayersVerifyCommand(b, "/in/v.mp4", "", "", "/o/v.json"); len(got) != 9 {
		t.Fatalf("minimal command %v", got)
	}
	// input names in pictureFrom / soundWindows[].source become local paths; the caller's map is not changed
	x := map[string]any{"pictureFrom": "reference", "soundWindows": []any{map[string]any{"atMs": 1.0, "source": "voice"}, map[string]any{"source": "/abs/x.wav"}}}
	l := VerifyExpectFor(x, map[string]string{"reference": "/in/ref.mp4", "voice": "/in/voice.wav"})
	ws := l["soundWindows"].([]any)
	if l["pictureFrom"] != "/in/ref.mp4" || ws[0].(map[string]any)["source"] != "/in/voice.wav" || ws[1].(map[string]any)["source"] != "/abs/x.wav" {
		t.Fatalf("local expect %v", l)
	}
	if x["pictureFrom"] != "reference" || x["soundWindows"].([]any)[0].(map[string]any)["source"] != "voice" {
		t.Fatal("VerifyExpectFor changed its input")
	}
}

// fakeVerify is a shell act3-motion-verify that writes verify.json with `result` and exits `code`.
func fakeVerify(t *testing.T, result string, code int, writeJSON bool) (Env, string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell fakes")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "act3-motion-verify")
	write := ""
	if writeJSON {
		write = `printf '{"version":1,"result":"` + result + `","checks":[]}' > "$out"`
	}
	script := `#!/bin/sh
cmd=$1; shift
while [ $# -gt 0 ]; do
  case $1 in --out) out=$2;; --expect) expect=$2;; --in) in=$2;; esac; shift 2
done
[ -n "$expect" ] && cp "$expect" "$(dirname "$out")/expect_seen.json"
echo '{"event":"step","step":"decode"}' >&2
` + write + `
echo '{"ok":` + map[bool]string{true: "true", false: "false"}[result != "FAIL"] + `,"result":"` + result + `"}'
exit ` + map[int]string{0: "0", 2: "2", 3: "3"}[code] + "\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	out := filepath.Join(work, "out")
	_ = os.MkdirAll(out, 0o700)
	video := filepath.Join(work, "final.mp4")
	_ = os.WriteFile(video, []byte("MP4"), 0o600)
	t.Setenv("ACT3_MOTION_CACHE_DIR", filepath.Join(work, "cache"))
	e := map[string]*hardware.Engine{"act3_motion_verify": {Path: bin}, "ffmpeg": {Path: "/bin/sh"}, "ffprobe": {Path: "/bin/sh"}}
	return Env{WorkDir: work, OutDir: out, Inputs: map[string]string{"video": video, "voice": "/in/voice.wav"}, Engines: e}, out
}

func TestLayersRunVerifyFakeBinary(t *testing.T) {
	expect := map[string]any{"version": 1, "audio": true, "soundWindows": []any{map[string]any{"atMs": 1000, "durationMs": 500, "source": "voice"}}}
	for _, c := range []struct {
		result string
		code   int
	}{{"PASS", 0}, {"WARN", 0}, {"FAIL", 3}} {
		env, out := fakeVerify(t, c.result, c.code, true)
		job := layersJobWith(t, map[string]any{"mode": "verify", "expect": expect}, "video", "voice")
		res, err := (LayersRender{}).Run(context.Background(), job, env, &layersRep{})
		if err != nil {
			t.Fatalf("%s: %v", c.result, err)
		}
		if len(res.Outputs) != 1 || res.Outputs[0].Name != "verify" || res.Outputs[0].Path != filepath.Join(out, "verify.json") {
			t.Fatalf("%s: outputs %+v", c.result, res.Outputs)
		}
		d := res.Data.(map[string]any)
		if d["result"] != c.result || d["verify"].(map[string]any)["version"] != float64(1) || res.Outputs[0].Meta["result"] != c.result {
			t.Fatalf("%s: data %v", c.result, d)
		}
		var seen map[string]any
		b, _ := os.ReadFile(filepath.Join(out, "expect_seen.json"))
		_ = json.Unmarshal(b, &seen)
		if src := seen["soundWindows"].([]any)[0].(map[string]any)["source"]; src != "/in/voice.wav" {
			t.Fatalf("the window's source input was not made local: %v", src)
		}
	}
	// exit 3 with no verify.json: the check itself broke → a failed task
	env, _ := fakeVerify(t, "FAIL", 3, false)
	if _, err := (LayersRender{}).Run(context.Background(), layersJobWith(t, map[string]any{"mode": "verify"}, "video"), env, &layersRep{}); err == nil {
		t.Fatal("exit 3 without verify.json must fail the task")
	}
	// exit 2: bad command line → invalid input
	env, _ = fakeVerify(t, "PASS", 2, true)
	_, err := (LayersRender{}).Run(context.Background(), layersJobWith(t, map[string]any{"mode": "verify"}, "video"), env, &layersRep{})
	if err == nil || !strings.Contains(err.Error(), "invalid_input") && !strings.Contains(strings.ToLower(err.Error()), "invalid") {
		t.Fatalf("exit 2: %v", err)
	}
}

// The real act3-motion-verify (when built): a good file PASSes, a file with no sound where sound is expected FAILs as a
// finished task.
func TestLayersRunVerifyRealBinary(t *testing.T) {
	if testing.Short() {
		t.Skip("real check")
	}
	var bin string
	for _, c := range hardware.MotionBinCandidates("act3-motion-verify") {
		if p := expandTilde(c); fileAtLeast(p, 1) {
			bin = p
			break
		}
	}
	ff, err1 := exec.LookPath("ffmpeg")
	fp, err2 := exec.LookPath("ffprobe")
	if bin == "" || err1 != nil || err2 != nil {
		t.Skip("act3-motion-verify / ffmpeg not installed (build ~/BGit/all/film/code/render_tasks or set ACT3_MOTION_BIN_DIR)")
	}
	in := t.TempDir()
	good, mute := filepath.Join(in, "good.mp4"), filepath.Join(in, "mute.mp4")
	for _, args := range [][]string{
		{"-v", "error", "-y", "-f", "lavfi", "-i", "testsrc2=s=160x90:r=30:d=1", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "1",
			"-pix_fmt", "yuv420p", "-c:v", "libx264", "-c:a", "aac", "-ac", "2", good},
		{"-v", "error", "-y", "-f", "lavfi", "-i", "testsrc2=s=160x90:r=30:d=1", "-pix_fmt", "yuv420p", "-c:v", "libx264", mute},
	} {
		if out, err := exec.Command(ff, args...).CombinedOutput(); err != nil {
			t.Fatalf("ffmpeg %v: %v %s", args, err, out)
		}
	}
	expect := map[string]any{"version": 1, "width": 160, "height": 90, "fps": "30/1", "seconds": 1, "audio": true}
	for file, want := range map[string]string{good: "PASS", mute: "FAIL"} {
		work := t.TempDir()
		out := filepath.Join(work, "out")
		_ = os.MkdirAll(out, 0o700)
		t.Setenv("ACT3_MOTION_CACHE_DIR", filepath.Join(work, "cache"))
		env := Env{WorkDir: work, OutDir: out, Inputs: map[string]string{"video": file},
			Engines: map[string]*hardware.Engine{"act3_motion_verify": {Path: bin}, "ffmpeg": {Path: ff}, "ffprobe": {Path: fp}}}
		rep := &layersRep{}
		res, err := (LayersRender{}).Run(context.Background(), layersJobWith(t, map[string]any{"mode": "verify", "expect": expect}, "video"), env, rep)
		if err != nil {
			t.Fatalf("%s: %v\n%s", filepath.Base(file), err, strings.Join(rep.logs, "\n"))
		}
		d := res.Data.(map[string]any)
		if d["result"] != want {
			t.Fatalf("%s: result %v, want %s: %v", filepath.Base(file), d["result"], want, d["summary"])
		}
		if want == "FAIL" && !strings.Contains(d["summary"].(map[string]any)["first"].(string), "sound track") {
			t.Fatalf("the FAIL names its first error: %v", d["summary"])
		}
	}
}
