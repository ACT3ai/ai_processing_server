package runners

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
)

func whisperJob(t *testing.T, params map[string]any, inputs ...string) *jobs.Job {
	t.Helper()
	b, _ := json.Marshal(params)
	j := &jobs.Job{ID: "w1", Type: "act3.transcription", Payload: jobs.Envelope{Service: "act3.transcription", Params: b}}
	for _, n := range inputs {
		j.Payload.Inputs = append(j.Payload.Inputs, jobs.Input{Name: n})
	}
	return j
}

func whisperEnv() Env {
	return Env{Engines: map[string]*hardware.Engine{
		"whisper": {Path: "/x/whisper", Version: "unknown"}, "ffmpeg": {Path: "/x/ffmpeg"}, "ffprobe": {Path: "/x/ffprobe"},
	}}
}

func TestWhisperCheck(t *testing.T) {
	ctx := context.Background()
	ok := []map[string]any{
		{},
		{"model": "whisper-small", "language": "en", "word_timestamps": true, "render_type": "whisper", "quality": nil, "kind": "media"},
		{"model": "whisper-large-v3-turbo", "language": nil},
		{"language": "auto"},
	}
	for _, p := range ok {
		if err := (Whisper{}).Check(ctx, whisperJob(t, p, "source_audio"), whisperEnv()); err != nil {
			t.Errorf("%v: unexpected %v", p, err)
		}
	}
	bad := []map[string]any{{"model": "whisper-huge"}, {"model": "large-v3"}, {"language": "english!"}}
	for _, p := range bad {
		err := (Whisper{}).Check(ctx, whisperJob(t, p, "source_audio"), whisperEnv())
		if kindOf(err) != jobs.KindInvalidInput {
			t.Errorf("%v: want invalid_input, got %v", p, err)
		}
	}
	if err := (Whisper{}).Check(ctx, whisperJob(t, nil), whisperEnv()); kindOf(err) != jobs.KindInvalidInput {
		t.Errorf("no input: want invalid_input, got %v", err)
	}
	if err := (Whisper{}).Check(ctx, whisperJob(t, nil, "a", "b"), whisperEnv()); kindOf(err) != jobs.KindInvalidInput {
		t.Errorf("two inputs: want invalid_input, got %v", err)
	}
	// wtc names its input differently: any single input is accepted
	if err := (Whisper{}).Check(ctx, whisperJob(t, nil, "media"), whisperEnv()); err != nil {
		t.Errorf("wtc input name: %v", err)
	}
	env := whisperEnv()
	env.Engines["whisper"] = nil
	if err := (Whisper{}).Check(ctx, whisperJob(t, nil, "source_audio"), env); kindOf(err) != jobs.KindInfra {
		t.Errorf("no engine: want infra, got %v", err)
	}
}

const whisperFixture = `{"text": " Hello there. General Kenobi.", "language": "en", "segments": [
 {"id": 0, "start": 0.0, "end": 1.2, "text": " Hello there.", "words": [
   {"word": " Hello", "start": 0.0, "end": 0.42, "probability": 0.981},
   {"word": " there.", "start": 0.42, "end": 1.2, "probability": 0.9}]},
 {"id": 1, "start": 1.5, "end": 3.04, "text": " General Kenobi.", "words": [
   {"word": " General", "start": 1.5, "end": 2.0, "probability": 0.77},
   {"word": " ", "start": 2.0, "end": 2.0, "probability": 0.1},
   {"word": " Kenobi.", "start": 2.0, "end": 3.0444, "probability": 0.65}]}]}`

func TestConvertWhisperJSON(t *testing.T) {
	tr, err := ConvertWhisperJSON([]byte(whisperFixture), "whisper-small", 0, 1234)
	if err != nil {
		t.Fatal(err)
	}
	if tr.Schema != "act3.transcript.v1" || tr.Engine != "openai-whisper" || tr.Model != "whisper-small" || tr.Language != "en" {
		t.Fatalf("header: %+v", tr)
	}
	if tr.Text != "Hello there. General Kenobi." || tr.RuntimeMS != 1234 || tr.DurationS != 3.04 {
		t.Fatalf("text/runtime/duration: %q %d %v", tr.Text, tr.RuntimeMS, tr.DurationS)
	}
	if len(tr.Segments) != 2 || tr.wordCount() != 4 {
		t.Fatalf("segments %d words %d", len(tr.Segments), tr.wordCount())
	}
	w := tr.Segments[1].Words[1]
	if w.Word != "Kenobi." || w.StartS != 2.0 || w.EndS != 3.044 || w.P != 0.65 {
		t.Fatalf("word trimmed/rounded: %+v", w)
	}
	if tr2, _ := ConvertWhisperJSON([]byte(whisperFixture), "whisper-small", 12.5, 0); tr2.DurationS != 12.5 {
		t.Fatalf("probed duration wins: %v", tr2.DurationS)
	}
	if _, err := ConvertWhisperJSON([]byte("{"), "m", 0, 0); err == nil {
		t.Fatal("bad JSON must fail")
	}
}

// Runs the real openai-whisper CLI (model tiny) on two seconds of generated tone, only when whisper, ffmpeg and the
// tiny model are already on this machine (no download in a test).
func TestWhisperRunReal(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	home, _ := os.UserHomeDir()
	wh, err := exec.LookPath("whisper")
	if err != nil {
		wh = filepath.Join(home, ".local", "bin", "whisper")
		if _, err := os.Stat(wh); err != nil {
			t.Skip("whisper not installed")
		}
	}
	ff, err1 := exec.LookPath("ffmpeg")
	fp, err2 := exec.LookPath("ffprobe")
	if err1 != nil || err2 != nil {
		t.Skip("ffmpeg not installed")
	}
	if _, err := os.Stat(filepath.Join(home, ".cache", "whisper", "tiny.pt")); err != nil {
		t.Skip("whisper tiny model not cached")
	}
	dir := t.TempDir()
	wav := filepath.Join(dir, "tone.wav")
	if out, err := exec.Command(ff, "-hide_banner", "-y", "-f", "lavfi", "-i", "sine=frequency=440:duration=2", "-ar", "16000", "-ac", "1", wav).CombinedOutput(); err != nil {
		t.Fatalf("ffmpeg: %v %s", err, out)
	}
	work := filepath.Join(dir, "work")
	out := filepath.Join(work, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	env := Env{WorkDir: work, OutDir: out, Inputs: map[string]string{"source_audio": wav}, Engines: map[string]*hardware.Engine{
		"whisper": {Path: wh}, "ffmpeg": {Path: ff}, "ffprobe": {Path: fp}}}
	job := whisperJob(t, map[string]any{"model": "whisper-tiny", "language": "en"}, "source_audio")
	res, err := (Whisper{}).Run(context.Background(), job, env, &nopRep{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Outputs) != 1 || res.Outputs[0].Name != "transcript" || res.Outputs[0].ContentType != "application/json" {
		t.Fatalf("outputs: %+v", res.Outputs)
	}
	b, err := os.ReadFile(res.Outputs[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	var tr Transcript
	if err := json.Unmarshal(b, &tr); err != nil || tr.Schema != "act3.transcript.v1" || tr.Model != "whisper-tiny" || tr.RuntimeMS <= 0 {
		t.Fatalf("transcript: %v %+v", err, tr)
	}
	if tr.DurationS < 1.9 || tr.DurationS > 2.1 {
		t.Fatalf("duration %v", tr.DurationS)
	}
}
