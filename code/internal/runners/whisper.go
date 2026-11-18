package runners

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/procs"
)

// Whisper is the runner behind act3.transcription (ACT3 dialogue audio: which words a recorded clip speaks, with word
// timestamps, so the web app can align them to the script's dialogue lines) and wtc.transcription (We The Citizens).
//
// One input (act3 names it source_audio; any name is accepted): any audio or video file ffmpeg reads. It runs the
// open-source openai-whisper CLI (`whisper`, engine probe "whisper") with word timestamps and writes one output,
// `transcript` (application/json, schema act3.transcript.v1, see Transcript).
//
// params: {model: whisper-large-v3-turbo (default) | whisper-large-v3 | whisper-medium | whisper-small | whisper-base |
// whisper-tiny, language: "en" | null (auto), word_timestamps: bool (default true)}. Other keys (render_type, quality,
// kind, ...) are ignored.
//
// Not detached: a transcription takes seconds to a few minutes, so a controller restart re-runs it (like FFmpeg)
// instead of re-attaching. Spec: ~/BGit/act3/ai_processing_server/pm/ai_processing_server.mdx (act3.transcription).
type Whisper struct{}

// WhisperModels maps the service's model id to the openai-whisper model name.
var WhisperModels = map[string]string{
	"whisper-large-v3-turbo": "large-v3-turbo",
	"whisper-large-v3":       "large-v3",
	"whisper-medium":         "medium",
	"whisper-small":          "small",
	"whisper-base":           "base",
	"whisper-tiny":           "tiny",
}

// DefaultWhisperModel is used when params name none.
const DefaultWhisperModel = "whisper-large-v3-turbo"

// ProfileWhisper: the Python process stops on the interrupt within a second or two (pm/cancel.mdx §8).
var ProfileWhisper = CancelProfile{Name: "whisper", Grace: 5 * time.Second}

type whisperParams struct {
	Model          string  `json:"model"`
	Language       *string `json:"language"`
	WordTimestamps *bool   `json:"word_timestamps"`
}

var languageRe = regexp.MustCompile(`^[a-z]{2,3}$`)

func (Whisper) Name() string { return "whisper" }

func parseWhisperParams(job *jobs.Job) (whisperParams, error) {
	var p whisperParams
	if err := decodeParams(job, &p); err != nil {
		return p, err
	}
	if p.Model == "" {
		p.Model = DefaultWhisperModel
	}
	if _, ok := WhisperModels[p.Model]; !ok {
		names := make([]string, 0, len(WhisperModels))
		for k := range WhisperModels {
			names = append(names, k)
		}
		sort.Strings(names)
		return p, jobs.Errf(jobs.KindInvalidInput, "unknown whisper model %q (one of %s)", p.Model, strings.Join(names, ", "))
	}
	if p.Language != nil {
		l := strings.ToLower(strings.TrimSpace(*p.Language))
		if l == "" || l == "auto" {
			p.Language = nil
		} else if !languageRe.MatchString(l) {
			return p, jobs.Errf(jobs.KindInvalidInput, "language must be an ISO code like \"en\", got %q", *p.Language)
		} else {
			p.Language = &l
		}
	}
	return p, nil
}

func (Whisper) Check(_ context.Context, job *jobs.Job, env Env) error {
	if _, err := parseWhisperParams(job); err != nil {
		return err
	}
	if n := len(job.Payload.Inputs); n != 1 {
		return jobs.Errf(jobs.KindInvalidInput, "transcription needs exactly one input (source_audio), got %d", n)
	}
	if env.Engines["whisper"] == nil {
		return jobs.Errf(jobs.KindInfra, "whisper is not installed on this server (pip install openai-whisper; or set engines.whisper in this_server.yaml)")
	}
	if env.Engines["ffmpeg"] == nil {
		return jobs.Errf(jobs.KindInfra, "ffmpeg is required by whisper and is not installed on this server")
	}
	return nil
}

// TranscriptWord is one spoken word.
type TranscriptWord struct {
	StartS float64 `json:"start_s"`
	EndS   float64 `json:"end_s"`
	Word   string  `json:"word"`
	P      float64 `json:"p"`
}

// TranscriptSegment is one phrase whisper decoded.
type TranscriptSegment struct {
	StartS float64          `json:"start_s"`
	EndS   float64          `json:"end_s"`
	Text   string           `json:"text"`
	Words  []TranscriptWord `json:"words"`
}

// Transcript is the `transcript` output: schema act3.transcript.v1 (the ACT3 web app reads it).
type Transcript struct {
	Schema    string              `json:"schema"`
	Engine    string              `json:"engine"`
	Model     string              `json:"model"`
	Language  string              `json:"language"`
	DurationS float64             `json:"duration_s"`
	RuntimeMS int64               `json:"runtime_ms"`
	Text      string              `json:"text"`
	Segments  []TranscriptSegment `json:"segments"`
}

// openai-whisper's --output_format json.
type whisperJSON struct {
	Text     string `json:"text"`
	Language string `json:"language"`
	Segments []struct {
		Start float64 `json:"start"`
		End   float64 `json:"end"`
		Text  string  `json:"text"`
		Words []struct {
			Word        string  `json:"word"`
			Start       float64 `json:"start"`
			End         float64 `json:"end"`
			Probability float64 `json:"probability"`
		} `json:"words"`
	} `json:"segments"`
}

// ConvertWhisperJSON turns openai-whisper's JSON into act3.transcript.v1. duration 0 = the last segment's end.
func ConvertWhisperJSON(raw []byte, model string, durationS float64, runtimeMS int64) (Transcript, error) {
	var w whisperJSON
	if err := json.Unmarshal(raw, &w); err != nil {
		return Transcript{}, fmt.Errorf("whisper wrote unreadable JSON: %w", err)
	}
	t := Transcript{Schema: "act3.transcript.v1", Engine: "openai-whisper", Model: model, Language: w.Language,
		DurationS: durationS, RuntimeMS: runtimeMS, Text: strings.TrimSpace(w.Text), Segments: []TranscriptSegment{}}
	for _, s := range w.Segments {
		seg := TranscriptSegment{StartS: round3(s.Start), EndS: round3(s.End), Text: strings.TrimSpace(s.Text), Words: []TranscriptWord{}}
		for _, wd := range s.Words {
			word := strings.TrimSpace(wd.Word)
			if word == "" {
				continue
			}
			seg.Words = append(seg.Words, TranscriptWord{StartS: round3(wd.Start), EndS: round3(wd.End), Word: word, P: round3(wd.Probability)})
		}
		t.Segments = append(t.Segments, seg)
		if durationS == 0 {
			t.DurationS = max(t.DurationS, seg.EndS)
		}
	}
	return t, nil
}

func round3(f float64) float64 { return float64(int64(f*1000+0.5)) / 1000 }

func (t Transcript) wordCount() int {
	n := 0
	for _, s := range t.Segments {
		n += len(s.Words)
	}
	return n
}

func (Whisper) Run(ctx context.Context, job *jobs.Job, env Env, rep Reporter) (*Result, error) {
	p, err := parseWhisperParams(job)
	if err != nil {
		return nil, err
	}
	if len(job.Payload.Inputs) != 1 {
		return nil, jobs.Errf(jobs.KindInvalidInput, "transcription needs exactly one input, got %d", len(job.Payload.Inputs))
	}
	in := env.Inputs[job.Payload.Inputs[0].Name]
	if in == "" {
		return nil, jobs.Errf(jobs.KindInvalidInput, "input %q was not staged", job.Payload.Inputs[0].Name)
	}
	wh := env.Engines["whisper"]
	if wh == nil {
		return nil, jobs.Errf(jobs.KindInfra, "whisper is not installed on this server")
	}

	rep.Stage("probe")
	duration := 0.0
	if fp := env.Engines["ffprobe"]; fp != nil {
		d, err := audioDuration(ctx, fp.Path, in)
		if err != nil {
			return nil, jobs.Wrap(jobs.KindInvalidInput, err, "input is not readable audio")
		}
		duration = d
	}

	args := []string{in, "--model", WhisperModels[p.Model], "--output_format", "json", "--output_dir", env.WorkDir,
		"--verbose", "False", "--task", "transcribe"}
	if p.WordTimestamps == nil || *p.WordTimestamps {
		args = append(args, "--word_timestamps", "True")
	}
	if p.Language != nil {
		args = append(args, "--language", *p.Language)
	}
	if env.Slot.GPU == nil {
		args = append(args, "--fp16", "False") // fp16 is CUDA-only; on CPU it only prints a warning per file
	}
	var extra []string
	if dir := os.Getenv("APS_WHISPER_MODEL_DIR"); dir != "" {
		args = append(args, "--model_dir", dir)
	}
	if ff := env.Engines["ffmpeg"]; ff != nil {
		// whisper decodes through `ffmpeg` on PATH: make it the one the probe found
		extra = append(extra, "PATH="+filepath.Dir(ff.Path)+string(os.PathListSeparator)+os.Getenv("PATH"))
	}

	rep.Stage("transcribe")
	rep.Progress(0.05, "loading "+p.Model)
	var tail []string
	t0 := time.Now()
	err = procs.Run(ctx, procs.Spec{
		Name: wh.Path, Args: args, Dir: env.WorkDir, GPU: env.Slot.GPU, Grace: ProfileWhisper.Grace, Env: extra,
		Stdout: func(line string) { rep.Log("whisper: " + line); tail = appendTail(tail, line, 10) },
		Stderr: func(line string) {
			rep.Log("whisper: " + line)
			tail = appendTail(tail, line, 10)
			// tqdm frames: " 45%|####      | 1234/2741 [00:03<00:04, ...]"
			if i := strings.Index(line, "%|"); i > 0 {
				f := strings.Fields(line[:i])
				if len(f) > 0 {
					if pct, err := strconv.ParseFloat(f[len(f)-1], 64); err == nil {
						rep.Progress(min(0.95, 0.05+pct/100*0.9), fmt.Sprintf("transcribed %.0f%%", pct))
					}
				}
			}
		},
	})
	runtime := time.Since(t0).Milliseconds()
	if err != nil {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		msg := strings.Join(tail, " | ")
		if strings.Contains(msg, "Failed to load audio") || strings.Contains(msg, "Invalid data found") {
			return nil, jobs.Wrap(jobs.KindInvalidInput, err, "whisper could not read the input: "+msg)
		}
		return nil, jobs.Wrap(jobs.KindTransient, err, "whisper failed: "+msg)
	}

	base := strings.TrimSuffix(filepath.Base(in), filepath.Ext(in))
	raw, err := os.ReadFile(filepath.Join(env.WorkDir, base+".json"))
	if err != nil {
		return nil, jobs.Wrap(jobs.KindInternal, err, "whisper wrote no JSON")
	}
	tr, err := ConvertWhisperJSON(raw, p.Model, duration, runtime)
	if err != nil {
		return nil, jobs.Wrap(jobs.KindInternal, err, "convert the transcript")
	}
	out, err := writeJSONOutput(env, "transcript", tr)
	if err != nil {
		return nil, err
	}
	summary := map[string]any{"schema": tr.Schema, "engine": tr.Engine, "model": tr.Model, "language": tr.Language,
		"duration_s": tr.DurationS, "runtime_ms": tr.RuntimeMS, "words": tr.wordCount(), "segments": len(tr.Segments)}
	out.Meta = summary
	rep.Progress(1, fmt.Sprintf("%d words in %.1fs of audio", tr.wordCount(), tr.DurationS))
	return &Result{Outputs: []jobs.Output{out}, Data: summary}, nil
}

// audioDuration reads a file's duration with ffprobe; it fails when the file has no audio stream.
func audioDuration(ctx context.Context, ffprobe, path string) (float64, error) {
	var out, errOut strings.Builder
	err := procs.Run(ctx, procs.Spec{
		Name:   ffprobe,
		Args:   []string{"-v", "error", "-select_streams", "a:0", "-show_entries", "format=duration:stream=codec_type", "-of", "json", path},
		Stdout: func(l string) { out.WriteString(l) },
		Stderr: func(l string) { errOut.WriteString(l + " ") },
	})
	if err != nil {
		return 0, fmt.Errorf("ffprobe: %v %s", err, errOut.String())
	}
	var doc struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
		Streams []struct {
			CodecType string `json:"codec_type"`
		} `json:"streams"`
	}
	if err := json.Unmarshal([]byte(out.String()), &doc); err != nil {
		return 0, fmt.Errorf("ffprobe output: %w", err)
	}
	if len(doc.Streams) == 0 {
		return 0, fmt.Errorf("no audio stream")
	}
	d, _ := strconv.ParseFloat(doc.Format.Duration, 64)
	return d, nil
}
