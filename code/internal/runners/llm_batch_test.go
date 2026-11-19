package runners

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
)

type llmRep struct {
	mu     sync.Mutex
	stages []string
}

func (r *llmRep) Stage(n string)         { r.mu.Lock(); r.stages = append(r.stages, n); r.mu.Unlock() }
func (*llmRep) Progress(float64, string) {}
func (*llmRep) Log(string)               {}

// fakeOllama answers /api/version, /api/tags, /api/ps and /api/chat. fail(n) decides, per chat call number, whether
// to answer 500. It records the highest number of chat calls in flight at once.
type fakeOllama struct {
	models   []string
	fail     func(n int64, prompt string) bool
	delay    time.Duration
	calls    atomic.Int64
	inflight atomic.Int64
	maxIn    atomic.Int64
}

func (f *fakeOllama) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/version", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(`{"version":"0.40.16"}`)) })
	mux.HandleFunc("/api/tags", func(w http.ResponseWriter, _ *http.Request) {
		var ms []map[string]any
		for _, m := range f.models {
			ms = append(ms, map[string]any{"name": m})
		}
		json.NewEncoder(w).Encode(map[string]any{"models": ms})
	})
	mux.HandleFunc("/api/ps", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"models":[{"name":"qwen3.8-flash-next:latest","size":120000000000,"size_vram":120000000000,"context_length":65536}]}`))
	})
	mux.HandleFunc("/api/chat", func(w http.ResponseWriter, r *http.Request) {
		n := f.calls.Add(1)
		in := f.inflight.Add(1)
		defer f.inflight.Add(-1)
		for {
			m := f.maxIn.Load()
			if in <= m || f.maxIn.CompareAndSwap(m, in) {
				break
			}
		}
		var req ollamaChatReq
		json.NewDecoder(r.Body).Decode(&req)
		last := req.Messages[len(req.Messages)-1].Content
		if f.delay > 0 && !strings.Contains(last, "single word OK") {
			time.Sleep(f.delay)
		}
		if f.fail != nil && f.fail(n, last) {
			w.WriteHeader(500)
			w.Write([]byte(`{"error":"llama runner process has terminated: exit status 2"}`))
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"model": req.Model, "message": map[string]any{"role": "assistant",
			"content": "answer to: " + last}, "done": true, "done_reason": "stop", "prompt_eval_count": 50,
			"prompt_eval_duration": 1e8, "eval_count": 20, "eval_duration": 4e8, "load_duration": 1e6})
	})
	return mux
}

func llmJob(t *testing.T, params map[string]any) *jobs.Job {
	t.Helper()
	b, _ := json.Marshal(params)
	return &jobs.Job{ID: "llmtest_1", Type: "act3.llm_batch", Payload: jobs.Envelope{Service: "act3.llm_batch", Params: b}}
}

func llmEnv(t *testing.T) Env {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("APS_LLM_LOG_DIR", filepath.Join(dir, "logs"))
	out := filepath.Join(dir, "out")
	os.MkdirAll(out, 0o755)
	return Env{WorkDir: dir, OutDir: out, Inputs: map[string]string{}, Engines: map[string]*hardware.Engine{}}
}

func prompts(n int) []map[string]any {
	var out []map[string]any
	for i := 0; i < n; i++ {
		out = append(out, map[string]any{"prompt_id": "q" + string(rune('a'+i%26)) + strings.Repeat("x", i/26), "prompt": "question " + strings.Repeat("z", i)})
	}
	return out
}

func readReport(t *testing.T, env Env) LLMBatchReport {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(env.OutDir, "llm_report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var r LLMBatchReport
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestLLMBatchRunsAllPromptsSixInFlight(t *testing.T) {
	f := &fakeOllama{models: []string{"qwen3.8-flash-next:latest", "qwen3.8:27b"}, delay: 40 * time.Millisecond}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	env := llmEnv(t)
	job := llmJob(t, map[string]any{"models": []string{"qwen3.8-flash-next"}, "max_parallel": 6, "ollama_url": srv.URL,
		"stagger_ms": -1, "prompts": prompts(30)})
	rep := &llmRep{}
	res, err := LLMBatch{}.Run(context.Background(), job, env, rep)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Outputs) != 2 {
		t.Fatalf("outputs %d", len(res.Outputs))
	}
	r := readReport(t, env)
	if r.Summary.OK != 30 || r.Summary.Failed != 0 {
		t.Fatalf("summary %+v", r.Summary)
	}
	if r.Model != "qwen3.8-flash-next:latest" {
		t.Fatalf("model %q", r.Model)
	}
	if got := f.maxIn.Load(); got != 6 {
		t.Fatalf("max in flight %d, want 6", got)
	}
	want := []string{"load_prompts", "preflight", "select_model", "warm", "prompts", "report"}
	if strings.Join(rep.stages, ",") != strings.Join(want, ",") {
		t.Fatalf("stages %v", rep.stages)
	}
	for _, st := range r.Stages {
		if st.Status != "ok" {
			t.Fatalf("stage %s %s", st.Name, st.Status)
		}
	}
	if r.HealthBefore == nil || !r.HealthBefore.Answering || r.HealthBefore.Version != "0.40.16" {
		t.Fatalf("health %+v", r.HealthBefore)
	}
}

func TestLLMBatchRetriesAndDiagnosesFailures(t *testing.T) {
	// every prompt containing "bad" always fails; the 5th chat call fails once (a transient crash, then fine)
	f := &fakeOllama{models: []string{"qwen3.8:27b"}, fail: func(n int64, p string) bool { return strings.Contains(p, "bad") || n == 5 }}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	env := llmEnv(t)
	ps := prompts(8)
	ps = append(ps, map[string]any{"prompt_id": "broken", "prompt": "a bad one"})
	job := llmJob(t, map[string]any{"model": "qwen3.8:27b", "max_parallel": 3, "ollama_url": srv.URL, "retries": 1,
		"stagger_ms": -1, "warm": false, "prompts": ps})
	if _, err := (LLMBatch{}).Run(context.Background(), job, env, &llmRep{}); err != nil {
		t.Fatal(err) // some prompts worked: the job succeeds and lists the failure
	}
	r := readReport(t, env)
	if r.Summary.OK != 8 || r.Summary.Failed != 1 {
		t.Fatalf("summary %+v", r.Summary)
	}
	var broken LLMPromptResult
	for _, p := range r.Prompts {
		if p.PromptID == "broken" {
			broken = p
		}
	}
	if broken.Status != "failed" || len(broken.Attempts) != 2 {
		t.Fatalf("broken %+v", broken)
	}
	a := broken.Attempts[0]
	if a.ErrorClass != "http_5xx" || a.HTTPStatus != 500 || a.Diagnosis == nil || !a.Diagnosis.Answering {
		t.Fatalf("attempt %+v", a)
	}
	if r.Summary.Retried < 1 {
		t.Fatalf("the transient failure was not retried: %+v", r.Summary)
	}
	errLog, _ := os.ReadFile(filepath.Join(os.Getenv("APS_LLM_LOG_DIR"), "llm_batch.err"))
	if !strings.Contains(string(errLog), "prompt broken attempt 1 failed") || !strings.Contains(string(errLog), "stage prompts partial") {
		t.Fatalf("error file:\n%s", errLog)
	}
}

func TestLLMBatchOllamaDownFailsInfraWithDiagnosis(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	u := srv.URL
	srv.Close() // nothing listens there now
	env := llmEnv(t)
	t.Setenv("APS_OLLAMA_URL", "")
	t.Setenv("OLLAMA_HOST", "")
	old := DefaultOllamaURLs
	DefaultOllamaURLs = nil
	defer func() { DefaultOllamaURLs = old }()
	job := llmJob(t, map[string]any{"model": "x", "ollama_url": u, "prompts": prompts(2)})
	_, err := LLMBatch{}.Run(context.Background(), job, env, &llmRep{})
	if kindOf(err) != jobs.KindInfra || !strings.Contains(err.Error(), "port closed") {
		t.Fatalf("err %v", err)
	}
	r := readReport(t, env) // the report is written even though the job failed
	if r.Stages[len(r.Stages)-1].Name != "preflight" || r.Stages[len(r.Stages)-1].Status != "failed" {
		t.Fatalf("stages %+v", r.Stages)
	}
}

func TestLLMBatchModelMissing(t *testing.T) {
	f := &fakeOllama{models: []string{"gemma4:26b"}}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	env := llmEnv(t)
	job := llmJob(t, map[string]any{"models": []string{"qwen3.8-flash-next", "glm-5.3-flash"}, "ollama_url": srv.URL, "prompts": prompts(1)})
	_, err := LLMBatch{}.Run(context.Background(), job, env, &llmRep{})
	if kindOf(err) != jobs.KindInfra || !strings.Contains(err.Error(), "none of the asked models") {
		t.Fatalf("err %v", err)
	}
}

func TestLLMBatchParams(t *testing.T) {
	for _, tc := range []struct {
		p    map[string]any
		kind jobs.ErrorKind
	}{
		{map[string]any{"prompts": prompts(1), "ollama_url": "http://192.168.1.9:11434"}, jobs.KindInvalidInput},
		{map[string]any{"prompts": prompts(1), "max_parallel": 99}, jobs.KindInvalidInput},
		{map[string]any{"prompts": prompts(1), "retries": 9}, jobs.KindInvalidInput},
		{map[string]any{}, jobs.KindInvalidInput},
		{map[string]any{"prompts": prompts(1), "ollama_url": "http://localhost:19391"}, ""},
	} {
		err := LLMBatch{}.Check(context.Background(), llmJob(t, tc.p), Env{})
		if kindOf(err) != tc.kind {
			t.Fatalf("%v: got %v, want %q", tc.p, err, tc.kind)
		}
	}
}

func TestPickModel(t *testing.T) {
	m, _ := pickModel([]string{"a", "qwen3.8:27b"}, []string{"qwen3.8:27b", "b:latest"})
	if m != "qwen3.8:27b" {
		t.Fatal(m)
	}
	if m, _ := pickModel([]string{"b"}, []string{"b:latest"}); m != "b:latest" {
		t.Fatal(m)
	}
	if m, _ := pickModel(nil, []string{"b"}); m != "" {
		t.Fatal("a model was guessed")
	}
}

func TestClassifyOllamaErr(t *testing.T) {
	bg := context.Background()
	for msg, want := range map[string]string{
		"dial tcp 127.0.0.1:11434: connect: connection refused":         "ollama_down",
		"context deadline exceeded":                                     "timeout",
		"model \"x\" not found, try pulling it first":                   "model_missing",
		"model requires more system memory (300 GiB) than is available": "oom",
	} {
		if got, _, _ := classifyOllamaErr(&ollamaErr{msg: msg}, bg); got != want {
			t.Fatalf("%q: %s, want %s", msg, got, want)
		}
	}
}
