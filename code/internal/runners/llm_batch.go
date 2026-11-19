package runners

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/hardware"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
)

// LLMBatch is the runner behind act3.llm_batch: an LLM render task. One job carries ONE OR MANY LLM calls (1 … a few
// hundred prompts) and runs them through the local Ollama on this machine, keeping K prompts in flight (default 6)
// and refilling a slot the moment a prompt finishes (spec ~/BGit/all/film/pm/subp/llm_gen_ai/mac_studio/llm_render_task.mdx
// §6, the slot scheduler of on_prem_chinese_llm.mdx §4.3).
//
// Inputs: optional input "prompts" (a JSON file: {"prompts":[...]} or a bare array) for big batches; small batches may
// carry params.prompts inline (the queue envelope is capped at 256 KB).
//
// Stages (rep.Stage, the job log, and the report): preflight → select_model → warm → prompts → report. Every stage is
// recorded ok / failed / skipped with its timing and detail. When a prompt fails, the runner DIAGNOSES Ollama on the spot
// (is the port open, does /api/version answer, is the model listed, what is loaded in /api/ps, is an `ollama` process
// running, free memory) and stores the diagnosis with the failed attempt, so the caller learns WHY, not only that it failed.
//
// Outputs: `llm_results` (act3.llm_batch_results.v1: one record per prompt with the text) and `llm_report`
// (act3.llm_batch_report.v1: stages, Ollama health before and after, every attempt, errors, timings, throughput).
// Errors are also appended to ~/_act3_logs/llm_batch.err (WARN/ERROR) and every stage to ~/_act3_logs/llm_batch.info.
//
// A batch where at least one prompt succeeded SUCCEEDS (failed prompts are listed in the report). A batch where no prompt
// could run fails `infra` with the diagnosis in the message, because a failed job carries only kind + stage + message.
//
// Not detached: prompts are HTTP calls; a controller restart re-runs the job (idempotent per prompt_id on the caller's
// side). Ollama must listen on loopback; params.ollama_url is accepted only for 127.0.0.1 / localhost / ::1.
type LLMBatch struct{}

// ProfileLLMBatch: cancelling only closes HTTP requests; Ollama stops generating when the connection drops.
var ProfileLLMBatch = CancelProfile{Name: "llm_batch", Grace: 3 * time.Second}

// LLM batch limits and defaults.
const (
	LLMBatchMaxPrompts       = 2000
	LLMBatchMaxParallel      = 16
	LLMBatchDefaultParallel  = 6
	LLMBatchDefaultRetries   = 2
	LLMBatchDefaultTimeoutS  = 900
	LLMBatchDefaultKeepAlive = "30m"
	LLMBatchDefaultNumCtx    = 65536
)

// DefaultOllamaURLs are tried in order when nothing names the server: the stock port, then BMS's port (19391, where
// OpenClaw's Ollama has lived since 2026-10, servers/Bryan_Mac_Studio/server.yaml).
var DefaultOllamaURLs = []string{"http://127.0.0.1:11434", "http://127.0.0.1:19391"}

// LLMPrompt is one LLM call of the batch.
type LLMPrompt struct {
	PromptID        string           `json:"prompt_id"`
	Category        string           `json:"category,omitempty"`
	System          string           `json:"system,omitempty"`
	Prompt          string           `json:"prompt,omitempty"`
	Messages        []LLMChatMessage `json:"messages,omitempty"`
	MaxOutputTokens int              `json:"max_output_tokens,omitempty"`
	Temperature     *float64         `json:"temperature,omitempty"`
	Format          string           `json:"format,omitempty"` // "json" asks Ollama for JSON output
	NumCtx          int              `json:"num_ctx,omitempty"`
	Metadata        map[string]any   `json:"metadata,omitempty"`
}

// LLMChatMessage is one chat turn.
type LLMChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type llmBatchParams struct {
	Version         int         `json:"version"`
	BatchID         string      `json:"batch_id"`
	Model           string      `json:"model"`
	Models          []string    `json:"models"` // preference order; the first one this Ollama has is used
	MaxParallel     int         `json:"max_parallel"`
	NumCtx          int         `json:"num_ctx"`
	MaxOutputTokens int         `json:"max_output_tokens"`
	Temperature     *float64    `json:"temperature"`
	Think           *bool       `json:"think"`
	KeepAlive       string      `json:"keep_alive"`
	PromptTimeoutS  int         `json:"prompt_timeout_s"`
	Retries         *int        `json:"retries"`
	Warm            *bool       `json:"warm"`
	StaggerMS       int         `json:"stagger_ms"`
	OllamaURL       string      `json:"ollama_url"`
	Prompts         []LLMPrompt `json:"prompts"`
}

func (LLMBatch) Name() string { return "llm_batch" }

func parseLLMBatchParams(job *jobs.Job) (llmBatchParams, error) {
	var p llmBatchParams
	if err := decodeParams(job, &p); err != nil {
		return p, err
	}
	if p.Model != "" {
		p.Models = append([]string{p.Model}, p.Models...)
	}
	if p.MaxParallel <= 0 {
		p.MaxParallel = LLMBatchDefaultParallel
	}
	if p.MaxParallel > LLMBatchMaxParallel {
		return p, jobs.Errf(jobs.KindInvalidInput, "max_parallel %d is above the limit %d", p.MaxParallel, LLMBatchMaxParallel)
	}
	if p.NumCtx <= 0 {
		p.NumCtx = LLMBatchDefaultNumCtx
	}
	if p.KeepAlive == "" {
		p.KeepAlive = LLMBatchDefaultKeepAlive
	}
	if p.PromptTimeoutS <= 0 {
		p.PromptTimeoutS = LLMBatchDefaultTimeoutS
	}
	if p.Retries == nil {
		r := LLMBatchDefaultRetries
		p.Retries = &r
	}
	if *p.Retries < 0 || *p.Retries > 5 {
		return p, jobs.Errf(jobs.KindInvalidInput, "retries must be 0-5, got %d", *p.Retries)
	}
	if p.OllamaURL != "" {
		if err := checkLoopbackURL(p.OllamaURL); err != nil {
			return p, err
		}
	}
	return p, nil
}

// checkLoopbackURL refuses any Ollama URL that is not this machine's loopback (on_prem_chinese_llm.mdx §4.6).
func checkLoopbackURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return jobs.Errf(jobs.KindInvalidInput, "ollama_url %q is not an http(s) URL", raw)
	}
	h := u.Hostname()
	if h != "127.0.0.1" && h != "localhost" && h != "::1" {
		return jobs.Errf(jobs.KindInvalidInput, "ollama_url must be loopback (127.0.0.1, localhost or ::1), got %q", h)
	}
	return nil
}

func (LLMBatch) Check(_ context.Context, job *jobs.Job, _ Env) error {
	p, err := parseLLMBatchParams(job)
	if err != nil {
		return err
	}
	if len(p.Prompts) == 0 && len(job.Payload.Inputs) == 0 {
		return jobs.Errf(jobs.KindInvalidInput, "llm_batch needs prompts: params.prompts or an input named \"prompts\" (a JSON file)")
	}
	if len(job.Payload.Inputs) > 1 {
		return jobs.Errf(jobs.KindInvalidInput, "llm_batch takes at most one input (prompts), got %d", len(job.Payload.Inputs))
	}
	return nil
}

/* ── report types ─────────────────────────────────────────────────────────────────────────────── */

// LLMStage is one stage of the render task, as the report records it.
type LLMStage struct {
	Name      string    `json:"name"`
	Status    string    `json:"status"` // ok | failed | skipped | partial
	StartedAt time.Time `json:"started_at"`
	MS        int64     `json:"ms"`
	Detail    string    `json:"detail,omitempty"`
	Error     string    `json:"error,omitempty"`
}

// OllamaHealth is one look at the local Ollama.
type OllamaHealth struct {
	At        time.Time         `json:"at"`
	URL       string            `json:"url"`
	PortOpen  bool              `json:"port_open"`
	Version   string            `json:"version,omitempty"`
	Answering bool              `json:"answering"`
	Models    []string          `json:"models,omitempty"`
	Loaded    []OllamaLoaded    `json:"loaded,omitempty"`
	Processes []string          `json:"processes,omitempty"`
	FreeRAMGB float64           `json:"free_ram_gb,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	Problems  []string          `json:"problems,omitempty"`
	ElapsedMS int64             `json:"elapsed_ms"`
}

// OllamaLoaded is one model resident in Ollama (/api/ps).
type OllamaLoaded struct {
	Name      string  `json:"name"`
	SizeGB    float64 `json:"size_gb"`
	VRAMGB    float64 `json:"vram_gb"`
	ExpiresAt string  `json:"expires_at,omitempty"`
	Context   int     `json:"context_length,omitempty"`
}

// LLMAttempt is one try of one prompt.
type LLMAttempt struct {
	N          int           `json:"n"`
	Slot       int           `json:"slot"`
	StartedAt  time.Time     `json:"started_at"`
	MS         int64         `json:"ms"`
	OK         bool          `json:"ok"`
	HTTPStatus int           `json:"http_status,omitempty"`
	ErrorClass string        `json:"error_class,omitempty"` // ollama_down | timeout | http_5xx | http_4xx | model_missing | empty | context_overflow | oom | cancelled | other
	Error      string        `json:"error,omitempty"`
	Diagnosis  *OllamaHealth `json:"diagnosis,omitempty"`
}

// LLMPromptResult is one prompt's outcome.
type LLMPromptResult struct {
	PromptID    string         `json:"prompt_id"`
	Category    string         `json:"category,omitempty"`
	Status      string         `json:"status"` // ok | failed | cancelled | skipped
	Model       string         `json:"model,omitempty"`
	Text        string         `json:"text,omitempty"`
	Thinking    string         `json:"thinking,omitempty"`
	DoneReason  string         `json:"done_reason,omitempty"`
	TokensIn    int            `json:"tokens_in"`
	TokensOut   int            `json:"tokens_out"`
	PrefillMS   int64          `json:"prefill_ms"`
	DecodeMS    int64          `json:"decode_ms"`
	LoadMS      int64          `json:"load_ms"`
	QueueMS     int64          `json:"queue_ms"`
	TotalMS     int64          `json:"total_ms"`
	DecodeTPS   float64        `json:"decode_tps"`
	Attempts    []LLMAttempt   `json:"attempts"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	promptChars int
}

// LLMBatchReport is the `llm_report` output: everything needed to learn why a batch did or did not work.
type LLMBatchReport struct {
	Schema       string            `json:"schema"`
	BatchID      string            `json:"batch_id"`
	JobID        string            `json:"job_id"`
	Host         string            `json:"host"`
	StartedAt    time.Time         `json:"started_at"`
	EndedAt      time.Time         `json:"ended_at"`
	Model        string            `json:"model"`
	ModelsAsked  []string          `json:"models_asked"`
	MaxParallel  int               `json:"max_parallel"`
	NumCtx       int               `json:"num_ctx"`
	Stages       []LLMStage        `json:"stages"`
	HealthBefore *OllamaHealth     `json:"health_before,omitempty"`
	HealthAfter  *OllamaHealth     `json:"health_after,omitempty"`
	Summary      LLMBatchSummary   `json:"summary"`
	Errors       []string          `json:"errors"`
	Prompts      []LLMPromptResult `json:"prompts"`
}

// LLMBatchSummary is the headline numbers.
type LLMBatchSummary struct {
	Total          int            `json:"total"`
	OK             int            `json:"ok"`
	Failed         int            `json:"failed"`
	Cancelled      int            `json:"cancelled"`
	Retried        int            `json:"retried"`
	WallMS         int64          `json:"wall_ms"`
	TokensIn       int            `json:"tokens_in"`
	TokensOut      int            `json:"tokens_out"`
	AggregateTPS   float64        `json:"aggregate_decode_tps"`
	MedianPromptMS int64          `json:"median_prompt_ms"`
	P95PromptMS    int64          `json:"p95_prompt_ms"`
	ErrorClasses   map[string]int `json:"error_classes,omitempty"`
}

/* ── the run ──────────────────────────────────────────────────────────────────────────────────── */

func (r LLMBatch) Run(ctx context.Context, job *jobs.Job, env Env, rep Reporter) (*Result, error) {
	p, err := parseLLMBatchParams(job)
	if err != nil {
		return nil, err
	}
	logs := newLLMLog(job.ID)
	host, _ := os.Hostname()
	report := &LLMBatchReport{Schema: "act3.llm_batch_report.v1", BatchID: p.BatchID, JobID: job.ID, Host: host,
		StartedAt: time.Now(), MaxParallel: p.MaxParallel, NumCtx: p.NumCtx, ModelsAsked: p.Models, Errors: []string{}}
	if report.BatchID == "" {
		report.BatchID = job.ID
	}
	stage := func(name string) func(status, detail string, err error) {
		rep.Stage(name)
		logs.info("stage %s started", name)
		st := LLMStage{Name: name, StartedAt: time.Now()}
		return func(status, detail string, err error) {
			st.MS, st.Status, st.Detail = time.Since(st.StartedAt).Milliseconds(), status, detail
			if err != nil {
				st.Error = err.Error()
				report.Errors = append(report.Errors, name+": "+err.Error())
				logs.err("stage %s %s: %v (%s)", name, status, err, detail)
				rep.Log(fmt.Sprintf("llm_batch: stage %s %s: %v", name, status, err))
			} else {
				logs.info("stage %s %s in %d ms: %s", name, status, st.MS, detail)
				rep.Log(fmt.Sprintf("llm_batch: stage %s %s (%d ms) %s", name, status, st.MS, detail))
			}
			report.Stages = append(report.Stages, st)
		}
	}
	// whatever happens, the report is written (and kept in the work dir when the job fails)
	finish := func() (*Result, error) {
		report.EndedAt = time.Now()
		report.Summary.WallMS = report.EndedAt.Sub(report.StartedAt).Milliseconds()
		return writeLLMOutputs(env, report)
	}

	// 1. load the prompts
	done := stage("load_prompts")
	prompts, err := loadLLMPrompts(p, job, env)
	if err != nil {
		done("failed", "", err)
		_, _ = finish()
		return nil, err
	}
	done("ok", fmt.Sprintf("%d prompts", len(prompts)), nil)

	// 2. preflight: is Ollama there and answering?
	done = stage("preflight")
	base, health := resolveOllama(ctx, p.OllamaURL, env)
	report.HealthBefore = health
	if !health.Answering {
		err := fmt.Errorf("Ollama is not answering at %s: %s", base, strings.Join(health.Problems, "; "))
		done("failed", healthLine(health), err)
		_, _ = finish()
		return nil, jobs.Errf(jobs.KindInfra, "%s", trimMsg(err.Error()+" | "+healthLine(health), 1800))
	}
	done("ok", healthLine(health), nil)

	// 3. pick the model
	done = stage("select_model")
	model, why := pickModel(p.Models, health.Models)
	if model == "" {
		err := fmt.Errorf("none of the asked models is installed in Ollama (asked %v, installed %v)", p.Models, health.Models)
		done("failed", why, err)
		_, _ = finish()
		return nil, jobs.Errf(jobs.KindInfra, "%s", trimMsg(err.Error(), 1800))
	}
	report.Model = model
	done("ok", why, nil)

	client := &ollamaClient{base: base, http: &http.Client{}}

	// 4. warm: load the model once, so six prompts do not race to load it
	if p.Warm == nil || *p.Warm {
		done = stage("warm")
		t0 := time.Now()
		wctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
		_, werr := client.chat(wctx, ollamaChatReq{Model: model, Messages: []LLMChatMessage{{Role: "user", Content: "Reply with the single word OK."}},
			Stream: false, KeepAlive: p.KeepAlive, Think: falsePtr(), Options: map[string]any{"num_ctx": p.NumCtx, "num_predict": 8}})
		cancel()
		if werr != nil {
			diag := diagnoseOllama(context.Background(), base, model)
			done("failed", healthLine(diag), werr)
			report.HealthAfter = diag
			_, _ = finish()
			return nil, jobs.Errf(jobs.KindInfra, "%s", trimMsg("warming "+model+" failed: "+werr.Error()+" | "+healthLine(diag), 1800))
		}
		done("ok", fmt.Sprintf("%s loaded in %.1f s", model, time.Since(t0).Seconds()), nil)
	}

	// 5. the prompts, K at a time, refilling as each finishes
	done = stage("prompts")
	results := runSlots(ctx, client, p, model, prompts, rep, logs)
	report.Prompts = results
	summarize(report)
	status := "ok"
	switch {
	case report.Summary.OK == 0:
		status = "failed"
	case report.Summary.Failed > 0 || report.Summary.Cancelled > 0:
		status = "partial"
	}
	var perr error
	if status != "ok" {
		perr = fmt.Errorf("%d of %d prompts failed (%v)", report.Summary.Failed, report.Summary.Total, report.Summary.ErrorClasses)
	}
	done(status, fmt.Sprintf("%d ok, %d failed, %d cancelled, %.1f tok/s aggregate", report.Summary.OK, report.Summary.Failed,
		report.Summary.Cancelled, report.Summary.AggregateTPS), perr)

	// 6. report
	done = stage("report")
	report.HealthAfter = diagnoseOllama(context.Background(), base, model)
	done("ok", healthLine(report.HealthAfter), nil)
	res, werr := finish()
	if werr != nil {
		return nil, werr
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if report.Summary.OK == 0 {
		return nil, jobs.Errf(jobs.KindInfra, "%s", trimMsg(fmt.Sprintf("no prompt of %d succeeded on %s: %v | %s",
			report.Summary.Total, model, report.Summary.ErrorClasses, healthLine(report.HealthAfter)), 1800))
	}
	rep.Progress(1, fmt.Sprintf("%d/%d prompts ok on %s in %.0f s", report.Summary.OK, report.Summary.Total, model,
		float64(report.Summary.WallMS)/1000))
	return res, nil
}

func falsePtr() *bool { f := false; return &f }

func trimMsg(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// loadLLMPrompts reads params.prompts and/or the "prompts" input file, gives every prompt an id, and checks them.
func loadLLMPrompts(p llmBatchParams, job *jobs.Job, env Env) ([]LLMPrompt, error) {
	prompts := append([]LLMPrompt{}, p.Prompts...)
	for _, in := range job.Payload.Inputs {
		path := env.Inputs[in.Name]
		if path == "" {
			return nil, jobs.Errf(jobs.KindInvalidInput, "input %q was not staged", in.Name)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, jobs.Wrap(jobs.KindInvalidInput, err, "read the prompts file")
		}
		var doc struct {
			Prompts []LLMPrompt `json:"prompts"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil || doc.Prompts == nil {
			var arr []LLMPrompt
			if err2 := json.Unmarshal(raw, &arr); err2 != nil {
				return nil, jobs.Errf(jobs.KindInvalidInput, "the prompts file is neither {\"prompts\":[...]} nor an array: %v", err)
			}
			doc.Prompts = arr
		}
		prompts = append(prompts, doc.Prompts...)
	}
	if len(prompts) == 0 {
		return nil, jobs.Errf(jobs.KindInvalidInput, "the batch has no prompts")
	}
	if len(prompts) > LLMBatchMaxPrompts {
		return nil, jobs.Errf(jobs.KindInvalidInput, "%d prompts is above the limit %d; split the batch", len(prompts), LLMBatchMaxPrompts)
	}
	seen := map[string]bool{}
	for i := range prompts {
		if prompts[i].PromptID == "" {
			prompts[i].PromptID = fmt.Sprintf("p%04d", i+1)
		}
		if seen[prompts[i].PromptID] {
			return nil, jobs.Errf(jobs.KindInvalidInput, "prompt_id %q appears twice", prompts[i].PromptID)
		}
		seen[prompts[i].PromptID] = true
		if strings.TrimSpace(prompts[i].Prompt) == "" && len(prompts[i].Messages) == 0 {
			return nil, jobs.Errf(jobs.KindInvalidInput, "prompt %s has neither prompt nor messages", prompts[i].PromptID)
		}
	}
	return prompts, nil
}

// pickModel returns the first asked model this Ollama has ("name" matches "name:latest" too). No model asked = the
// first installed model is NOT guessed: the caller must say which.
func pickModel(asked, installed []string) (string, string) {
	if len(asked) == 0 {
		return "", "no model was asked (params.model or params.models)"
	}
	have := map[string]string{}
	for _, m := range installed {
		have[m] = m
		have[strings.TrimSuffix(m, ":latest")] = m
	}
	for i, a := range asked {
		if m, ok := have[a]; ok {
			if i == 0 {
				return m, "first choice " + m + " is installed"
			}
			return m, fmt.Sprintf("%s is not installed; using choice %d: %s", asked[0], i+1, m)
		}
	}
	return "", "none installed"
}

/* ── the slot scheduler ───────────────────────────────────────────────────────────────────────── */

// runSlots keeps K prompts in flight; a slot that frees up takes the next prompt at once (no waves). Prompts go
// longest-first so one huge prompt does not start last and set the batch's end time. The first fill is staggered.
func runSlots(ctx context.Context, c *ollamaClient, p llmBatchParams, model string, prompts []LLMPrompt, rep Reporter, logs *llmLog) []LLMPromptResult {
	order := make([]int, len(prompts))
	for i := range order {
		order[i] = i
	}
	size := func(pr LLMPrompt) int {
		n := len(pr.Prompt) + len(pr.System)
		for _, m := range pr.Messages {
			n += len(m.Content)
		}
		return n
	}
	sort.SliceStable(order, func(a, b int) bool { return size(prompts[order[a]]) > size(prompts[order[b]]) })

	k := min(p.MaxParallel, len(prompts))
	results := make([]LLMPromptResult, len(prompts))
	queue := make(chan int)
	var mu sync.Mutex
	finished := 0
	stagger := time.Duration(p.StaggerMS) * time.Millisecond
	if p.StaggerMS == 0 {
		stagger = 400 * time.Millisecond
	}
	batchStart := time.Now()
	var wg sync.WaitGroup
	for slot := 0; slot < k; slot++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			if slot > 0 && stagger > 0 {
				select {
				case <-ctx.Done():
				case <-time.After(time.Duration(slot) * stagger):
				}
			}
			for i := range queue {
				pr := prompts[i]
				res := runOnePrompt(ctx, c, p, model, pr, slot, time.Since(batchStart), logs)
				mu.Lock()
				results[i] = res
				finished++
				n := finished
				mu.Unlock()
				rep.Progress(0.05+0.9*float64(n)/float64(len(prompts)), fmt.Sprintf("%d/%d prompts (%s %s)", n, len(prompts), pr.PromptID, res.Status))
			}
		}(slot)
	}
feed:
	for _, i := range order {
		select {
		case <-ctx.Done():
			break feed
		case queue <- i:
		}
	}
	close(queue)
	wg.Wait()
	for i := range results {
		if results[i].PromptID == "" { // never started (cancelled)
			results[i] = LLMPromptResult{PromptID: prompts[i].PromptID, Category: prompts[i].Category, Status: "cancelled",
				Attempts: []LLMAttempt{}, Metadata: prompts[i].Metadata}
		}
	}
	return results
}

func runOnePrompt(ctx context.Context, c *ollamaClient, p llmBatchParams, model string, pr LLMPrompt, slot int, queued time.Duration, logs *llmLog) LLMPromptResult {
	res := LLMPromptResult{PromptID: pr.PromptID, Category: pr.Category, Model: model, Attempts: []LLMAttempt{},
		Metadata: pr.Metadata, QueueMS: queued.Milliseconds(), promptChars: len(pr.Prompt)}
	msgs := pr.Messages
	if len(msgs) == 0 {
		if pr.System != "" {
			msgs = append(msgs, LLMChatMessage{Role: "system", Content: pr.System})
		}
		msgs = append(msgs, LLMChatMessage{Role: "user", Content: pr.Prompt})
	}
	opts := map[string]any{"num_ctx": p.NumCtx}
	if pr.NumCtx > 0 {
		opts["num_ctx"] = pr.NumCtx
	}
	if n := firstPos(pr.MaxOutputTokens, p.MaxOutputTokens); n > 0 {
		opts["num_predict"] = n
	}
	if t := pr.Temperature; t != nil {
		opts["temperature"] = *t
	} else if p.Temperature != nil {
		opts["temperature"] = *p.Temperature
	}
	req := ollamaChatReq{Model: model, Messages: msgs, Stream: false, KeepAlive: p.KeepAlive, Options: opts, Think: p.Think}
	if pr.Format == "json" {
		req.Format = "json"
	}
	t0 := time.Now()
	for n := 1; n <= 1+*p.Retries; n++ {
		if ctx.Err() != nil {
			res.Status = "cancelled"
			break
		}
		att := LLMAttempt{N: n, Slot: slot, StartedAt: time.Now()}
		actx, cancel := context.WithTimeout(ctx, time.Duration(p.PromptTimeoutS)*time.Second)
		out, err := c.chat(actx, req)
		cancel()
		att.MS = time.Since(att.StartedAt).Milliseconds()
		if err == nil && strings.TrimSpace(out.Message.Content) == "" && strings.TrimSpace(out.Message.Thinking) == "" {
			err = &ollamaErr{class: "empty", msg: "Ollama answered with no text (done_reason " + out.DoneReason + ")"}
		}
		if err == nil {
			att.OK = true
			res.Attempts = append(res.Attempts, att)
			res.Status = "ok"
			res.Text = out.Message.Content
			res.Thinking = out.Message.Thinking
			res.DoneReason = out.DoneReason
			res.TokensIn, res.TokensOut = out.PromptEvalCount, out.EvalCount
			res.PrefillMS, res.DecodeMS, res.LoadMS = out.PromptEvalDuration/1e6, out.EvalDuration/1e6, out.LoadDuration/1e6
			if out.EvalDuration > 0 {
				res.DecodeTPS = float64(out.EvalCount) / (float64(out.EvalDuration) / 1e9)
			}
			break
		}
		att.ErrorClass, att.HTTPStatus, att.Error = classifyOllamaErr(err, ctx)
		if att.ErrorClass == "cancelled" {
			res.Attempts = append(res.Attempts, att)
			res.Status = "cancelled"
			break
		}
		// the stage did not work: look at Ollama NOW and keep what we saw with the attempt
		att.Diagnosis = diagnoseOllama(context.Background(), c.base, model)
		res.Attempts = append(res.Attempts, att)
		logs.err("prompt %s attempt %d failed on slot %d after %d ms: %s %s | %s", pr.PromptID, n, slot, att.MS,
			att.ErrorClass, att.Error, healthLine(att.Diagnosis))
		res.Status = "failed"
		if att.ErrorClass == "context_overflow" || att.ErrorClass == "http_4xx" || att.ErrorClass == "model_missing" {
			break // retrying the same request cannot help
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(n*n) * 2 * time.Second):
		}
	}
	res.TotalMS = time.Since(t0).Milliseconds()
	return res
}

func firstPos(v ...int) int {
	for _, x := range v {
		if x > 0 {
			return x
		}
	}
	return 0
}

/* ── Ollama client ────────────────────────────────────────────────────────────────────────────── */

type ollamaClient struct {
	base string
	http *http.Client
}

type ollamaChatReq struct {
	Model     string           `json:"model"`
	Messages  []LLMChatMessage `json:"messages"`
	Stream    bool             `json:"stream"`
	KeepAlive string           `json:"keep_alive,omitempty"`
	Format    string           `json:"format,omitempty"`
	Options   map[string]any   `json:"options,omitempty"`
	Think     *bool            `json:"think,omitempty"`
}

type ollamaChatResp struct {
	Model   string `json:"model"`
	Message struct {
		Role     string `json:"role"`
		Content  string `json:"content"`
		Thinking string `json:"thinking"`
	} `json:"message"`
	Done               bool   `json:"done"`
	DoneReason         string `json:"done_reason"`
	TotalDuration      int64  `json:"total_duration"`
	LoadDuration       int64  `json:"load_duration"`
	PromptEvalCount    int    `json:"prompt_eval_count"`
	PromptEvalDuration int64  `json:"prompt_eval_duration"`
	EvalCount          int    `json:"eval_count"`
	EvalDuration       int64  `json:"eval_duration"`
	Error              string `json:"error"`
}

type ollamaErr struct {
	class  string
	status int
	msg    string
}

func (e *ollamaErr) Error() string { return e.msg }

func (c *ollamaClient) chat(ctx context.Context, body ollamaChatReq) (*ollamaChatResp, error) {
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/api/chat", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	var out ollamaChatResp
	_ = json.Unmarshal(raw, &out)
	if resp.StatusCode != http.StatusOK {
		msg := out.Error
		if msg == "" {
			msg = strings.TrimSpace(string(raw))
		}
		return nil, &ollamaErr{status: resp.StatusCode, msg: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, trimMsg(msg, 600))}
	}
	if out.Error != "" {
		return nil, &ollamaErr{status: resp.StatusCode, msg: out.Error}
	}
	return &out, nil
}

// classifyOllamaErr names what went wrong, in the words the report and the health page use.
func classifyOllamaErr(err error, parent context.Context) (string, int, string) {
	msg := err.Error()
	low := strings.ToLower(msg)
	var oe *ollamaErr
	status := 0
	if errors.As(err, &oe) {
		status = oe.status
		if oe.class != "" {
			return oe.class, status, msg
		}
	}
	switch {
	case parent.Err() != nil:
		return "cancelled", status, msg
	case errors.Is(err, context.DeadlineExceeded) || strings.Contains(low, "deadline exceeded") || strings.Contains(low, "timeout"):
		return "timeout", status, msg
	case strings.Contains(low, "connection refused") || strings.Contains(low, "no such host") || strings.Contains(low, "eof") && status == 0:
		return "ollama_down", status, msg
	case strings.Contains(low, "not found") && strings.Contains(low, "model"):
		return "model_missing", status, msg
	case strings.Contains(low, "context") && (strings.Contains(low, "exceed") || strings.Contains(low, "too long") || strings.Contains(low, "length")):
		return "context_overflow", status, msg
	case strings.Contains(low, "out of memory") || strings.Contains(low, "insufficient memory") || strings.Contains(low, "requires more system memory"):
		return "oom", status, msg
	case status >= 500:
		return "http_5xx", status, msg
	case status >= 400:
		return "http_4xx", status, msg
	}
	return "other", status, msg
}

/* ── health and diagnosis ─────────────────────────────────────────────────────────────────────── */

// resolveOllama picks the Ollama to talk to: params (loopback only) → the engine probe → APS_OLLAMA_URL → OLLAMA_HOST →
// the default ports. The first that answers wins; when none does, the last diagnosis says why.
func resolveOllama(ctx context.Context, asked string, env Env) (string, *OllamaHealth) {
	var cands []string
	add := func(u string) {
		u = strings.TrimRight(strings.TrimSpace(u), "/")
		if u == "" {
			return
		}
		if !strings.Contains(u, "://") {
			u = "http://" + u
		}
		for _, c := range cands {
			if c == u {
				return
			}
		}
		cands = append(cands, u)
	}
	add(asked)
	if e := env.Engines["ollama"]; e != nil {
		add(e.URL)
	}
	add(os.Getenv("APS_OLLAMA_URL"))
	if h := os.Getenv("OLLAMA_HOST"); h != "" {
		h = strings.Replace(h, "0.0.0.0", "127.0.0.1", 1)
		add(h)
	}
	for _, d := range DefaultOllamaURLs {
		add(d)
	}
	var last *OllamaHealth
	for _, c := range cands {
		if checkLoopbackURL(c) != nil {
			continue
		}
		h := diagnoseOllama(ctx, c, "")
		if h.Answering {
			return c, h
		}
		last = h
	}
	if last == nil {
		last = &OllamaHealth{At: time.Now(), Problems: []string{"no loopback Ollama URL to try"}}
	}
	last.Problems = append(last.Problems, "tried "+strings.Join(cands, ", "))
	return cands[len(cands)-1], last
}

// diagnoseOllama looks at Ollama from every side it can, fast (each probe ≤ 5 s): port, /api/version, /api/tags (is
// `model` installed), /api/ps (what is loaded), `ollama` processes, free memory, the OLLAMA_* environment.
func diagnoseOllama(ctx context.Context, base, model string) *OllamaHealth {
	t0 := time.Now()
	h := &OllamaHealth{At: t0, URL: base, Env: map[string]string{}}
	defer func() { h.ElapsedMS = time.Since(t0).Milliseconds() }()
	for _, k := range []string{"OLLAMA_HOST", "OLLAMA_NUM_PARALLEL", "OLLAMA_MAX_LOADED_MODELS", "OLLAMA_KEEP_ALIVE",
		"OLLAMA_CONTEXT_LENGTH", "OLLAMA_FLASH_ATTENTION", "OLLAMA_KV_CACHE_TYPE", "OLLAMA_MODELS"} {
		if v := os.Getenv(k); v != "" {
			h.Env[k] = v
		}
	}
	if u, err := url.Parse(base); err == nil && u.Host != "" {
		hostport := u.Host
		if u.Port() == "" {
			hostport = net.JoinHostPort(u.Hostname(), "80")
		}
		conn, err := net.DialTimeout("tcp", hostport, 2*time.Second)
		if err == nil {
			h.PortOpen = true
			conn.Close()
		} else {
			h.Problems = append(h.Problems, "port closed: "+err.Error())
		}
	}
	get := func(path string, v any) error {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(c, http.MethodGet, base+path, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("%s: HTTP %d", path, resp.StatusCode)
		}
		return json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(v)
	}
	if h.PortOpen {
		var ver struct {
			Version string `json:"version"`
		}
		if err := get("/api/version", &ver); err == nil {
			h.Answering, h.Version = true, ver.Version
		} else {
			h.Problems = append(h.Problems, "/api/version: "+err.Error())
		}
		var tags struct {
			Models []struct {
				Name string `json:"name"`
			} `json:"models"`
		}
		if err := get("/api/tags", &tags); err == nil {
			for _, m := range tags.Models {
				h.Models = append(h.Models, m.Name)
			}
			sort.Strings(h.Models)
		} else {
			h.Problems = append(h.Problems, "/api/tags: "+err.Error())
		}
		var ps struct {
			Models []struct {
				Name      string `json:"name"`
				Size      int64  `json:"size"`
				SizeVRAM  int64  `json:"size_vram"`
				ExpiresAt string `json:"expires_at"`
				Context   int    `json:"context_length"`
			} `json:"models"`
		}
		if err := get("/api/ps", &ps); err == nil {
			for _, m := range ps.Models {
				h.Loaded = append(h.Loaded, OllamaLoaded{Name: m.Name, SizeGB: bytesGB(m.Size), VRAMGB: bytesGB(m.SizeVRAM), ExpiresAt: m.ExpiresAt, Context: m.Context})
			}
		} else {
			h.Problems = append(h.Problems, "/api/ps: "+err.Error())
		}
		if model != "" && h.Answering {
			found := false
			for _, m := range h.Models {
				if m == model || strings.TrimSuffix(m, ":latest") == model {
					found = true
				}
			}
			if !found {
				h.Problems = append(h.Problems, "model "+model+" is not installed")
			}
		}
	}
	h.Processes = ollamaProcesses(ctx)
	if len(h.Processes) == 0 {
		h.Problems = append(h.Problems, "no ollama process is running")
	}
	if free, err := freeRAMGBForDiag(); err == nil {
		h.FreeRAMGB = free
	}
	return h
}

func bytesGB(b int64) float64 { return float64(int64(float64(b)/1e9*10+0.5)) / 10 }

// ollamaProcesses lists running ollama processes (pgrep), so "the port is closed" and "Ollama is not running" can be
// told apart from "Ollama runs but listens elsewhere".
func ollamaProcesses(ctx context.Context) []string {
	pg, err := exec.LookPath("pgrep")
	if err != nil {
		return nil
	}
	c, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, _ := exec.CommandContext(c, pg, "-fl", "ollama").Output()
	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if l = strings.TrimSpace(l); l != "" && !strings.Contains(l, "pgrep") {
			lines = append(lines, trimMsg(l, 200))
		}
	}
	return lines
}

// freeRAMGBForDiag is a hook for tests; in production it reads the machine's free memory.
var freeRAMGBForDiag = func() (float64, error) {
	free, _, ok := hardware.FreeRAM()
	if !ok {
		return 0, errors.New("free RAM unknown on this OS")
	}
	return free, nil
}

func healthLine(h *OllamaHealth) string {
	if h == nil {
		return "no health sample"
	}
	parts := []string{"ollama " + h.URL}
	if h.Answering {
		parts = append(parts, "answering v"+h.Version)
	} else if h.PortOpen {
		parts = append(parts, "port open but not answering")
	} else {
		parts = append(parts, "port closed")
	}
	parts = append(parts, fmt.Sprintf("%d models installed", len(h.Models)))
	if len(h.Loaded) > 0 {
		var l []string
		for _, m := range h.Loaded {
			l = append(l, fmt.Sprintf("%s %.0fGB", m.Name, m.SizeGB))
		}
		parts = append(parts, "loaded: "+strings.Join(l, ", "))
	} else {
		parts = append(parts, "nothing loaded")
	}
	parts = append(parts, fmt.Sprintf("%d ollama processes", len(h.Processes)))
	if h.FreeRAMGB > 0 {
		parts = append(parts, fmt.Sprintf("free RAM %.0f GB", h.FreeRAMGB))
	}
	if len(h.Problems) > 0 {
		parts = append(parts, "problems: "+strings.Join(h.Problems, "; "))
	}
	return strings.Join(parts, " · ")
}

/* ── summary and outputs ──────────────────────────────────────────────────────────────────────── */

func summarize(r *LLMBatchReport) {
	s := LLMBatchSummary{Total: len(r.Prompts), ErrorClasses: map[string]int{}}
	var times []int64
	decodeMS := int64(0)
	for _, pr := range r.Prompts {
		switch pr.Status {
		case "ok":
			s.OK++
			times = append(times, pr.TotalMS)
		case "cancelled":
			s.Cancelled++
		default:
			s.Failed++
		}
		if len(pr.Attempts) > 1 {
			s.Retried++
		}
		for _, a := range pr.Attempts {
			if !a.OK && a.ErrorClass != "" {
				s.ErrorClasses[a.ErrorClass]++
			}
		}
		s.TokensIn += pr.TokensIn
		s.TokensOut += pr.TokensOut
		decodeMS += pr.DecodeMS
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	if n := len(times); n > 0 {
		s.MedianPromptMS = times[n/2]
		s.P95PromptMS = times[min(n-1, int(float64(n)*0.95))]
	}
	wall := time.Since(r.StartedAt).Seconds()
	if wall > 0 {
		s.AggregateTPS = float64(int(float64(s.TokensOut)/wall*10)) / 10
	}
	if len(s.ErrorClasses) == 0 {
		s.ErrorClasses = nil
	}
	r.Summary = s
}

func writeLLMOutputs(env Env, r *LLMBatchReport) (*Result, error) {
	if err := os.MkdirAll(env.OutDir, 0o755); err != nil {
		return nil, jobs.Wrap(jobs.KindInfra, err, "make the output directory")
	}
	type resultRow struct {
		PromptID  string         `json:"prompt_id"`
		Status    string         `json:"status"`
		Text      string         `json:"text,omitempty"`
		Model     string         `json:"model,omitempty"`
		TokensIn  int            `json:"tokens_in"`
		TokensOut int            `json:"tokens_out"`
		TotalMS   int64          `json:"total_ms"`
		Error     string         `json:"error,omitempty"`
		Metadata  map[string]any `json:"metadata,omitempty"`
	}
	rows := make([]resultRow, 0, len(r.Prompts))
	for _, p := range r.Prompts {
		row := resultRow{PromptID: p.PromptID, Status: p.Status, Text: p.Text, Model: p.Model, TokensIn: p.TokensIn,
			TokensOut: p.TokensOut, TotalMS: p.TotalMS, Metadata: p.Metadata}
		if n := len(p.Attempts); n > 0 && !p.Attempts[n-1].OK {
			row.Error = p.Attempts[n-1].ErrorClass + ": " + p.Attempts[n-1].Error
		}
		rows = append(rows, row)
	}
	resOut, err := writeJSONOutput(env, "llm_results", map[string]any{"schema": "act3.llm_batch_results.v1",
		"batch_id": r.BatchID, "model": r.Model, "results": rows})
	if err != nil {
		return nil, err
	}
	repOut, err := writeJSONOutput(env, "llm_report", r)
	if err != nil {
		return nil, err
	}
	summary := map[string]any{"schema": r.Schema, "batch_id": r.BatchID, "model": r.Model, "total": r.Summary.Total,
		"ok": r.Summary.OK, "failed": r.Summary.Failed, "cancelled": r.Summary.Cancelled, "wall_ms": r.Summary.WallMS,
		"aggregate_decode_tps": r.Summary.AggregateTPS, "median_prompt_ms": r.Summary.MedianPromptMS,
		"p95_prompt_ms": r.Summary.P95PromptMS, "error_classes": r.Summary.ErrorClasses, "stages": stageWords(r.Stages),
		"errors": len(r.Errors)}
	repOut.Meta = summary
	return &Result{Outputs: []jobs.Output{resOut, repOut}, Data: summary}, nil
}

func stageWords(st []LLMStage) []string {
	out := make([]string, 0, len(st))
	for _, s := range st {
		out = append(out, fmt.Sprintf("%s:%s", s.Name, s.Status))
	}
	return out
}

/* ── logs: ~/_act3_logs/llm_batch.{info,err} ──────────────────────────────────────────────────── */

type llmLog struct {
	jobID string
	mu    sync.Mutex
}

var llmLogDir = func() string {
	if d := os.Getenv("APS_LLM_LOG_DIR"); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "_act3_logs")
}

func newLLMLog(jobID string) *llmLog { return &llmLog{jobID: jobID} }

var pst = func() *time.Location {
	if l, err := time.LoadLocation("America/Los_Angeles"); err == nil {
		return l
	}
	return time.Local
}()

func (l *llmLog) write(file, level, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	dir := llmLogDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	f, err := os.OpenFile(filepath.Join(dir, file), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "[%s] [%s] [llm_batch %s] %s\n", time.Now().In(pst).Format("2006-01-02T15:04:05.000-07:00"), level, l.jobID,
		strings.ReplaceAll(fmt.Sprintf(format, args...), "\n", " "))
}

func (l *llmLog) info(format string, args ...any) { l.write("llm_batch.info", "INFO", format, args...) }

// err goes to BOTH files: the .err file is what a person (or the next AI pass) reads to learn what broke.
func (l *llmLog) err(format string, args ...any) {
	l.write("llm_batch.err", "ERROR", format, args...)
	l.write("llm_batch.info", "ERROR", format, args...)
}
