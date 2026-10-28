// Package api is the local HTTP API (spec §13): JSON in and out, bearer token on every route except
// GET /v1/health (and on every route when bound to a non-loopback address), errors as {"error":{code,message}}.
package api

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/errlog"
	"github.com/ACT3ai/ai_processing_server/internal/jobs"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
	"github.com/ACT3ai/ai_processing_server/internal/scheduler"
)

// Options configure the router.
type Options struct {
	Token           string
	AllowDirectJobs func() bool
	Loopback        bool // listening on 127.0.0.1 / ::1 only
	Logger          *slog.Logger
}

// NewRouter builds the handler over the scheduler.
func NewRouter(s *scheduler.Server, o Options) http.Handler {
	h := &handler{s: s, o: o, log: o.Logger.With("ctx", "api")}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", h.health)
	mux.HandleFunc("GET /v1/server", h.auth(h.server))
	mux.HandleFunc("GET /v1/hardware", h.auth(h.hardware))
	mux.HandleFunc("POST /v1/hardware/refresh", h.auth(h.hardwareRefresh))
	mux.HandleFunc("GET /v1/capabilities", h.auth(h.capabilities))
	mux.HandleFunc("GET /v1/services", h.auth(h.services))
	mux.HandleFunc("POST /v1/services/{id}/pause", h.auth(h.pauseService))
	mux.HandleFunc("POST /v1/services/{id}/resume", h.auth(h.resumeService))
	mux.HandleFunc("GET /v1/queues", h.auth(h.queues))
	mux.HandleFunc("GET /v1/jobs", h.auth(h.listJobs))
	mux.HandleFunc("POST /v1/jobs", h.auth(h.submit))
	mux.HandleFunc("GET /v1/jobs/{id}", h.auth(h.getJob))
	mux.HandleFunc("GET /v1/jobs/{id}/log", h.auth(h.jobLog))
	mux.HandleFunc("GET /v1/jobs/{id}/outputs/{name}", h.auth(h.output))
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", h.auth(h.cancel))
	mux.HandleFunc("GET /v1/broker", h.auth(h.broker))
	mux.HandleFunc("POST /v1/queue/jobs", h.auth(h.queueSubmit))
	mux.HandleFunc("GET /v1/queue/jobs", h.auth(h.queueList))
	mux.HandleFunc("GET /v1/queue/jobs/{id}", h.auth(h.queueJob))
	mux.HandleFunc("POST /v1/queue/jobs/{id}/cancel", h.auth(h.queueCancel))
	mux.HandleFunc("POST /v1/drain", h.auth(h.drain))
	mux.HandleFunc("POST /v1/resume", h.auth(h.resume))
	mux.HandleFunc("POST /v1/reload", h.auth(h.reload))
	mux.HandleFunc("GET /metrics", h.authIfLAN(h.metrics))
	mux.HandleFunc("GET /{$}", h.authIfLAN(h.status))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeErr(w, http.StatusNotFound, "not_found", "no route "+r.Method+" "+r.URL.Path)
	})
	return h.recoverer(mux)
}

type handler struct {
	s   *scheduler.Server
	o   Options
	log *slog.Logger
}

func (h *handler) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if h.o.Token == "" || subtle.ConstantTimeCompare([]byte(got), []byte(h.o.Token)) != 1 {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "send Authorization: Bearer <token from local_api.token_file>")
			return
		}
		next(w, r)
	}
}

func (h *handler) authIfLAN(next http.HandlerFunc) http.HandlerFunc {
	if h.o.Loopback {
		return next
	}
	return h.auth(next)
}

func (h *handler) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				h.log.Error("api panic", "path", r.URL.Path, "panic", fmt.Sprint(rec))
				errlog.For("api").Panic("serving "+r.Method+" "+r.URL.Path, rec)
				writeErr(w, http.StatusInternalServerError, "internal", "internal error")
			}
		}()
		if h.o.Loopback && !localRequest(r) {
			writeErr(w, http.StatusForbidden, "forbidden_origin", "the local API only answers requests addressed to localhost from localhost pages")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// localRequest blocks DNS rebinding and cross-site browser calls against a loopback listener
// (apis.mdx §10, security.mdx §13): the Host must be a loopback name, and an Origin, when sent, must be too.
func localRequest(r *http.Request) bool {
	loop := func(hostport string) bool {
		host := hostport
		if h, _, err := net.SplitHostPort(hostport); err == nil {
			host = h
		}
		host = strings.Trim(host, "[]")
		if host == "localhost" {
			return true
		}
		ip := net.ParseIP(host)
		return ip != nil && ip.IsLoopback()
	}
	if !loop(r.Host) {
		return false
	}
	if o := r.Header.Get("Origin"); o != "" && o != "null" {
		u, err := url.Parse(o)
		if err != nil || !loop(u.Host) {
			return false
		}
	}
	return true
}

func (h *handler) health(w http.ResponseWriter, _ *http.Request)   { writeJSON(w, 200, h.s.Health()) }
func (h *handler) server(w http.ResponseWriter, _ *http.Request)   { writeJSON(w, 200, h.s.Info()) }
func (h *handler) hardware(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, h.s.Hardware()) }
func (h *handler) capabilities(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{"offers": h.s.Offers()})
}
func (h *handler) services(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{"services": h.s.Services()})
}
func (h *handler) queues(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, h.s.Queues()) }

func (h *handler) hardwareRefresh(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	h.s.RefreshHardware(ctx)
	writeJSON(w, 200, h.s.Hardware())
}

func (h *handler) pauseService(w http.ResponseWriter, r *http.Request) {
	if err := h.s.PauseService(r.PathValue("id"), "owner"); err != nil {
		writeSchedErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "service": r.PathValue("id"), "paused": true})
}

func (h *handler) resumeService(w http.ResponseWriter, r *http.Request) {
	if err := h.s.ResumeService(r.PathValue("id")); err != nil {
		writeSchedErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "service": r.PathValue("id"), "paused": false})
}

func (h *handler) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	list := h.s.List(scheduler.Filter{State: q.Get("state"), Service: q.Get("service"), Queue: q.Get("queue")})
	if n, err := strconv.Atoi(q.Get("limit")); err == nil && n >= 0 && n < len(list) {
		list = list[:n]
	}
	writeJSON(w, 200, map[string]any{"jobs": list, "count": len(list)})
}

func (h *handler) submit(w http.ResponseWriter, r *http.Request) {
	if h.o.AllowDirectJobs != nil && !h.o.AllowDirectJobs() {
		writeErr(w, http.StatusForbidden, "direct_jobs_disabled", "direct mode is off: set local_api.allow_direct_jobs: true in this_server.yaml")
		return
	}
	var env jobs.Envelope
	dec := json.NewDecoder(io.LimitReader(r.Body, 4<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if env.Service == "" {
		writeErr(w, http.StatusBadRequest, "invalid_input", "service is required")
		return
	}
	j, err := h.s.Submit(env, "direct")
	if err != nil {
		writeSchedErr(w, err)
		return
	}
	w.Header().Set("Location", "/v1/jobs/"+j.ID)
	writeJSON(w, http.StatusAccepted, j)
}

// ---- task queue (pm/task_queue.mdx §10, §13) ----

func (h *handler) broker(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	writeJSON(w, 200, h.s.Broker(ctx))
}

// queueSubmit stores a task envelope (§13.2) in the queue as caller "local". Any server offering the service runs it.
func (h *handler) queueSubmit(w http.ResponseWriter, r *http.Request) {
	var env queue.Envelope
	dec := json.NewDecoder(io.LimitReader(r.Body, queue.MaxEnvelopeBytes+1))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	ack, err := h.s.QueueSubmit(ctx, &env)
	if err != nil {
		writeQueueErr(w, err)
		return
	}
	w.Header().Set("Location", "/v1/queue/jobs/"+ack.JobID)
	writeJSON(w, http.StatusAccepted, ack)
}

// queueList is GET /v1/queue/jobs?limit=N: what the WORK stream holds (waiting + claimed), with job_state each.
func (h *handler) queueList(w http.ResponseWriter, r *http.Request) {
	limit := 200
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 && n <= 1000 {
		limit = n
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	items, err := h.s.QueueList(ctx, limit)
	if err != nil {
		writeQueueErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"jobs": items})
}

func (h *handler) queueJob(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	j, err := h.s.QueueJob(ctx, r.PathValue("id"))
	if err != nil {
		writeQueueErr(w, err)
		return
	}
	writeJSON(w, 200, j)
}

func (h *handler) queueCancel(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := h.s.QueueCancel(ctx, r.PathValue("id"), "owner (local API)"); err != nil {
		writeQueueErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "job_id": r.PathValue("id"), "cancel": "requested"})
}

func writeQueueErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, scheduler.ErrBroker), errors.Is(err, queue.ErrUnavailable):
		writeErr(w, http.StatusServiceUnavailable, "broker_missing", err.Error())
	case errors.Is(err, queue.ErrInvalid):
		writeErr(w, http.StatusBadRequest, "invalid_input", err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		writeErr(w, http.StatusGatewayTimeout, "broker_timeout", err.Error())
	default:
		writeSchedErr(w, err)
	}
}

func (h *handler) getJob(w http.ResponseWriter, r *http.Request) {
	j, err := h.s.Get(r.PathValue("id"))
	if err != nil {
		writeSchedErr(w, err)
		return
	}
	writeJSON(w, 200, j)
}

func (h *handler) jobLog(w http.ResponseWriter, r *http.Request) {
	p, err := h.s.LogPath(r.PathValue("id"))
	if err != nil {
		writeSchedErr(w, err)
		return
	}
	f, err := os.Open(p)
	if err != nil {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		return // job exists but has not logged yet
	}
	defer f.Close()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	tail, _ := strconv.Atoi(r.URL.Query().Get("tail"))
	if tail <= 0 {
		io.Copy(w, f)
		return
	}
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		lines = append(lines, sc.Text())
		if len(lines) > tail {
			lines = lines[1:]
		}
	}
	for _, l := range lines {
		fmt.Fprintln(w, l)
	}
}

func (h *handler) output(w http.ResponseWriter, r *http.Request) {
	p, ct, err := h.s.OutputPath(r.PathValue("id"), r.PathValue("name"))
	if err != nil {
		writeErr(w, http.StatusNotFound, "not_found", "no kept output named "+r.PathValue("name")+" for this job")
		return
	}
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	http.ServeFile(w, r, p)
}

func (h *handler) cancel(w http.ResponseWriter, r *http.Request) {
	j, err := h.s.Cancel(r.PathValue("id"), "the machine owner")
	if err != nil {
		writeSchedErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, j)
}

func (h *handler) drain(w http.ResponseWriter, _ *http.Request) {
	h.s.Drain()
	writeJSON(w, 200, h.s.Health())
}

func (h *handler) resume(w http.ResponseWriter, _ *http.Request) {
	h.s.Resume()
	writeJSON(w, 200, h.s.Health())
}

func (h *handler) reload(w http.ResponseWriter, _ *http.Request) {
	warns, err := h.s.Reload()
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_config", err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "warnings": warns, "offers": h.s.Offers()})
}

func (h *handler) metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	io.WriteString(w, h.s.Metrics())
}

// status is a plain server-rendered page for a quick look in a browser (no JavaScript).
func (h *handler) status(w http.ResponseWriter, _ *http.Request) {
	info := h.s.Info()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	e := html.EscapeString
	fmt.Fprintf(w, `<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="5"><title>%s processing server</title>
<style>body{font:14px system-ui,sans-serif;margin:24px;color:#222;background:#fff}table{border-collapse:collapse}td,th{padding:4px 10px;border-bottom:1px solid #ddd;text-align:left}
@media (prefers-color-scheme:dark){body{background:#111;color:#ddd}td,th{border-color:#333}}</style>
<h1>%s · %s</h1><p>State <b>%s</b> %s · version %s · up %ds · slots %d/%d cores</p>`,
		e(info.Identity.Key), e(info.Identity.Key), e(info.Identity.DisplayName), e(info.State), e(info.Reason), e(info.Version),
		info.UptimeS, info.Slots.CoresUsed, info.Slots.CoresTotal)
	fmt.Fprint(w, "<h2>Services</h2><table><tr><th>service</th><th>parallel</th><th>running</th><th>waiting</th><th>reason</th></tr>")
	for _, sv := range h.s.Services() {
		fmt.Fprintf(w, "<tr><td>%s</td><td>%d</td><td>%d</td><td>%d</td><td>%s</td></tr>", e(sv.Service), sv.Parallel, sv.Running, sv.Waiting, e(sv.Reason))
	}
	fmt.Fprint(w, "</table><h2>Recent jobs</h2><table><tr><th>id</th><th>service</th><th>state</th><th>stage</th><th>progress</th><th>note</th></tr>")
	for i, j := range h.s.List(scheduler.Filter{}) {
		if i >= 30 {
			break
		}
		fmt.Fprintf(w, "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%.0f%%</td><td>%s</td></tr>", e(j.ID), e(j.Type), e(string(j.State)), e(string(j.Stage)), j.Progress*100, e(j.Note+" "+j.LastErr))
	}
	fmt.Fprint(w, "</table>")
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func writeErr(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]string{"code": kind, "message": msg}})
}

func writeSchedErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, scheduler.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, scheduler.ErrUnknownService):
		writeErr(w, http.StatusNotFound, "unknown_service", err.Error())
	case errors.Is(err, scheduler.ErrNotOffered):
		writeErr(w, http.StatusConflict, "service_not_offered", err.Error())
	case errors.Is(err, scheduler.ErrConflict):
		writeErr(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, scheduler.ErrFinal):
		writeErr(w, http.StatusConflict, "already_final", err.Error())
	case errors.Is(err, scheduler.ErrInvalid):
		writeErr(w, http.StatusBadRequest, "invalid_input", err.Error())
	default:
		errlog.For("api").Error("answering a local API call (no error code mapped)", err)
		writeErr(w, http.StatusInternalServerError, "internal", err.Error())
	}
}

// IsLoopback reports whether a listen address only accepts local connections.
func IsLoopback(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
