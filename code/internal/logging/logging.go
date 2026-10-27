// Package logging writes the ACT3 log format to ~/_act3_logs/ (spec §18):
//
//	[ISO_TIMESTAMP_PST] [LEVEL] [CONTEXT] message key=value ...
//
// INFO and DEBUG go to processing_server.info; WARN and ERROR go to processing_server.err.
// Both files are append-only and rotated by size by this process, so no external tool is needed.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	_ "time/tzdata" // America/Los_Angeles on machines without a zoneinfo database (Windows)

	"github.com/ACT3ai/ai_processing_server/internal/errlog"
	"github.com/ACT3ai/ai_processing_server/internal/redact"
)

// ContextKey is the attribute that becomes the [CONTEXT] column instead of a key=value pair.
const ContextKey = "ctx"

const (
	rotateBytes = 50 << 20
	rotateKeep  = 5
)

var (
	pst  = loadPST()
	elog = errlog.For("logging")
)

func loadPST() *time.Location {
	if loc, err := time.LoadLocation("America/Los_Angeles"); err == nil {
		return loc
	}
	return time.Local
}

// Open creates the info and err writers in dir and returns a logger that also echoes to console (may be nil).
func Open(dir string, console io.Writer, level slog.Level) (*slog.Logger, func() error, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create log dir %s: %w", dir, err)
	}
	info, err := newRotatingFile(filepath.Join(dir, "processing_server.info"))
	if err != nil {
		return nil, nil, err
	}
	errf, err := newRotatingFile(filepath.Join(dir, "processing_server.err"))
	if err != nil {
		info.Close()
		return nil, nil, err
	}
	h := &Handler{mu: &sync.Mutex{}, info: info, err: errf, console: console, level: level}
	closer := func() error {
		e1 := info.Close()
		e2 := errf.Close()
		if e1 != nil {
			return e1
		}
		return e2
	}
	return slog.New(h), closer, nil
}

// DefaultDir is ~/_act3_logs, or $ACT3_LOG_DIR when set.
func DefaultDir() string {
	if d := os.Getenv("ACT3_LOG_DIR"); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "_act3_logs")
}

// Handler is a slog.Handler producing the ACT3 line format.
type Handler struct {
	mu      *sync.Mutex
	info    io.Writer
	err     io.Writer
	console io.Writer
	level   slog.Level
	attrs   []slog.Attr
	prefix  string
	context string
}

func (h *Handler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }

func (h *Handler) Handle(_ context.Context, r slog.Record) error {
	ctxName := h.context
	var kv strings.Builder
	write := func(a slog.Attr) {
		if a.Key == ContextKey {
			ctxName = a.Value.String()
			return
		}
		writeAttr(&kv, h.prefix, a)
	}
	for _, a := range h.attrs {
		write(a)
	}
	r.Attrs(func(a slog.Attr) bool { write(a); return true })
	if ctxName == "" {
		ctxName = "main"
	}
	line := fmt.Sprintf("[%s] [%s] [%s] %s%s\n",
		r.Time.In(pst).Format("2006-01-02T15:04:05.000-07:00"), levelName(r.Level), ctxName, r.Message, kv.String())

	h.mu.Lock()
	defer h.mu.Unlock()
	w := h.info
	if r.Level >= slog.LevelWarn {
		w = h.err
	}
	_, err := io.WriteString(w, line)
	if h.console != nil {
		io.WriteString(h.console, line)
	}
	return err
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	n := *h
	n.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &n
}

func (h *Handler) WithGroup(name string) slog.Handler {
	n := *h
	n.prefix = h.prefix + name + "."
	return &n
}

func levelName(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return "ERROR"
	case l >= slog.LevelWarn:
		return "WARN"
	case l >= slog.LevelInfo:
		return "INFO"
	default:
		return "DEBUG"
	}
}

func writeAttr(b *strings.Builder, prefix string, a slog.Attr) {
	if a.Equal(slog.Attr{}) {
		return
	}
	if a.Value.Kind() == slog.KindGroup {
		for _, g := range a.Value.Group() {
			writeAttr(b, prefix+a.Key+".", g)
		}
		return
	}
	val := Redact(a.Key, a.Value.Resolve().String())
	if strings.ContainsAny(val, " \t\n\"=") {
		val = fmt.Sprintf("%q", val)
	}
	fmt.Fprintf(b, " %s%s=%s", prefix, a.Key, val)
}

// Redact hides secret-looking values, PEM keys, enrollment codes, bearer tokens and presigned query strings
// (pm/security.mdx §14). The rules live in package redact, shared with error.err.
func Redact(key, val string) string { return redact.Value(key, val) }

// RedactURL returns scheme://host/path for an http(s) URL with a query string, and anything else unchanged.
func RedactURL(s string) string { return redact.URL(s) }

// rotatingFile is an O_APPEND file that rolls to .1 ... .5 past rotateBytes.
type rotatingFile struct {
	mu   sync.Mutex
	path string
	f    *os.File
	size int64
}

func newRotatingFile(path string) (*rotatingFile, error) {
	r := &rotatingFile{path: path}
	if err := r.open(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *rotatingFile) open() error {
	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open log %s: %w", r.path, err)
	}
	st, _ := f.Stat()
	r.f = f
	if st != nil {
		r.size = st.Size()
	}
	return nil
}

func (r *rotatingFile) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size+int64(len(p)) > rotateBytes {
		r.rotate()
	}
	n, err := r.f.Write(p)
	r.size += int64(n)
	return n, err
}

func (r *rotatingFile) rotate() {
	elog.Ignore("closing a log before rotating it", r.f.Close(), "path", r.path)
	for i := rotateKeep - 1; i >= 1; i-- {
		elog.Ignore("shifting an old log", os.Rename(fmt.Sprintf("%s.%d", r.path, i), fmt.Sprintf("%s.%d", r.path, i+1)), "path", r.path)
	}
	elog.Check("rotating a log", os.Rename(r.path, r.path+".1"), "path", r.path)
	if err := r.open(); err != nil {
		// Fall back to stderr rather than losing log lines.
		elog.Error("reopening a log after rotation; writing to stderr instead", err, "path", r.path)
		r.f = os.Stderr
	}
	r.size = 0
}

func (r *rotatingFile) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.f == os.Stderr {
		return nil
	}
	return r.f.Close()
}
