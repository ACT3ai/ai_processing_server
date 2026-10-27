// Package errlog is the one place that records errors that should not happen: bugs, panics, lost writes, broken
// state on disk. Everything goes to one rolling file, by default ~/T/ai_processing_server/error.err, capped at 4 MB
// (pm/errors.mdx). Expected failures (a job's bad input, a download that is retried, a missing optional engine)
// do not belong here; they go to the job log and ~/_act3_logs/ as before.
//
// One line per entry, the same shape as error.err in our other apps (the_starbucks, we_citizens, all/app):
//
//	[2026-10-27T16:20:11.402Z] [ERROR] [scheduler] failed saving the job journal: open …/x.json: no space left on device job_id=7c0e at=/Users/…/code/internal/scheduler/server.go:612 (scheduler.(*Server).save) via=…/execute.go:233
//
// Times are UTC. at= is the absolute source path and line of the call site, via= the two callers above it.
// Panics and FATAL entries add the whole stack, folded onto the same line with " | ". Control characters are
// stripped so an entry is always one line, capped at 8000 bytes. An identical failure (same context, attempt and
// cause) repeated within 30 s is counted, not repeated: the next line that gets through ends with
// "(+N identical in the previous 30s)".
//
// Usage:
//
//	var elog = errlog.For("scheduler")
//	if err := s.journal.Save(j); err != nil {
//		elog.Error("saving the job journal", err, "job_id", j.ID)
//	}
//	defer elog.Recover("the admit loop")   // in every goroutine we start
//
// Before Init is called (unit tests, the CLI subcommands) entries go to stderr only. errlog never panics and never
// returns an error to the caller: if the file cannot be written, the line goes to stderr.
package errlog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/redact"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Levels written to error.err.
const (
	LevelWarn  = "WARN"
	LevelError = "ERROR"
	LevelFatal = "FATAL"
)

const (
	// ModulePath is stripped from -trimpath file names and replaced by Options.SourceRoot.
	ModulePath = "github.com/ACT3ai/ai_processing_server"
	maxLine    = 8000
	burstEvery = 30 * time.Second
)

// Options configure the process-wide error log.
type Options struct {
	Dir        string    // default DefaultDir()
	MaxMB      int       // live file cap; default 4
	Backups    int       // rolled files kept (error.err-<time>.err); default 3
	SourceRoot string    // absolute path of code/, so at= paths are full paths even in a -trimpath build
	Mirror     io.Writer // also write each line here (the server's .err log or stderr); may be nil
}

// DefaultDir is ~/T/ai_processing_server, or $APS_ERROR_DIR when set (tests point it at a temp dir).
func DefaultDir() string {
	if d := os.Getenv("APS_ERROR_DIR"); d != "" {
		return d
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "ai_processing_server")
	}
	return filepath.Join(home, "T", "ai_processing_server")
}

type sink struct {
	mu         sync.Mutex
	file       *lumberjack.Logger
	mirror     io.Writer
	sourceRoot string
	path       string
	bursts     map[string]*burst
	now        func() time.Time
}

type burst struct {
	first      time.Time
	suppressed int
}

var std = &sink{mirror: os.Stderr, bursts: map[string]*burst{}, now: time.Now}

// Init opens the error log. It returns a close function that flushes pending repeat counts.
func Init(o Options) (func() error, error) {
	if o.Dir == "" {
		o.Dir = DefaultDir()
	}
	if o.MaxMB <= 0 {
		o.MaxMB = 4
	}
	if o.Backups <= 0 {
		o.Backups = 3
	}
	if err := os.MkdirAll(o.Dir, 0o700); err != nil {
		return func() error { return nil }, fmt.Errorf("create error log dir %s: %w", o.Dir, err)
	}
	path := filepath.Join(o.Dir, "error.err")
	// Probe once so a bad directory is reported at boot, not at the first error.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return func() error { return nil }, fmt.Errorf("open %s: %w", path, err)
	}
	f.Close()
	std.mu.Lock()
	std.file = &lumberjack.Logger{Filename: path, MaxSize: o.MaxMB, MaxBackups: o.Backups, LocalTime: false}
	std.mirror = o.Mirror
	std.sourceRoot = o.SourceRoot
	std.path = path
	std.mu.Unlock()
	return Close, nil
}

// Path is the error log file, or "" before Init.
func Path() string {
	std.mu.Lock()
	defer std.mu.Unlock()
	return std.path
}

// Close writes the pending repeat counts and closes the file.
func Close() error {
	std.mu.Lock()
	defer std.mu.Unlock()
	now := std.now()
	for key, b := range std.bursts {
		if b.suppressed > 0 {
			std.writeLocked(fmt.Sprintf("[%s] [%s] [errlog] %s (+%d identical in the previous %s)\n",
				stamp(now), LevelWarn, key, b.suppressed, now.Sub(b.first).Round(time.Second)))
		}
	}
	std.bursts = map[string]*burst{}
	if std.file == nil {
		return nil
	}
	err := std.file.Close()
	std.file, std.path, std.mirror = nil, "", os.Stderr
	return err
}

// Logger records errors for one context (a package or a subsystem: "scheduler", "api", "runner.ffmpeg").
type Logger struct{ ctx string }

// For returns the logger for a context.
func For(ctx string) Logger { return Logger{ctx: ctx} }

// Error records an unexpected failure. attempting says what we were trying to do ("saving the job journal").
func (l Logger) Error(attempting string, err error, kv ...any) {
	write(LevelError, l.ctx, attempting, err, kv, 2, false)
}

// Warn records something wrong that the server worked around (a cleanup that failed, a fallback taken).
func (l Logger) Warn(attempting string, err error, kv ...any) {
	write(LevelWarn, l.ctx, attempting, err, kv, 2, false)
}

// Fatal records a failure that stops the process (or would have), with the full stack. It does not exit.
func (l Logger) Fatal(attempting string, err error, kv ...any) {
	write(LevelFatal, l.ctx, attempting, err, kv, 2, true)
}

// Panic records a recovered panic value with the full stack.
func (l Logger) Panic(attempting string, recovered any, kv ...any) {
	write(LevelFatal, l.ctx, attempting, fmt.Errorf("panic: %v", recovered), kv, 2, true)
}

// Recover is deferred at the top of a goroutine: it records a panic with its stack and swallows it, so one bad
// goroutine does not take the server down. Use RecoverAndRepanic where the process must still die.
func (l Logger) Recover(attempting string, kv ...any) {
	if r := recover(); r != nil {
		write(LevelFatal, l.ctx, attempting, fmt.Errorf("panic: %v", r), kv, 2, true)
	}
}

// RecoverAndRepanic records a panic, flushes the log, then panics again (main: the crash still shows on stderr
// and the exit code stays non-zero).
func (l Logger) RecoverAndRepanic(attempting string) {
	if r := recover(); r != nil {
		write(LevelFatal, l.ctx, attempting, fmt.Errorf("panic: %v", r), nil, 2, true)
		Close()
		panic(r)
	}
}

// Go runs fn in a goroutine that records (and survives) a panic.
func (l Logger) Go(attempting string, fn func()) {
	go func() {
		defer l.Recover(attempting)
		fn()
	}()
}

// Check records err when it is not nil and returns it unchanged, for one-line call sites:
//
//	return elog.Check("closing the upload", f.Close())
func (l Logger) Check(attempting string, err error, kv ...any) error {
	if err != nil {
		write(LevelError, l.ctx, attempting, err, kv, 2, false)
	}
	return err
}

// Ignore records err as a WARN when it is not nil, for cleanups whose failure does not change the outcome
// (removing a temp file, closing a read-only file). Missing files are not worth a line.
func (l Logger) Ignore(attempting string, err error, kv ...any) {
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		write(LevelWarn, l.ctx, attempting, err, kv, 2, false)
	}
}

func write(level, ctx, attempting string, err error, kv []any, skip int, stack bool) {
	defer func() { recover() }() // errlog must never take the caller down
	now := std.now()
	cause := redact.Text(describe(err))
	key := ctx + "\x00" + attempting + "\x00" + cause
	var b strings.Builder
	fmt.Fprintf(&b, "[%s] [%s] [%s] failed %s", stamp(now), level, ctx, attempting)
	if cause != "" {
		b.WriteString(": " + cause)
	}
	for i := 0; i+1 < len(kv); i += 2 {
		fmt.Fprintf(&b, " %v=%s", kv[i], quote(redact.Value(fmt.Sprint(kv[i]), fmt.Sprint(kv[i+1]))))
	}
	std.mu.Lock()
	root := std.sourceRoot
	std.mu.Unlock()
	b.WriteString(" " + location(skip+1, root))
	if stack {
		b.WriteString(" | stack: " + strings.Join(stackLines(root), " | "))
	}

	std.mu.Lock()
	defer std.mu.Unlock()
	if level != LevelFatal {
		if bu := std.bursts[key]; bu != nil && now.Sub(bu.first) < burstEvery {
			bu.suppressed++
			return
		} else if bu != nil && bu.suppressed > 0 {
			fmt.Fprintf(&b, " (+%d identical in the previous %s)", bu.suppressed, now.Sub(bu.first).Round(time.Second))
		}
		std.bursts[key] = &burst{first: now}
		if len(std.bursts) > 1000 {
			for k, v := range std.bursts {
				if now.Sub(v.first) >= burstEvery {
					delete(std.bursts, k)
				}
			}
		}
	}
	std.writeLocked(oneLine(b.String()) + "\n")
}

func (s *sink) writeLocked(line string) {
	if s.file != nil {
		if _, err := io.WriteString(s.file, line); err != nil {
			fmt.Fprintf(os.Stderr, "errlog: cannot write %s: %v\n", s.path, err)
			if s.mirror == nil {
				io.WriteString(os.Stderr, line)
			}
		}
	}
	if s.mirror != nil {
		io.WriteString(s.mirror, line)
	}
}

func stamp(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

// describe renders an error chain: "outer: inner (code=… path=…)". Wrapped errors already carry their causes in
// Error(), so only *os.PathError-style details are added.
func describe(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	var pe *os.PathError
	if errors.As(err, &pe) && !strings.Contains(s, pe.Path) {
		s += " (path=" + pe.Path + ")"
	}
	return s
}

// location is "at=<abs file>:<line> (<func>) via=<file>:<line> <file>:<line>".
func location(skip int, root string) string {
	pcs := make([]uintptr, 3)
	n := runtime.Callers(skip+1, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var parts []string
	for i := 0; ; i++ {
		f, more := frames.Next()
		if f.File == "" {
			break
		}
		if i == 0 {
			parts = append(parts, fmt.Sprintf("at=%s:%d (%s)", fullPath(f.File, root), f.Line, shortFunc(f.Function)))
		} else {
			if i == 1 {
				parts = append(parts, "via=")
			}
			parts[len(parts)-1] += fmt.Sprintf("%s:%d ", fullPath(f.File, root), f.Line)
		}
		if !more {
			break
		}
	}
	return strings.TrimSpace(strings.Join(parts, " "))
}

// stackLines is the goroutine stack with full paths, without errlog's own frames.
func stackLines(root string) []string {
	var out []string
	skipping := true
	for _, l := range strings.Split(string(debug.Stack()), "\n") {
		l = strings.TrimSpace(l)
		if l == "" || strings.HasPrefix(l, "goroutine ") {
			continue
		}
		if skipping {
			if strings.Contains(l, "/internal/errlog/") || strings.Contains(l, "errlog.") || strings.HasPrefix(l, "runtime/debug.") ||
				strings.Contains(l, "runtime/debug/stack.go") {
				continue
			}
			skipping = false
		}
		if i := strings.LastIndex(l, " +0x"); i > 0 {
			l = l[:i]
		}
		out = append(out, fullPath(l, root))
	}
	return out
}

// fullPath turns a -trimpath file name (github.com/ACT3ai/ai_processing_server/internal/x.go) into the absolute
// path under code/. Paths that are already absolute (go test, non-trimpath builds) are returned as they are.
func fullPath(file, root string) string {
	if root == "" {
		return file
	}
	if i := strings.Index(file, ModulePath+"/"); i >= 0 && !filepath.IsAbs(file[:max(i, 1)]) {
		return file[:i] + filepath.ToSlash(filepath.Join(root, file[i+len(ModulePath)+1:]))
	}
	return file
}

func shortFunc(fn string) string {
	if i := strings.LastIndex(fn, "/"); i >= 0 {
		return fn[i+1:]
	}
	return fn
}

// oneLine replaces control characters (newlines in stacks and wrapped errors) and caps the length.
func oneLine(s string) string {
	s = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return ' '
		}
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
	if len(s) > maxLine {
		s = s[:maxLine-len(" …[truncated]")] + " …[truncated]"
	}
	return s
}

func quote(v string) string {
	if strings.ContainsAny(v, " \t\n\"=") {
		return fmt.Sprintf("%q", v)
	}
	return v
}
