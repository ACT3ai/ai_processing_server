package errlog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

func setup(t *testing.T, maxMB int) (string, *bytes.Buffer) {
	t.Helper()
	dir := t.TempDir()
	var mirror bytes.Buffer
	if _, err := Init(Options{Dir: dir, MaxMB: maxMB, Mirror: &mirror}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { Close(); std.now = time.Now })
	return filepath.Join(dir, "error.err"), &mirror
}

func read(t *testing.T, path string) string {
	Close() // flush
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestLineFormatAndFullSourcePath(t *testing.T) {
	path, mirror := setup(t, 4)
	_, thisFile, _, _ := runtime.Caller(0)
	For("scheduler").Error("saving the job journal", errors.New("disk full\nsecond line"), "job_id", "7c0e", "token", "s3cret")
	got := read(t, path)
	re := regexp.MustCompile(`^\[\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z\] \[ERROR\] \[scheduler\] failed saving the job journal: disk full second line job_id=7c0e token=\[redacted\] at=(\S+):(\d+) \(errlog\.TestLineFormatAndFullSourcePath\)`)
	m := re.FindStringSubmatch(got)
	if m == nil {
		t.Fatalf("line format:\n%s", got)
	}
	if m[1] != thisFile || !filepath.IsAbs(m[1]) {
		t.Fatalf("at= %s, want the absolute path %s", m[1], thisFile)
	}
	if strings.Count(got, "\n") != 1 {
		t.Fatalf("entry is not one line: %q", got)
	}
	if mirror.String() != got {
		t.Fatal("mirror did not get the same line")
	}
}

func TestTrimpathNamesBecomeFullPaths(t *testing.T) {
	got := fullPath(ModulePath+"/internal/scheduler/execute.go:12", "/Users/b/ai_processing_server/code")
	if got != "/Users/b/ai_processing_server/code/internal/scheduler/execute.go:12" {
		t.Fatal(got)
	}
	if got := fullPath("/abs/already.go", "/x"); got != "/abs/already.go" {
		t.Fatal(got)
	}
}

func TestIdenticalBurstIsCounted(t *testing.T) {
	path, _ := setup(t, 4)
	now := time.Date(2026, 10, 27, 16, 0, 0, 0, time.UTC)
	std.now = func() time.Time { return now }
	l := For("storage")
	for i := 0; i < 16; i++ {
		l.Error("uploading", errors.New("HTTP 503"))
	}
	l.Error("uploading", errors.New("HTTP 404")) // different cause: its own line
	now = now.Add(31 * time.Second)
	l.Error("uploading", errors.New("HTTP 503"))
	got := read(t, path)
	lines := strings.Split(strings.TrimSpace(got), "\n")
	if len(lines) != 3 {
		t.Fatalf("want 3 lines, got %d:\n%s", len(lines), got)
	}
	if !strings.Contains(lines[2], "(+15 identical in the previous 31s)") {
		t.Fatalf("repeat count missing: %s", lines[2])
	}
}

func TestRecoverWritesFatalWithStack(t *testing.T) {
	path, _ := setup(t, 4)
	func() {
		defer For("api").Recover("serving GET /v1/jobs")
		var m map[string]int
		m["x"] = 1 // panics
	}()
	got := read(t, path)
	if !strings.Contains(got, "[FATAL] [api] failed serving GET /v1/jobs: panic: assignment to entry in nil map") ||
		!strings.Contains(got, "| stack:") || !strings.Contains(got, "errlog_test.go") {
		t.Fatalf("panic entry:\n%s", got)
	}
}

// The live file never grows past MaxMB: lumberjack rolls it first.
func TestFileStaysUnderCap(t *testing.T) {
	path, _ := setup(t, 1)
	l := For("stress")
	big := strings.Repeat("x", 5000)
	for i := 0; i < 600; i++ { // ~3 MB of distinct entries
		l.Error("filling the log", fmt.Errorf("%s %d", big, i))
	}
	Close()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() > 1<<20 {
		t.Fatalf("error.err is %d bytes, over the 1 MB cap", st.Size())
	}
	rolled, _ := filepath.Glob(filepath.Join(filepath.Dir(path), "error-*.err"))
	if len(rolled) == 0 || len(rolled) > 3 {
		t.Fatalf("want 1..3 rolled backups, got %v", rolled)
	}
}

func TestBeforeInitGoesToStderrWithoutPanicking(t *testing.T) {
	For("x").Error("doing nothing", nil)
	For("x").Ignore("removing a missing file", os.ErrNotExist)
}
