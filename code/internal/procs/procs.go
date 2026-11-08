// Package procs runs external tools for runners: its own process group, a cleaned environment, the GPU pin,
// line-by-line output capture, and a graceful-then-kill stop of the whole tree on cancel (spec §8.4, §16.5).
// No runner calls exec.Command directly.
package procs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/errlog"
)

var elog = errlog.For("procs")

// GraceBeforeKill is how long a cancelled process gets after the interrupt before the tree is killed.
var GraceBeforeKill = 10 * time.Second

// ErrKillNow as a context cause skips that grace: the tree is killed at once (the machine is running out of memory).
var ErrKillNow = errors.New("killed at once")

// ErrForcedKill is wrapped into Run's error when a stopped process did not exit within its grace period and the
// whole tree was killed (pm/cancel.mdx §5.2: the stop report says "forced", not "graceful").
var ErrForcedKill = errors.New("did not stop within its grace period; the process tree was killed")

// PipeDrainWait bounds how long Run waits, after killing the tree, for the output pipes to close. A grandchild that
// left the process group (setsid, a daemonising server) can hold them open forever; Run must still return so the
// controller can verify, escalate and report (pm/cancel.mdx §5.4).
var PipeDrainWait = 10 * time.Second

// Spec is one command to run.
type Spec struct {
	Name     string
	Args     []string
	Dir      string
	Env      []string // extra KEY=VALUE pairs on top of the cleaned environment
	GPU      *int     // sets CUDA_VISIBLE_DEVICES
	Stdout   func(line string)
	Stderr   func(line string)
	StdinNil bool
	// Grace is how long the tree gets after the interrupt before it is killed; 0 means GraceBeforeKill. Each render
	// type sets its own (runners/cancel.go CancelProfile): ffmpeg stops in a second, a private ComfyUI needs longer.
	Grace time.Duration
	// OnStart is called once the process has started, with its pid. On Unix the pid is also its process group id
	// (Setpgid), so the task host records it in procs.json and the controller can kill the group if the task host
	// itself is wedged (pm/cancel.mdx §5.3).
	OnStart func(pid int)
}

// Run starts the command and waits. Cancelling ctx stops the process tree; the returned error then wraps ctx.Err().
func Run(ctx context.Context, s Spec) error {
	cmd := exec.Command(s.Name, s.Args...)
	cmd.Dir = s.Dir
	cmd.Env = cleanEnv(s)
	setGroup(cmd)

	var wg sync.WaitGroup
	pipe := func(r io.Reader, fn func(string)) {
		defer wg.Done()
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 64*1024), 1024*1024)
		sc.Split(scanLinesOrCR)
		for sc.Scan() {
			if fn != nil {
				fn(sc.Text())
			}
		}
		if err := sc.Err(); err != nil {
			// A line over 1 MB (or a read error) stops the scanner. Keep draining, or the child blocks forever on a
			// full pipe and the job hangs until its timeout.
			elog.Warn("reading output of "+s.Name+"; the rest is discarded", err)
			io.Copy(io.Discard, r)
		}
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", s.Name, err)
	}
	lowerPriority(cmd)
	if s.OnStart != nil && cmd.Process != nil {
		s.OnStart(cmd.Process.Pid)
	}
	wg.Add(2)
	go pipe(stdout, s.Stdout)
	go pipe(stderr, s.Stderr)

	done := make(chan error, 1)
	go func() { wg.Wait(); done <- cmd.Wait() }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		if errors.Is(context.Cause(ctx), ErrKillNow) {
			killGroup(cmd)
			waitDrained(done, s.Name)
			return fmt.Errorf("%s stopped: %w", s.Name, context.Cause(ctx))
		}
		grace := s.Grace
		if grace <= 0 {
			grace = GraceBeforeKill
		}
		interruptGroup(cmd)
		select {
		case <-done:
			return fmt.Errorf("%s stopped: %w", s.Name, context.Cause(ctx))
		case <-time.After(grace):
			killGroup(cmd)
			waitDrained(done, s.Name)
			return fmt.Errorf("%s stopped: %w: %w", s.Name, ErrForcedKill, context.Cause(ctx))
		}
	}
}

// waitDrained waits for the process to be reaped and its pipes to close, at most PipeDrainWait.
func waitDrained(done <-chan error, name string) {
	select {
	case <-done:
	case <-time.After(PipeDrainWait):
		elog.Warn("a killed process tree still holds its output pipes open (a child left the process group); "+
			"returning without them", errors.New(name), "process", name)
	}
}

// GroupAlive reports whether any process of the process group pgid still exists (Unix), or the process pid
// (Windows, where taskkill /T ends the tree by parentage). The controller uses it to VERIFY a stop.
func GroupAlive(pgid int) bool { return groupAlive(pgid) }

// KillGroup kills the process group pgid (Unix) or the process tree rooted at pgid (Windows) at once. It is the
// controller's escalation when a task host did not stop its own tree (pm/cancel.mdx §5.3).
func KillGroup(pgid int) { killGroupID(pgid) }

// cleanEnv passes only PATH, HOME, locale and temp variables, the GPU pin, and the spec's extras.
// Platform tokens never reach child processes.
func cleanEnv(s Spec) []string {
	keep := []string{"PATH", "HOME", "USER", "LANG", "LC_ALL", "TMPDIR", "TMP", "TEMP"}
	if runtime.GOOS == "windows" {
		keep = append(keep, "SystemRoot", "SYSTEMROOT", "USERPROFILE", "PATHEXT", "COMSPEC", "LOCALAPPDATA", "APPDATA")
	}
	var env []string
	for _, k := range keep {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	if s.GPU != nil {
		env = append(env, "CUDA_VISIBLE_DEVICES="+strconv.Itoa(*s.GPU))
	}
	// Python's UTF-8 mode: on Windows a piped stdout is cp1252, and a printed emoji (SeedVR2's) kills the run
	env = append(env, "PYTHONUTF8=1")
	return append(env, s.Env...)
}

// scanLinesOrCR is bufio.ScanLines that also ends a line at a lone '\r': a tqdm bar redraws itself with '\r', and
// read as one line its progress only arrived when the bar closed. "\r\n" is one line break.
func scanLinesOrCR(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for i, b := range data {
		switch b {
		case '\n':
			return i + 1, data[:i], nil
		case '\r':
			if i+1 == len(data) && !atEOF {
				return 0, nil, nil // is it "\r\n"? wait for the next byte
			}
			if i+1 < len(data) && data[i+1] == '\n' {
				return i + 2, data[:i], nil
			}
			return i + 1, data[:i], nil
		}
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}
