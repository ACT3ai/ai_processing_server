// Package procs runs external tools for runners: its own process group, a cleaned environment, the GPU pin,
// line-by-line output capture, and a graceful-then-kill stop of the whole tree on cancel (spec §8.4, §16.5).
// No runner calls exec.Command directly.
package procs

import (
	"bufio"
	"context"
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
	wg.Add(2)
	go pipe(stdout, s.Stdout)
	go pipe(stderr, s.Stderr)

	done := make(chan error, 1)
	go func() { wg.Wait(); done <- cmd.Wait() }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		interruptGroup(cmd)
		select {
		case <-done:
		case <-time.After(GraceBeforeKill):
			killGroup(cmd)
			<-done
		}
		return fmt.Errorf("%s stopped: %w", s.Name, context.Cause(ctx))
	}
}

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
	return append(env, s.Env...)
}
