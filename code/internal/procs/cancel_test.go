//go:build !windows

package procs

import (
	"context"
	"errors"
	"testing"
	"time"
)

// pm/cancel.mdx §11 C-P1: a process that ignores the interrupt is killed when its own grace runs out, and Run says
// so (ErrForcedKill); the group is gone afterwards.
func TestGraceThenForcedKill(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	pid := 0
	go func() { time.Sleep(300 * time.Millisecond); cancel(errors.New("cancelled by test")) }()
	start := time.Now()
	err := Run(ctx, Spec{Name: "/bin/sh", Args: []string{"-c", `trap "" INT; sleep 30`}, Grace: 400 * time.Millisecond,
		OnStart: func(p int) { pid = p }})
	if !errors.Is(err, ErrForcedKill) {
		t.Fatalf("want ErrForcedKill, got %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("took %s: the per-call grace was not used", time.Since(start))
	}
	if pid == 0 || GroupAlive(pid) {
		t.Fatalf("pid %d: the group must be recorded and gone", pid)
	}
}

// C-P2: a process that stops on the interrupt is not reported as forced.
func TestInterruptStopsGracefully(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel(errors.New("cancelled by test")) }()
	err := Run(ctx, Spec{Name: "/bin/sh", Args: []string{"-c", "sleep 30"}, Grace: 5 * time.Second})
	if err == nil || errors.Is(err, ErrForcedKill) {
		t.Fatalf("want a graceful stop, got %v", err)
	}
}
