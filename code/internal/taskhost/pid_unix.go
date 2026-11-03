//go:build !windows

package taskhost

import (
	"errors"
	"syscall"
)

// pidAlive reports whether a process with this pid exists (signal 0; EPERM means it exists under another user).
func pidAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
