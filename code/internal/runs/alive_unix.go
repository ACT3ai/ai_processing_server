//go:build !windows

package runs

import "syscall"

// pidAlive reports whether a process with this pid exists (signal 0 checks without sending anything).
func pidAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}
