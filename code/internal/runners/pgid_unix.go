//go:build !windows

package runners

import "syscall"

// pgidOf is a process's process group id.
func pgidOf(pid int) (int, bool) {
	g, err := syscall.Getpgid(pid)
	return g, err == nil
}
