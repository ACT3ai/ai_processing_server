//go:build !windows

package taskhost

import (
	"os/exec"
	"syscall"
)

// detach puts the task host in its own session, so it outlives the controller and is not in its process group.
func detach(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} }
