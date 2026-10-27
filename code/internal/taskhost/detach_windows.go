//go:build windows

package taskhost

import (
	"os"
	"os/exec"
	"syscall"
)

const (
	createNewProcessGroup  = 0x00000200
	detachedProcess        = 0x00000008
	createBreakawayFromJob = 0x01000000
)

// startDetached starts name with DETACHED_PROCESS in a new process group, broken away from any job object, so it
// outlives the controller (pm/task_queue.mdx §9.3). stdout and stderr go to outPath.
func startDetached(name string, args []string, outPath string) error {
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	cmd := exec.Command(name, args...)
	cmd.Stdout, cmd.Stderr = out, out
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: createNewProcessGroup | detachedProcess | createBreakawayFromJob}
	if err := cmd.Start(); err != nil {
		// Not every job object allows breakaway; try again without it.
		cmd = exec.Command(name, args...)
		cmd.Stdout, cmd.Stderr = out, out
		cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: createNewProcessGroup | detachedProcess}
		if err := cmd.Start(); err != nil {
			return err
		}
	}
	go cmd.Wait()
	return nil
}
