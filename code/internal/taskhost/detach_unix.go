//go:build !windows

package taskhost

import (
	"os"
	"os/exec"
	"syscall"
)

// startDetached starts name in its own session (setsid): no controlling terminal, its own process group, so neither
// the controller's exit nor a Ctrl-C in the controller's terminal reaches it. stdout and stderr go to outPath.
func startDetached(name string, args []string, outPath string) error {
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	cmd := exec.Command(name, args...)
	cmd.Stdout, cmd.Stderr = out, out
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	// Reap it if it ends while we live; if we die first, init/launchd adopts and reaps it.
	go cmd.Wait()
	return nil
}
