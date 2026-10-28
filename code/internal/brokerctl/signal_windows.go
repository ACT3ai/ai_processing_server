//go:build windows

package brokerctl

import "os/exec"

// signalReload asks the Windows service to reload its config.
func signalReload(string) error {
	return exec.Command("nats-server", "--signal", "reload="+WindowsService).Run()
}
