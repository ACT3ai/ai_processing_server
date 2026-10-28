//go:build windows

package brokerctl

import (
	"fmt"
	"os/exec"
)

// installService registers nats-server as a Windows service (nats-server runs as one natively). Needs an
// administrator shell.
func installService(p Paths) error {
	_ = exec.Command("sc.exe", "stop", WindowsService).Run()
	_ = exec.Command("sc.exe", "delete", WindowsService).Run()
	bin := fmt.Sprintf(`"%s" -c "%s"`, p.Server, p.Conf)
	if out, err := exec.Command("sc.exe", "create", WindowsService, "binPath=", bin, "start=", "auto").CombinedOutput(); err != nil {
		return fmt.Errorf("sc.exe create (run from an administrator shell): %v: %s", err, out)
	}
	if out, err := exec.Command("sc.exe", "start", WindowsService).CombinedOutput(); err != nil {
		return fmt.Errorf("sc.exe start: %v: %s", err, out)
	}
	return nil
}

func Uninstall(p Paths) error {
	_ = exec.Command("sc.exe", "stop", WindowsService).Run()
	return exec.Command("sc.exe", "delete", WindowsService).Run()
}

func ServiceState() string {
	out, err := exec.Command("sc.exe", "query", WindowsService).CombinedOutput()
	if err != nil {
		return "not installed (Windows service " + WindowsService + ")"
	}
	return "Windows service " + WindowsService + ": " + trim(string(out))
}
