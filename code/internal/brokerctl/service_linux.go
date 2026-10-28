//go:build linux

package brokerctl

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

const unitTmpl = `# Written by ai_processing_server broker install (pm/task_queue.mdx §8.2). Remove with just broker-uninstall.
[Unit]
Description=ACT3 processing server task queue broker (nats-server)
After=network-online.target

[Service]
ExecStart=%s -c %s
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=2

[Install]
WantedBy=default.target
`

func installService(p Paths) error {
	if err := os.MkdirAll(filepath.Dir(p.Unit), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(p.Unit, []byte(fmt.Sprintf(unitTmpl, p.Server, p.Conf)), 0o644); err != nil {
		return err
	}
	for _, args := range [][]string{{"--user", "daemon-reload"}, {"--user", "enable", SystemdUnit}, {"--user", "restart", SystemdUnit}} {
		if out, err := exec.Command("systemctl", args...).CombinedOutput(); err != nil {
			return fmt.Errorf("systemctl %v: %v: %s (on a server, also run `loginctl enable-linger $USER`)", args, err, out)
		}
	}
	return nil
}

func Uninstall(p Paths) error {
	_ = exec.Command("systemctl", "--user", "disable", "--now", SystemdUnit).Run()
	if err := os.Remove(p.Unit); err != nil && !os.IsNotExist(err) {
		return err
	}
	return exec.Command("systemctl", "--user", "daemon-reload").Run()
}

func ServiceState() string {
	out, _ := exec.Command("systemctl", "--user", "is-active", SystemdUnit).CombinedOutput()
	return "systemd --user " + SystemdUnit + ": " + trim(string(out))
}
