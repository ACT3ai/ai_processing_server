//go:build darwin

package brokerctl

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

const plistTmpl = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- Written by ai_processing_server broker install (pm/task_queue.mdx §8.2). Remove with just broker-uninstall. -->
<plist version="1.0"><dict>
  <key>Label</key><string>%s</string>
  <key>ProgramArguments</key><array><string>%s</string><string>-c</string><string>%s</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>%s</string>
  <key>StandardErrorPath</key><string>%s</string>
</dict></plist>
`

func domain() string { return "gui/" + strconv.Itoa(os.Getuid()) }

func installService(p Paths) error {
	if err := os.MkdirAll(filepath.Dir(p.Plist), 0o755); err != nil {
		return err
	}
	body := fmt.Sprintf(plistTmpl, LaunchdLabel, p.Server, p.Conf, p.Log, p.Log)
	if err := os.WriteFile(p.Plist, []byte(body), 0o644); err != nil {
		return err
	}
	_ = exec.Command("launchctl", "bootout", domain()+"/"+LaunchdLabel).Run() // not loaded yet is fine
	if out, err := exec.Command("launchctl", "bootstrap", domain(), p.Plist).CombinedOutput(); err != nil {
		return fmt.Errorf("launchctl bootstrap %s: %v: %s", p.Plist, err, out)
	}
	return nil
}

// Uninstall stops the service and removes its definition. The broker's stored data is kept.
func Uninstall(p Paths) error {
	_ = exec.Command("launchctl", "bootout", domain()+"/"+LaunchdLabel).Run()
	if err := os.Remove(p.Plist); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ServiceState describes the OS service.
func ServiceState() string {
	out, err := exec.Command("launchctl", "print", domain()+"/"+LaunchdLabel).CombinedOutput()
	if err != nil {
		return "not installed (launchd " + LaunchdLabel + ")"
	}
	for _, l := range bytesLines(out) {
		if len(l) > 0 && (contains(l, "state = ") || contains(l, "pid = ")) {
			return "launchd " + LaunchdLabel + ": " + trim(l)
		}
	}
	return "launchd " + LaunchdLabel + ": loaded"
}
