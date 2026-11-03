package service

import (
	"strings"
	"testing"
)

func TestUnitsPerOS(t *testing.T) {
	spec := Spec{Root: "/r", Binary: "/r/ai_processing_server", NATSServer: "/bin/nats-server", BrokerConf: "/c/standalone.conf", LogDir: "/r/logs"}
	mac, err := Units("darwin", spec)
	if err != nil || len(mac) != 2 {
		t.Fatalf("darwin: %v %d", err, len(mac))
	}
	if !strings.Contains(mac[0].Body, "<string>/c/standalone.conf</string>") || !strings.Contains(mac[1].Body, "<string>run</string>") ||
		!strings.Contains(mac[1].Body, "<key>SuccessfulExit</key><false/>") {
		t.Fatalf("plist:\n%s\n%s", mac[0].Body, mac[1].Body)
	}
	if !strings.HasSuffix(mac[1].Path, "LaunchAgents/ai.act3.processing_server.plist") || mac[0].Label != "ai.act3.nats-server" ||
		!strings.Contains(mac[1].Body, "<key>AbandonProcessGroup</key><true/>") {
		t.Fatalf("labels must match the justfile: %s %s", mac[0].Label, mac[1].Path)
	}
	lin, _ := Units("linux", spec)
	if !strings.Contains(lin[1].Body, "ExecStart=/r/ai_processing_server run") || !strings.Contains(lin[1].Body, "Restart=on-failure") ||
		!strings.HasSuffix(lin[0].Path, "act3-processing-broker.service") {
		t.Fatalf("systemd:\n%s", lin[1].Body)
	}
	spec.NATSServer = ""
	win, _ := Units("windows", spec)
	if len(win) != 1 || win[0].Name != ServerName {
		t.Fatalf("no broker → only the server: %+v", win)
	}
	if _, err := Units("plan9", spec); err == nil {
		t.Fatal("plan9 accepted")
	}
}
