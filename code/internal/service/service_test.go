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
	for _, want := range []string{"<LogonTrigger>", "<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>", "<RestartOnFailure>",
		"<Command>/r/ai_processing_server</Command>", "<Arguments>run</Arguments>", "<WorkingDirectory>/r</WorkingDirectory>",
		"<DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>"} {
		if !strings.Contains(win[0].Body, want) {
			t.Fatalf("task XML lacks %s:\n%s", want, win[0].Body)
		}
	}
	if b := utf16LE("A<"); len(b) != 6 || b[0] != 0xFF || b[1] != 0xFE || b[2] != 'A' || b[3] != 0 || b[4] != '<' {
		t.Fatalf("utf16LE: % x", b)
	}
	if _, err := Units("plan9", spec); err == nil {
		t.Fatal("plan9 accepted")
	}
}
