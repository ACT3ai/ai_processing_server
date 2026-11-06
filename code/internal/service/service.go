// Package service installs this machine's nats-server and processing server as OS services, so a power cut or a
// reboot brings both back without anyone logging in to type `just run` (pm/task_authoring.mdx §8 G4, a real power
// cycle; pm/ai_processing_server.mdx §19).
//
//	macOS    launchd agents in ~/Library/LaunchAgents (start at login; turn on automatic login on a render box)
//	Linux    systemd user units in ~/.config/systemd/user (run `loginctl enable-linger` to start at boot)
//	Windows  two Task Scheduler tasks that start at logon and restart on failure
//
// Both services restart when they crash. A clean `ai_processing_server stop` (exit 0) is not restarted, so `just stop`
// and `just update` keep working; the next boot starts the server again. The server needs no start order: it keeps
// retrying the broker until it answers and runs direct mode meanwhile.
//
// A dev machine (server.dev: true, Bryan's Mac Tower) is not a render server: Install refuses it unless forced.
package service

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"unicode/utf16"
)

// Names of the two services.
const (
	BrokerName = "act3-processing-broker"
	ServerName = "act3-processing-server"
)

// launchd labels. They match the justfile (run, stop, broker-run, broker-stop load and unload them by label) and
// pm/ai_processing_server.mdx §19.2.
var darwinLabel = map[string]string{BrokerName: "ai.act3.nats-server", ServerName: "ai.act3.processing_server"}

// Spec is what to run.
type Spec struct {
	Root       string // repo root: working directory of the server
	Binary     string // the server binary (repo root)
	NATSServer string // nats-server; "" = no broker service
	BrokerConf string // its config file (brokerctl.WriteConfig)
	LogDir     string // stdout/stderr of both services
}

// Unit is one file to write (or, on Windows, one task to register).
type Unit struct {
	Name  string
	Label string // launchd label (macOS only)
	Path  string // where it goes; "" on Windows
	Body  string
	Argv  []string // the command it runs
}

// Units returns the service definitions for goos.
func Units(goos string, s Spec) ([]Unit, error) {
	if s.Binary == "" || s.Root == "" {
		return nil, errors.New("service: no binary or repo root")
	}
	type svc struct {
		name string
		argv []string
	}
	var svcs []svc
	if s.NATSServer != "" {
		if s.BrokerConf == "" {
			return nil, errors.New("service: nats-server without a config file")
		}
		svcs = append(svcs, svc{BrokerName, []string{s.NATSServer, "-c", s.BrokerConf}})
	}
	svcs = append(svcs, svc{ServerName, []string{s.Binary, "run"}})
	home, _ := os.UserHomeDir()
	var out []Unit
	for _, v := range svcs {
		u := Unit{Name: v.name, Argv: v.argv}
		switch goos {
		case "darwin":
			u.Label = darwinLabel[v.name]
			u.Path = filepath.Join(home, "Library", "LaunchAgents", u.Label+".plist")
			u.Body = plist(u.Label, v.argv, s.Root, filepath.Join(s.LogDir, v.name+".log"))
		case "linux":
			u.Path = filepath.Join(home, ".config", "systemd", "user", v.name+".service")
			u.Body = systemdUnit(v.name, v.argv, s.Root)
		case "windows":
			u.Body = taskXML(v.name, v.argv, s.Root, windowsUser())
		default:
			return nil, fmt.Errorf("service: %s is not supported", goos)
		}
		out = append(out, u)
	}
	return out, nil
}

func plist(lbl string, argv []string, dir, log string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>` + xmlEsc(lbl) + `</string>
  <key>ProgramArguments</key>
  <array>
`)
	for _, a := range argv {
		b.WriteString("    <string>" + xmlEsc(a) + "</string>\n")
	}
	b.WriteString(`  </array>
  <key>WorkingDirectory</key><string>` + xmlEsc(dir) + `</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>ProcessType</key><string>Standard</string>
  <key>AbandonProcessGroup</key><true/>
  <key>EnvironmentVariables</key><dict><key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string></dict>
  <key>StandardOutPath</key><string>` + xmlEsc(log) + `</string>
  <key>StandardErrorPath</key><string>` + xmlEsc(log) + `</string>
</dict>
</plist>
`)
	return b.String()
}

func systemdUnit(name string, argv []string, dir string) string {
	return "# " + name + ": installed by `ai_processing_server service install` (just install-service)\n" +
		"[Unit]\nDescription=ACT3 " + name + "\nAfter=network-online.target\nWants=network-online.target\n\n" +
		"[Service]\nWorkingDirectory=" + dir + "\nExecStart=" + strings.Join(quoteAll(argv), " ") + "\n" +
		"Restart=on-failure\nRestartSec=10\nKillSignal=SIGTERM\nTimeoutStopSec=660\n\n" +
		"[Install]\nWantedBy=default.target\n"
}

func xmlEsc(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;").Replace(s)
}

func quoteAll(argv []string) []string {
	out := make([]string, len(argv))
	for i, a := range argv {
		if strings.ContainsAny(a, " \t\"") {
			a = `"` + strings.ReplaceAll(a, `"`, `\"`) + `"`
		}
		out[i] = a
	}
	return out
}

// Install writes and loads the units. It replaces an earlier install of the same names.
func Install(units []Unit, logDir string) ([]string, error) {
	var did []string
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return nil, err
	}
	for _, u := range units {
		switch runtime.GOOS {
		case "darwin":
			// Idempotent, and never restarts a loaded service: `just build` calls this on every build, and a reload
			// would bounce a running server. A changed plist takes effect at the next `just stop` / `just run`.
			old, _ := os.ReadFile(u.Path)
			loaded := run("launchctl", "print", fmt.Sprintf("gui/%d/%s", os.Getuid(), u.Label)) == nil
			if string(old) == u.Body && loaded {
				did = append(did, fmt.Sprintf("%s already installed and loaded (%s)", u.Name, u.Path))
				continue
			}
			if err := writeUnit(u); err != nil {
				return did, err
			}
			if loaded {
				did = append(did, fmt.Sprintf("updated %s (%s); the loaded copy is older: takes effect after just stop / just run", u.Name, u.Path))
				continue
			}
			if err := run("launchctl", "load", "-w", u.Path); err != nil {
				return did, err
			}
		case "linux":
			if err := writeUnit(u); err != nil {
				return did, err
			}
			if err := run("systemctl", "--user", "daemon-reload"); err != nil {
				return did, err
			}
			if err := run("systemctl", "--user", "enable", "--now", u.Name+".service"); err != nil {
				return did, err
			}
		case "windows":
			// From XML: the schtasks command line cannot set a restart on failure, and leaves Windows' default
			// 72 h execution limit, which would stop the server after three days.
			f, err := os.CreateTemp("", "act3-task-*.xml")
			if err != nil {
				return did, err
			}
			_, werr := f.Write(utf16LE(u.Body))
			_ = f.Close()
			if werr != nil {
				_ = os.Remove(f.Name())
				return did, werr
			}
			_ = run("schtasks", "/Delete", "/F", "/TN", u.Name)
			err = run("schtasks", "/Create", "/F", "/TN", u.Name, "/XML", f.Name())
			_ = os.Remove(f.Name())
			if err != nil {
				return did, err
			}
		}
		did = append(did, fmt.Sprintf("installed %s (%s)", u.Name, strings.Join(u.Argv, " ")))
	}
	return did, nil
}

// Uninstall stops and removes the units. Running task hosts are not touched: they are detached from the server.
func Uninstall(units []Unit) []string {
	var did []string
	for _, u := range units {
		switch runtime.GOOS {
		case "darwin":
			_ = run("launchctl", "unload", "-w", u.Path)
			_ = os.Remove(u.Path)
		case "linux":
			_ = run("systemctl", "--user", "disable", "--now", u.Name+".service")
			_ = os.Remove(u.Path)
			_ = run("systemctl", "--user", "daemon-reload")
		case "windows":
			_ = run("schtasks", "/Delete", "/F", "/TN", u.Name)
		}
		did = append(did, "removed "+u.Name)
	}
	return did
}

// Installed reports, per unit, whether its file (or task) exists.
func Installed(units []Unit) map[string]bool {
	out := map[string]bool{}
	for _, u := range units {
		if runtime.GOOS == "windows" {
			out[u.Name] = exec.Command("schtasks", "/Query", "/TN", u.Name).Run() == nil
			continue
		}
		_, err := os.Stat(u.Path)
		out[u.Name] = err == nil
	}
	return out
}

// taskXML is a Task Scheduler task that starts at the user's logon, restarts on a crash (a clean exit 0 is not
// a failure, so `just stop` still holds) and never hits an execution time limit.
func taskXML(name string, argv []string, dir, user string) string {
	userID := ""
	if user != "" {
		userID = "<UserId>" + xmlEsc(user) + "</UserId>"
	}
	return `<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>` + xmlEsc(name) + `: written by ai_processing_server service install.</Description></RegistrationInfo>
  <Triggers><LogonTrigger><Enabled>true</Enabled>` + userID + `</LogonTrigger></Triggers>
  <Principals><Principal id="Author">` + userID + `<LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <RestartOnFailure><Interval>PT1M</Interval><Count>999</Count></RestartOnFailure>
    <Enabled>true</Enabled>
  </Settings>
  <Actions Context="Author"><Exec><Command>` + xmlEsc(argv[0]) + `</Command><Arguments>` + xmlEsc(strings.Join(quoteAll(argv[1:]), " ")) + `</Arguments><WorkingDirectory>` + xmlEsc(dir) + `</WorkingDirectory></Exec></Actions>
</Task>
`
}

// windowsUser is the account the task runs as and starts for: the bare user name, which Task Scheduler resolves
// on the machine (USERDOMAIN reads WORKGROUP in an SSH session, which is no account).
func windowsUser() string {
	return os.Getenv("USERNAME")
}

// utf16LE is s as UTF-16 little endian with a byte-order mark: what `schtasks /Create /XML` requires.
func utf16LE(s string) []byte {
	units := utf16.Encode([]rune(s))
	b := make([]byte, 2+2*len(units))
	b[0], b[1] = 0xFF, 0xFE
	for i, u := range units {
		binary.LittleEndian.PutUint16(b[2+2*i:], u)
	}
	return b
}

func writeUnit(u Unit) error {
	if err := os.MkdirAll(filepath.Dir(u.Path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(u.Path, []byte(u.Body), 0o644)
}

func run(name string, args ...string) error {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %v: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}
