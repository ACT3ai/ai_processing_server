# ai_processing_server — build and operate the processing server on this machine.
# `just build` then `just run` must work on a clean checkout with Go installed (pm/ai_processing_server.mdx §17).

set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

version := "0.2.0"
bin := if os_family() == "windows" { "ai_processing_server.exe" } else { "ai_processing_server" }
git_sha := `git rev-parse --short HEAD 2>/dev/null || echo unknown`
ldflags := "-s -w -X main.version=" + version + " -X main.gitSHA=" + git_sha + " -X main.buildTime=" + datetime_utc("%Y-%m-%dT%H:%M:%SZ")

export GOWORK := "off"        # a parent go.work (e.g. ~/BGit/act3/go.work) must not capture this module
export CGO_ENABLED := "0"     # pure Go: no C toolchain needed (spec §3.1, D-005)

# List recipes
default:
    @just --list

# this_server.yaml is created once and never overwritten (pm/ai_processing_server.mdx §6.1).
# Compile, install nats-server if missing, create this_server.yaml if missing, write the broker config
build:
    @echo "building {{bin}} {{version}} (git {{git_sha}}) with `go version | cut -d' ' -f3`"
    @cd code && go build -trimpath -ldflags "{{ldflags}}" -o ../{{bin}} ./cmd/ai_processing_server
    @echo "built ./{{bin}}"
    -@git config core.hooksPath .githooks
    @just _broker-ensure
    @./{{bin}} init
    @./{{bin}} broker config >/dev/null 2>&1 && echo "broker config: `./{{bin}} broker config`" || echo "broker config: skipped until this_server.yaml loads"
    @just _service-ensure

# Start the server in the background and print where its API and logs are
[unix]
run: _binary
    #!/usr/bin/env bash
    set -euo pipefail
    if ./{{bin}} status >/dev/null 2>&1; then echo "already running:"; ./{{bin}} status; exit 0; fi
    # The queue needs this machine's nats-server: start it first when it is not answering (problems 2026_oct_27 #1).
    if command -v nats-server >/dev/null && ! ./{{bin}} broker status >/dev/null 2>&1; then
      just broker-run || echo "warning: the broker did not start; the server runs direct mode only (just broker-status)"
    fi
    mkdir -p data/git_ignore/logs
    # Installed as a LaunchAgent (just install-service): launchd starts it, so it is the only copy and restarts on a crash.
    plist="$HOME/Library/LaunchAgents/ai.act3.processing_server.plist"
    if [ "$(uname)" = "Darwin" ] && [ -f "$plist" ]; then
      launchctl bootstrap "gui/$(id -u)" "$plist" 2>/dev/null || launchctl kickstart "gui/$(id -u)/ai.act3.processing_server"
      pid=launchd
    else
      nohup ./{{bin}} run > data/git_ignore/logs/server.stdout.log 2>&1 &
      pid=$!
    fi
    for _ in $(seq 1 80); do
      if ./{{bin}} status >/dev/null 2>&1; then
        ./{{bin}} status
        echo
        echo "started ($pid) · stop: just stop · status: just status"
        echo "logs: just logs (~/_act3_logs/processing_server.info, .err) · stdout: data/git_ignore/logs/server.stdout.log"
        exit 0
      fi
      if [ "$pid" != launchd ] && ! kill -0 "$pid" 2>/dev/null; then
        echo "server exited during start:"; tail -n 30 data/git_ignore/logs/server.stdout.log; exit 1
      fi
      sleep 0.25
    done
    echo "server did not answer within 20 s; see data/git_ignore/logs/server.stdout.log"; exit 1

[windows]
run: _binary
    if (./{{bin}} status 2>$null) { ./{{bin}} status; exit 0 }
    New-Item -ItemType Directory -Force data/git_ignore/logs | Out-Null
    Start-Process -FilePath ./{{bin}} -ArgumentList run -WindowStyle Hidden -RedirectStandardOutput data/git_ignore/logs/server.stdout.log -RedirectStandardError data/git_ignore/logs/server.stderr.log
    Start-Sleep -Seconds 3
    ./{{bin}} status

# Run in the foreground (development; Ctrl+C drains and stops)
run-fg: _binary
    ./{{bin}} run

# Drain running jobs, then stop. `just stop force` kills now.
[unix]
stop mode="": _binary
    @./{{bin}} stop {{ if mode == "force" { "--force" } else { "" } }}
    @# unload the LaunchAgent so launchd does not restart it (a killed server is not a clean exit); `just run` reloads it
    -@[ "`uname`" = Darwin ] && launchctl bootout "gui/`id -u`/ai.act3.processing_server" 2>/dev/null || true

[windows]
stop mode="": _binary
    @./{{bin}} stop {{ if mode == "force" { "--force" } else { "" } }}

# Health, services and running jobs of the server on this machine
status: _binary
    @./{{bin}} status
    @echo
    @./{{bin}} jobs

# Tail the server logs in ~/_act3_logs
[unix]
logs:
    tail -n 50 -F ~/_act3_logs/processing_server.info ~/_act3_logs/processing_server.err

[windows]
logs:
    Get-Content -Wait -Tail 50 $HOME/_act3_logs/processing_server.info, $HOME/_act3_logs/processing_server.err

# Only the init step: create this_server.yaml if missing, else validate it (never overwrites)
init: _binary
    ./{{bin}} init

# Write data/git_ignore/this_server.yaml.new with fresh detection for you to compare and merge
reinit: _binary
    ./{{bin}} init --write-new
    -git --no-pager diff --no-index --no-ext-diff data/git_ignore/this_server.yaml data/git_ignore/this_server.yaml.new

# Print detected hardware and the offers it would make, without starting the server
detect: _binary
    ./{{bin}} detect

# Check this_server.yaml and data/ without starting
validate: _binary
    ./{{bin}} validate

# ---- task queue broker: NATS JetStream in its own nats-server process (pm/task_queue.mdx §8.2, §18, D-014) ----

# Once per machine: install nats-server (data/queue/broker.yaml lists the command per OS), then check its version
[unix]
broker-install:
    #!/usr/bin/env bash
    set -euo pipefail
    if command -v nats-server >/dev/null; then echo "nats-server present: $(nats-server --version)"; exit 0; fi
    # quiet install; `just broker-run` starts it with our config, so skip brew's "brew services start" advice
    if [ "$(uname)" = "Darwin" ] && command -v brew >/dev/null; then HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_ENV_HINTS=1 brew install --quiet nats-server >/dev/null; echo "installed $(nats-server --version)"; exit 0; fi
    echo "nats-server is not installed. Install the 2.15.x line:"; grep -A3 '^install:' data/queue/broker.yaml; exit 1

[windows]
broker-install:
    if (Get-Command nats-server -ErrorAction SilentlyContinue) { nats-server --version } else { winget install NATS.Server }

# `just build` calls this: install nats-server when missing; never fails the build (without a broker the server
# still runs direct mode and GET /v1/health says broker_missing with the fix)
[unix]
_broker-ensure:
    #!/usr/bin/env bash
    if just broker-install; then exit 0; fi
    echo "WARNING: nats-server is not installed, so the task queue cannot start on this machine (see above)."

[windows]
_broker-ensure:
    try { just broker-install } catch { Write-Output "WARNING: nats-server is not installed; the task queue cannot start on this machine." }

# Write this machine's nats-server config from this_server.yaml (to ~/.act3_processing/nats/, never hand edited)
broker-config: _binary
    ./{{bin}} broker config

# Start nats-server in the background with the generated config (until it is installed as an OS service)
[unix]
broker-run: _binary
    #!/usr/bin/env bash
    set -euo pipefail
    if ./{{bin}} broker status >/dev/null 2>&1; then ./{{bin}} broker status; exit 0; fi
    conf=$(./{{bin}} broker config)
    dir=$(dirname "$conf")
    plist="$HOME/Library/LaunchAgents/ai.act3.nats-server.plist"
    if [ "$(uname)" = "Darwin" ] && [ -f "$plist" ]; then
      launchctl bootstrap "gui/$(id -u)" "$plist" 2>/dev/null || launchctl kickstart "gui/$(id -u)/ai.act3.nats-server"
      started="launchd ai.act3.nats-server"
    else
      nohup nats-server -c "$conf" > "$dir/nats-server.log" 2>&1 &
      echo $! > "$dir/nats-server.pid"
      started="pid $!"
    fi
    for _ in $(seq 1 40); do
      if ./{{bin}} broker status >/dev/null 2>&1; then ./{{bin}} broker status; echo "$started · log $dir/nats-server.log · stop: just broker-stop"; exit 0; fi
      sleep 0.25
    done
    echo "nats-server did not answer; see $dir/nats-server.log"; tail -n 20 "$dir/nats-server.log"; exit 1

# Stop the nats-server (LaunchAgent or broker-run's); queued jobs and claims stay in its store
[unix]
broker-stop:
    #!/usr/bin/env bash
    if [ "$(uname)" = "Darwin" ] && launchctl bootout "gui/$(id -u)/ai.act3.nats-server" 2>/dev/null; then echo "stopped (LaunchAgent unloaded; just broker-run reloads it)"; exit 0; fi
    f="${APS_BROKER_DIR:-$HOME/.act3_processing/nats}/nats-server.pid"
    if [ -f "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null; then kill "$(cat "$f")" && rm -f "$f" && echo stopped; else echo "no broker started by broker-run"; fi

# ---- start at login/boot: LaunchAgents on macOS (pm/ai_processing_server.mdx §19.2) ----

# `just build` calls this so a machine never misses it. APS_NO_SERVICE=1 skips it (dev laptops, CI).
[unix]
_service-ensure:
    #!/usr/bin/env bash
    if [ -n "${APS_NO_SERVICE:-}" ]; then echo "service: skipped (APS_NO_SERVICE set)"; exit 0; fi
    if [ "$(uname)" != "Darwin" ]; then echo "service: not automated on $(uname) yet (systemd unit: pm/TO_DO.md)"; exit 0; fi
    just install-service || echo "WARNING: service install failed (see above); the server still runs with just run"

[windows]
_service-ensure:
    Write-Output "service: not automated on Windows yet (pm/TO_DO.md)"

# Idempotent: only rewrites a plist whose content changed. Never starts or stops anything: `just run` loads them.
# Install LaunchAgents so nats-server and this server start at login and restart after a crash (just build runs it)
[unix]
install-service: _binary
    #!/usr/bin/env bash
    set -euo pipefail
    [ "$(uname)" = "Darwin" ] || { echo "install-service: macOS only so far (systemd/Windows: pm/TO_DO.md)"; exit 1; }
    agents="$HOME/Library/LaunchAgents"; mkdir -p "$agents" "$PWD/data/git_ignore/logs"
    path="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
    # write $1 (label) from stdin only when it differs from what is installed
    put() {
      local f="$agents/$1.plist" new; new=$(cat)
      if [ -f "$f" ] && [ "$(cat "$f")" = "$new" ]; then echo "service: $1 already installed ($f)"; return; fi
      printf '%s\n' "$new" > "$f"; plutil -lint -s "$f"
      if launchctl print "gui/$(id -u)/$1" >/dev/null 2>&1; then
        echo "service: $1 updated ($f); loaded copy is older: takes effect after just stop / just run"
      else
        echo "service: $1 installed ($f); starts at login, or now with just run"
      fi
    }
    head='<?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
    <plist version="1.0"><dict>'
    if conf=$(./{{bin}} broker config 2>/dev/null) && nats=$(command -v nats-server); then
    put ai.act3.nats-server <<EOF
    $head
      <key>Label</key><string>ai.act3.nats-server</string>
      <key>ProgramArguments</key><array><string>$nats</string><string>-c</string><string>$conf</string></array>
      <key>EnvironmentVariables</key><dict><key>PATH</key><string>$path</string></dict>
      <key>RunAtLoad</key><true/>
      <key>KeepAlive</key><true/>
      <key>ThrottleInterval</key><integer>30</integer>
      <key>StandardOutPath</key><string>$(dirname "$conf")/nats-server.log</string>
      <key>StandardErrorPath</key><string>$(dirname "$conf")/nats-server.log</string>
    </dict></plist>
    EOF
    else
      echo "service: ai.act3.nats-server skipped (nats-server missing or this_server.yaml does not load)"
    fi
    # KeepAlive only on a crash: `just stop` drains and exits 0, which must stay stopped. Renders run in their own
    # session (taskhost setsid), and AbandonProcessGroup keeps launchd from touching anything the server spawned.
    put ai.act3.processing_server <<EOF
    $head
      <key>Label</key><string>ai.act3.processing_server</string>
      <key>ProgramArguments</key><array><string>$PWD/{{bin}}</string><string>run</string></array>
      <key>WorkingDirectory</key><string>$PWD</string>
      <key>EnvironmentVariables</key><dict><key>PATH</key><string>$path</string></dict>
      <key>RunAtLoad</key><true/>
      <key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
      <key>ThrottleInterval</key><integer>30</integer>
      <key>AbandonProcessGroup</key><true/>
      <key>StandardOutPath</key><string>$PWD/data/git_ignore/logs/server.stdout.log</string>
      <key>StandardErrorPath</key><string>$PWD/data/git_ignore/logs/server.stdout.log</string>
    </dict></plist>
    EOF

[windows]
install-service:
    Write-Output "install-service: macOS only so far (Windows service: pm/TO_DO.md)"

# Unload and remove the LaunchAgents (a running render keeps going: its task host is detached)
[unix]
uninstall-service:
    #!/usr/bin/env bash
    for l in ai.act3.processing_server ai.act3.nats-server; do
      launchctl bootout "gui/$(id -u)/$l" 2>/dev/null && echo "unloaded $l"
      rm -f "$HOME/Library/LaunchAgents/$l.plist" && echo "removed $l.plist"
    done

# Is this machine's broker up, which version, how many jobs are stored
broker-status: _binary
    ./{{bin}} broker status

# ---- model services and real renders (problems 2026_oct_27, pm/task_authoring.mdx §7.1) ----

# Turn services (or the queue) on in this_server.yaml, keeping its comments: just enable act3.upscale_open_source
enable +items: _binary
    ./{{bin}} enable {{items}}

# Turn services (or the queue) off in this_server.yaml
disable +items: _binary
    ./{{bin}} disable {{items}}

# Is this machine ready to render Wan 2.2 and SeedVR2? Broker, config, and each model service's offer + reason
render-check: _binary
    -./{{bin}} broker status
    @echo
    -./{{bin}} validate
    @echo
    @./{{bin}} status 2>/dev/null | grep -E "SERVICE|open_source|local API" || echo "server not running: just run"

# One real job through the queue on this machine's installed models: wan, upscale, or all (Wan, then upscale its clip)
render-test what="all": _binary
    python3 tests/real_render.py {{what}}

# On the Mac Studio: build (installs the broker), turn the queue + both model services on, (re)start, check
render-setup: build
    ./{{bin}} enable queue act3.video_gen_open_source act3.upscale_open_source
    ./{{bin}} validate
    -./{{bin}} stop
    just run
    @echo
    just render-check

# Secret scan, Go unit tests (with vet), then the black-box test suite in tests/
test: check-secrets test-go test-suite

# Fail if any tracked file holds a private key: a PEM private key block or a JWK with a "d" member
# (pm/security.mdx §4.4). *_test.go files are skipped: they hold the public RFC 9421 / RFC 8037 test vectors.
[unix]
check-secrets:
    #!/usr/bin/env bash
    set -uo pipefail
    hits=$(git grep -l -E '^-----BEGIN ([A-Z0-9]+ )*PRIVATE KEY-----' -- . ':!*_test.go'; git grep -l -E '"d"[[:space:]]*:' -- '*.json' '*.jwk')
    if [ -n "$hits" ]; then echo "PRIVATE KEY material in tracked files (revoke that key, then remove it):"; echo "$hits"; exit 1; fi
    echo "check-secrets: no private keys in tracked files"

# Go unit tests next to the code
test-go:
    cd code && go vet ./... && go test ./...

# Black-box suite: starts its own servers on free ports and calls the API (see tests/README.md)
test-suite: build
    cd tests && just

# Cross-compile check for every fleet target (never committed)
cross:
    cd code && GOOS=darwin GOARCH=arm64 go build -o /dev/null ./cmd/ai_processing_server
    cd code && GOOS=windows GOARCH=amd64 go build -o /dev/null ./cmd/ai_processing_server
    cd code && GOOS=linux GOARCH=amd64 go build -o /dev/null ./cmd/ai_processing_server
    cd code && GOOS=linux GOARCH=arm64 go build -o /dev/null ./cmd/ai_processing_server

_binary:
    @{{ if path_exists(bin) == "true" { "true" } else { "echo 'no binary yet: run `just build` first' && exit 1" } }}
