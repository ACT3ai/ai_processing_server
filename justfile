# ai_processing_server — build and operate the processing server on this machine.
# `just build` then `just run` must work on a clean checkout with Go installed (pm/ai_processing_server.mdx §17).

set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

version := "0.2.0"
bin := if os_family() == "windows" { "ai_processing_server.exe" } else { "ai_processing_server" }
git_sha := if os_family() == "windows" { `$s = git rev-parse --short HEAD 2>$null; if ($s) { $s } else { 'unknown' }` } else { `git rev-parse --short HEAD 2>/dev/null || echo unknown` }
ldflags := "-s -w -X main.version=" + version + " -X main.gitSHA=" + git_sha + " -X main.buildTime=" + datetime_utc("%Y-%m-%dT%H:%M:%SZ")

nats_image := "nats:2.15-alpine"

export GOWORK := "off"        # a parent go.work (e.g. ~/BGit/act3/go.work) must not capture this module
export CGO_ENABLED := "0"     # pure Go: no C toolchain needed (spec §3.1, D-005)

# List recipes
default:
    @just --list

# this_server.yaml is created once and never overwritten (pm/ai_processing_server.mdx §6.1).
# Compile, install nats-server if missing, create this_server.yaml if missing, write the broker config
# Every step's output is kept; the last lines say BUILD OK, BUILD OK with N warning(s) (repeated there), or BUILD FAILED.
[unix]
build:
    #!/usr/bin/env bash
    set -uo pipefail
    out=$(mktemp); trap 'rm -f "$out"' EXIT
    failed=()
    # step NAME CMD...: run it, show and keep its output, remember a failure (later steps still run)
    step() { local name=$1; shift; "$@" 2>&1 | tee -a "$out"; local rc=${PIPESTATUS[0]}; [ "$rc" -eq 0 ] || failed+=("$name (exit $rc)"); return "$rc"; }
    step tools just _warn-tools
    echo "building {{bin}} {{version}} (git {{git_sha}}) with $(go version 2>/dev/null | cut -d' ' -f3)"
    if ! step compile bash -c 'cd code && go build -trimpath -ldflags "{{ldflags}}" -o ../{{bin}} ./cmd/ai_processing_server'; then
      echo; echo "==== BUILD FAILED: the Go compile failed (above); ./{{bin}} was not updated"; exit 1
    fi
    echo "built ./{{bin}}"
    git config core.hooksPath .githooks 2>/dev/null || true
    step broker just _broker-ensure
    step init ./{{bin}} init
    if conf=$(./{{bin}} broker config 2>&1); then echo "broker config: $conf"
    else echo "warning: broker config not written: $conf" | tee -a "$out"; fi
    step service just _service-ensure
    echo
    # warnings and errors, each with the "next:" hints printed under it
    problems=$(grep -E '^[[:space:]]*(warning|WARNING|error|ERROR|next)[: ]|NOT UPDATED' "$out" | sed -E 's/^[[:space:]]+//; s/^next:/  next:/')
    count=$(echo "$problems" | grep -cvE '^(  next:|$)')
    if [ ${#failed[@]} -gt 0 ]; then
      echo "==== BUILD FAILED in: ${failed[*]}"
      [ -n "$problems" ] && echo "$problems" | sed 's/^/  /'
      echo "./{{bin}} was compiled, but this machine is not ready: fix the above, then just build again"
      exit 1
    fi
    if [ "$count" -gt 0 ]; then
      echo "==== BUILD OK with $count warning(s); the server runs, but look at:"
      echo "$problems" | sed 's/^/  /'
    else
      echo "==== BUILD OK: no warnings. Start it with just run"
    fi

# Compile, install nats-server if missing, create this_server.yaml if missing, write the broker config
[windows]
build:
    @just _warn-tools
    @Write-Output "building {{bin}} {{version}} (git {{git_sha}}) with $((go version).Split(' ')[2])"
    @go build -C code -trimpath -ldflags "{{ldflags}}" -o ../{{bin}} ./cmd/ai_processing_server
    @Write-Output "built ./{{bin}}"
    -@git config core.hooksPath .githooks
    @just _broker-ensure
    @./{{bin}} init
    @$c = ./{{bin}} broker config 2>&1; if ($LASTEXITCODE -eq 0) { Write-Output "broker config: $c" } else { Write-Output "warning: broker config not written: $c" }
    @just _service-ensure
    @Write-Output ""; Write-Output "==== BUILD OK (any warning: lines above still need a look)"

# Start the server in the background and print where its API and logs are
[unix]
run: _binary
    #!/usr/bin/env bash
    set -euo pipefail
    if ./{{bin}} status >/dev/null 2>&1; then echo "already running:"; ./{{bin}} status; exit 0; fi
    # The queue needs this machine's broker: start it first when it is not answering (problems 2026_oct_27 #1).
    if ! ./{{bin}} broker status >/dev/null 2>&1; then
      if docker inspect act3-nats >/dev/null 2>&1; then just broker-docker || echo "warning: the broker did not start; the server runs direct mode only (just broker-status)"
      elif command -v nats-server >/dev/null; then just broker-run || echo "warning: the broker did not start; the server runs direct mode only (just broker-status)"; fi
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
    ./{{bin}} broker status *> $null; if ($LASTEXITCODE -ne 0) { docker inspect act3-nats *> $null; if ($LASTEXITCODE -eq 0) { just broker-docker } elseif (Get-Command nats-server -ErrorAction SilentlyContinue) { just broker-run }; if ($LASTEXITCODE -ne 0) { Write-Output "warning: the broker did not start; the server runs direct mode only (just broker-status)" } }
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
    @echo ""
    @./{{bin}} jobs

# The task queue: `just queue` lists what waits or runs; `just queue show <id>` is one job's states, events and log
queue *args: _binary
    @./{{bin}} queue {{args}}

# Cancel jobs on whichever route holds them and wait until they stop: `just cancel <id>...`, `just cancel --prefix act3_<hex>_`
cancel *args: _binary
    @./{{bin}} cancel {{args}}

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

# Warn for each tool the build or the model runners need that is missing (never stops the build)
[unix]
_warn-tools:
    @for t in go git ffmpeg ffprobe; do command -v "$t" >/dev/null || echo "WARNING: $t is not installed"; done

[windows]
_warn-tools:
    @foreach ($t in 'go', 'git', 'ffmpeg', 'ffprobe') { if (-not (Get-Command $t -ErrorAction SilentlyContinue)) { Write-Output "WARNING: $t is not installed" } }

# Run the broker as the act3-nats Docker container (NATS 2.15, data/queue/nats/docker.conf), restarting with Docker
[unix]
broker-docker:
    #!/usr/bin/env bash
    set -uo pipefail
    if ! command -v docker >/dev/null || ! docker info >/dev/null 2>&1; then echo "nats: Docker is not installed or not running"; exit 1; fi
    if docker inspect act3-nats >/dev/null 2>&1; then docker start act3-nats >/dev/null && echo "nats: act3-nats is running"; exit; fi
    if (exec 3<>/dev/tcp/127.0.0.1/4222) 2>/dev/null; then echo "nats: port 4222 already answers (another nats-server, native or in a container): keeping it"; exit 0; fi
    docker create --name act3-nats --restart unless-stopped -p 127.0.0.1:4222:4222 -p 127.0.0.1:8222:8222 \
      -v act3-nats-data:/data {{nats_image}} -c /etc/nats/act3.conf >/dev/null || exit 1
    docker cp data/queue/nats/docker.conf act3-nats:/etc/nats/act3.conf && docker start act3-nats >/dev/null || exit 1
    echo "nats: started act3-nats ({{nats_image}}, 127.0.0.1:4222; JetStream data in the act3-nats-data volume)"

[windows]
broker-docker:
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Write-Output "nats: Docker is not installed"; exit 1 }; docker info *> $null; if ($LASTEXITCODE -ne 0) { Write-Output "nats: Docker is not running"; exit 1 }
    docker inspect act3-nats *> $null; if ($LASTEXITCODE -eq 0) { docker start act3-nats > $null; if ($LASTEXITCODE -ne 0) { exit 1 }; Write-Output "nats: act3-nats is running" } elseif ($(try { $c = New-Object Net.Sockets.TcpClient; $c.Connect('127.0.0.1', 4222); $c.Close(); $true } catch { $false })) { Write-Output "nats: port 4222 already answers (another nats-server, native or in a container): keeping it" } else { docker create --name act3-nats --restart unless-stopped -p 127.0.0.1:4222:4222 -p 127.0.0.1:8222:8222 -v act3-nats-data:/data {{nats_image}} -c /etc/nats/act3.conf > $null; if ($LASTEXITCODE -ne 0) { exit 1 }; docker cp data/queue/nats/docker.conf act3-nats:/etc/nats/act3.conf; if ($LASTEXITCODE -ne 0) { exit 1 }; docker start act3-nats > $null; if ($LASTEXITCODE -ne 0) { exit 1 }; Write-Output "nats: started act3-nats ({{nats_image}}, 127.0.0.1:4222; JetStream data in the act3-nats-data volume)" }

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

# `just build` calls this: a broker for the queue, never failing the build (without one the server still runs direct
# mode and GET /v1/health says broker_missing with the fix). The act3-nats container, else a native nats-server or any
# broker already on 4222, else a new act3-nats container when Docker runs, else the native install
[unix]
_broker-ensure:
    #!/usr/bin/env bash
    if docker inspect act3-nats >/dev/null 2>&1; then just broker-docker; exit 0; fi
    if command -v nats-server >/dev/null; then echo "broker: nats-server $(nats-server --version | sed 's/^nats-server: //') installed"; exit 0; fi
    if (exec 3<>/dev/tcp/127.0.0.1/4222) 2>/dev/null; then echo "nats: port 4222 already answers: keeping it"; exit 0; fi
    if command -v docker >/dev/null && docker info >/dev/null 2>&1 && just broker-docker; then exit 0; fi
    if just broker-install; then exit 0; fi
    echo "WARNING: no broker: nats-server is not installed and Docker is not running, so the task queue cannot start (see above)."

[windows]
_broker-ensure:
    docker inspect act3-nats *> $null; if ($LASTEXITCODE -eq 0) { just broker-docker; exit 0 }; if (Get-Command nats-server -ErrorAction SilentlyContinue) { nats-server --version; exit 0 }; if ($(try { $c = New-Object Net.Sockets.TcpClient; $c.Connect('127.0.0.1', 4222); $c.Close(); $true } catch { $false })) { Write-Output "nats: port 4222 already answers: keeping it"; exit 0 }; if (Get-Command docker -ErrorAction SilentlyContinue) { docker info *> $null; if ($LASTEXITCODE -eq 0) { just broker-docker; if ($LASTEXITCODE -eq 0) { exit 0 } } }; just broker-install; if ($LASTEXITCODE -ne 0) { Write-Output "WARNING: no broker: nats-server is not installed and Docker is not running, so the task queue cannot start." }

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

[windows]
broker-run: _binary
    ./{{bin}} broker status *> $null; if ($LASTEXITCODE -eq 0) { ./{{bin}} broker status; exit 0 }; $conf = ./{{bin}} broker config; if ($LASTEXITCODE -ne 0) { exit 1 }; $dir = Split-Path $conf; $p = Start-Process -FilePath nats-server -ArgumentList '-c', "`"$conf`"" -WindowStyle Hidden -RedirectStandardOutput "$dir/nats-server.log" -RedirectStandardError "$dir/nats-server.err.log" -PassThru; $p.Id | Set-Content "$dir/nats-server.pid"; foreach ($i in 1..40) { ./{{bin}} broker status *> $null; if ($LASTEXITCODE -eq 0) { ./{{bin}} broker status; Write-Output "pid $($p.Id) · log $dir/nats-server.log · stop: just broker-stop"; exit 0 }; Start-Sleep -Milliseconds 250 }; Write-Output "nats-server did not answer; see $dir/nats-server.err.log"; exit 1

# Stop the nats-server (LaunchAgent or broker-run's); queued jobs and claims stay in its store
[unix]
broker-stop:
    #!/usr/bin/env bash
    if [ "$(uname)" = "Darwin" ] && launchctl bootout "gui/$(id -u)/ai.act3.nats-server" 2>/dev/null; then echo "stopped (LaunchAgent unloaded; just broker-run reloads it)"; exit 0; fi
    f="${APS_BROKER_DIR:-$HOME/.act3_processing/nats}/nats-server.pid"
    if [ -f "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null; then kill "$(cat "$f")" && rm -f "$f" && echo stopped; else echo "no broker started by broker-run"; fi

[windows]
broker-stop:
    $d = if ($env:APS_BROKER_DIR) { $env:APS_BROKER_DIR } else { "$HOME/.act3_processing/nats" }; $f = "$d/nats-server.pid"; if ((Test-Path $f) -and (Get-Process -Id (Get-Content $f) -ErrorAction SilentlyContinue)) { Stop-Process -Id (Get-Content $f); Remove-Item $f; Write-Output stopped } else { Write-Output "no broker started by broker-run" }

# ---- start at login/boot (pm/ai_processing_server.mdx §19.2). install-service / uninstall-service / service-status
# are below, next to render-setup: one Go implementation (code/internal/service), LaunchAgents ai.act3.* on macOS ----

# `just build` calls this so a machine never misses it. APS_NO_SERVICE=1 skips it (dev laptops, CI).
[unix]
_service-ensure:
    #!/usr/bin/env bash
    if [ -n "${APS_NO_SERVICE:-}" ]; then echo "service: skipped (APS_NO_SERVICE set)"; exit 0; fi
    if [ "$(uname)" != "Darwin" ]; then echo "service: not automated on $(uname) yet (systemd unit: pm/TO_DO.md)"; exit 0; fi
    ./{{bin}} service ensure || { echo "warning: service: the OS services were not installed (error above); the server still runs with just run"; }

[windows]
_service-ensure:
    Write-Output "service: not automated on Windows yet (pm/TO_DO.md)"

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
[unix]
render-check: _binary
    -./{{bin}} broker status
    @echo ""
    -./{{bin}} validate
    @echo ""
    @./{{bin}} status 2>/dev/null | grep -E "SERVICE|open_source|local API" || echo "server not running: just run"

[windows]
render-check: _binary
    -./{{bin}} broker status
    @echo ""
    -./{{bin}} validate
    @echo ""
    @$o = ./{{bin}} status 2>$null; if ($LASTEXITCODE -eq 0) { $o | Select-String -Pattern 'SERVICE|open_source|local API' } else { Write-Output 'server not running: just run' }

# One real job through the queue on this machine's installed models: wan, upscale, or all (Wan, then upscale its clip)
render-test what="all": _binary
    python3 tests/real_render.py {{what}}

# On the Mac Studio: build (installs the broker), turn the queue + both model services on, (re)start, check
render-setup: build
    ./{{bin}} enable queue act3.video_gen_open_source act3.upscale_open_source
    ./{{bin}} validate
    -./{{bin}} stop
    just run
    @echo ""
    just render-check

# ---- start at boot (pm/task_authoring.mdx §8 G4: a real power cycle recovers unattended) ----

# Install nats-server and the server as OS services (launchd / systemd user / Task Scheduler). Refused on a dev machine.
install-service: _binary
    ./{{bin}} service install

# Remove both OS services (running renders keep going: task hosts are detached)
uninstall-service: _binary
    ./{{bin}} service uninstall

# Are the OS services installed? `./ai_processing_server service print` shows the files without installing
service-status: _binary
    ./{{bin}} service status

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

# \x22 instead of a double quote: Windows PowerShell 5.1 strips double quotes from a native command's arguments
[windows]
check-secrets:
    $hits = @(git grep -l -E '^-----BEGIN ([A-Z0-9]+ )*PRIVATE KEY-----' -- . ':!*_test.go') + @(git grep -l -P '\x22d\x22[[:space:]]*:' -- '*.json' '*.jwk'); if ($hits.Count) { Write-Output 'PRIVATE KEY material in tracked files (revoke that key, then remove it):'; $hits; exit 1 }; Write-Output 'check-secrets: no private keys in tracked files'

# Go unit tests next to the code
test-go:
    go vet -C code ./...
    go test -C code ./...

# Black-box suite: starts its own servers on free ports and calls the API (see tests/README.md)
test-suite: build
    just --justfile tests/justfile --working-directory tests

# Cross-compile check for every fleet target (never committed)
[unix]
cross:
    cd code && GOOS=darwin GOARCH=arm64 go build -o /dev/null ./cmd/ai_processing_server
    cd code && GOOS=windows GOARCH=amd64 go build -o /dev/null ./cmd/ai_processing_server
    cd code && GOOS=linux GOARCH=amd64 go build -o /dev/null ./cmd/ai_processing_server
    cd code && GOOS=linux GOARCH=arm64 go build -o /dev/null ./cmd/ai_processing_server

_binary:
    @{{ if path_exists(bin) == "true" { "exit 0" } else { "echo 'no binary yet: run just build first'; exit 1" } }}
