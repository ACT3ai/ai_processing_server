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
    nohup ./{{bin}} run > data/git_ignore/logs/server.stdout.log 2>&1 &
    pid=$!
    for _ in $(seq 1 80); do
      if ./{{bin}} status >/dev/null 2>&1; then
        ./{{bin}} status
        echo
        echo "started pid $pid · stop: just stop · status: just status"
        echo "logs: just logs (~/_act3_logs/processing_server.info, .err) · stdout: data/git_ignore/logs/server.stdout.log"
        exit 0
      fi
      if ! kill -0 "$pid" 2>/dev/null; then
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

# Run in the foreground (development; Ctrl+C stops the controller, running jobs keep going)
run-fg: _binary
    ./{{bin}} run

# Stop the controller. Jobs running in task hosts keep going; the next `just run` re-adopts them (pm/task_queue.mdx §9.5)
stop mode="": _binary
    @./{{bin}} stop {{ if mode == "force" { "--force" } else { "" } }}

# Restart the controller only; running jobs are re-adopted, not stopped
restart: _binary
    ./{{bin}} stop
    just run

# git pull, build, restart the controller: a code update that never stops a running job (§9.5)
update:
    git pull --ff-only
    just build
    just restart

# Jobs running in detached task hosts on this machine: job, service, code snapshot, pid, heartbeat
runs: _binary
    ./{{bin}} runs

# Once per machine: install nats-server (pinned in data/queue/broker.yaml) as an OS service on 127.0.0.1:4244
broker-install: _binary
    ./{{bin}} broker install

# Regenerate the broker config from this_server.yaml and reload it without dropping connections
broker-config: _binary
    ./{{bin}} broker config

# Is the broker service up, which version, work stored, servers seen
broker-status: _binary
    ./{{bin}} broker status

# Remove the broker OS service (its stored jobs stay in queue.store_dir)
broker-uninstall: _binary
    ./{{bin}} broker uninstall

# Health, services and recent jobs of the server on this machine
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
    nohup nats-server -c "$conf" > "$dir/nats-server.log" 2>&1 &
    echo $! > "$dir/nats-server.pid"
    for _ in $(seq 1 40); do
      if ./{{bin}} broker status >/dev/null 2>&1; then ./{{bin}} broker status; echo "pid $(cat "$dir/nats-server.pid") · log $dir/nats-server.log · stop: just broker-stop"; exit 0; fi
      sleep 0.25
    done
    echo "nats-server did not answer; see $dir/nats-server.log"; tail -n 20 "$dir/nats-server.log"; exit 1

# Stop the nats-server started by broker-run (queued jobs and claims stay in its store)
[unix]
broker-stop:
    #!/usr/bin/env bash
    f="${APS_BROKER_DIR:-$HOME/.act3_processing/nats}/nats-server.pid"
    if [ -f "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null; then kill "$(cat "$f")" && rm -f "$f" && echo stopped; else echo "no broker started by broker-run"; fi

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
