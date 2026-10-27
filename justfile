# ai_processing_server — build and operate the processing server on this machine.
# `just build` then `just run` must work on a clean checkout with Go installed (pm/ai_processing_server.mdx §17).

set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

version := "0.1.0"
bin := if os_family() == "windows" { "ai_processing_server.exe" } else { "ai_processing_server" }
git_sha := `git rev-parse --short HEAD 2>/dev/null || echo unknown`
ldflags := "-s -w -X main.version=" + version + " -X main.gitSHA=" + git_sha + " -X main.buildTime=" + datetime_utc("%Y-%m-%dT%H:%M:%SZ")

export GOWORK := "off"        # a parent go.work (e.g. ~/BGit/act3/go.work) must not capture this module
export CGO_ENABLED := "0"     # pure Go: no C toolchain needed (spec §3.1, D-005)

# List recipes
default:
    @just --list

# Compile code/ into ./ai_processing_server, then create data/git_ignore/this_server.yaml if missing (never overwrites it)
build:
    @echo "building {{bin}} {{version}} (git {{git_sha}}) with `go version | cut -d' ' -f3`"
    @cd code && go build -trimpath -ldflags "{{ldflags}}" -o ../{{bin}} ./cmd/ai_processing_server
    @echo "built ./{{bin}}"
    @./{{bin}} init

# Start the server in the background and print where its API and logs are
[unix]
run: _binary
    #!/usr/bin/env bash
    set -euo pipefail
    if ./{{bin}} status >/dev/null 2>&1; then echo "already running:"; ./{{bin}} status; exit 0; fi
    mkdir -p data/git_ignore/logs
    nohup ./{{bin}} run > data/git_ignore/logs/server.stdout.log 2>&1 &
    pid=$!
    for _ in $(seq 1 80); do
      if ./{{bin}} status >/dev/null 2>&1; then
        ./{{bin}} status
        echo
        echo "pid $pid · stop with: just stop"
        echo "logs: ~/_act3_logs/processing_server.info and .err (just logs) · stdout: data/git_ignore/logs/server.stdout.log"
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

# Run in the foreground (development; Ctrl+C drains and stops)
run-fg: _binary
    ./{{bin}} run

# Drain running jobs, then stop. `just stop force` kills now.
stop mode="": _binary
    ./{{bin}} stop {{ if mode == "force" { "--force" } else { "" } }}

# Health, services and running jobs of the server on this machine
status: _binary
    ./{{bin}} status
    @echo
    ./{{bin}} jobs

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
