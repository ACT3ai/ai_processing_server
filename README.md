# ai_processing_server

The worker program for ACT3's GPU and CPU machines. It is one Go binary that runs on every processing server and
render-queue server. It does the heavy work behind the [ACT3 AI filmmaking](https://act3ai.com) web app and
We The Citizens: image and video generation, upscaling, Blender renders, transcodes, video merges, TTS,
transcription, OCR and AI descriptions.

**Status (2026-10-27): v0.1.0 built.** `just build` and `just run` work. The server detects its hardware, computes
offers, and runs jobs in parallel slots with priorities, retries, cancel, drain and crash recovery. Jobs arrive by
**direct mode** (`POST /v1/jobs` on the local API); the platform lease transport (phase 3) is not built yet. Built
runners: `compute`, `render` (test render to H.264 MP4) and `ffmpeg` (transcode, merge). The test suite is in
[tests/](tests/README.md): 74 black-box tests that send real work and check the answers. See [Build plan](#build-plan).

## What it does

On every machine it runs on, the server:

1. **Knows its own hardware.** It detects the CPU cores, RAM, disks, GPUs (model, count, VRAM, CUDA or Metal) and the
   installed engines (ffmpeg, Blender, ComfyUI, Python, Whisper, and so on).
2. **Turns that into offers.** For each service the machine's owner allows, it works out whether this machine can run
   it, on which GPU, and how many at once. Each offer carries a reason ("needs 24 GB VRAM; best GPU has 16").
3. **Leases jobs** from the platforms (ACT3 and We The Citizens) over outbound HTTPS. It only takes as many as it can
   start soon.
4. **Runs jobs in parallel** inside hardware slots. A slot is one GPU or a set of CPU cores and RAM, held by one job.
5. **Reports back** progress, logs, outputs and failures. It uploads outputs straight to storage with presigned URLs.

```
   ACT3 appsrv (NestJS)                 WTC backend (Go)
   job table, priorities                job table, priorities
        ▲  │  HTTPS: register, heartbeat, lease, renew, complete, fail
        │  ▼                                  ▲  │
 ┌──────┴─────────────────────────────────────┴──▼──────────┐
 │ ai_processing_server  (one Go binary per machine)        │
 │  config     this_server.yaml + data/ catalogs            │
 │  hardware   detect CPU, RAM, GPUs, VRAM, disks, engines  │
 │  capability allowed services × hardware → offers         │
 │  scheduler  slots, local queue, priorities, leases       │
 │  runners    comfy, ffmpeg, blender, python, whisper, ... │
 │  local API  127.0.0.1:9440                               │
 └──────────────────────────────────────────────────────────┘
```

All connections start from the server, so machines behind a home or office NAT work without opening ports.

## Who decides what

**The owner sets the fence, and the platforms set the order inside it.**

| Decision | Who decides |
| --- | --- |
| Which services this machine may run, and how many at once | The machine owner, in `this_server.yaml` |
| Each job's priority | Its platform (ACT3 or WTC), on ACT3's shared priority scale |
| Service weights, pauses, drain | The platform, sent in heartbeat responses. These can never turn on a service the owner turned off. |
| ACT3 vs WTC share of the machine when both have work | The machine owner (`platforms.*.share`) |

The coordination logic is ours: matching, priority, slots, claims and retries are Go code in this repo. The task queue
is **NATS with JetStream, running as its own `nats-server` process** on every machine (decided 2026-10-27, D-014; see
[pm/task_queue.mdx](pm/task_queue.mdx)). Our binary is a client of it: it pulls jobs for the services it offers, claims
them with a fencing token, reports events, and requeues or dead-letters failures. Queued jobs and claims stay in the
broker while our binary restarts. Set it up once per machine:

```bash
just broker-install    # nats-server (Homebrew on macOS, winget on Windows)
just broker-config     # writes ~/.act3_processing/nats/<role>.conf from this_server.yaml
just broker-run        # starts it in the background (until it is installed as an OS service)
just broker-status
```

Without a broker the server still runs direct mode (`POST /v1/jobs`), and `GET /v1/health` reports
`broker: broker_missing` with the fix. Owners and local software submit to the queue with `POST /v1/queue/jobs`.

## `this_server.yaml`: what makes a machine a server

`data/git_ignore/this_server.yaml` is the per-machine file. It holds:

* **Identity:** the server key (`B5090`, `BPC`, `BMT`, `BMS`, …), display name, profile, site, and the hostname and
  machine id it was created on.
* **Hardware snapshot:** cores, RAM, disks, GPUs, engines. The owner can lower numbers to offer less. Detection still
  decides what exists, so the effective value is `min(detected, declared)`.
* **Services allow-list:** each service id with `enabled`, `max_parallel`, allowed GPUs and a small `local_bias`.
* **Platforms, limits, paths, local API:** platform base URLs and *paths* to credential files (never the secrets
  themselves), reserves for CPU, RAM and VRAM, a minimum free disk, a GPU temperature limit, quiet hours, and work and
  cache directories.

Rules:

* `just build` runs `./ai_processing_server init`. **If the file is missing, `init` creates it** from detected
  hardware and the known-server presets. **If the file exists, `init` never changes a byte**. It only validates the
  file.
* `just reinit` writes `this_server.yaml.new` with fresh detection for the owner to compare and merge.
* Machine-written facts (detected hardware, platform overrides, job journal, PID) go to `data/git_ignore/state/`, never
  into `this_server.yaml`.

## Services (initial list)

| Service id | Runner | Hardware |
| --- | --- | --- |
| `act3.comfy_on_prem` | comfy | NVIDIA, 16 GB+ |
| `act3.video_gen_open_source` (Wan 2.2, LTX 2.5, Hunyuan 1.5) | comfy / python | NVIDIA 24 GB+ or Apple 96 GB+ unified |
| `act3.image_gen_open_source` (FLUX, SDXL) | comfy | GPU 12 GB+ |
| `act3.upscale_open_source` (SeedVR2, FlashVSR) | python | NVIDIA 24 GB+ |
| `act3.video_merge`, `act3.transcode` | ffmpeg | CPU (NVENC / VideoToolbox optional) |
| `act3.blender_render` | blender | GPU preferred |
| `act3.tts_local` (Kokoro) | python | CPU or Apple |
| `wtc.transcription` | whisper | Apple or NVIDIA, CPU fallback |
| `wtc.ai_description`, `wtc.ocr` | python | GPU / CPU |
| `wtc.transcode` | ffmpeg | CPU. Outputs H.264 + AAC + yuv420p, never AV1 for X. |
| `sys.self_test`, `sys.compute` | compute | any CPU. Primes, sha256 chains, matrices, file stats: checkable results |
| `sys.render_test` | render | any CPU. Mandelbrot frames to PNG, then H.264 MP4 when ffmpeg is present |

**Built in v0.1.0:** `sys.*`, `act3.transcode`, `act3.video_merge`, `wtc.transcode`. The others are in the catalog
and get offers with reasons, but their runners (`comfy`, `blender`, `whisper`, `python`) come in phases 4 and 5.

Each service is defined by one catalog file in `data/services/` (requirements, resources per job, timeouts, retry
policy, params schema). Adding a service means adding one runner package and one catalog file.

## Repository layout

```
ai_processing_server/
├── README.md                  # this file
├── CLAUDE.md                  # rules for working in this repo
├── .gitignore                 # binary, build output, /data/git_ignore/, private keys
├── pm/                        # specifications only, never code
│   ├── ai_processing_server.mdx   # the engineering spec
│   ├── Decisions.yaml             # D-001 … D-017
│   └── TO_DO.md
├── servers/                   # one server.yaml per known machine (checked in)
│   ├── Bryan_5090/server.yaml
│   ├── Bryan_Laptop/server.yaml
│   └── Bryan_Mac_Studio/server.yaml
├── data/                      # checked in, except git_ignore/
│   ├── templates/this_server.template.yaml
│   ├── services/*.yaml        # one catalog file per service
│   ├── servers/known_servers.yaml
│   └── git_ignore/            # per machine, NEVER committed
│       ├── this_server.yaml   # owner-edited; created once by init, never rewritten
│       ├── state/             # journal/<job_id>.json, server.pid
│       ├── work/<job_id>/     # scratch per job, deleted after
│       ├── results/<job_id>/  # outputs kept for GET /v1/jobs/{id}/outputs/{name} (24 h)
│       └── logs/jobs/         # per-job logs (7 days)
├── code/                      # all Go source (module root: code/go.mod)
│   ├── cmd/ai_processing_server/main.go
│   └── internal/{api,capability,config,hardware,jobs,logging,procs,runners,scheduler,storage}/
├── tests/                     # black-box suite (Python, stdlib only): see tests/README.md
└── justfile
```

Still planned under `data/`: `models/`, `sites/sites.yaml` and `schemas/`.

## Known machines

| Machine | File | Data | Role and notes |
| --- | --- | --- | --- |
| Bryan's RTX 5090 PC | [servers/Bryan_5090/server.yaml](servers/Bryan_5090/server.yaml) | guessed | The only CUDA node (32 GB GDDR7). It is the main node for local image and video generation. One heavy GPU job at a time. |
| Bryan's MacBook Pro (M4 Max, 128 GB) | [servers/Bryan_Laptop/server.yaml](servers/Bryan_Laptop/server.yaml) | detected | Development workstation. Long jobs only on AC power. Low parallelism. |
| Bryan's Mac Studio ("CLAW") | [servers/Bryan_Mac_Studio/server.yaml](servers/Bryan_Mac_Studio/server.yaml) | guessed | Always on. Shares the machine with OpenClaw agents and voice-memo transcription. |

Values marked `guess` must be replaced by running hardware detection on each machine. The spec also names `BPC`
(two-GPU PC) and `BMT` (Mac Tower), which have no `servers/` file yet. Two points still need to line up with the spec:

* **Server keys:** the `servers/` folders use names like `Bryan_5090`, while the spec uses short keys like `B5090`.
* **The 5090's OS:** the spec assumes Windows, while `servers/Bryan_5090` guesses Linux.

## Build and run

Each machine builds its own binary. Nothing else should be needed: a missing `nvidia-smi`, Blender or ComfyUI only
turns services off, with reasons. It never breaks the build.

| Command | Does |
| --- | --- |
| `just build` | compile `code/` into `./ai_processing_server` (git-ignored), then `init` |
| `just run` / `just run-fg` | start in the background / foreground; prints `Local API: http://127.0.0.1:9440/` |
| `just stop` | drain (wait up to 10 min for running jobs), then stop |
| `just status` | health plus a table of services and jobs |
| `just logs` | tail `~/_act3_logs/processing_server.info` and `.err` |
| `just detect` | print detected hardware and offers without starting |
| `just reinit` | write `this_server.yaml.new` and show the diff |
| `just validate` | check `this_server.yaml` and `data/` without starting |
| `just test` | `go vet` + `go test ./...`, then the black-box suite in `tests/` |
| `just cross` | compile check for darwin/arm64, windows/amd64, linux/amd64, linux/arm64 |
| `just install-service` | *(planned)* launchd (macOS), Windows service, or systemd user unit |

Try it:

```bash
just build && just run
TOKEN=$(cat ~/.credentials/ai_processing_server_local.token)
curl -s -H "Authorization: Bearer $TOKEN" -X POST http://127.0.0.1:9440/v1/jobs \
  -d '{"service":"sys.compute","priority":50,"params":{"op":"primes","limit":10000000,"segments":20}}'
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:9440/v1/jobs     # state, stage, progress, result
open http://127.0.0.1:9440/                                                  # plain status page
just stop
```

The job record mirrors Asynq's `TaskInfo` (`id, queue, type, payload, state, max_retry, retried, last_err,
completed_at, result`, states `pending · active · retry · completed · archived`, plus `cancelled`), so a real task
system can sit behind the server later with the API passing its data through (`pm/Decisions.yaml` D-008).

Build targets: `darwin/arm64`, `windows/amd64`, `linux/amd64` and `linux/arm64`. The default build is pure Go
(`CGO_ENABLED=0`). GPU facts come from vendor command-line tools (`nvidia-smi`, `system_profiler`).

## Logs

The server logs the same way as the other ACT3 components: append-only files in `~/_act3_logs/`.

* `processing_server.info`: INFO and DEBUG
* `processing_server.err`: WARN and ERROR

Errors that should not happen (panics, `internal` job failures, a journal that cannot be saved, a work dir that
cannot be deleted) also go to **`~/T/ai_processing_server/error.err`**: one line each with a UTC timestamp and the
absolute source path of the call site, capped at 4 MB with 3 rolled files. Read it first when something looks wrong.
See [pm/errors.mdx](pm/errors.mdx).

Each job also gets its own log at `data/git_ignore/logs/jobs/<job_id>.log`. That log is uploaded with the result.

## Rules

* Specs go in `pm/` and code goes in `code/`. Never mix them.
* Never commit the binary, anything under `data/git_ignore/`, or private keys.
* Never put secrets in `data/` or `servers/`. Config files hold *paths* to credential files.
* Never compute credit prices here. Pricing lives in the ACT3 web app backend.
* Machine-specific behavior lives only in its profile (`code/internal/profiles/`). There is never an
  `if key == "B5090"` anywhere else.

## Build plan

| Phase | Delivers |
| --- | --- |
| 0. Specs and scaffold | `pm/Goal.md`, `pm/TO_DO.md`, `pm/Decisions.yaml`; `code/go.mod`; justfile `build`/`run` |
| 1. Identity and hardware | config, hardware detection, capability offers, `init`, `detect`, local API read routes, logging |
| 2. Jobs locally | job state machine, scheduler, `selftest` and `ffmpeg` runners, direct mode |
| 3. ACT3 connected | ACT3 submits through the NATS task queue (task_queue.mdx §20, §22); queue v1 built in v0.2.0 |
| 4. GPU services | `comfy` and `python` runners, model cache, `b5090` / `bpc` / `bms` profiles |
| 5. WTC connected | `wtc.transcription`, `wtc.transcode`, `wtc.ocr` |
| 6. Fleet | service install, metrics, automated updates, Nepal and cloud machines |

## Further reading

* [pm/ai_processing_server.mdx](pm/ai_processing_server.mdx): the full engineering spec. It covers hardware detection
  per OS, the job envelope and lifecycle, leases and crash recovery, priorities, the local and platform APIs,
  security, tests and acceptance, decisions, and open questions.
* [CLAUDE.md](CLAUDE.md): the rules for working in this repo.
* Related specs in the ACT3 film repo: `~/BGit/all/film/pm/subp/render_queue/` (`RenderEngine.mdx`,
  `bryan_servers.mdx`, `render_queue.mdx`).
