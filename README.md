# ai_processing_server

The worker program for ACT3's GPU and CPU machines. It is one Go binary that runs on every processing server and
render-queue server. It does the heavy work behind the [ACT3 AI filmmaking](https://act3ai.com) web app and
We The Citizens: image and video generation, upscaling, Blender renders, transcodes, video merges, TTS,
transcription, OCR and AI descriptions.

**Status (2026-10-27): v0.1.0 plus task queue phase Q1.** `just build` and `just run` work. The task system is
**NATS JetStream**, run as its own `nats-server` service on 127.0.0.1:4244 ([pm/task_queue.mdx](pm/task_queue.mdx),
D-Q1). Jobs submitted on the local API go SUBMIT → admission (priority band) → WORK → pulled when a slot is free →
run in a **detached task host** from a frozen code snapshot, so `just update` (pull, build, restart) never stops a
running render: the new controller re-adopts it. Platform callers (ACT3, WTC) arrive with the hub in phase Q3. Built
runners: `compute`, `render` (test render to H.264 MP4) and `ffmpeg` (transcode, merge). The test suite is in
[tests/](tests/README.md): 86 black-box tests that send real work through a real broker and check the answers. See
[Build plan](#build-plan).

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

The coordination logic is ours: matching, priority, slots, leases and retries are Go code in this repo. The proposed
transport is an HTTPS lease API (`POST /processing/lease`, long-poll). A Temporal adapter is optional for migrating off
today's TypeScript `job_runner`. RabbitMQ is not planned for now. That choice is still **proposed**: decision D-001 in
the spec waits for Bryan to confirm it.

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
│   ├── task_queue.mdx             # the task queue: NATS JetStream, continuity, priority, contract
│   ├── Decisions.yaml             # D-001 … D-017, D-Q1
│   └── TO_DO.md
├── servers/                   # one server.yaml per known machine (checked in)
│   ├── Bryan_5090/server.yaml
│   ├── Bryan_Laptop/server.yaml
│   └── Bryan_Mac_Studio/server.yaml
├── data/                      # checked in, except git_ignore/
│   ├── templates/this_server.template.yaml
│   ├── services/*.yaml        # one catalog file per service
│   ├── servers/known_servers.yaml
│   ├── queue/                 # streams.yaml, bands.yaml, callers.yaml, categories.yaml, broker.yaml, nats/*.conf.tmpl
│   ├── schemas/               # task_envelope.v1, task_event.v1, run_record.v1 (JSON Schema)
│   └── git_ignore/            # per machine, NEVER committed
│       ├── this_server.yaml   # owner-edited; created once by init, never rewritten
│       ├── state/             # jobs/<job_id>.json (the local record), server.pid
│       ├── runs/<job_id>/     # run directory shared with the job's task host (run.json, events.jsonl, ...)
│       ├── snapshots/<id>/    # frozen code a job runs from: <git_sha>-<binary hash>
│       ├── work/<job_id>/     # scratch per job, deleted after
│       ├── results/<job_id>/  # outputs kept for GET /v1/jobs/{id}/outputs/{name} (24 h)
│       └── logs/jobs/         # per-job logs (7 days)
├── code/                      # all Go source (module root: code/go.mod)
│   ├── cmd/ai_processing_server/main.go
│   └── internal/{api,brokerctl,capability,config,errlog,hardware,jobs,logging,priority,procs,queue,
│                 redact,runners,runs,scheduler,security,snapshot,storage,taskhost}/
├── tests/                     # black-box suite (Python, stdlib only): see tests/README.md
└── justfile
```

Still planned under `data/`: `models/`, `sites/sites.yaml`, and JSON Schemas for each service's params.

The broker's own files live outside the repo, in `~/.act3_processing/nats/` (generated `nats.conf`, its log, the
JetStream store).

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
| `just broker-install` | **once per machine**: install `nats-server` (pinned in `data/queue/broker.yaml`, sha256-checked) as an OS service (launchd, systemd --user, Windows service) |
| `just broker-config` · `broker-status` · `broker-uninstall` | regenerate and reload its config · is it up, which version, work stored · remove the service (stored jobs stay) |
| `just build` | compile `code/` into `./ai_processing_server` (git-ignored), then `init` |
| `just run` / `just run-fg` | start the controller in the background / foreground; without a broker it starts and reports `broker_missing` with the fix |
| `just stop` | stop the controller; running jobs keep going in their task hosts |
| `just restart` · `just update` | restart the controller (running jobs are re-adopted) · `git pull`, build, restart |
| `just runs` | jobs running in task hosts: service, code snapshot, pid, last heartbeat |
| `just status` | health plus a table of services and jobs |
| `just logs` | tail `~/_act3_logs/processing_server.info` and `.err` |
| `just detect` | print detected hardware and offers without starting |
| `just reinit` | write `this_server.yaml.new` and show the diff |
| `just validate` | check `this_server.yaml` and `data/` without starting |
| `just test` | `go vet` + `go test ./...`, then the black-box suite in `tests/` |
| `just cross` | compile check for darwin/arm64, windows/amd64, linux/amd64, linux/arm64 |
| `just install-service` | *(planned)* the controller itself as a launchd / Windows / systemd service |

Try it:

```bash
just broker-install          # once per machine
just build && just run
TOKEN=$(cat ~/.credentials/ai_processing_server_local.token)
curl -s -H "Authorization: Bearer $TOKEN" -X POST http://127.0.0.1:9440/v1/jobs \
  -d '{"service":"sys.compute","priority":50,"params":{"op":"primes","limit":10000000,"segments":20}}'
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:9440/v1/jobs     # state, band, attempt, progress, result
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:9440/v1/jobs/<job_id>/events   # the caller's events
open http://127.0.0.1:9440/                                                  # plain status page
just stop
```

The job record passes the task queue's own contract through (`pm/Decisions.yaml` D-014): the envelope v1
(`data/schemas/task_envelope.v1.schema.json`), the states `SUBMITTED · QUEUED · CLAIMED · STAGING · RUNNING ·
UPLOADING · SUCCEEDED · FAILED · CANCELLED · EXPIRED · REJECTED` (task_queue.mdx §13.4), and the events on
`evt.<caller>.<job_id>` (§13.5).

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
| 3. ACT3 connected | HTTPS lease broker; appsrv lease endpoints; heartbeat feeds the admin Server page |
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
