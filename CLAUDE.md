# CLAUDE.md — ai_processing_server

Repo: https://github.com/ACT3ai/ai_processing_server.git
ROOT_DIR dir is ~/BGit/act3/ai_processing_server

## What this repo is

`ai_processing_server` is the program that runs on ACT3's **processing servers and render-queue
servers**: the GPU and CPU machines that do the heavy work behind the ACT3 AI filmmaking web app
(image generation, video generation, upscaling, Blender renders, transcoding, and similar jobs).

On every machine it runs on, it:

1. **Receives jobs.** Work comes in from the ACT3 platform (the web app backend / render queue).
2. **Knows its own hardware.** On startup it detects what the machine has: GPUs (model, count,
   VRAM), CPU cores, RAM, disk, and installed engines/models. It turns that into a **capability
   list**: which job types this machine can run, and how many at once.
3. **Runs jobs, one or several in parallel**, within what the hardware allows (for example, two
   jobs on two GPUs, or several CPU-only jobs side by side).
4. **Reports status and results** back: progress, logs, output files, success/failure.

Coordination is **ours**. The transport is NATS JetStream in its own nats-server process
(pm/Decisions.yaml D-014); our binary is only a client. The scheduling rules (which machine takes
which job, priority, retries, how capabilities are matched) are our own logic, written in this repo
and specified in `pm/`.

## Language and build

* **Go.** All code is Go, compiled to **one binary**.
* The binary is built into the **repo root** (`./ai_processing_server`) and is **git-ignored on
  every computer** (see `.gitignore`). Never commit it. Each machine builds its own.
* Every machine uses the `justfile` at the repo root:
  * `just build` compiles the binary from `code/` into the repo root.
  * `just run` runs it.
  * Add `just stop`, `just status`, and `just logs` as they become useful, matching our other apps.
* No other build step should be needed. If a machine needs extra setup (drivers, CUDA, Blender,
  ComfyUI), document it in `pm/` and have the server detect and report it, not fail silently.

## Directory layout

```
ai_processing_server/
├── CLAUDE.md          # This file: what the repo is and where everything lives
├── README.md          # Short product summary for GitHub, links into pm/
├── justfile           # just build / just run (and later stop/status/logs)
├── .gitignore         # Ignores the compiled binary and local build output
├── ai_processing_server   # Compiled binary — built locally, NEVER committed
├── pm/                # Product management specifications ONLY (no code)
└── code/              # ALL Go source code, in a hierarchy
```

### pm/ — specifications only

Everything that explains **how the system works and why**: goals, architecture, job types,
the hardware/capability model, the scheduling and parallelism rules, the job message format,
the broker decision, failure and retry behavior, deployment to servers, to-dos, and decisions.
Follow the standard PROJECT layout where it fits (`pm/Goal.md`, `pm/TO_DO.md`,
`pm/Decisions.yaml`, `pm/docs/{topic}.md`, and so on).

* **Never put code in `pm/`.**
* Specs point at the code that implements them, with two lines: one naming the functionality,
  one giving the full path from `~` to the source file, then the type/function.

### code/ — all Go source

All application code lives under `code/`, and only there. The Go module root is `code/`
(`code/go.mod`). Suggested hierarchy (adjust as the design firms up in `pm/`):

```
code/
├── go.mod
├── cmd/ai_processing_server/main.go   # Entry point for the single binary
└── internal/
    ├── hardware/     # Detect GPUs, VRAM, CPU, RAM, disk, installed engines
    ├── capability/   # Turn hardware into "job types I can run, and how many"
    ├── jobs/         # Job definitions, intake, lifecycle (queued → running → done/failed)
    ├── scheduler/    # Our coordination: matching, priority, parallel slots, retries
    ├── queue/        # Task queue transport: NATS JetStream (queue/natsq), contract tests
    ├── brokerctl/    # This machine's nats-server: config, status
    ├── runners/      # One runner per job type (image, video, upscale, Blender, ...)
    ├── taskhost/     # Detached task host: a render outlives a controller restart
    ├── snapshot/     # Frozen, content-keyed copy of the task code a job runs
    ├── scripts/      # (code/scripts/) task code that gets snapshotted
    └── report/       # Progress, logs, and result reporting back to the platform
```

## Rules for working in this repo

* Specs in `pm/`, code in `code/`. Never mix them.
* The binary goes to the repo root and stays ignored. Never commit binaries.
* `just build` then `just run` must always work on a clean checkout.
* Write logs the same way as the other ACT3 components: append-only files in `~/_act3_logs/`,
  `processing_server.info` for INFO/DEBUG and `processing_server.err` for WARN/ERROR.
* Errors that should not happen go through `internal/errlog` (`var elog = errlog.For("pkg")`; `elog.Error`,
  `Check`, `Ignore`, `Recover`, `Go`) into `~/T/ai_processing_server/error.err` (4 MB, rolling). Never write a
  bare `recover()` or drop an error silently. Spec: `pm/errors.mdx`.
* Never compute or hard-code credit prices here; pricing lives in the ACT3 web app backend.
* Work on the current branch. Do not create, switch, or push branches unless asked.

## Current state (2026-10-27)

v0.2.0: jobs arrive through NATS JetStream (POST /v1/queue/jobs, or NATS directly) or direct mode.
Model runners oss_video (Wan 2.2) and oss_upscale (SeedVR2) run in detached task hosts from frozen
snapshots (data/git_ignore/runs/, snapshots/), so a git pull and a restart never stop a render. Task
scripts live in code/scripts/ (copies from ~/BGit/all; refresh with code/scripts/sync_from_all.sh).
`just build` also installs nats-server; on the Mac Studio, `just render-setup` then `just render-test`. Builds use `GOWORK=off` because the
parent `~/BGit/act3/go.work` would otherwise capture this module. See `pm/TO_DO.md` and
`pm/Decisions.yaml`.
