# ai_processing_server — TO DO

**As of 2026-10-28, v0.2.0 + D-025/D-026 resilience (uncommitted).** Phase 0 (scaffold) and most of phases 1–2 of the build plan
(`ai_processing_server.mdx` §21) are done.

## Done 2026-10-28: hardening for broker outages, power cuts, duplicates, pulls and full disks (D-026)
* [x] Settlement outbox: a queued job's outcome goes into its journal entry first and is retried until the broker takes
  it; resumed at boot; a redelivery of a job in the outbox is settled from it, never run again; a final event is never
  published twice (lost-reply checks). `scheduler/queue.go` settleLocked, runSettlement, settleOnce, handToSettlement.
* [x] natsq: a failed Ack no longer marks the delivery done (retries were silent no-ops). `natsq.go` delivery.send.
* [x] Boot reclaim keeps a claim until the broker answers for it (a blip used to drop it and the sweep then cancelled
  its render). `scheduler/queue.go` reclaimAtBoot, reclaimOne.
* [x] A stop signal (`just update`) leaves queued task-host jobs to the next controller at once instead of waiting out
  the drain timeout and requeueing them while they kept rendering. `scheduler/server.go` detachForRestart, leaveLocked.
* [x] Durable control files (journal, task.json, checkpoints, exit.json, snapshot manifests): `internal/fsx`.
* [x] Task host liveness by boot id and pid (clock jumps, pid reuse), StaleHard for a stalled live host; checkpoints with
  fsync + SHA-256; at most 3 task host deaths per job per machine. `taskhost.go` Alive, writeCheckpoint, RecordDeath.
* [x] Output validated with ffprobe (frame count for segmented upscales); ENOSPC is infra. `runners/oss.go` validateVideo.
* [x] Snapshots never mix two commits under a concurrent pull; fsynced; verified before reuse; safe Prune. `snapshot.go`.
* [x] Disk low measured on the state dir too; waiting jobs say why they are parked. `scheduler/server.go` diskReasonLocked.
* [x] nats-server templates: `jetstream { sync_interval: always }` (run `just broker-config` and restart the broker on
  each machine to pick it up).
* [ ] Still open from this audit (D-026 notes):
  * Returning a job without running it (service not offered, model refused on a fleet worker) uses up one of its
    attempts, so a model only one server allows can reach DEAD after enough refusals. Needs a "returns" count in
    job_state that acceptDelivery subtracts.
  * A hard nats-server kill loses the claim's delivery state (nats-server writes consumer state asynchronously): the
    running job can be handed out again. Standalone: it comes back to the same server, which keeps running it. Fleet:
    the hub cluster's replicas keep it; fencing guarantees one result either way. Verify on the hub before go-live.
  * A direct-mode (not queued) task-host job is still failed at boot and its task host cancelled (D-025 rule).
  * pm/Decisions.yaml does not parse as YAML (D-003 `status:` holds an unquoted colon); fix by quoting it.

## Done in v0.2.0 (2026-10-27): the task queue, NATS JetStream (D-014 to D-017)
* `queue` seam, `queue/natsq` (client of a separate nats-server), `queue/queuetest` (in-memory queue + contract suite).
* Scheduler queue link: strict band pull order, claim by CAS on `job_state` (fencing), events, requeue with backoff,
  `DEAD`, cancel through `job_control`, `+WPI` heartbeat, `servers` record, claims saved in the journal and settled at
  boot (re-attach through `runners.Reattacher`, else requeue at once).
* Config `queue` / `continuity` / `callers` (off when the section is absent); template writes it on for new files.
* Local API: `POST /v1/queue/jobs`, `GET /v1/queue/jobs/{id}`, `POST /v1/queue/jobs/{id}/cancel`, `GET /v1/broker`;
  `broker` object in `/v1/health` (`broker_missing` + fix).
* `brokerctl` + `ai_processing_server broker status|config`; `just broker-install|config|run|stop|status`;
  `data/queue/broker.yaml`, `data/queue/nats/{standalone,worker}.conf.tmpl`.
* Tests: 14 Go test functions (12-case contract on both queues, real nats-server), 14 black-box tests in `tests/broker/`;
  93/93 black-box and all Go packages pass.

## Done in v0.1.0
* Go module, single binary, `justfile` (build, run, run-fg, stop, status, logs, init, reinit, detect, validate, test, cross).
* `init`: creates `data/git_ignore/this_server.yaml` once, never overwrites; `--write-new`; legacy files left alone.
* Hardware detection on macOS, Linux and Windows (CPU, RAM, disk, NVIDIA via nvidia-smi, Apple GPU, engines, ffmpeg encoders).
* Capability offers with reasons; min(detected, declared); drift.
* Scheduler: slot pool (per GPU card), effective priority with owner bias and aging, retries with backoff, cancel,
  owner pause, drain, reload, journal and crash recovery, sweeper.
* Runners: `compute` (sys.compute, sys.self_test), `render` (sys.render_test), `ffmpeg` (act3.transcode, wtc.transcode, act3.video_merge).
* Local API (§13) including direct mode, `/metrics`, the status page.
* Go unit tests and the black-box suite in `tests/` (79 tests).
* `error.err` (pm/errors.mdx): `internal/errlog` + `internal/redact`; every package upgraded; panics recorded with
  full-path stacks; the scheduler loop survives a panicking tick; `procs` no longer hangs on a >1 MB output line.
* Security building blocks (not wired into a transport yet): `internal/security/keys` (Ed25519 identity key,
  create-only, JWK Set, RFC 7638 kid, fingerprint) and `internal/security/httpsig` (RFC 9421 sign/verify of
  requests and responses, Content-Digest, replay cache), tested against RFC 9421 B.2.6 and RFC 8037 A.3.

## Next
* [x] Bryan: D-001 and §23 answered (D-014, D-022). Real hostnames get fixed in known_servers.yaml after each machine's first `init`.
* [x] Laptop (BLAP): generated config promoted to `data/git_ignore/this_server.yaml` on 2026-10-27; the hand-written probe is kept as `this_server.legacy_probe_2026_10_27.yaml` (nothing in it needed merging).
* [x] Wire `keys.InitKey` into `init` (write `servers/{Server}/keys/server_identity.jwks.json`), add `keys show`
  (done 2026-10-27; BMS key in `act3ai-credentials/keys/ai_processing_server/BMS/`, security.mdx §4, §5.1).
* [ ] Other machines (BLAP, B5090, …): `git pull` + `just build`, add `servers_dir` and `security.key_dir` to their
  existing `this_server.yaml` by hand first (init never rewrites it), then commit `servers/{Server}/keys/`.
* [ ] `data/platforms/{act3,wtc}.yaml` + pinned `.jwks.json` (security.mdx §8.1); `keys rotate`, `enroll` (§5.2, §10).
* [ ] Retire `platforms.*.credentials_file` in the template (security.mdx §14: signed requests need no token).
* [ ] SSRF guard and `SafeJoin` for platform jobs (`storage/safedial.go`, `storage/paths.go`, security.mdx §12.2–12.3);
  direct mode keeps loopback inputs allowed (the test FakeStorage is on 127.0.0.1).
* [x] Bryan: D-001 vs D-Q1. Decided 2026-10-27: NATS JetStream as a separate nats-server process (D-014); D-001 superseded.
* [x] `queue/natsq`, the scheduler's queue link and `just broker-*` (v0.2.0, task_queue.mdx §22.1).
* [x] `nats-server` as an OS service: `just install-service` installs it beside the server (2026-10-28).
* [ ] Restart-storm and code-change tests (task_queue.mdx §21.2) once task hosts and snapshots land.
* [ ] Q2: admission (signatures, caller policy, categories, models), DRR over bands and caller shares, aging (D-015).
* [x] All open questions in pm/ decided 2026-10-27 (D-020 to D-023).
* [x] Caller band ceilings, vendor refusal, local modes in the pull order, owner model fence, NPL1–3 presets (D-021).
* [ ] Run `init` on BMS, B5090 and NPL1–3; correct their hostnames in known_servers.yaml.
* [x] BMT (`Bryans-Mac-Pro-2024`) is Bryan's dev machine, not a render server: preset `dev: true` (D-024, 2026-10-28).
* [ ] FlashVSR (CUDA preview) and Real-ESRGAN (low VRAM) upscalers after SeedVR2 (D-020).
* [ ] Caller shares and DRR (build phase Q2) using the decided shares (D-022 §23 Q3).
* [ ] `data/services/act3.upscale_open_source.yaml` and its runner: the first ACT3 task type (task_authoring.mdx §4, §9).
* [ ] Continuity gate G1–G7 as integration tests, run per task type before it goes live (task_authoring.mdx §8).
* [ ] Task continuity (task_queue.mdx §9): detached task hosts, frozen code snapshots, re-adoption after restart, so `git pull` + restart never kills a running render. The v0.1 in-process runners do not survive a restart yet.
* [ ] ACT3 / WTC submit through NATS (task_queue.mdx §20); `broker/httplease` is dropped (D-001 superseded).
* [ ] Phase 4: runners `comfy`, `python`, `whisper`, `blender`; model cache; profiles b5090, bpc, bms.
* [ ] JSON Schemas in `data/schemas/` for params (today each runner validates its params in Go).
* [ ] `data/sites/sites.yaml` and the customer-data rule (§16.3).
* [ ] GPU telemetry (nvidia-smi load, temperature) and the DEGRADED thermal guard (§12.4).
* [x] `just install-service` (launchd agents, systemd user units, Task Scheduler); refused on a dev machine (2026-10-28).
* [x] macOS LaunchAgents are `ai.act3.processing_server` + `ai.act3.nats-server` (AbandonProcessGroup on the server); every `just build` installs them (`_service-ensure`, skipped with APS_NO_SERVICE=1 and refused on a dev machine); `just run` / `just stop` / `just broker-run` / `just broker-stop` load and unload them (2026-11-02 merge: one Go implementation, origin's labels).
* [ ] Run `just install-service` on BMS, B5090, BPC and NPL1–3, and turn on automatic login (macOS) or `loginctl enable-linger` (Linux), then do one real power cycle (G4 by hand).
* [ ] Systemd user unit and Windows Task Scheduler paths are written but not yet tried on a real Linux / Windows render box; `just build` only auto-installs on macOS.
