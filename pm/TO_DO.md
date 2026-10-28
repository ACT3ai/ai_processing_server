# ai_processing_server — TO DO

**As of 2026-10-27, v0.2.0.** Phase 0 (scaffold) and most of phases 1–2 of the build plan
(`ai_processing_server.mdx` §21) are done.

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
* [ ] `nats-server` as an OS service (launchd, systemd, Windows service) in `just broker-install`; today `just broker-run`
  starts it in the background.
* [ ] Restart-storm and code-change tests (task_queue.mdx §21.2) once task hosts and snapshots land.
* [ ] Q2: admission (signatures, caller policy, categories, models), DRR over bands and caller shares, aging (D-015).
* [x] All open questions in pm/ decided 2026-10-27 (D-020 to D-023).
* [x] Caller band ceilings, vendor refusal, local modes in the pull order, owner model fence, NPL1–3 presets (D-021).
* [ ] Run `init` on BMS, B5090 and NPL1–3; correct their hostnames in known_servers.yaml (and add `Bryans-Mac-Pro-2024` to BMT if that is BMT).
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
* [x] `just install-service` on macOS: LaunchAgents `ai.act3.processing_server` + `ai.act3.nats-server`, run by every `just build`.
* [ ] `just install-service` for Windows service and systemd user unit.
