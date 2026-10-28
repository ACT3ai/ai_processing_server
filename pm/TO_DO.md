# ai_processing_server — TO DO

**As of 2026-10-27, v0.1.0 plus task queue phase Q1.** Phase 0 (scaffold) and most of phases 1–2 of the build plan
(`ai_processing_server.mdx` §21) are done, and phase Q1 of `task_queue.mdx` §22 (NATS JetStream, D-Q1).

## Done in v0.1.0
* Go module, single binary, `justfile` (build, run, run-fg, stop, status, logs, init, reinit, detect, validate, test, cross).
* `init`: creates `data/git_ignore/this_server.yaml` once, never overwrites; `--write-new`; legacy files left alone.
* Hardware detection on macOS, Linux and Windows (CPU, RAM, disk, NVIDIA via nvidia-smi, Apple GPU, engines, ffmpeg encoders).
* Capability offers with reasons; min(detected, declared); drift.
* Scheduler: slot pool (per GPU card), effective priority with owner bias and aging, retries with backoff, cancel,
  owner pause, drain, reload, journal and crash recovery, sweeper.
* Runners: `compute` (sys.compute, sys.self_test), `render` (sys.render_test), `ffmpeg` (act3.transcode, wtc.transcode, act3.video_merge).
* Local API (§13) including direct mode, `/metrics`, the status page.
* Go unit tests and the black-box suite in `tests/` (86 tests).
* **Task queue phase Q1** (task_queue.mdx, D-Q1 confirmed by Bryan): `nats-server` as a separate service
  (`just broker-install`, `broker-config`, `broker-status`, `broker-uninstall`; port 4244, D-015); streams and KV from
  `data/queue/streams.yaml`; envelope v1 and events (`data/schemas/`); standalone admission into bands; pull only when
  a slot is free; job_state fencing; final event before ack; dead-lettering; frozen code snapshots; detached task
  hosts; re-adoption after a controller restart (`just restart`, `just update`, `just runs`). The queue contract
  passes on the in-memory queue and a real nats-server.
* `error.err` (pm/errors.mdx): `internal/errlog` + `internal/redact`; every package upgraded; panics recorded with
  full-path stacks; the scheduler loop survives a panicking tick; `procs` no longer hangs on a >1 MB output line.
* Security building blocks (not wired into a transport yet): `internal/security/keys` (Ed25519 identity key,
  create-only, JWK Set, RFC 7638 kid, fingerprint) and `internal/security/httpsig` (RFC 9421 sign/verify of
  requests and responses, Content-Digest, replay cache), tested against RFC 9421 B.2.6 and RFC 8037 A.3.

## Next
* [ ] **Bryan: run `just broker-install` on the laptop**, then `just restart`. (The install registers a launchd user
  agent, `~/Library/LaunchAgents/ai.act3.processing.nats.plist`, so it was left for you; `just broker-uninstall`
  removes it.) Until then the BLAP server keeps running the v0.1 build started at 09:17; after a restart without the
  broker it reports `broker_missing` and the fix.
* [ ] Bryan: answer spec §23 Q2 (real hostnames for B5090, BPC, BMT, BMS) and task_queue.mdx §24 Q2 (where the hub runs).
* [x] Laptop (BLAP): generated config promoted to `data/git_ignore/this_server.yaml` on 2026-10-27; the hand-written probe is kept as `this_server.legacy_probe_2026_10_27.yaml` (nothing in it needed merging).
* [ ] Wire `keys.InitKey` into `init` (write `servers/{Server}/keys/server_identity.jwks.json`), add `keys show`,
  and `data/platforms/{act3,wtc}.yaml` + pinned `.jwks.json` (security.mdx §4, §5.1, §8.1).
* [ ] SSRF guard and `SafeJoin` for platform jobs (`storage/safedial.go`, `storage/paths.go`, security.mdx §12.2–12.3);
  direct mode keeps loopback inputs allowed (the test FakeStorage is on 127.0.0.1).
* [x] D-001 vs D-Q1: Bryan picked D-Q1 (2026-10-27).
* [x] Task continuity (task_queue.mdx §9): detached task hosts, frozen code snapshots, re-adoption after restart.
* [ ] Task queue Q2: DRR over bands and callers, aging (promoter), category and model rules, quotas, `callers.local`
  modes `share` and `idle_only` (§14).
* [ ] Task queue Q3: hub and worker roles, leaf nodes, accounts and credentials, signed envelopes (security.mdx), the
  HTTPS gateway (apis.mdx), self-claim mode.
* [ ] TypeScript task hosts (task_queue.mdx §9.3, acceptance Q11) and esbuild bundling into the snapshot.
* [ ] Phase 4: runners `comfy`, `python`, `whisper`, `blender`; model cache; profiles b5090, bpc, bms.
* [ ] JSON Schemas in `data/schemas/` for params (today each runner validates its params in Go).
* [ ] `data/sites/sites.yaml` and the customer-data rule (§16.3).
* [ ] GPU telemetry (nvidia-smi load, temperature) and the DEGRADED thermal guard (§12.4).
* [ ] `just install-service` (launchd, Windows service, systemd).
