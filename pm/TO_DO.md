# ai_processing_server — TO DO

**As of 2026-10-27, v0.1.0.** Phase 0 (scaffold) and most of phases 1–2 of the build plan
(`ai_processing_server.mdx` §21) are done.

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
* [ ] Bryan: confirm D-001 (transport) and answer spec §23 Q2 (real hostnames for B5090, BPC, BMT, BMS).
* [x] Laptop (BLAP): generated config promoted to `data/git_ignore/this_server.yaml` on 2026-10-27; the hand-written probe is kept as `this_server.legacy_probe_2026_10_27.yaml` (nothing in it needed merging).
* [x] Wire `keys.InitKey` into `init` (write `servers/{Server}/keys/server_identity.jwks.json`), add `keys show`
  (done 2026-10-27; BMS key in `act3ai-credentials/keys/ai_processing_server/BMS/`, security.mdx §4, §5.1).
* [ ] Other machines (BLAP, B5090, …): `git pull` + `just build`, add `servers_dir` and `security.key_dir` to their
  existing `this_server.yaml` by hand first (init never rewrites it), then commit `servers/{Server}/keys/`.
* [ ] `data/platforms/{act3,wtc}.yaml` + pinned `.jwks.json` (security.mdx §8.1); `keys rotate`, `enroll` (§5.2, §10).
* [ ] Retire `platforms.*.credentials_file` in the template (security.mdx §14: signed requests need no token).
* [ ] SSRF guard and `SafeJoin` for platform jobs (`storage/safedial.go`, `storage/paths.go`, security.mdx §12.2–12.3);
  direct mode keeps loopback inputs allowed (the test FakeStorage is on 127.0.0.1).
* [ ] Bryan: D-001 (HTTPS lease, apis.mdx) and D-Q1 (NATS JetStream as a separate nats-server service, task_queue.mdx §3) contradict each other; pick one.
* [ ] Task continuity (task_queue.mdx §9): detached task hosts, frozen code snapshots, re-adoption after restart, so `git pull` + restart never kills a running render. The v0.1 in-process runners do not survive a restart yet.
* [ ] Phase 3: `broker/httplease` against appsrv's `/processing/*` endpoints; heartbeat and directives (§14).
* [ ] Phase 4: runners `comfy`, `python`, `whisper`, `blender`; model cache; profiles b5090, bpc, bms.
* [ ] JSON Schemas in `data/schemas/` for params (today each runner validates its params in Go).
* [ ] `data/sites/sites.yaml` and the customer-data rule (§16.3).
* [ ] GPU telemetry (nvidia-smi load, temperature) and the DEGRADED thermal guard (§12.4).
* [ ] `just install-service` (launchd, Windows service, systemd).
