# ai_processing_server — test suite

Black-box tests for the processing server. They start the **real compiled binary** and a **real `nats-server`**
(the task queue broker, as its own process, the way production runs it), send jobs over the local HTTP API, watch
the jobs while they run, and then **check the answers**: every calculation the server does is recomputed
independently in Python, and every video it writes is inspected with `ffprobe`.

Every job goes the whole way through the task queue (pm/task_queue.mdx): stored on `SUBMIT` → admitted to a band on
`WORK` → pulled by the worker when a slot is free → run in a **detached task host** from a frozen code snapshot →
events on `evt.local.<job_id>`.

Go unit tests are separate. They live next to the code as `*_test.go` (see [Unit_Test/README.md](Unit_Test/README.md)).
They include the queue contract run against both the in-memory queue and a real `nats-server`.

## Run it

```bash
cd ~/BGit/act3/ai_processing_server/tests
just                      # build the binary if any Go or data/ file changed, then run every suite
just quick                # every suite except the stress burst
just suite queue render   # only some suites (directory names)
just only cancel          # only tests whose id contains "cancel"
just results              # print the last run's report
```

Without `just`: `python3 run_tests.py [suites...] [-k text] [--no-build]`.
From the repo root, `just test` runs the Go unit tests and then this suite.

A full run is 86 tests in about 3 minutes on the Bryan laptop (M4 Max). The `continuity` suite restarts controllers
and brokers on purpose and is most of that time. Each run writes `Test_Results/<date_time>_results.md`,
`Test_Results/latest.md` and `Test_Results/latest.json`. These files are git-ignored.

**Requirements:** Python 3.11 or newer, standard library only (nothing to `pip install`); Go to build the binary;
and `nats-server` 2.10 or newer, found on `PATH`, at `~/.act3_processing/nats/bin/nats-server` (after
`just broker-install`), or named by `$NATS_SERVER`. The tests start their own `nats-server` processes on free ports
and never use the machine's broker service on 4244. `ffmpeg` and `ffprobe` are optional: without them the video tests
are **skipped** (reported as `skip`), not failed.

## Why Python

* The server is Go. The tests should not be Go, so they cannot share a bug with it: the reference calculations in
  [lib/reference.py](lib/reference.py) are written separately, in a different language.
* Python's standard library already has everything a black-box API test needs: an HTTP client, a fake HTTP
  object store (`http.server`), subprocess control, hashing and PNG header parsing. There are no dependencies to
  install on a GPU box.
* The same tests run unchanged on macOS, Linux and Windows machines in the fleet.

## How it works

```
run_tests.py ── builds ../ai_processing_server if stale, discovers tests in the suite directories, writes Test_Results/
   │
   └─ each test class (lib/base.py ServerTestCase)
        ├─ ServerProcess (lib/harness.py)    starts `ai_processing_server run` (the controller) with its OWN temp
        │                                    state dir, this_server.yaml, token, log dir, error dir and a free port
        ├─ Broker        (lib/harness.py)    its own `nats-server -js` on another free port; it outlives controller
        │                                    restarts, exactly like the OS service in production
        ├─ FakeStorage   (lib/storage.py)    an HTTP server standing in for S3: serves inputs, receives PUT uploads,
        │                                    can inject 503s
        ├─ Client        (lib/client.py)     the local API: submit, job, events, wait (polls), cancel, log, outputs
        └─ reference     (lib/reference.py)  independent primes, sha256 chains, matrices, file stats, Mandelbrot,
                                             PNG size, ffprobe
```

Tests never touch the machine's real `data/git_ignore/this_server.yaml`, `~/_act3_logs/`, `~/T/`, port 9440 or the
broker on 4244, so they can run while a real server is running.

Running jobs live in detached task hosts that outlive the controller on purpose. Cleanup kills them by the pids in
their run directories, and a task host also stops by itself when its run directory disappears. The temporary
directories are deleted after each class; set `KEEP_TEST_DIRS=1` to keep them for debugging.

The test `this_server.yaml` is written as JSON (JSON is valid YAML). It declares 8 CPU cores and 16 GB RAM. The
server offers `min(detected, declared)`, so the results are the same on any machine with at least that much. It sets
`continuity.ack_wait: 8s` and `heartbeat: 1s` (production: 5m and 20s) so redelivery tests take seconds.

## The suites

| Directory | File | What it proves |
| --- | --- | --- |
| `api/` | `test_health_and_auth.py` | health without a token (with the broker's state); every other route needs the bearer token; server info (queue role, NATS transport), hardware with min(detected, declared) and drift; JSON 404s; `/metrics`; the status page |
| `api/` | `test_capabilities_and_validation.py` | offers carry reasons (off, runner not built, not listed); the owner's fence refuses an off service (409); unknown service, bad JSON, unknown fields, duplicate ids; bad params fail fast without retry; priority clamp; the task-queue job record (envelope, `job_state` fields, event seq) and `/v1/queues` |
| `compute/` | `test_compute_results.py` | primes to 2,000,000 (count 148,933), sha256 chains, integer matrix products: every answer recomputed in Python; the kept `result.json` matches; the state history SUBMITTED → QUEUED → CLAIMED → STAGING → RUNNING → UPLOADING → SUCCEEDED; the caller's events on the broker, numbered 1..n, ending in `succeeded` with the result |
| `compute/` | `test_inputs_and_uploads.py` | inputs downloaded over HTTP and checked by sha256; mismatches and 404s are `invalid_input`; a 503 is retried inside the stage; local paths for the `local` caller; outputs PUT to presigned-style URLs; **signatures never appear in the API or job logs** |
| `render/` | `test_mandelbrot_render.py` | rendered frames match a Python Mandelbrot **exactly** (iteration sums per frame); real PNGs of the right size; H.264 / yuv420p MP4 with every frame (ffprobe) |
| `render/` | `test_ffmpeg_jobs.py` | MPEG-4 → H.264 + AAC transcode with scaling; `wtc.transcode` (never AV1); `act3.video_merge` concatenates in order (durations add up); bad inputs are `invalid_input` |
| `queue/` | `test_parallel_slots.py` | at most `max_parallel` per service and `max_parallel_jobs` per machine, measured live by sampling the API; the wall time proves the jobs really overlapped; slots are released; `BUSY` state |
| `queue/` | `test_priority_order.py` | priority → band at every threshold (`data/queue/bands.yaml`); more urgent bands are pulled first; first in, first out inside a band; the owner's `local_bias` orders services inside a band |
| `queue/` | `test_cancel.py` | cancelling a running job stops it mid-work, uploads nothing and frees the slot; waiting jobs cancel without starting; 409 and 404 cases |
| `queue/` | `test_retries_and_errors.py` | transient failures go back on the queue with the service's backoff (`requeued` events) and succeed on a later delivery; exhausted retries are dead-lettered ("gave up after N attempts"); `invalid_input` and `content_policy` never retry; timeouts; a runner panic becomes `internal`; deadlines |
| `queue/` | `test_pause_and_drain.py` | pausing one service holds its jobs in the broker while others run; drain lets running work finish and holds new work; resume |
| `lifecycle/` | `test_progress_while_running.py` | while a job runs, the API shows RUNNING, the runner step, rising progress (5 or more distinct values, never backwards), notes, and a growing per-job log, and other API calls keep answering |
| `lifecycle/` | `test_restart_and_recovery.py` | `kill -9` of the controller mid-job: the task host keeps running, the next controller re-adopts it, and the job finishes **once, on attempt 1, with no gap in its events**; SIGTERM leaves the job running (the runner finishes and waits at the fence until a controller is back); a killed task host is requeued as `infra`; log lines go to `processing_server.info` / `.err` |
| `lifecycle/` | `test_config_and_init.py` | `init` creates `this_server.yaml` once (mode 0600) and never changes a byte after (A1, A2); `--write-new`; legacy files untouched; `CHANGE_ME` and a file copied from another machine stop `run`; reload is all-or-nothing; direct mode can be turned off |
| `lifecycle/` | `test_error_log.py` | failures that should not happen reach `error.err` (from the controller and from task hosts) with full source paths and stacks; expected failures stay out |
| `continuity/` | `test_continuity.py` | pm/task_queue.mdx §21.2: a **restart storm** (six controller restarts, SIGTERM and SIGKILL, during one job); the controller away **longer than the ack wait** (the broker redelivers; the running task host is kept, not doubled); a **broker restart** mid-job; a **new build replacing the binary** under a running job (it finishes with its old `code_version`, the next job runs on the new one); **no broker** (`broker_missing`, 503, the fix command, then it connects by itself) |
| `stress/` | `test_many_jobs.py` | 150 mixed jobs (set `APS_STRESS_JOBS`) at random priorities, every answer verified; the counters add up and nothing is left on `WORK` |

[Test_cases.csv](Test_cases.csv) lists every test with the spec section it covers.

## The API the tests call

All under the server's local API (spec §13). The task system is NATS JetStream (pm/task_queue.mdx, D-Q1). The job
record passes its names through with no renames: the envelope (`data/schemas/task_envelope.v1.schema.json`), the
`job_state` fields and states (§13.4), and the events (`data/schemas/task_event.v1.schema.json`).

| Call | Used for |
| --- | --- |
| `POST /v1/jobs` | submit as caller `local`: the envelope fields `{service, params, inputs[], outputs[], priority, max_attempts, timeout_seconds, deadline, job_id}`; the server fills `envelope_version`, `caller`, `category`, `issued_at`, `expires_at`, `reply` and stores it on SUBMIT |
| `GET /v1/jobs/{id}` | `state` (`SUBMITTED`, `QUEUED`, `CLAIMED`, `STAGING`, `RUNNING`, `UPLOADING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, `EXPIRED`, `REJECTED`), `band`, `attempt`, `step`, `progress`, `note`, `error_kind`, `error_message`, `result`, `outputs`, `code_version`, `events_seq`, `envelope`, `history` |
| `GET /v1/jobs/{id}/events` | the job's events as its caller reads them from the broker (`evt.local.<job_id>`) |
| `GET /v1/jobs?state=&service=&caller=` | lists |
| `GET /v1/jobs/{id}/log?tail=N` | the per-job log while it runs (written by the task host) |
| `GET /v1/jobs/{id}/outputs/{name}` | download an output kept on the server |
| `POST /v1/jobs/{id}/cancel` | cancel (sets `job_control` in the broker) |
| `GET /v1/capabilities` · `/v1/services` · `/v1/queues` · `/v1/server` · `/v1/hardware` · `/v1/health` · `/metrics` | what the server offers and is doing; `/v1/queues` shows WORK depth per band × caller × service |
| `POST /v1/services/{id}/pause` · `/resume` · `POST /v1/drain` · `/v1/resume` · `/v1/reload` | owner controls |

## Adding a test

1. Pick the suite directory (or make a new one, add an empty `__init__.py`, and add its name to `SUITES` in
   `run_tests.py`).
2. Subclass `lib.base.ServerTestCase`. Set `CONFIG` (merged into the test `this_server.yaml`), `SERVICES`
   (per-service settings such as `{"sys.compute": {"max_parallel": 2}}`) and `STORAGE = True` if you need inputs or
   uploads. Tests that restart the controller make their own `ServerProcess` (see `continuity/`).
3. Use `self.run_job(service, params, ...)` to submit and wait, then **check the result against an independent
   calculation** in `lib/reference.py`, not just the status.
4. Add a row to `Test_cases.csv`.

## Troubleshooting

* `... not found: run just build` — build the binary from the repo root.
* `nats-server not found` — `brew install nats-server`, or `just broker-install` from the repo root, or set
  `NATS_SERVER=/path/to/nats-server`.
* A test fails with the job log printed under `--- job log ---`: that log is the task host's per-job log.
* `KEEP_TEST_DIRS=1 just only <name>` keeps the temp dirs. The path is in `/tmp/aps_test_*`; look at
  `<ClassName>.stdout.log`, `logs/processing_server.err`, `state/state/jobs/*.json`, `state/runs/<job_id>/`
  (`run.json`, `events.jsonl`, `taskhost.log`, `result.json`) and `broker/nats.log`.
* The parallel and priority tests measure timing with margins of a few hundred milliseconds. On a heavily loaded
  machine run them alone (`just suite queue`).
