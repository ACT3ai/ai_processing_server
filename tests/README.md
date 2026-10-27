# ai_processing_server — test suite

Black-box tests for the processing server. They start the **real compiled binary**, send it jobs over its local
HTTP API, watch the jobs while they run, and then **check the answers**: every calculation the server does is
recomputed independently in Python, and every video it writes is inspected with `ffprobe`.

Go unit tests are separate. They live next to the code as `*_test.go` (see [Unit_Test/README.md](Unit_Test/README.md)).

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

A full run is 74 tests in about 45 seconds on the Bryan laptop (M4 Max). Each run writes
`Test_Results/<date_time>_results.md`, `Test_Results/latest.md` and `Test_Results/latest.json`. These files are
git-ignored.

**Requirements:** Python 3.11 or newer, standard library only (nothing to `pip install`), plus Go to build the
binary. `ffmpeg` and `ffprobe` are optional: without them the video tests are **skipped** (reported as `skip`), not
failed.

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
        ├─ ServerProcess (lib/harness.py)    starts `ai_processing_server run` with its OWN temp state dir,
        │                                    this_server.yaml, token, log dir and a free 127.0.0.1 port
        ├─ FakeStorage   (lib/storage.py)    an HTTP server standing in for S3: serves inputs, receives PUT uploads,
        │                                    can inject 503s
        ├─ Client        (lib/client.py)     the local API: submit, job, wait (polls), cancel, log, output download
        └─ reference     (lib/reference.py)  independent primes, sha256 chains, matrices, file stats, Mandelbrot,
                                             PNG size, ffprobe
```

Tests never touch the machine's real `data/git_ignore/this_server.yaml`, `~/_act3_logs/`, or port 9440, so they
can run while a real server is running. The temporary directories are deleted after each class. Set
`KEEP_TEST_DIRS=1` to keep them for debugging: each one holds the server's stdout, logs, journal and results.

The test `this_server.yaml` is written as JSON (JSON is valid YAML). It declares 8 CPU cores and 16 GB RAM. The
server offers `min(detected, declared)`, so the results are the same on any machine with at least that much.

## The suites

| Directory | File | What it proves |
| --- | --- | --- |
| `api/` | `test_health_and_auth.py` | health without a token; every other route needs the bearer token; server info, hardware with min(detected, declared) and drift; JSON 404s; `/metrics`; the status page |
| `api/` | `test_capabilities_and_validation.py` | offers carry reasons (off, runner not built, not listed); the owner's fence refuses an off service (409); unknown service, bad JSON, unknown fields, duplicate ids; bad params fail fast without retry; priority clamp; the Asynq-style job record and queue counters |
| `compute/` | `test_compute_results.py` | primes to 2,000,000 (count 148,933), sha256 chains, integer matrix products: every answer recomputed in Python; the kept `result.json` matches; the full stage history |
| `compute/` | `test_inputs_and_uploads.py` | inputs downloaded over HTTP and checked by sha256; mismatches and 404s are `invalid_input`; a 503 is retried inside the stage; local paths in direct mode; outputs PUT to presigned-style URLs; **signatures never appear in the API or job logs** |
| `render/` | `test_mandelbrot_render.py` | rendered frames match a Python Mandelbrot **exactly** (iteration sums per frame); real PNGs of the right size; H.264 / yuv420p MP4 with every frame (ffprobe) |
| `render/` | `test_ffmpeg_jobs.py` | MPEG-4 → H.264 + AAC transcode with scaling; `wtc.transcode` (never AV1); `act3.video_merge` concatenates in order (durations add up); bad inputs are `invalid_input` |
| `queue/` | `test_parallel_slots.py` | at most `max_parallel` per service and `max_parallel_jobs` per machine, measured live by sampling the API; the wall time proves the jobs really overlapped; slots are released; `BUSY` state |
| `queue/` | `test_priority_order.py` | waiting jobs start in effective-priority order; ties go to the oldest; the owner's `local_bias` counts |
| `queue/` | `test_cancel.py` | cancelling a running job stops it mid-work, uploads nothing and frees the slot; waiting jobs cancel without starting; 409 and 404 cases |
| `queue/` | `test_retries_and_errors.py` | transient failures retry with backoff and then succeed; retries run out; `invalid_input` and `content_policy` never retry; timeouts; a runner panic becomes `internal` and the server survives; deadlines |
| `queue/` | `test_pause_and_drain.py` | pausing one service holds its jobs while others run; drain lets running work finish and holds new work; resume |
| `lifecycle/` | `test_progress_while_running.py` | while a job runs, the API shows the RUNNING stage, the runner step, rising progress (5 or more distinct values, never backwards), notes, and a growing per-job log, and other API calls keep answering |
| `lifecycle/` | `test_restart_and_recovery.py` | `kill -9` mid-job: after restart the job is `archived` / `infra` "server restarted during RUNNING" and its work dir is gone (acceptance A7); SIGTERM drains and the running job completes; the drain timeout stops jobs as `infra`; log lines go to `processing_server.info` / `.err` in the ACT3 format |
| `lifecycle/` | `test_config_and_init.py` | `init` creates `this_server.yaml` once (mode 0600) and never changes a byte after (A1, A2); `--write-new`; legacy files untouched; `CHANGE_ME` and a file copied from another machine stop `run`; reload is all-or-nothing; direct mode can be turned off |
| `stress/` | `test_many_jobs.py` | 150 mixed jobs (set `APS_STRESS_JOBS`) at random priorities, every answer verified; the queue and Prometheus counters add up |

[Test_cases.csv](Test_cases.csv) lists every test with the spec section it covers.

## The API the tests call

All under the server's local API (spec §13). The job record mirrors Asynq's `TaskInfo` (see
`code/internal/jobs/job.go`), so a real task system can sit behind the server later with few or no renames.

| Call | Used for |
| --- | --- |
| `POST /v1/jobs` | submit (direct mode): `{service, params, inputs[], outputs[], priority, max_retry, timeout_seconds, deadline, job_id}` |
| `GET /v1/jobs/{id}` | state (`pending`, `active`, `retry`, `completed`, `archived`, `cancelled`), `stage`, `step`, `progress`, `note`, `result`, `outputs`, `history` |
| `GET /v1/jobs?state=&service=&queue=` | lists |
| `GET /v1/jobs/{id}/log?tail=N` | the per-job log while it runs |
| `GET /v1/jobs/{id}/outputs/{name}` | download an output kept on the server |
| `POST /v1/jobs/{id}/cancel` | cancel |
| `GET /v1/capabilities` · `/v1/services` · `/v1/queues` · `/v1/server` · `/v1/hardware` · `/v1/health` · `/metrics` | what the server offers and is doing |
| `POST /v1/services/{id}/pause` · `/resume` · `POST /v1/drain` · `/v1/resume` · `/v1/reload` | owner controls |

## Adding a test

1. Pick the suite directory (or make a new one, add an empty `__init__.py`, and add its name to `SUITES` in
   `run_tests.py`).
2. Subclass `lib.base.ServerTestCase`. Set `CONFIG` (merged into the test `this_server.yaml`), `SERVICES`
   (per-service settings such as `{"sys.compute": {"max_parallel": 2}}`) and `STORAGE = True` if you need inputs or
   uploads.
3. Use `self.run_job(service, params, ...)` to submit and wait, then **check the result against an independent
   calculation** in `lib/reference.py`, not just the status.
4. Add a row to `Test_cases.csv`.

## Troubleshooting

* `... not found: run just build` — build the binary from the repo root.
* A test fails with the job log printed under `--- job log ---`: that log is the server's own per-job log.
* `KEEP_TEST_DIRS=1 just only <name>` keeps the temp dirs. The path is in `/tmp/aps_test_*`; look at
  `<ClassName>.stdout.log`, `logs/processing_server.err` and `state/state/journal/*.json`.
* The parallel and priority tests measure timing with margins of a few hundred milliseconds. On a heavily loaded
  machine run them alone (`just suite queue`).
