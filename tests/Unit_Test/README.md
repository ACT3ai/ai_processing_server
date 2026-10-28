# Unit_Test

Go unit tests live next to the code they test, as `*_test.go` files under `../../code/internal/`
(spec §20.1). Run them from the repo root with `just test-go`. This directory only holds this note so the
standard PROJECT layout is complete.

| Package | What the unit tests prove |
| --- | --- |
| `config` | `init` creates this_server.yaml once and never changes a byte afterwards; `--write-new`; legacy files untouched; unknown machine → `CHANGE_ME`; validation errors |
| `hardware` | parsers for `nvidia-smi`, `system_profiler`, versions; min(detected, declared) and drift |
| `capability` | offers and reasons for a 5090, a Mac (no CUDA), two 16 GB cards never summed, the owner's fence and caps |
| `scheduler` | slot pool per card, release, CPU accounting |
| `runners` | primes, sha256 chains, matrices, Mandelbrot against known values; cancellation |
| `queue/queuetest`, `queue/natsq` | the queue contract (pm/task_queue.mdx §13.7, acceptance Q8), run on the in-memory queue and on a real `nats-server` process: idempotent submit, one delivery at a time, delayed requeue as the next attempt, a resumed claim acks the same delivery, redelivery after the ack wait, create-only and fenced `job_state`, ordered and deduplicated events, control watch |
| `priority` | priority → band at every threshold, caller ceilings, pull order band → caller → local_bias |
| `snapshot` | the same binary reuses its snapshot, a new build gets its own, garbage collection keeps snapshots in use |
| `runs` | a torn last line of `events.jsonl` is left for the next read; a task host is alive only with a fresh heartbeat from the same pid |
