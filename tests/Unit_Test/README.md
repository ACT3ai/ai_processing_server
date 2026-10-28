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
