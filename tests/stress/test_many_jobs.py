"""A burst of mixed jobs: every one completes, every answer is right, and the counters add up."""

import os
import random
import time

from lib import reference
from lib.base import ServerTestCase

N = int(os.environ.get("APS_STRESS_JOBS", "150"))


class ManyJobs(ServerTestCase):
    CONFIG = {"limits": {"max_parallel_jobs": 6}}
    SERVICES = {"sys.compute": {"max_parallel": 6}}

    def test_burst(self):
        rnd = random.Random(2026)
        expected = {}
        t0 = time.time()
        for i in range(N):
            kind = rnd.choice(["primes", "sha", "matrix"])
            if kind == "primes":
                limit = rnd.randint(1_000, 300_000)
                j = self.api.submit("sys.compute", {"op": "primes", "limit": limit, "segments": rnd.randint(1, 8)}, priority=rnd.randint(0, 100))
                expected[j["id"]] = ("count", reference.primes(limit)["count"])
            elif kind == "sha":
                seed, it = f"seed-{i}", rnd.randint(1, 20_000)
                j = self.api.submit("sys.compute", {"op": "sha256_chain", "seed": seed, "iterations": it}, priority=rnd.randint(0, 100))
                expected[j["id"]] = ("digest", reference.sha256_chain(seed, it))
            else:
                n, seed = rnd.randint(1, 30), rnd.randint(0, 2**31 - 1)
                j = self.api.submit("sys.compute", {"op": "matrix", "n": n, "matrix_seed": seed}, priority=rnd.randint(0, 100))
                expected[j["id"]] = ("trace", reference.matrix(n, seed)["trace"])
        for job_id, (field, want) in expected.items():
            done = self.api.wait(job_id, timeout=120, interval=0.1)
            self.assertEqual(done["state"], "completed", done.get("last_err"))
            self.assertEqual(done["result"][field], want, job_id)
        elapsed = time.time() - t0
        q = next(x for x in self.api.get("/v1/queues")["types"] if x["queue"] == "sys.compute")
        self.assertEqual(q["completed"], N)
        self.assertEqual(q["size"], 0)
        metrics = self.api.request("GET", "/metrics", token=False, raw=True)[0].decode()
        self.assertIn(f'aps_jobs_processed_total{{service="sys.compute"}} {N}', metrics)
        print(f"\n    {N} jobs verified in {elapsed:.1f}s ({N / elapsed:.0f} jobs/s)", end=" ")
