"""The compute runner does real work: every answer is recomputed independently in Python and compared."""

import json

from lib import reference
from lib.base import ServerTestCase


class ComputeResults(ServerTestCase):
    def test_primes_two_million(self):
        j = self.run_job("sys.compute", {"op": "primes", "limit": 2_000_000, "segments": 16})
        expect = reference.primes(2_000_000)
        r = j["result"]
        self.assertEqual((r["count"], r["sum"], r["largest"]), (expect["count"], expect["sum"], expect["largest"]))
        self.assertEqual(r["count"], 148933)  # pi(2,000,000), a known constant
        body = self.assertOutputIntact(j, "result")
        self.assertEqual(json.loads(body), r)  # the kept result.json is the same answer

    def test_primes_segment_count_does_not_change_answer(self):
        a = self.run_job("sys.compute", {"op": "primes", "limit": 99_991, "segments": 1})["result"]
        b = self.run_job("sys.compute", {"op": "primes", "limit": 99_991, "segments": 37})["result"]
        self.assertEqual((a["count"], a["sum"]), (b["count"], b["sum"]))
        self.assertEqual(a["largest"], 99_991)  # the limit itself is prime

    def test_sha256_chain(self):
        j = self.run_job("sys.compute", {"op": "sha256_chain", "seed": "ACT3 render queue", "iterations": 250_000})
        self.assertEqual(j["result"]["digest"], reference.sha256_chain("ACT3 render queue", 250_000))

    def test_matrix_product(self):
        for n, seed in ((1, 7), (17, 1), (64, 12345)):
            j = self.run_job("sys.compute", {"op": "matrix", "n": n, "matrix_seed": seed})
            want = reference.matrix(n, seed)
            got = {k: j["result"][k] for k in want}
            self.assertEqual(got, want, f"n={n} seed={seed}")

    def test_self_test_service(self):
        j = self.run_job("sys.self_test", {"op": "primes", "limit": 100})
        self.assertEqual(j["result"]["count"], 25)
        self.assertEqual(j["platform_status"], "COMPLETED")
        self.assertEqual(j["attempt"], 1)
        self.assertEqual(j["progress"], 1)

    def test_history_records_every_state(self):
        """The record walks the task queue lifecycle (pm/task_queue.mdx §13.4), in order, once each."""
        j = self.run_job("sys.compute", {"op": "sleep", "ms": 50})
        states = [h["state"] for h in j["history"]]
        self.assertEqual(states, ["SUBMITTED", "QUEUED", "CLAIMED", "STAGING", "RUNNING", "UPLOADING", "SUCCEEDED"])
        self.assertIsNotNone(j["started_at"])
        self.assertIsNotNone(j["finished_at"])
        self.assertEqual((j["caller"], j["category"], j["band"]), ("local", "system", "p4"))
        self.assertRegex(j["code_version"], r"^[0-9a-z]+-[0-9a-f]{8}$")

    def test_events_on_the_broker(self):
        """The caller's view: events on evt.local.<job_id>, numbered 1..n, ending with succeeded and the result."""
        j = self.run_job("sys.compute", {"op": "primes", "limit": 1000})
        evs = self.api.events(j["job_id"])
        self.assertEqual([e["seq"] for e in evs], list(range(1, len(evs) + 1)))
        types = [e["type"] for e in evs]
        self.assertEqual(types[0], "admitted")
        self.assertEqual(types[1], "claimed")
        self.assertEqual(types[-1], "succeeded")
        self.assertIn("stage", types)
        final = evs[-1]
        self.assertEqual(final["state"], "SUCCEEDED")
        self.assertEqual(final["result"]["count"], 168)
        self.assertEqual(final["code_version"], j["code_version"])
        self.assertEqual(final["outputs"][0]["sha256"], j["outputs"][0]["sha256"])
        self.assertEqual(evs[-1]["seq"], j["events_seq"])
