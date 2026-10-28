"""Every failure is classified once (spec §12.2); only retryable kinds go back on the queue (pm/task_queue.mdx §16.2),
with the service's backoff, up to max_attempts; exhausted retries are dead-lettered."""

from lib.base import ServerTestCase


class RetriesAndErrors(ServerTestCase):
    def test_transient_failure_retries_then_succeeds(self):
        j = self.run_job("sys.compute", {"op": "flaky", "fail_before_attempt": 3, "kind": "transient"}, max_attempts=4, timeout=30)
        self.assertEqual((j["attempt"] - 1), 2)
        self.assertEqual(j["result"]["succeeded_on_attempt"], 3)
        retries = [h for h in j["history"] if h["state"] == "QUEUED" and "requeued" in h.get("note", "")]
        self.assertEqual(len(retries), 2)
        self.assertIn("transient", retries[0]["note"])
        claims = [h["attempt"] for h in j["history"] if h["state"] == "CLAIMED"]
        self.assertEqual(claims, [1, 2, 3])  # each attempt is a fresh broker delivery
        evs = self.api.events(j["job_id"])
        requeued = [e for e in evs if e["type"] == "requeued"]
        self.assertEqual([e["kind"] for e in requeued], ["transient", "transient"])
        self.assertTrue(all(e.get("next_attempt_after") for e in requeued))

    def test_retries_run_out(self):
        j = self.run_job("sys.compute", {"op": "flaky", "fail_before_attempt": 9, "kind": "infra"}, max_attempts=2,
                         expect="FAILED", timeout=30)
        self.assertEqual(((j["attempt"] - 1), j["error_kind"]), (1, "infra"))
        self.assertIn("attempt 2", j["error_message"])

    def test_non_retryable_kinds(self):
        for kind in ("invalid_input", "content_policy"):
            j = self.run_job("sys.compute", {"op": "fail", "kind": kind, "message": "nope"}, max_attempts=6, expect="FAILED")
            self.assertEqual((j["error_kind"], (j["attempt"] - 1)), (kind, 0), kind)

    def test_timeout(self):
        j = self.run_job("sys.compute", {"op": "sleep", "ms": 5000, "steps": 50}, timeout_seconds=1, max_attempts=1, expect="FAILED")
        self.assertEqual(j["error_kind"], "timeout")
        self.assertEqual(j["error_stage"], "RUNNING")
        self.assertLess(j["progress"], 0.5)

    def test_runner_panic_is_contained(self):
        j = self.run_job("sys.compute", {"op": "panic"}, max_attempts=1, expect="FAILED")
        self.assertEqual(j["error_kind"], "internal")
        self.assertIn("panicked", j["error_message"])
        self.assertIn("panic", self.api.log(j["job_id"]))
        self.assertTrue(self.api.health()["ok"])  # the server survived
        self.run_job("sys.compute", {"op": "sleep", "ms": 5})

    def test_deadline_in_the_past(self):
        j = self.run_job("sys.compute", {"op": "sleep", "ms": 5}, deadline="2020-01-01T00:00:00Z", expect="FAILED")
        self.assertEqual(j["error_kind"], "timeout")
        self.assertIn("deadline", j["error_message"])
