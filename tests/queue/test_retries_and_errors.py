"""Every failure is classified once (spec §12.2); only retryable kinds retry, with backoff, up to max_retry."""

from lib.base import ServerTestCase


class RetriesAndErrors(ServerTestCase):
    def test_transient_failure_retries_then_succeeds(self):
        j = self.run_job("sys.compute", {"op": "flaky", "fail_before_attempt": 3, "kind": "transient"}, max_retry=3, timeout=30)
        self.assertEqual(j["retried"], 2)
        self.assertEqual(j["result"]["succeeded_on_attempt"], 3)
        retries = [h for h in j["history"] if h["state"] == "retry"]
        self.assertEqual(len(retries), 2)
        self.assertIn("transient", retries[0]["note"])
        self.assertIn("last_failed_at", j)  # Asynq keeps the last failure on a later success

    def test_retries_run_out(self):
        j = self.run_job("sys.compute", {"op": "flaky", "fail_before_attempt": 9, "kind": "infra"}, max_retry=1,
                         expect="archived", timeout=30)
        self.assertEqual((j["retried"], j["error_kind"]), (1, "infra"))
        self.assertIn("attempt 2", j["last_err"])

    def test_non_retryable_kinds(self):
        for kind in ("invalid_input", "content_policy"):
            j = self.run_job("sys.compute", {"op": "fail", "kind": kind, "message": "nope"}, max_retry=5, expect="archived")
            self.assertEqual((j["error_kind"], j["retried"]), (kind, 0), kind)

    def test_timeout(self):
        j = self.run_job("sys.compute", {"op": "sleep", "ms": 5000, "steps": 50}, timeout_seconds=1, max_retry=0, expect="archived")
        self.assertEqual(j["error_kind"], "timeout")
        self.assertEqual(j["error_stage"], "RUNNING")
        self.assertLess(j["progress"], 0.5)

    def test_runner_panic_is_contained(self):
        j = self.run_job("sys.compute", {"op": "panic"}, max_retry=0, expect="archived")
        self.assertEqual(j["error_kind"], "internal")
        self.assertIn("panicked", j["last_err"])
        self.assertIn("panic", self.api.log(j["id"]))
        self.assertTrue(self.api.health()["ok"])  # the server survived
        self.run_job("sys.compute", {"op": "sleep", "ms": 5})

    def test_deadline_in_the_past(self):
        j = self.run_job("sys.compute", {"op": "sleep", "ms": 5}, deadline="2020-01-01T00:00:00Z", expect="archived")
        self.assertEqual(j["error_kind"], "timeout")
        self.assertIn("deadline", j["last_err"])
