"""While a job runs, the API answers with live state: stage, step, rising progress, notes, and a growing log."""

from lib import reference
from lib.base import ServerTestCase


class ProgressWhileRunning(ServerTestCase):
    def test_progress_moves_and_api_stays_responsive(self):
        j = self.api.submit("sys.compute", {"op": "primes", "limit": 1_500_000, "segments": 20, "throttle_ms": 80})
        snaps, logs_mid_run = [], []

        def watch(s):
            snaps.append(s)
            if s["state"] == "RUNNING" and 0.3 < s["progress"] < 0.9 and not logs_mid_run:
                logs_mid_run.append(self.api.log(j["job_id"]))
                # Other API calls keep answering while the job runs.
                self.assertEqual(self.api.get("/v1/server")["active_jobs"], 1)
                self.assertTrue(any(x["job_id"] == j["job_id"] for x in self.api.jobs(state="RUNNING")))

        done = self.api.wait(j["job_id"], timeout=30, interval=0.03, on_poll=watch)
        self.assertEqual(done["state"], "SUCCEEDED")
        running = [s for s in snaps if s["state"] == "RUNNING"]
        self.assertTrue(running, "never saw the RUNNING stage")
        self.assertTrue(all(s["step"] == "compute:primes" for s in running if s.get("step")))
        values = [s["progress"] for s in running]
        self.assertEqual(values, sorted(values), "progress went backwards")
        inside = {v for v in values if 0 < v < 1}
        self.assertGreaterEqual(len(inside), 5, f"too few distinct progress values: {sorted(inside)}")
        self.assertTrue(any("segment" in (s.get("note") or "") for s in running))
        self.assertTrue(logs_mid_run and "progress" in logs_mid_run[0], "log was empty mid-run")
        self.assertEqual(done["progress"], 1)
        self.assertEqual(done["result"]["count"], reference.primes(1_500_000)["count"])

    def test_log_tail(self):
        j = self.run_job("sys.compute", {"op": "primes", "limit": 200_000, "segments": 10})
        full = self.api.log(j["job_id"]).strip().splitlines()
        tail = self.api.log(j["job_id"], tail=3).strip().splitlines()
        self.assertGreater(len(full), 10)
        self.assertEqual(tail, full[-3:])

    def test_list_filters(self):
        a = self.run_job("sys.compute", {"op": "sleep", "ms": 1})
        b = self.run_job("sys.self_test", {"op": "sleep", "ms": 1})
        ids = {x["job_id"] for x in self.api.jobs(service="sys.self_test")}
        self.assertIn(b["job_id"], ids)
        self.assertNotIn(a["job_id"], ids)
        self.assertTrue(all(x["state"] == "SUCCEEDED" for x in self.api.jobs(state="SUCCEEDED")))
