"""Jobs through the task queue on one server (pm/task_queue.mdx §13): claim, events, retries, DEAD, bands, cancel."""

import time

from broker.base import BrokerTestCase
from lib.client import APIError


class QueueJobs(BrokerTestCase):
    def test_health_reports_broker(self):
        b = self.api.health()["broker"]
        self.assertEqual(b["state"], "ok")
        self.assertTrue(b["server_version"].startswith("2."), b)
        v = self.api.broker()
        self.assertEqual(v["status"]["kind"], "nats")
        self.assertTrue(v["status"]["jetstream"])
        self.assertIn("TEST1", [s["key"] for s in v["servers"]])

    def test_job_runs_through_queue(self):
        job_id, done = self.run_queued({"op": "primes", "limit": 1000})
        st = done["job_state"]
        self.assertEqual((st["caller"], st["service"], st["attempt"], st["server"], st["band"]), ("local", "sys.compute", 1, "TEST1", "p4"))
        types = self.types(done)
        self.assertEqual(types[0], "claimed")
        self.assertEqual(done["events"][0]["state"], "CLAIMED")
        self.assertIn("stage", types)
        self.assertEqual(types[-1], "succeeded")
        self.assertEqual(len(types), len(set(e["seq"] for e in done["events"])))
        final = sorted(done["events"], key=lambda e: e["seq"])[-1]
        self.assertEqual(final["result"]["count"], 168)  # primes below 1000
        # The same job in the server's own table: caller-facing state and no claim left behind.
        local = self.api.job(job_id)
        self.assertEqual((local["queue"], local["state"], local["caller_state"]), ("local", "completed", "SUCCEEDED"))
        self.assertNotIn("claim", local)

    def test_duplicate_submit_is_noop(self):
        a = self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 10}, job_id="dup-job-1")
        b = self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 10}, job_id="dup-job-1")
        self.assertFalse(a["duplicate"])
        self.assertTrue(b["duplicate"])
        done = self.api.queue_wait("dup-job-1")
        self.assertEqual(self.types(done).count("claimed"), 1)

    def test_transient_failure_is_requeued(self):
        _, done = self.run_queued({"op": "flaky", "fail_before_attempt": 2, "kind": "transient"}, max_attempts=3)
        self.assertEqual(done["job_state"]["attempt"], 2)
        types = self.types(done)
        self.assertEqual(types.count("claimed"), 2)
        self.assertIn("requeued", types)
        req = next(e for e in done["events"] if e["type"] == "requeued")
        self.assertEqual(req["kind"], "transient")
        self.assertIn("next_attempt_after", req)

    def test_attempts_run_out_to_dead(self):
        before = self.api.broker()["status"]["streams"]["DEAD"]
        _, done = self.run_queued({"op": "flaky", "fail_before_attempt": 9, "kind": "infra"}, expect="FAILED", max_attempts=2)
        self.assertEqual((done["job_state"]["attempt"], done["job_state"]["kind"]), (2, "infra"))
        self.assertEqual(self.api.broker()["status"]["streams"]["DEAD"], before + 1)

    def test_non_retryable_failure_is_final(self):
        _, done = self.run_queued({"op": "fail", "kind": "invalid_input", "message": "nope"}, expect="FAILED", max_attempts=5)
        st = done["job_state"]
        self.assertEqual((st["attempt"], st["kind"]), (1, "invalid_input"))
        self.assertEqual(self.types(done)[-1], "failed")

    def test_higher_band_runs_first(self):
        self.api.post("/v1/drain")
        try:
            low = self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 50}, priority=0)
            high = self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 50}, priority=200)
            self.assertEqual((low["band"], high["band"]), ("p4", "p0"))
            time.sleep(1.0)
            with self.assertRaises(APIError) as e:  # nothing is pulled (no job_state yet) while draining
                self.api.queue_job(low["job_id"])
            self.assertEqual(e.exception.status, 404)
        finally:
            self.api.post("/v1/resume")
        lo, hi = self.api.queue_wait(low["job_id"]), self.api.queue_wait(high["job_id"])
        claimed = lambda d: next(e["at"] for e in d["events"] if e["type"] == "claimed")
        self.assertLess(claimed(hi), claimed(lo))

    def test_cancel_running_job(self):
        ack = self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 20000, "steps": 200})
        self.wait_local(self.api, ack["job_id"], lambda j: j["caller_state"] == "RUNNING")
        self.api.queue_cancel(ack["job_id"])
        done = self.api.queue_wait(ack["job_id"], timeout=15)
        self.assertEqual(done["job_state"]["state"], "CANCELLED")
        self.assertEqual(self.types(done)[-1], "cancelled")

    def test_cancel_before_claim(self):
        self.api.post("/v1/drain")
        try:
            ack = self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 10})
            self.api.queue_cancel(ack["job_id"])
        finally:
            self.api.post("/v1/resume")
        done = self.api.queue_wait(ack["job_id"], timeout=15)
        self.assertEqual(done["job_state"]["state"], "CANCELLED")
        self.assertNotIn("claimed", self.types(done))

    def test_bad_envelope_rejected(self):
        with self.assertRaises(APIError) as e:
            self.api.queue_submit("sys.compute", {"op": "sleep"}, job_id="has.dot")
        self.assertEqual((e.exception.status, e.exception.code), (400, "invalid_input"))
        with self.assertRaises(APIError) as e:
            self.api.queue_submit("no.such.service", {})
        self.assertEqual(e.exception.code, "unknown_service")
