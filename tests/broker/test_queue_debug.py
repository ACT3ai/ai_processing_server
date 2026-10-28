"""Debugging the task queue (pm/task_queue.mdx §16.5): GET /v1/queue/jobs, cancel of a waiting job settling at once,
honest answers for finished and unknown jobs, and the CLI (`queue ls`, `queue show`, `cancel`, `cancel --prefix`)."""

import time

from broker.base import BrokerTestCase
from lib.client import APIError


class QueueDebug(BrokerTestCase):
    def drained(self):
        self.api.post("/v1/drain")
        self.addCleanup(self.api.post, "/v1/resume")

    def test_list_shows_waiting_jobs(self):
        self.drained()
        ack = self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 10}, job_id="dbg-list-1")
        rows = self.api.get("/v1/queue/jobs")["jobs"]
        row = next(r for r in rows if r["job_id"] == ack["job_id"])
        self.assertEqual((row["state"], row["service"], row["band"], row["held"]), ("QUEUED", "sys.compute", "p4", False))
        self.assertGreater(row["sequence"], 0)

    def test_cancel_waiting_job_settles_at_once(self):
        # Before 2026-10-28 a waiting job only got a flag and stayed QUEUED until a slot pulled it.
        self.drained()
        ack = self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 10}, job_id="dbg-wait-1")
        self.api.queue_cancel(ack["job_id"])
        j = self.api.queue_job(ack["job_id"])
        self.assertEqual(j["job_state"]["state"], "CANCELLED")
        self.assertTrue(j["control"]["cancel"])
        self.assertEqual(self.types(j), ["cancelled"])
        self.api.post("/v1/resume")
        time.sleep(1.5)  # the server pulls it, reads the final state, and drops it without running
        j = self.api.queue_job(ack["job_id"])
        self.assertNotIn("claimed", self.types(j))
        self.assertEqual(self.api.broker()["status"]["streams"]["WORK"], 0)

    def test_cancel_finished_or_unknown_job_says_so(self):
        job_id, _ = self.run_queued({"op": "sleep", "ms": 10})
        with self.assertRaises(APIError) as e:
            self.api.queue_cancel(job_id)
        self.assertEqual((e.exception.status, e.exception.code), (409, "already_final"))
        with self.assertRaises(APIError) as e:
            self.api.queue_cancel("dbg-no-such-job")
        self.assertEqual((e.exception.status, e.exception.code), (404, "not_found"))

    def test_cli_queue_ls_show_and_cancel(self):
        self.drained()
        self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 10}, job_id="dbg-cli-1")
        r = self.server.run_cli("queue", "ls")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("dbg-cli-1", r.stdout)
        self.assertIn("QUEUED", r.stdout)
        r = self.server.run_cli("queue", "show", "dbg-cli-1")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("queue      QUEUED", r.stdout)
        r = self.server.run_cli("cancel", "dbg-cli-1")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("CANCELLED", r.stdout)
        r = self.server.run_cli("cancel", "dbg-cli-1")  # again: already final, still exit 0
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("nothing to cancel", r.stdout)
        r = self.server.run_cli("cancel", "dbg-nope")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("not on the task queue", r.stdout)

    def test_cli_cancel_prefix_takes_every_try(self):
        # The 2026-10-28 shape: one caller job submitted as _a1, _a2, _a3, all waiting.
        self.drained()
        for n in (1, 2, 3):
            self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 10}, job_id=f"act3_dbgprefix_a{n}")
        self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 10}, job_id="act3_other_a1")
        r = self.server.run_cli("cancel", "--prefix", "act3_dbgprefix_")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        for n in (1, 2, 3):
            self.assertEqual(self.api.queue_job(f"act3_dbgprefix_a{n}")["job_state"]["state"], "CANCELLED")
        self.assertEqual(self.api.queue_job("act3_other_a1")["job_state"]["state"], "QUEUED")

    def test_cli_cancel_running_job_waits_until_stopped(self):
        ack = self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 20000, "steps": 200}, job_id="dbg-run-1")
        self.wait_local(self.api, ack["job_id"], lambda j: j["caller_state"] == "RUNNING")
        r = self.server.run_cli("cancel", "dbg-run-1")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn("CANCELLED after", r.stdout)
        self.assertEqual(self.api.queue_job("dbg-run-1")["job_state"]["state"], "CANCELLED")
