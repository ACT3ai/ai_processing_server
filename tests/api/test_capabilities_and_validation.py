"""Offers follow the owner's allow-list and the hardware; bad submissions are refused with a clear code."""

from lib.base import ServerTestCase
from lib.client import APIError


class CapabilitiesAndValidation(ServerTestCase):
    SERVICES = {"act3.video_merge": {"enabled": False}, "sys.compute": {"max_parallel": 3}}

    def test_offers_have_reasons(self):
        offers = self.api.offers()
        self.assertEqual(offers["sys.compute"]["reason"], "ok")
        self.assertEqual(offers["sys.compute"]["parallel"], 3)          # owner's max_parallel
        self.assertEqual(offers["act3.video_merge"]["parallel"], 0)
        self.assertEqual(offers["act3.video_merge"]["reason"], "off in this_server.yaml")
        self.assertIn("not built", offers["wtc.transcription"]["reason"])  # runner arrives in a later phase
        self.assertIn("not listed", offers["act3.comfy_on_prem"]["reason"])  # absent from the allow-list = off

    def test_services_view(self):
        rows = {s["service"]: s for s in self.api.get("/v1/services")["services"]}
        for field in ("running", "waiting", "parallel", "reason", "resources_per_job"):
            self.assertIn(field, rows["sys.compute"])
        self.assertLessEqual(rows["sys.compute"]["running"], rows["sys.compute"]["parallel"])

    def test_owner_fence_refuses_off_service(self):
        with self.assertRaises(APIError) as e:
            self.api.submit("act3.video_merge", {})
        self.assertEqual(e.exception.status, 409)
        self.assertEqual(e.exception.code, "service_not_offered")

    def test_unknown_service(self):
        with self.assertRaises(APIError) as e:
            self.api.submit("act3.does_not_exist", {})
        self.assertEqual(e.exception.code, "unknown_service")

    def test_bad_json_and_unknown_fields(self):
        with self.assertRaises(APIError) as e:
            self.api.post("/v1/jobs", b"{not json")
        self.assertEqual(e.exception.status, 400)
        with self.assertRaises(APIError) as e:
            self.api.post("/v1/jobs", {"service": "sys.compute", "surprise": 1})
        self.assertEqual(e.exception.code, "invalid_json")

    def test_duplicate_job_id(self):
        self.api.submit("sys.compute", {"op": "sleep", "ms": 10}, job_id="dup-1")
        with self.assertRaises(APIError) as e:
            self.api.submit("sys.compute", {"op": "sleep", "ms": 10}, job_id="dup-1")
        self.assertEqual(e.exception.code, "conflict")

    def test_invalid_params_fail_fast_without_retry(self):
        j = self.run_job("sys.compute", {"op": "primes", "limit": 1}, expect="FAILED", max_attempts=4)
        self.assertEqual(j["error_kind"], "invalid_input")
        self.assertEqual((j["attempt"] - 1), 0)
        self.assertEqual(j["platform_status"], "ERROR")
        self.assertIn("limit", j["error_message"])

    def test_priority_is_clamped(self):
        j = self.api.submit("sys.compute", {"op": "sleep", "ms": 1}, priority=9999)
        self.assertEqual(j["priority"], 300)

    def test_task_queue_record(self):
        """The record passes the task queue's own names through (pm/task_queue.mdx §13, D-008): the envelope as
        submitted on SUBMIT, the job_state fields, the event sequence."""
        j = self.run_job("sys.compute", {"op": "sleep", "ms": 1})
        for field in ("job_id", "caller", "category", "service", "priority", "band", "state", "attempt", "max_attempts",
                      "server", "timeout_seconds", "finished_at", "result", "code_version", "events_seq", "envelope", "history"):
            self.assertIn(field, j)
        env = j["envelope"]
        self.assertEqual((env["envelope_version"], env["caller"], env["service"]), (1, "local", "sys.compute"))
        self.assertEqual(env["reply"]["events"], "evt.local." + j["job_id"])
        q = self.api.get("/v1/queues")
        self.assertEqual(q["broker"]["state"], "ok")
        self.assertGreaterEqual(q["jobs_by_service"]["sys.compute"]["SUCCEEDED"], 1)
