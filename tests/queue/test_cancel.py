"""Cancel stops running work at once, uploads nothing, frees the slot; waiting jobs cancel without starting."""

from lib.base import ServerTestCase
from lib.client import APIError


class Cancel(ServerTestCase):
    STORAGE = True
    CONFIG = {"limits": {"max_parallel_jobs": 1}}

    def test_cancel_running_uploads_nothing(self):
        url = self.storage.upload_url("never/result.json")
        j = self.api.submit("sys.compute", {"op": "primes", "limit": 3_000_000, "segments": 60, "throttle_ms": 100},
                            outputs=[{"name": "result", "upload": {"method": "PUT", "url": url}}])
        self.api.wait_until(j["id"], lambda s: s["state"] == "active" and s["progress"] >= 0.1)
        self.api.cancel(j["id"])
        done = self.api.wait(j["id"], timeout=10)
        self.assertEqual(done["state"], "cancelled")
        self.assertEqual(done["error_kind"], "cancelled")
        self.assertEqual(done["platform_status"], "CANCELLED")
        self.assertLess(done["progress"], 1)
        self.assertNotIn("never/result.json", self.storage.uploads)
        self.assertEqual(self.api.get("/v1/server")["slots"]["cores_used"], 0)

    def test_cancel_waiting_job(self):
        blocker = self.api.submit("sys.compute", {"op": "sleep", "ms": 600})["id"]
        self.api.wait_until(blocker, lambda s: s["state"] == "active")
        waiting = self.api.submit("sys.compute", {"op": "sleep", "ms": 10})["id"]
        c = self.api.cancel(waiting)
        self.assertEqual(c["state"], "cancelled")
        self.assertIsNone(self.api.job(waiting).get("started_at"))
        self.assertEqual(self.api.wait(blocker)["state"], "completed")

    def test_cancel_external_process(self):
        # Cancelling during the ffmpeg-free render step (many frames, throttled) also stops promptly.
        j = self.api.submit("sys.render_test", {"frames": 200, "throttle_ms": 50, "encode_video": False})
        self.api.wait_until(j["id"], lambda s: s["progress"] > 0.02)
        self.api.cancel(j["id"])
        done = self.api.wait(j["id"], timeout=10)
        self.assertEqual(done["state"], "cancelled")
        self.assertEqual(done["error_stage"], "RUNNING")

    def test_cancel_finished_and_unknown(self):
        j = self.run_job("sys.compute", {"op": "sleep", "ms": 5})
        with self.assertRaises(APIError) as e:
            self.api.cancel(j["id"])
        self.assertEqual(e.exception.code, "already_final")
        with self.assertRaises(APIError) as e:
            self.api.cancel("no-such-job")
        self.assertEqual(e.exception.status, 404)
