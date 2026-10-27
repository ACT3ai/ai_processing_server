"""The owner can pause one service or drain the whole server; running work finishes, new work waits."""

import time

from lib.base import ServerTestCase


class PauseAndDrain(ServerTestCase):
    def test_pause_one_service(self):
        self.api.post("/v1/services/sys.compute/pause")
        try:
            held = self.api.submit("sys.compute", {"op": "sleep", "ms": 10})["id"]
            other = self.run_job("sys.self_test", {"op": "sleep", "ms": 10})  # other services keep running
            self.assertEqual(other["state"], "completed")
            time.sleep(0.6)
            self.assertEqual(self.api.job(held)["state"], "pending")
            row = next(s for s in self.api.get("/v1/services")["services"] if s["service"] == "sys.compute")
            self.assertEqual((row["paused_by"], row["waiting"]), ("owner", 1))
        finally:
            self.api.post("/v1/services/sys.compute/resume")
        self.assertEqual(self.api.wait(held)["state"], "completed")

    def test_drain_and_resume(self):
        running = self.api.submit("sys.compute", {"op": "sleep", "ms": 700})["id"]
        self.api.wait_until(running, lambda j: j["state"] == "active")
        h = self.api.post("/v1/drain")
        self.assertEqual(h["state"], "DRAINING")
        try:
            later = self.api.submit("sys.compute", {"op": "sleep", "ms": 10})["id"]  # accepted, not started
            self.assertEqual(self.api.wait(running)["state"], "completed")  # running work finishes
            time.sleep(0.5)
            self.assertEqual(self.api.job(later)["state"], "pending")
        finally:
            self.api.post("/v1/resume")
        self.assertEqual(self.api.wait(later)["state"], "completed")
        self.assertEqual(self.api.health()["state"], "ONLINE")

    def test_pause_unknown_service(self):
        from lib.client import APIError
        with self.assertRaises(APIError) as e:
            self.api.post("/v1/services/nope.nope/pause")
        self.assertEqual(e.exception.status, 404)
