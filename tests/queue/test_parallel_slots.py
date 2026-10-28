"""Jobs run in parallel up to the service cap and the machine cap, never beyond, while the API shows it live."""

import threading
import time
from datetime import datetime

from lib.base import ServerTestCase


class Sampler:
    """Polls /v1/services in the background and records the peak running count per service and in total."""

    def __init__(self, api):
        self.api, self.peak, self.peak_total, self.samples = api, {}, 0, 0
        self._stop = threading.Event()
        self.t = threading.Thread(target=self.loop, daemon=True)
        self.t.start()

    def loop(self):
        while not self._stop.is_set():
            rows = self.api.get("/v1/services")["services"]
            total = 0
            for r in rows:
                self.peak[r["service"]] = max(self.peak.get(r["service"], 0), r["running"])
                total += r["running"]
            self.peak_total = max(self.peak_total, total)
            self.samples += 1
            time.sleep(0.02)

    def stop(self):
        self._stop.set()
        self.t.join()


class ParallelSlots(ServerTestCase):
    CONFIG = {"limits": {"max_parallel_jobs": 3}}
    SERVICES = {"sys.compute": {"max_parallel": 2}, "sys.self_test": {"max_parallel": 2}}

    def test_service_cap(self):
        s = Sampler(self.api)
        ids = [self.api.submit("sys.compute", {"op": "sleep", "ms": 500, "steps": 5})["id"] for _ in range(6)]
        jobs = [self.api.wait(i, timeout=30) for i in ids]
        s.stop()
        self.assertTrue(all(j["state"] == "completed" for j in jobs))
        self.assertEqual(s.peak["sys.compute"], 2, f"peak {s.peak} over {s.samples} samples")
        # Six 0.5 s jobs two at a time take about 1.5 s; one at a time would take 3 s, all at once 0.5 s.
        first = min(datetime.fromisoformat(j["started_at"]) for j in jobs)
        last = max(datetime.fromisoformat(j["finished_at"]) for j in jobs)
        self.assertGreater((last - first).total_seconds(), 1.4)
        self.assertLess((last - first).total_seconds(), 2.8)

    def test_machine_cap_across_services(self):
        s = Sampler(self.api)
        ids = [self.api.submit(svc, {"op": "sleep", "ms": 600, "steps": 3})["id"]
               for svc in ("sys.compute", "sys.self_test") for _ in range(4)]
        for i in ids:
            self.assertEqual(self.api.wait(i, timeout=30)["state"], "completed")
        s.stop()
        self.assertEqual(s.peak_total, 3, f"machine cap is 3; peaks {s.peak}")
        self.assertLessEqual(s.peak["sys.compute"], 2)
        self.assertLessEqual(s.peak["sys.self_test"], 2)

    def test_slots_are_released(self):
        for _ in range(3):
            self.run_job("sys.compute", {"op": "sleep", "ms": 20})
        info = self.api.get("/v1/server")
        self.assertEqual(info["active_jobs"], 0)
        self.assertEqual(info["slots"]["cores_used"], 0)
        self.assertEqual(info["state"], "ONLINE")

    def test_busy_state_when_full(self):
        ids = [self.api.submit("sys.compute", {"op": "sleep", "ms": 800})["id"] for _ in range(2)]
        ids.append(self.api.submit("sys.self_test", {"op": "sleep", "ms": 800})["id"])
        for i in ids:
            self.api.wait_until(i, lambda j: j["state"] == "active")
        self.assertEqual(self.api.health()["state"], "BUSY")
        for i in ids:
            self.api.wait(i)
        self.assertEqual(self.api.health()["state"], "ONLINE")
