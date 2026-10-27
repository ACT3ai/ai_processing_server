"""Waiting jobs start in effective-priority order (spec §10.3): priority + owner bias + aging, ties to the oldest."""

from lib.base import ServerTestCase


class PriorityOrder(ServerTestCase):
    CONFIG = {"limits": {"max_parallel_jobs": 1}}
    SERVICES = {"sys.self_test": {"local_bias": 30}}

    def start_order(self, ids):
        jobs = [self.api.wait(i, timeout=30) for i in ids]
        for j in jobs:
            self.assertEqual(j["state"], "completed")
        return [j["id"] for j in sorted(jobs, key=lambda j: j["started_at"])]

    def test_higher_priority_first(self):
        blocker = self.api.submit("sys.compute", {"op": "sleep", "ms": 700}, job_id="blocker-a")["id"]
        self.api.wait_until(blocker, lambda j: j["state"] == "active")
        low = self.api.submit("sys.compute", {"op": "sleep", "ms": 50}, priority=1, job_id="low")["id"]
        normal = self.api.submit("sys.compute", {"op": "sleep", "ms": 50}, priority=25, job_id="normal")["id"]
        critical = self.api.submit("sys.compute", {"op": "sleep", "ms": 50}, priority=75, job_id="critical")["id"]
        high = self.api.submit("sys.compute", {"op": "sleep", "ms": 50}, priority=50, job_id="high")["id"]
        waiting = self.api.job(low)
        self.assertEqual(waiting["state"], "pending")
        self.assertEqual(waiting["platform_status"], "INQUEUE")
        self.assertEqual(self.start_order([blocker, low, normal, critical, high]), [blocker, critical, high, normal, low])

    def test_ties_go_to_the_oldest(self):
        blocker = self.api.submit("sys.compute", {"op": "sleep", "ms": 500}, job_id="blocker-b")["id"]
        self.api.wait_until(blocker, lambda j: j["state"] == "active")
        ids = [self.api.submit("sys.compute", {"op": "sleep", "ms": 20}, priority=25, job_id=f"tie-{i}")["id"] for i in range(4)]
        self.assertEqual(self.start_order([blocker, *ids]), [blocker, *ids])

    def test_owner_local_bias(self):
        blocker = self.api.submit("sys.compute", {"op": "sleep", "ms": 500}, job_id="blocker-c")["id"]
        self.api.wait_until(blocker, lambda j: j["state"] == "active")
        plain = self.api.submit("sys.compute", {"op": "sleep", "ms": 20}, priority=50, job_id="plain-50")["id"]
        biased = self.api.submit("sys.self_test", {"op": "sleep", "ms": 20}, priority=25, job_id="biased-25")["id"]
        self.assertEqual(self.api.job(biased)["effective_priority"], 55)  # 25 + local_bias 30
        self.assertEqual(self.start_order([blocker, plain, biased]), [blocker, biased, plain])
