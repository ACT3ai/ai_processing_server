"""Waiting jobs start in pull order (pm/task_queue.mdx §14): admission maps priority to a band, the worker pulls the
most urgent band first; inside a band, the owner's local_bias orders services, and each consumer is first in, first
out."""

from lib.base import ServerTestCase


class PriorityOrder(ServerTestCase):
    CONFIG = {"limits": {"max_parallel_jobs": 1}}
    SERVICES = {"sys.self_test": {"local_bias": 30}}

    def start_order(self, ids):
        jobs = [self.api.wait(i, timeout=30) for i in ids]
        for j in jobs:
            self.assertEqual(j["state"], "SUCCEEDED")
        return [j["job_id"] for j in sorted(jobs, key=lambda j: j["started_at"])]

    def test_higher_priority_first(self):
        blocker = self.api.submit("sys.compute", {"op": "sleep", "ms": 700}, job_id="blocker-a")["job_id"]
        self.api.wait_until(blocker, lambda j: j["state"] == "RUNNING")
        low = self.api.submit("sys.compute", {"op": "sleep", "ms": 50}, priority=1, job_id="low")["job_id"]
        normal = self.api.submit("sys.compute", {"op": "sleep", "ms": 50}, priority=25, job_id="normal")["job_id"]
        critical = self.api.submit("sys.compute", {"op": "sleep", "ms": 50}, priority=75, job_id="critical")["job_id"]
        high = self.api.submit("sys.compute", {"op": "sleep", "ms": 50}, priority=50, job_id="high")["job_id"]
        waiting = self.api.job(low)
        self.assertEqual(waiting["state"], "QUEUED")
        self.assertEqual(waiting["platform_status"], "INQUEUE")
        self.assertEqual(self.start_order([blocker, low, normal, critical, high]), [blocker, critical, high, normal, low])

    def test_ties_go_to_the_oldest(self):
        blocker = self.api.submit("sys.compute", {"op": "sleep", "ms": 500}, job_id="blocker-b")["job_id"]
        self.api.wait_until(blocker, lambda j: j["state"] == "RUNNING")
        ids = [self.api.submit("sys.compute", {"op": "sleep", "ms": 20}, priority=25, job_id=f"tie-{i}")["job_id"] for i in range(4)]
        self.assertEqual(self.start_order([blocker, *ids]), [blocker, *ids])

    def test_owner_local_bias(self):
        blocker = self.api.submit("sys.compute", {"op": "sleep", "ms": 500}, job_id="blocker-c")["job_id"]
        self.api.wait_until(blocker, lambda j: j["state"] == "RUNNING")
        plain = self.api.submit("sys.compute", {"op": "sleep", "ms": 20}, priority=40, job_id="plain-40")["job_id"]
        biased = self.api.submit("sys.self_test", {"op": "sleep", "ms": 20}, priority=25, job_id="biased-25")["job_id"]
        self.api.wait_until(biased, lambda j: j["state"] == "QUEUED")
        self.assertEqual((self.api.job(plain)["band"], self.api.job(biased)["band"]), ("p3", "p3"))
        self.assertEqual(self.start_order([blocker, plain, biased]), [blocker, biased, plain])

    def test_bands(self):
        """Admission's priority → band map (data/queue/bands.yaml), shown on the record and the admitted event."""
        for prio, band in ((0, "p4"), (24, "p4"), (25, "p3"), (49, "p3"), (50, "p2"), (75, "p1"), (149, "p1"), (150, "p0"), (300, "p0")):
            j = self.run_job("sys.self_test", {"op": "sleep", "ms": 1}, priority=prio)
            self.assertEqual(j["band"], band, prio)
            admitted = self.api.events(j["job_id"])[0]
            self.assertEqual((admitted["type"], admitted["band"]), ("admitted", band))
