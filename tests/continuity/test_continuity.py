"""Task continuity across code updates (pm/task_queue.mdx §9, §21.2): restart storms, a new build under a running
job, the controller away longer than the ack wait, a broker restart, and a missing broker."""

import os
import shutil
import signal
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from lib.client import APIError
from lib.harness import BINARY, REPO_ROOT, ServerProcess


def finished_once(test, api, job_id, timeout=90):
    j = api.wait(job_id, timeout=timeout)
    test.assertEqual(j["state"], "SUCCEEDED", j.get("error_message"))
    test.assertEqual(j["attempt"], 1)
    evs = api.events(job_id)
    seqs = [e["seq"] for e in evs]
    test.assertEqual(sorted(set(seqs)), list(range(1, max(seqs) + 1)), f"gaps or repeats in {seqs}")
    types = [e["type"] for e in evs]
    test.assertEqual(types.count("succeeded"), 1, types)
    test.assertEqual(types.count("claimed"), 1, types)
    test.assertNotIn("requeued", types)
    return j


class Continuity(unittest.TestCase):
    def setUp(self):
        self.server = ServerProcess(name=self._testMethodName).start()
        self.api = self.server.client

    def tearDown(self):
        self.server.cleanup()

    def test_restart_storm(self):
        """The controller is restarted six times during one job, alternating SIGTERM and SIGKILL (§21.2)."""
        j = self.api.submit("sys.compute", {"op": "sleep", "ms": 9000, "steps": 90})["job_id"]
        self.api.wait_until(j, lambda s: s["state"] == "RUNNING" and s["progress"] > 0)
        seen = []
        for i in range(6):
            if i % 2:
                self.server.kill()
            else:
                self.assertEqual(self.server.stop(), 0)
            time.sleep(0.3)
            self.server.start()
            seen.append(self.api.job(j)["progress"])
        self.assertEqual(seen, sorted(seen), "progress must only move forward across restarts")
        j = finished_once(self, self.api, j)
        self.assertEqual(j["result"]["steps"], 90)
        self.assertEqual(len(self.server.taskhost_pids()), 1, "one task host for the whole job")

    def test_controller_away_longer_than_the_ack_wait(self):
        """The claim expires while no controller runs (ack_wait 8 s); the broker redelivers the job, and the new
        controller keeps the running task host instead of starting it twice."""
        j = self.api.submit("sys.compute", {"op": "sleep", "ms": 16000, "steps": 80})["job_id"]
        self.api.wait_until(j, lambda s: s["state"] == "RUNNING" and s["progress"] > 0)
        self.server.kill()
        time.sleep(10)
        self.server.start()
        done = self.api.wait(j, timeout=60)
        self.assertEqual(done["state"], "SUCCEEDED", done.get("error_message"))
        self.assertEqual(len(self.server.taskhost_pids()), 1, "the job must have run in one task host only")
        types = [e["type"] for e in self.api.events(j)]
        self.assertEqual(types.count("succeeded"), 1, types)
        self.assertEqual(types.count("claimed"), 1, types)

    def test_broker_restart_mid_job(self):
        """The broker (its own process) restarts while a job runs; the claim survives and the job finishes once."""
        j = self.api.submit("sys.compute", {"op": "sleep", "ms": 5000, "steps": 50})["job_id"]
        self.api.wait_until(j, lambda s: s["state"] == "RUNNING" and s["progress"] > 0)
        self.server.broker.stop()
        time.sleep(1.5)
        self.server.broker.start()
        finished_once(self, self.api, j)
        self.assertEqual(self.api.health()["broker"]["state"], "ok")


class CodeUpdateUnderRunningJob(unittest.TestCase):
    """`git pull`, `just build`, restart: the running job finishes on the code it started with; the next job runs on
    the new build (§9.2, acceptance Q10)."""

    @classmethod
    def setUpClass(cls):
        cls.bindir = Path(tempfile.mkdtemp(prefix="aps_bins_"))
        cls.new_build = cls.bindir / "new_build"
        r = subprocess.run(["go", "build", "-trimpath", "-ldflags", "-X main.version=9.9.9-test -X main.gitSHA=newcode",
                            "-o", str(cls.new_build), "./cmd/ai_processing_server"], cwd=REPO_ROOT / "code",
                           env=dict(os.environ, GOWORK="off", CGO_ENABLED="0"), capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError("second build failed: " + r.stderr)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.bindir, ignore_errors=True)

    def test_running_job_keeps_its_code(self):
        live = self.bindir / "ai_processing_server"  # the file `just build` overwrites
        shutil.copy2(BINARY, live)
        server = ServerProcess(name="code_update", binary=live).start()
        try:
            api = server.client
            old_code = api.health()["code_version"]
            j = api.submit("sys.compute", {"op": "sleep", "ms": 4000, "steps": 40})["job_id"]
            api.wait_until(j, lambda s: s["state"] == "RUNNING" and s["progress"] > 0)
            tmp = live.with_suffix(".tmp")
            shutil.copy2(self.new_build, tmp)
            os.replace(tmp, live)  # `just build` replaces the file under the running controller
            server.stop()
            server.start()
            new_code = api.health()["code_version"]
            self.assertTrue(new_code.startswith("newcode-"), new_code)
            self.assertNotEqual(old_code, new_code)
            done = finished_once(self, api, j)
            self.assertEqual(done["code_version"], old_code, "the running job must finish on the code it started with")
            self.assertEqual(api.events(j)[-1]["code_version"], old_code)
            nxt = api.wait(api.submit("sys.compute", {"op": "sleep", "ms": 5})["job_id"])
            self.assertEqual(nxt["code_version"], new_code)
            snaps = sorted(p.name for p in (server.state_dir / "snapshots").iterdir() if not p.name.startswith("."))
            self.assertEqual(snaps, sorted([old_code, new_code]))
        finally:
            server.cleanup()


class BrokerMissing(unittest.TestCase):
    """Without a broker the server still starts, says broker_missing and how to fix it, refuses new jobs with 503,
    and connects by itself once the broker is up (acceptance Q1)."""

    def test_broker_missing_then_started(self):
        server = ServerProcess(name="no_broker").start(broker=False)
        try:
            api = server.client
            h = api.health()
            self.assertEqual(h["state"], "DEGRADED")
            self.assertEqual(h["broker"]["state"], "broker_missing")
            self.assertIn("broker-install", h["broker"]["fix"])
            self.assertIn("broker_missing", h["state_reason"])
            with self.assertRaises(APIError) as e:
                api.submit("sys.compute", {"op": "sleep", "ms": 1})
            self.assertEqual((e.exception.status, e.exception.code), (503, "broker_missing"))
            server.broker.start()
            deadline = time.time() + 15
            while api.health()["broker"]["state"] != "ok" and time.time() < deadline:
                time.sleep(0.2)
            self.assertEqual(api.health()["state"], "ONLINE")
            j = api.wait(api.submit("sys.compute", {"op": "primes", "limit": 100})["job_id"])
            self.assertEqual((j["state"], j["result"]["count"]), ("SUCCEEDED", 25))
        finally:
            server.cleanup()


if __name__ == "__main__":
    unittest.main()
