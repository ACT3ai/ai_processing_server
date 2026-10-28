"""Controller stop and crash (pm/task_queue.mdx §9.4, §9.5): a running job lives in its detached task host, so
stopping or killing the controller never stops it. The next controller re-adopts it and it finishes once, on its
first attempt. Only a dead task host (a reboot, a kill) makes the job go back on the queue as `infra`."""

import os
import signal
import time
import unittest

from lib.harness import ServerProcess


def pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


class RestartAndRecovery(unittest.TestCase):
    def setUp(self):
        self.server = ServerProcess(name=self._testMethodName).start()
        self.api = self.server.client

    def tearDown(self):
        self.server.cleanup()

    def assertFinishedOnce(self, job_id):
        """One claim, one attempt, one final event, and an event sequence with no gaps or repeats."""
        j = self.api.wait(job_id, timeout=60)
        self.assertEqual(j["state"], "SUCCEEDED", j.get("error_message"))
        self.assertEqual(j["attempt"], 1)
        self.assertEqual([h["state"] for h in j["history"]].count("CLAIMED"), 1)
        evs = self.api.events(job_id)
        seqs = [e["seq"] for e in evs]
        self.assertEqual(sorted(set(seqs)), list(range(1, max(seqs) + 1)), seqs)
        self.assertEqual([e["type"] for e in evs].count("succeeded"), 1)
        self.assertNotIn("requeued", [e["type"] for e in evs])
        return j

    def test_kill_controller_mid_job_job_finishes_once(self):
        done = self.api.submit("sys.compute", {"op": "primes", "limit": 1000})["job_id"]
        self.api.wait(done)
        j = self.api.submit("sys.compute", {"op": "sleep", "ms": 4000, "steps": 40})["job_id"]
        self.api.wait_until(j, lambda s: s["state"] == "RUNNING" and s["progress"] > 0)
        pids = self.server.taskhost_pids(running_only=True)
        self.assertEqual(len(pids), 1)

        self.server.kill()  # SIGKILL the controller: no shutdown at all
        time.sleep(0.5)
        self.assertTrue(pid_alive(next(iter(pids))), "the task host must outlive the controller")
        self.server.start()

        after = self.assertFinishedOnce(j)
        self.assertEqual(after["progress"], 1)
        self.assertEqual(after["result"]["steps"], 40)
        # Finished jobs survive a restart with their results, and the new controller takes new work.
        old = self.api.job(done)
        self.assertEqual((old["state"], old["result"]["count"]), ("SUCCEEDED", 168))
        self.assertEqual(self.api.wait(self.api.submit("sys.compute", {"op": "sleep", "ms": 5})["job_id"])["state"], "SUCCEEDED")
        self.assertIn("re-adopted running job", (self.server.log_dir / "processing_server.info").read_text())

    def test_sigterm_leaves_job_running_and_it_finishes_after_restart(self):
        j = self.api.submit("sys.compute", {"op": "sleep", "ms": 1500, "steps": 15})["job_id"]
        self.api.wait_until(j, lambda s: s["state"] == "RUNNING")
        code = self.server.stop(timeout=30)  # SIGTERM: stop pulling, keep the claim, exit
        self.assertEqual(code, 0, self.server.stdout_tail())
        # The runner finishes with no controller; before anything is uploaded it waits for a controller to confirm
        # the claim is still ours (the fence, pm/task_queue.mdx §16.3).
        run = self.server.state_dir / "runs" / j
        deadline = time.time() + 20
        while not (run / "fence_request").exists() and time.time() < deadline:
            time.sleep(0.1)
        self.assertTrue((run / "fence_request").exists(), "the runner must finish with no controller running")
        self.assertFalse((run / "exit.json").exists(), "nothing is delivered until a controller checks the fence")
        self.server.start()
        after = self.assertFinishedOnce(j)
        self.assertEqual(after["progress"], 1)

    def test_killed_taskhost_is_requeued_as_infra(self):
        j = self.api.submit("sys.compute", {"op": "sleep", "ms": 30000, "steps": 300}, max_attempts=2)["job_id"]
        self.api.wait_until(j, lambda s: s["state"] == "RUNNING" and s["progress"] > 0)
        for pid in self.server.taskhost_pids():
            os.kill(pid, signal.SIGKILL)  # as a reboot would
        requeued = self.api.wait_until(j, lambda s: s["attempt"] == 2, timeout=40)
        self.assertEqual(requeued["error_kind"], "infra")
        evs = self.api.events(j)
        rq = [e for e in evs if e["type"] == "requeued"]
        self.assertEqual(len(rq), 1)
        self.assertIn("task host stopped without a result", rq[0]["message"])
        self.api.cancel(j)
        self.assertEqual(self.api.wait(j)["state"], "CANCELLED")

    def test_logs_go_to_the_act3_log_files(self):
        self.api.wait(self.api.submit("sys.compute", {"op": "fail", "kind": "invalid_input", "message": "x"}, max_attempts=1)["job_id"])
        info = (self.server.log_dir / "processing_server.info").read_text()
        err = (self.server.log_dir / "processing_server.err").read_text()
        self.assertIn("[INFO] [scheduler] job claimed", info)
        self.assertIn("[INFO] [scheduler] broker connected", info)
        self.assertRegex(info, r"^\[\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}-0[78]:00\] \[INFO\]")
        self.assertIn("[ERROR] [scheduler] job failed", err)
        self.assertNotIn("[ERROR]", info)


if __name__ == "__main__":
    unittest.main()
