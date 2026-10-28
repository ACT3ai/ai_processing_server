"""Crash recovery (spec §9.5, acceptance A7) and graceful stop: the journal tells the truth after a restart."""

import unittest

from lib.harness import ServerProcess


class RestartAndRecovery(unittest.TestCase):
    def setUp(self):
        self.server = ServerProcess(name=self._testMethodName).start()
        self.api = self.server.client

    def tearDown(self):
        self.server.cleanup()

    def test_kill_mid_job_reports_infra_after_restart(self):
        done = self.api.submit("sys.compute", {"op": "primes", "limit": 1000})["id"]
        self.api.wait(done)
        j = self.api.submit("sys.compute", {"op": "sleep", "ms": 20000, "steps": 100})["id"]
        self.api.wait_until(j, lambda s: s["stage"] == "RUNNING" and s["progress"] > 0)
        work = self.server.state_dir / "work" / j
        self.assertTrue(work.exists())

        self.server.kill()  # SIGKILL: no drain, no cleanup
        self.server.start()

        after = self.api.job(j)
        self.assertEqual(after["state"], "archived")
        self.assertEqual(after["error_kind"], "infra")
        self.assertEqual(after["last_err"], "server restarted during RUNNING")
        self.assertFalse(work.exists(), "work dir must be deleted on recovery")
        # Finished jobs survive a restart with their results.
        old = self.api.job(done)
        self.assertEqual((old["state"], old["result"]["count"]), ("completed", 168))
        # And the restarted server takes new work.
        self.assertEqual(self.api.wait(self.api.submit("sys.compute", {"op": "sleep", "ms": 5})["id"])["state"], "completed")

    def test_sigterm_drains_running_job(self):
        j = self.api.submit("sys.compute", {"op": "sleep", "ms": 1500, "steps": 15})["id"]
        self.api.wait_until(j, lambda s: s["state"] == "active")
        code = self.server.stop(timeout=30)  # SIGTERM: drain, then exit
        self.assertEqual(code, 0, self.server.stdout_tail())
        self.server.start()
        after = self.api.job(j)
        self.assertEqual(after["state"], "completed", "a graceful stop must let the running job finish")
        self.assertEqual(after["progress"], 1)

    def test_drain_timeout_stops_jobs_as_infra(self):
        self.server.stop()
        cfg = self.server.config
        cfg["limits"]["drain_timeout_s"] = 1
        self.server.write_config(cfg)
        self.server.start()
        j = self.api.submit("sys.compute", {"op": "sleep", "ms": 30000, "steps": 300})["id"]
        self.api.wait_until(j, lambda s: s["state"] == "active")
        self.server.stop(timeout=30)
        self.server.start()
        after = self.api.job(j)
        self.assertEqual((after["state"], after["error_kind"]), ("archived", "infra"))
        self.assertIn("shutting down", after["last_err"])

    def test_logs_go_to_the_act3_log_files(self):
        self.api.wait(self.api.submit("sys.compute", {"op": "fail", "kind": "invalid_input", "message": "x"}, max_retry=0)["id"])
        info = (self.server.log_dir / "processing_server.info").read_text()
        err = (self.server.log_dir / "processing_server.err").read_text()
        self.assertIn("[INFO] [scheduler] job start", info)
        self.assertRegex(info, r"^\[\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}-0[78]:00\] \[INFO\]")
        self.assertIn("[ERROR] [scheduler] job failed", err)
        self.assertNotIn("[ERROR]", info)
