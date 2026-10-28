"""this_server.yaml rules from the command line (spec §6, acceptance A1/A2): init creates once and never
overwrites, --write-new leaves the original alone, invalid files stop `run`, and reload is all-or-nothing."""

import json
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path

from lib.client import APIError
from lib.harness import BINARY, REPO_ROOT, ServerProcess


class InitCommand(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="aps_init_"))
        self.cfg = self.tmp / "this_server.yaml"

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def init(self, *extra):
        import subprocess
        token = self.tmp / "tok"
        env = dict(os.environ, HOME=str(self.tmp))  # init creates the token under ~/.credentials: keep it in tmp
        return subprocess.run([str(BINARY), "init", *extra, "--root", str(REPO_ROOT), "--state-dir", str(self.tmp),
                               "--config", str(self.cfg)], capture_output=True, text=True, timeout=120, env=env)

    def test_creates_once_then_never_touches(self):
        r = self.init()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("created", r.stdout)
        before = self.cfg.read_bytes()
        mtime = self.cfg.stat().st_mtime_ns
        self.assertEqual(oct(self.cfg.stat().st_mode & 0o777), "0o600")
        self.assertIn(b"schema_version: 1", before)
        self.assertIn(b"services:", before)
        time.sleep(0.05)
        for _ in range(2):
            r = self.init()
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertIn("left untouched", r.stdout)
        self.assertEqual(self.cfg.read_bytes(), before)
        self.assertEqual(self.cfg.stat().st_mtime_ns, mtime)
        self.assertTrue((self.tmp / ".credentials" / "ai_processing_server_local.token").exists())

    def test_write_new(self):
        self.init()
        before = self.cfg.read_bytes()
        r = self.init("--write-new")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertTrue((self.tmp / "this_server.yaml.new").exists())
        self.assertEqual(self.cfg.read_bytes(), before)

    def test_legacy_file_is_left_alone(self):
        legacy = b"# hand-written probe\nthis_server:\n  name: old\n"
        self.cfg.write_bytes(legacy)
        r = self.init()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("legacy", r.stdout)
        self.assertEqual(self.cfg.read_bytes(), legacy)


class RunRefusesBadConfig(unittest.TestCase):
    def test_change_me_key(self):
        s = ServerProcess({"server": {"key": "CHANGE_ME"}})
        try:
            r = s.run_cli("validate")
            self.assertNotEqual(r.returncode, 0)
            self.assertIn("CHANGE_ME", r.stdout + r.stderr)
            with self.assertRaises(RuntimeError) as e:
                s.start(timeout=20)
            self.assertIn("refusing to start", str(e.exception))
        finally:
            s.cleanup()

    def test_copied_from_another_machine(self):
        s = ServerProcess({"server": {"bound_to": {"hostname": "some-other-machine", "machine_id": "ffff-not-this-one"}}})
        try:
            r = s.run_cli("validate")
            self.assertNotEqual(r.returncode, 0)
            self.assertIn("was created on some-other-machine", r.stdout + r.stderr)
        finally:
            s.cleanup()


class Reload(unittest.TestCase):
    def setUp(self):
        self.server = ServerProcess(name="reload").start()
        self.api = self.server.client

    def tearDown(self):
        self.server.cleanup()

    def test_reload_applies_owner_changes(self):
        self.assertGreater(self.api.offers()["sys.render_test"]["parallel"], 0)
        cfg = json.loads(self.server.config_path.read_text())
        for s in cfg["services"]:
            if s["id"] == "sys.render_test":
                s["enabled"] = False
        self.server.write_config(cfg)
        r = self.api.post("/v1/reload")
        self.assertTrue(r["ok"])
        self.assertEqual(self.api.offers()["sys.render_test"]["reason"], "off in this_server.yaml")
        with self.assertRaises(APIError) as e:
            self.api.submit("sys.render_test", {})
        self.assertEqual(e.exception.code, "service_not_offered")

    def test_invalid_reload_keeps_running_config(self):
        good = self.server.config_path.read_text()
        self.server.config_path.write_text(good.replace('"max_parallel_jobs": 4', '"max_parallel_jobs": 0'))
        with self.assertRaises(APIError) as e:
            self.api.post("/v1/reload")
        self.assertEqual(e.exception.code, "invalid_config")
        self.assertEqual(self.api.get("/v1/server")["limits"]["max_parallel_jobs"], 4)
        self.assertEqual(self.api.wait(self.api.submit("sys.compute", {"op": "sleep", "ms": 5})["job_id"])["state"], "SUCCEEDED")

    def test_direct_mode_can_be_turned_off(self):
        cfg = json.loads(self.server.config_path.read_text())
        cfg["local_api"]["allow_direct_jobs"] = False
        self.server.write_config(cfg)
        with self.assertRaises(APIError) as e:
            self.api.submit("sys.compute", {"op": "sleep", "ms": 1})
        self.assertEqual((e.exception.status, e.exception.code), (403, "direct_jobs_disabled"))
