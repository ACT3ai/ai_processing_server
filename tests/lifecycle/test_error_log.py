"""error.err (pm/errors.mdx): failures that should not happen are recorded in one rolling file, one line each, with
a UTC timestamp and the absolute source path of the call site. Expected failures (bad input) stay out of it."""

import re
import unittest
from pathlib import Path

from lib.harness import ServerProcess, REPO_ROOT

LINE = re.compile(r"^\[\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z\] \[(WARN|ERROR|FATAL)\] \[([\w.]+)\] failed .+ at=(\S+):(\d+) ")


class ErrorLog(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ServerProcess(name="ErrorLog").start()
        cls.api = cls.server.client
        cls.path = Path(cls.server.error_dir) / "error.err"

    @classmethod
    def tearDownClass(cls):
        cls.server.cleanup()

    def entries(self):
        if not self.path.exists():
            return []
        return [l for l in self.path.read_text().splitlines() if l.strip()]

    def test_runner_panic_is_recorded_with_full_source_path_and_stack(self):
        j = self.api.submit("sys.compute", {"op": "panic"}, max_attempts=1)["job_id"]
        self.api.wait(j)
        hits = [l for l in self.entries() if j in l and "[FATAL] [runner.compute]" in l]
        self.assertEqual(len(hits), 1, "\n".join(self.entries()))
        m = LINE.match(hits[0])
        self.assertIsNotNone(m, hits[0])
        src = Path(m.group(3))
        self.assertTrue(src.is_absolute(), src)
        self.assertTrue(src.exists(), f"at= points at a file that does not exist: {src}")
        self.assertTrue(str(src).startswith(str(REPO_ROOT / "code")), src)
        self.assertIn("| stack:", hits[0])
        self.assertIn("compute.go", hits[0])  # the runner frame that panicked is in the stack

    def test_expected_failures_stay_out(self):
        before = len(self.entries())
        j = self.api.submit("sys.compute", {"op": "fail", "kind": "invalid_input", "message": "bad params"}, max_attempts=1)["job_id"]
        self.api.wait(j)
        j2 = self.api.submit("sys.compute", {"op": "primes", "limit": 1000})["job_id"]
        self.api.wait(j2)
        self.assertEqual(len(self.entries()), before, "\n".join(self.entries()[before:]))

    def test_internal_failure_is_recorded(self):
        j = self.api.submit("sys.compute", {"op": "fail", "kind": "internal", "message": "bug seen"}, max_attempts=1)["job_id"]
        self.api.wait(j)
        hits = [l for l in self.entries() if j in l]
        self.assertEqual(len(hits), 1, "\n".join(self.entries()))
        self.assertIn("[ERROR] [taskhost] failed running sys.compute: internal: bug seen", hits[0])
        self.assertIsNotNone(LINE.match(hits[0]), hits[0])

    def test_every_line_is_one_entry_and_file_is_capped(self):
        for l in self.entries():
            self.assertIsNotNone(LINE.match(l), l)
            self.assertLessEqual(len(l.encode()), 8001)
        if self.path.exists():
            self.assertLessEqual(self.path.stat().st_size, 4 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
