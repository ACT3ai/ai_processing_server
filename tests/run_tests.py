#!/usr/bin/env python3
"""Run the ai_processing_server black-box test suite.

    python3 run_tests.py                 # everything
    python3 run_tests.py queue render    # only these subdirectories
    python3 run_tests.py -k cancel       # only tests whose id contains "cancel"
    python3 run_tests.py --no-build      # use the existing binary as is

The binary is rebuilt first (just build) when any Go file or data/ file is newer than it. Results are written to
Test_Results/<date_time>_results.md plus Test_Results/latest.md and latest.json.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import unittest
from datetime import datetime
from pathlib import Path

TESTS = Path(__file__).resolve().parent
ROOT = TESTS.parent
SUITES = ["api", "compute", "render", "queue", "lifecycle", "continuity", "stress"]
sys.path.insert(0, str(TESTS))

from lib.harness import BINARY  # noqa: E402


def needs_build():
    if not BINARY.exists():
        return True
    built = BINARY.stat().st_mtime
    for base in (ROOT / "code", ROOT / "data"):
        for p in base.rglob("*"):
            if "git_ignore" in p.parts:
                continue
            if p.suffix in (".go", ".mod", ".sum", ".yaml") and p.stat().st_mtime > built:
                return True
    return False


def build():
    print(f"building {BINARY.name} ...", flush=True)
    if shutil.which("just"):
        cmd = ["just", "build"]
    else:
        cmd = ["go", "build", "-o", str(BINARY), "./cmd/ai_processing_server"]
    cwd = ROOT if cmd[0] == "just" else ROOT / "code"
    r = subprocess.run(cmd, cwd=cwd, env=dict(os.environ, GOWORK="off", CGO_ENABLED="0"))
    if r.returncode != 0:
        sys.exit("build failed")


class RecordingResult(unittest.TextTestResult):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.records, self._t = [], {}

    def startTest(self, test):
        self._t[test.id()] = time.time()
        super().startTest(test)

    def _rec(self, test, status, detail=""):
        self.records.append({"test": test.id(), "status": status, "seconds": round(time.time() - self._t.get(test.id(), time.time()), 2),
                             "detail": detail})

    def addSuccess(self, test):
        super().addSuccess(test)
        self._rec(test, "pass")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self._rec(test, "FAIL", self._exc_info_to_string(err, test))

    def addError(self, test, err):
        super().addError(test, err)
        self._rec(test, "ERROR", self._exc_info_to_string(err, test))

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self._rec(test, "skip", reason)


def flatten(suite):
    for t in suite:
        if isinstance(t, unittest.TestSuite):
            yield from flatten(t)
        else:
            yield t


def write_results(result, elapsed, selection):
    out = TESTS / "Test_Results"
    out.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    counts = {s: sum(1 for r in result.records if r["status"] == s) for s in ("pass", "FAIL", "ERROR", "skip")}
    lines = [f"# ai_processing_server test run {stamp}", "",
             f"* Selection: {selection or 'all'} · binary: {BINARY}",
             f"* {counts['pass']} passed, {counts['FAIL']} failed, {counts['ERROR']} errors, {counts['skip']} skipped in {elapsed:.1f}s", "",
             "| Result | Test | Seconds |", "| --- | --- | --- |"]
    lines += [f"| {r['status']} | `{r['test']}` | {r['seconds']} |" for r in result.records]
    bad = [r for r in result.records if r["status"] in ("FAIL", "ERROR")]
    if bad:
        lines += ["", "## Failures", ""]
        for r in bad:
            lines += [f"### {r['test']}", "", "```", r["detail"].strip(), "```", ""]
    text = "\n".join(lines) + "\n"
    (out / f"{stamp}_results.md").write_text(text)
    (out / "latest.md").write_text(text)
    (out / "latest.json").write_text(json.dumps({"run": stamp, "elapsed_s": round(elapsed, 1), "counts": counts, "tests": result.records}, indent=2))
    return out / f"{stamp}_results.md"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("suites", nargs="*", help=f"subdirectories to run (default all: {' '.join(SUITES)})")
    ap.add_argument("-k", dest="pattern", help="only tests whose id contains this text")
    ap.add_argument("--no-build", action="store_true")
    ap.add_argument("-q", "--quiet", action="store_true")
    a = ap.parse_args()

    if not a.no_build and needs_build():
        build()
    os.chdir(TESTS)
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for name in a.suites or SUITES:
        if name not in SUITES:
            sys.exit(f"unknown suite {name!r}; choose from {SUITES}")
        suite.addTests(loader.discover(start_dir=str(TESTS / name), pattern="test_*.py", top_level_dir=str(TESTS)))
    if a.pattern:
        suite = unittest.TestSuite(t for t in flatten(suite) if a.pattern in t.id())
    print(f"running {suite.countTestCases()} tests against {BINARY}", flush=True)

    t0 = time.time()
    runner = unittest.TextTestRunner(verbosity=1 if a.quiet else 2, resultclass=RecordingResult)
    result = runner.run(suite)
    report = write_results(result, time.time() - t0, " ".join(a.suites) + (f" -k {a.pattern}" if a.pattern else ""))
    print(f"\nresults: {report}")
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
