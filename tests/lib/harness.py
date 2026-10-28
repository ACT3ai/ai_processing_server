"""Start and stop a real ai_processing_server for tests.

Every ServerProcess gets its own temporary state directory (its own this_server.yaml, journal, work and results
dirs), its own token, its own log directory and a free loopback port. Tests never touch the machine's real
data/git_ignore/, ~/_act3_logs/ or port 9440.
"""

import copy
import json
import os
import secrets
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

from .client import Client, APIError

TESTS_DIR = Path(__file__).resolve().parent.parent

# error.err goes to ~/T/ai_processing_server by default. Any binary a test starts (including direct `init` calls)
# writes into a temp dir instead; each ServerProcess also passes its own --error-dir.
if "APS_ERROR_DIR" not in os.environ:
    os.environ["APS_ERROR_DIR"] = tempfile.mkdtemp(prefix="aps_test_errors_")
REPO_ROOT = TESTS_DIR.parent
BINARY = Path(os.environ.get("APS_BINARY", REPO_ROOT / ("ai_processing_server.exe" if os.name == "nt" else "ai_processing_server")))

ALL_SERVICES = [
    "sys.self_test", "sys.compute", "sys.render_test",
    "act3.transcode", "act3.video_merge", "wtc.transcode", "wtc.transcription",
]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def deep_merge(base, over):
    out = copy.deepcopy(base)
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def base_config(port, token_file, key="TEST1"):
    """A this_server.yaml for tests. JSON is valid YAML, so no YAML library is needed."""
    return {
        "schema_version": 1,
        "created_at": "2026-10-27T00:00:00Z",
        "created_by": "tests/lib/harness.py",
        "server": {"key": key, "display_name": "Test server " + key, "profile": "generic", "family": "office",
                   "site": "test", "bound_to": {"hostname": "", "machine_id": ""}},
        # Declared hardware: the server offers min(detected, declared), so results do not depend on the machine
        # being bigger than this.
        "hardware": {"os": "", "arch": "", "cpu": {"model": "", "physical_cores": 8, "logical_cores": 8}, "ram_gb": 16},
        "engines": {},
        "services": [{"id": s, "enabled": True} for s in ALL_SERVICES],
        "platforms": {},
        "limits": {"max_parallel_jobs": 4, "cpu_reserve_pct": 0, "ram_reserve_gb": 0, "vram_headroom_gb": 1.5,
                   "disk_min_free_gb": 0, "gpu_temp_max_c": 85, "drain_timeout_s": 30},
        "paths": {"work_dir": "", "cache_dir": "", "cache_max_gb": 10},
        "local_api": {"listen": f"127.0.0.1:{port}", "token_file": str(token_file), "allow_direct_jobs": True},
        "debug": {"keep_work_dirs": False},
    }


def service_settings(overrides):
    """Turn {"sys.compute": {"max_parallel": 2}, "act3.transcode": {"enabled": False}} into a services list."""
    out = []
    for s in ALL_SERVICES:
        entry = {"id": s, "enabled": True}
        entry.update(overrides.get(s, {}))
        out.append(entry)
    return out


class ServerProcess:
    """One running server. Use start(), stop() (graceful), kill() (SIGKILL), restart()."""

    def __init__(self, config_overrides=None, services=None, name="server"):
        if not BINARY.exists():
            raise RuntimeError(f"{BINARY} not found: run `just build` in {REPO_ROOT}")
        self.tmp = Path(tempfile.mkdtemp(prefix="aps_test_"))
        self.state_dir = self.tmp / "state"
        self.log_dir = self.tmp / "logs"
        self.error_dir = self.tmp / "errors"
        self.state_dir.mkdir()
        self.port = free_port()
        self.token = secrets.token_urlsafe(32)
        self.token_file = self.tmp / "local.token"
        self.token_file.write_text(self.token + "\n")
        os.chmod(self.token_file, 0o600)
        cfg = base_config(self.port, self.token_file)
        if services:
            cfg["services"] = service_settings(services)
        self.config = deep_merge(cfg, config_overrides)
        self.config_path = self.state_dir / "this_server.yaml"
        self.write_config(self.config)
        self.name = name
        self.proc = None
        self.client = Client(f"http://127.0.0.1:{self.port}", self.token)

    def write_config(self, cfg):
        self.config_path.write_text(json.dumps(cfg, indent=2))

    def cmd(self, *args):
        return [str(BINARY), *args, "--root", str(REPO_ROOT), "--state-dir", str(self.state_dir),
                "--config", str(self.config_path), "--log-dir", str(self.log_dir),
                "--error-dir", str(self.error_dir)]

    def start(self, timeout=30):
        out = open(self.tmp / f"{self.name}.stdout.log", "ab")
        self.proc = subprocess.Popen(self.cmd("run"), stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited with {self.proc.returncode}:\n{self.stdout_tail()}")
            try:
                h = self.client.health()
                if h.get("state") not in (None, "STARTING"):
                    return self
            except (APIError, OSError):
                pass
            time.sleep(0.1)
        raise RuntimeError(f"server did not become healthy in {timeout}s:\n{self.stdout_tail()}")

    def stop(self, timeout=60):
        """Graceful: SIGTERM, the server drains running jobs, then exits."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM if os.name != "nt" else signal.CTRL_BREAK_EVENT)
            try:
                self.proc.wait(timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        return self.proc.returncode if self.proc else None

    def kill(self):
        """Crash: SIGKILL, no drain, no cleanup."""
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

    def restart(self):
        self.stop()
        return self.start()

    def stdout_tail(self, n=40):
        p = self.tmp / f"{self.name}.stdout.log"
        if not p.exists():
            return ""
        return "\n".join(p.read_text(errors="replace").splitlines()[-n:])

    def cleanup(self):
        self.kill()
        if os.environ.get("KEEP_TEST_DIRS") != "1":
            shutil.rmtree(self.tmp, ignore_errors=True)

    def run_cli(self, *args, timeout=60):
        """Run a one-shot subcommand (init, validate, detect ...) against this server's state dir."""
        return subprocess.run(self.cmd(*args), capture_output=True, text=True, timeout=timeout)
