"""Start and stop a real ai_processing_server for tests, with its own real nats-server.

Every ServerProcess gets its own temporary state directory (its own this_server.yaml, journal, runs, snapshots, work
and results dirs), its own token, its own log directory, a free loopback port, and its own nats-server process with
JetStream on another free port (pm/task_queue.mdx §21.2: tests start the broker as a separate process, as
production does; they never embed it). Tests never touch the machine's real data/git_ignore/, ~/_act3_logs/, port
9440 or the broker service on 4244.

Running jobs live in detached task hosts that outlive the controller, so cleanup kills them by the pids in their
run directories; a task host also stops by itself when its run directory disappears.
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
NATS_SERVER = os.environ.get("NATS_SERVER") or shutil.which("nats-server") or str(Path.home() / ".act3_processing/nats/bin/nats-server")

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


def base_config(port, token_file, broker_port, key="TEST1"):
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
        # Short ack wait and heartbeat so redelivery tests take seconds, not minutes.
        "queue": {"role": "standalone", "broker_url": f"nats://127.0.0.1:{broker_port}"},
        "continuity": {"ack_wait": "8s", "heartbeat": "1s", "keep_snapshots": 2},
        "callers": {"local": {"enabled": True, "mode": "first"}},
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


class Broker:
    """A nats-server with JetStream, on a free port, storing under dir. Survives controller restarts."""

    def __init__(self, store_dir):
        if not Path(NATS_SERVER).exists() and not shutil.which(NATS_SERVER):
            raise RuntimeError("nats-server not found: install it (`just broker-install`, or brew install nats-server) "
                               "or set NATS_SERVER")
        self.dir = Path(store_dir)
        self.port = free_port()
        self.proc = None

    def start(self):
        self.dir.mkdir(parents=True, exist_ok=True)
        log = open(self.dir / "nats.log", "ab")
        self.proc = subprocess.Popen([NATS_SERVER, "-js", "-a", "127.0.0.1", "-p", str(self.port), "-sd", str(self.dir / "js")],
                                     stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        deadline = time.time() + 10
        while time.time() < deadline:
            with socket.socket() as s:
                if s.connect_ex(("127.0.0.1", self.port)) == 0:
                    return self
            time.sleep(0.05)
        raise RuntimeError("nats-server did not start; see " + str(self.dir / "nats.log"))

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


class ServerProcess:
    """One running server. Use start(), stop() (graceful), kill() (SIGKILL), restart()."""

    def __init__(self, config_overrides=None, services=None, name="server", binary=None):
        self.binary = Path(binary or BINARY)
        if not self.binary.exists():
            raise RuntimeError(f"{BINARY} not found: run `just build` in {REPO_ROOT}")
        self.tmp = Path(tempfile.mkdtemp(prefix="aps_test_"))
        self.state_dir = self.tmp / "state"
        self.log_dir = self.tmp / "logs"
        self.error_dir = self.tmp / "errors"
        self.state_dir.mkdir()
        self.port = free_port()
        self.broker = Broker(self.tmp / "broker")
        self.token = secrets.token_urlsafe(32)
        self.token_file = self.tmp / "local.token"
        self.token_file.write_text(self.token + "\n")
        os.chmod(self.token_file, 0o600)
        cfg = base_config(self.port, self.token_file, self.broker.port)
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
        return [str(self.binary), *args, "--root", str(REPO_ROOT), "--state-dir", str(self.state_dir),
                "--config", str(self.config_path), "--log-dir", str(self.log_dir),
                "--error-dir", str(self.error_dir)]

    def start(self, timeout=30, broker=True):
        """Start the controller (and, the first time, its broker). broker=False starts no broker and returns as soon
        as the API answers, for broker_missing tests."""
        if broker and self.broker.proc is None:
            self.broker.start()
        out = open(self.tmp / f"{self.name}.stdout.log", "ab")
        self.proc = subprocess.Popen(self.cmd("run"), stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited with {self.proc.returncode}:\n{self.stdout_tail()}")
            try:
                h = self.client.health()
                if h.get("state") not in (None, "STARTING") and (not broker or h.get("broker", {}).get("state") == "ok"):
                    return self
            except (APIError, OSError):
                pass
            time.sleep(0.1)
        raise RuntimeError(f"server did not become healthy in {timeout}s:\n{self.stdout_tail()}")

    def stop(self, timeout=60):
        """Graceful: SIGTERM. The controller stops pulling and exits; running jobs keep going in their task hosts."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM if os.name != "nt" else signal.CTRL_BREAK_EVENT)
            try:
                self.proc.wait(timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        return self.proc.returncode if self.proc else None

    def kill(self):
        """Crash: SIGKILL the controller only. Task hosts and the broker keep running."""
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

    def taskhost_pids(self, running_only=False):
        """Pids of the task hosts recorded in this server's run directories (running_only: jobs not yet handled)."""
        pids = set()
        for f in (self.state_dir / "runs").glob("*/run.json"):
            if running_only and (f.parent / "handled").exists():
                continue
            try:
                pids.add(int(json.loads(f.read_text())["taskhost"]["pid"]))
            except (OSError, ValueError, KeyError, TypeError):
                pass
        return {p for p in pids if p > 0}

    def kill_taskhosts(self):
        for pid in self.taskhost_pids():
            try:
                os.kill(pid, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass

    def cleanup(self):
        self.kill()
        self.kill_taskhosts()
        self.broker.stop()
        if os.environ.get("KEEP_TEST_DIRS") != "1":
            shutil.rmtree(self.tmp, ignore_errors=True)

    def run_cli(self, *args, timeout=60):
        """Run a one-shot subcommand (init, validate, detect ...) against this server's state dir."""
        return subprocess.run(self.cmd(*args), capture_output=True, text=True, timeout=timeout)
