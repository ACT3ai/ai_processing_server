"""A real nats-server process for tests (pm/task_queue.mdx §21.2: the tests start the broker as its own process,
as production does; it is never embedded). Each BrokerProcess has its own port and JetStream store dir."""

import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

from .harness import free_port

NATS_SERVER = shutil.which("nats-server")


class BrokerProcess:
    def __init__(self):
        if not NATS_SERVER:
            raise RuntimeError("nats-server not on PATH")
        self.tmp = Path(tempfile.mkdtemp(prefix="aps_test_nats_"))
        self.port = free_port()
        self.url = f"nats://127.0.0.1:{self.port}"
        self.proc = None

    def start(self, timeout=10):
        log = open(self.tmp / "nats.log", "ab")
        self.proc = subprocess.Popen([NATS_SERVER, "-a", "127.0.0.1", "-p", str(self.port), "-js", "-sd", str(self.tmp / "js")],
                                     stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.2):
                    return self
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("nats-server did not start:\n" + (self.tmp / "nats.log").read_text(errors="replace")[-2000:])

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def cleanup(self):
        self.stop()
        if os.environ.get("KEEP_TEST_DIRS") != "1":
            shutil.rmtree(self.tmp, ignore_errors=True)


def queue_config(url, ack_wait="60s", heartbeat="1s"):
    """this_server.yaml overrides that turn the task queue on against url."""
    return {
        "queue": {"enabled": True, "role": "standalone", "broker_url": url, "broker_version": "2"},
        "continuity": {"ack_wait": ack_wait, "heartbeat": heartbeat},
        "callers": {"local": {"enabled": True}},
    }
