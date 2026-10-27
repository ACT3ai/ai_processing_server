"""The rest of the continuity gate for the model runners (pm/task_authoring.mdx §8 G3, G4, G6, G7; problems
2026_oct_27 #4). G1, G2 and G5 are in test_task_continuity.py.

Same setup as there: a real nats-server, the real binary, Wan 2.2 (oss_video) with APS_FAKE_MODELS=on so no weights
are needed. The render runs in a detached task host from a frozen snapshot.

* G3 broker restart: nats-server is stopped and started again mid-render. The claim survives, the job finishes once.
* G4 reboot: everything on the machine dies at once (controller SIGKILL, task host and render killed, broker
  restarted), then comes back. The job is redelivered, runs again, and has exactly one result.
* G6 twice: two servers on one broker. The worker holding a job is frozen (SIGSTOP) past the ack wait, so the
  broker delivers the job to the other worker too. Then the frozen one wakes up and its render finishes. Exactly one
  `succeeded` event, from the worker that holds the claim; the fenced-off worker publishes no result.
* G7 short jobs: a run of short jobs while the controller is restarted every few seconds (stop signal and SIGKILL in
  turn). Each finishes once, on attempt 1; the overhead per job, once the time the controller was down is taken
  out, stays under 2 s.

Needs a machine whose GPU the server detects (both services require one); skipped otherwise.
"""

import os
import signal
import subprocess
import threading
import time

from broker.base import BrokerTestCase
from lib.broker import BrokerProcess, queue_config
from lib.harness import ServerProcess, deep_merge

WAN = "act3.video_gen_open_source"
MODEL_SERVICES = [WAN, "act3.upscale_open_source"]


def wan_params(prompt="a red paper boat"):
    return {"model": "wan2.2-a14b", "kind": "t2v", "width": 320, "height": 176, "durationSeconds": 2, "prompt": prompt}


class ModelGateCase(BrokerTestCase):
    """One broker, SERVERS servers with both model services on, fake renders lasting FAKE_SECONDS."""
    ACK_WAIT = "60s"
    FAKE_SECONDS = "8"

    @classmethod
    def setUpClass(cls):
        cls._env = {k: os.environ.get(k) for k in ("APS_FAKE_MODELS", "APS_FAKE_SECONDS")}
        os.environ["APS_FAKE_MODELS"], os.environ["APS_FAKE_SECONDS"] = "on", cls.FAKE_SECONDS
        cls.broker = BrokerProcess().start()
        cls.servers = []
        try:
            for i in range(cls.SERVERS):
                srv = ServerProcess(deep_merge(queue_config(cls.broker.url, ack_wait=cls.ACK_WAIT), {}),
                                    name=f"{cls.__name__}_{i}")
                srv.config["server"]["key"] = f"TEST{i + 1}"
                srv.config["services"] += [{"id": s, "enabled": True, "max_parallel": 1} for s in MODEL_SERVICES]
                srv.config["hardware"]["ram_gb"] = 256  # the services reserve 64 GB per job
                srv.write_config(srv.config)
                cls.servers.append(srv.start())
                cls.wait_broker(srv)
        except Exception:
            cls.tearDownClass()
            raise
        cls.server, cls.api = cls.servers[0], cls.servers[0].client

    @classmethod
    def tearDownClass(cls):
        for s in getattr(cls, "servers", []):
            if s.proc and s.proc.poll() is None:
                try:
                    os.kill(s.proc.pid, signal.SIGCONT)  # never leave a frozen server behind
                except OSError:
                    pass
        super().tearDownClass()
        for k, v in cls._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def setUp(self):
        offers = {o["service"]: o for o in self.api.get("/v1/capabilities")["offers"]}
        if not offers.get(WAN, {}).get("parallel"):
            self.skipTest(f"{WAN} not offered here: {offers.get(WAN, {}).get('reason')}")

    @staticmethod
    def render_procs(job_id):
        out = subprocess.run(["pgrep", "-f", f"runs/{job_id}"], capture_output=True, text=True).stdout
        return [p for p in out.split() if p]

    def wait_rendering(self, client, job_id, timeout=30):
        return self.wait_local(client, job_id,
                               lambda j: j["caller_state"] == "RUNNING" and j.get("progress", 0) > 0.05, timeout=timeout)

    def assert_once(self, done, attempt=None):
        st = done["job_state"]
        self.assertEqual(st["state"], "SUCCEEDED", done)
        self.assertEqual(self.types(done).count("succeeded"), 1, self.types(done))
        if attempt is not None:
            self.assertEqual(st["attempt"], attempt, done)


class G3BrokerRestart(ModelGateCase):
    FAKE_SECONDS = "12"

    def test_g3_broker_restart_mid_render(self):
        job = self.api.queue_submit(WAN, wan_params(), max_attempts=1)["job_id"]
        self.wait_rendering(self.api, job)
        self.broker.stop()                                   # nats-server goes away mid-render
        time.sleep(3)
        self.assertTrue(self.render_procs(job), "the render died with the broker")
        self.broker.start()                                  # same port, same JetStream store
        self.wait_broker(self.server, "ok", timeout=30)
        done = self.api.queue_wait(job, timeout=90)
        self.assert_once(done, attempt=1)
        self.assertNotIn("requeued", self.types(done))
        self.assertFalse(self.render_procs(job))


class G4Reboot(ModelGateCase):
    ACK_WAIT = "5s"
    FAKE_SECONDS = "6"

    def test_g4_power_cycle_mid_render(self):
        job = self.api.queue_submit(WAN, wan_params(), max_attempts=2)["job_id"]
        self.wait_rendering(self.api, job)
        # The power goes: every process on the machine dies at the same moment.
        self.server.kill()
        for pid in self.render_procs(job):
            try:
                os.kill(int(pid), signal.SIGKILL)
            except OSError:
                pass
        self.broker.stop()
        time.sleep(1)
        self.assertFalse(self.render_procs(job), "a render outlived the power cut")
        # The machine boots: broker first, then the server (the order `just run` uses).
        self.broker.start()
        self.server.start()
        self.wait_broker(self.server, "ok", timeout=30)
        done = self.api.queue_wait(job, timeout=120)
        self.assert_once(done, attempt=2)                    # redelivered and run again, one result
        self.assertIn("requeued", self.types(done))
        self.assertFalse(self.render_procs(job))


class G6Twice(ModelGateCase):
    SERVERS = 2
    ACK_WAIT = "4s"
    FAKE_SECONDS = "10"

    def test_g6_duplicate_delivery_is_fenced(self):
        a, b = self.servers
        b.client.post("/v1/drain")                           # A takes the job
        job = a.client.queue_submit(WAN, wan_params(), max_attempts=3)["job_id"]
        self.wait_rendering(a.client, job)
        os.kill(a.proc.pid, signal.SIGSTOP)                  # A freezes: its claim stops being extended
        try:
            b.client.post("/v1/resume")
            # Past the ack wait the broker hands the job to B: a second delivery while A's render still runs.
            self.wait_rendering(b.client, job, timeout=40)
        finally:
            os.kill(a.proc.pid, signal.SIGCONT)              # A wakes up with a render that is no longer its job
        done = b.client.queue_wait(job, timeout=120)
        self.assert_once(done)
        st = done["job_state"]
        self.assertEqual(st["server"], "TEST2", done)        # the claim holder's result is the one
        final = [e for e in done["events"] if e["type"] == "succeeded"][0]
        self.assertEqual(final.get("server", "TEST2"), "TEST2", final)
        # A's late result never becomes a second success, even after A has had time to finish and report.
        time.sleep(float(self.FAKE_SECONDS) + 4)
        again = b.client.queue_job(job)
        self.assertEqual(self.types(again).count("succeeded"), 1, self.types(again))
        self.assertEqual(again["job_state"]["server"], "TEST2")


class G7ShortJobsUnderRestarts(ModelGateCase):
    FAKE_SECONDS = "3"
    JOBS = 5

    def test_g7_short_jobs_under_restart_storm(self):
        ids = [self.api.queue_submit(WAN, wan_params(f"boat {i}"), max_attempts=1)["job_id"] for i in range(self.JOBS)]
        stop, down = threading.Event(), [0.0]

        def storm():
            n = 0
            while not stop.wait(4):
                t0 = time.time()
                (self.server.stop if n % 2 == 0 else self.server.kill)()   # stop signal and SIGKILL in turn
                self.server.start()
                self.wait_broker(self.server, "ok", timeout=30)
                down[0] += time.time() - t0
                n += 1

        t0 = time.time()
        th = threading.Thread(target=storm, daemon=True)
        th.start()
        try:
            done = [self.api.queue_wait(i, timeout=180) for i in ids]
        finally:
            stop.set()
            th.join(60)
        wall = time.time() - t0
        for d in done:
            self.assert_once(d, attempt=1)
            self.assertNotIn("requeued", self.types(d))
        overhead = (wall - down[0] - self.JOBS * float(self.FAKE_SECONDS)) / self.JOBS
        print(f"\nG7: {self.JOBS} jobs in {wall:.1f} s, controller down {down[0]:.1f} s, overhead {overhead:.2f} s/job")
        self.assertLess(overhead, 2.0)
