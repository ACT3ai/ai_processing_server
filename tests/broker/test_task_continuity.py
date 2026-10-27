"""Task continuity for the model runners (pm/task_authoring.mdx §8 G1, G2, G5; pm/task_queue.mdx §9).

A Wan 2.2 job (oss_video, with APS_FAKE_MODELS=on so no weights are needed) renders in a detached task host from a
frozen snapshot. The controller is killed with SIGKILL mid-render and the runner script is changed on disk (a git
pull); the restarted controller re-adopts the claim, the render finishes once on attempt 1, and the output names the
snapshot it came from. A SeedVR2 upscale cancelled mid-render stops with nothing left running.

Needs a machine whose GPU the server detects (both services require one); skipped otherwise.
"""

import os
import subprocess
import time

from broker.base import BrokerTestCase
from lib.broker import queue_config
from lib.harness import REPO_ROOT, ServerProcess, deep_merge

FAKE_SCRIPT = REPO_ROOT / "code" / "scripts" / "fake" / "fake_render.py"
MODEL_SERVICES = ["act3.video_gen_open_source", "act3.upscale_open_source"]


class ModelTasksSurviveRestarts(BrokerTestCase):
    ACK_WAIT = "60s"

    @classmethod
    def setUpClass(cls):
        cls._env = {k: os.environ.get(k) for k in ("APS_FAKE_MODELS", "APS_FAKE_SECONDS")}
        os.environ["APS_FAKE_MODELS"], os.environ["APS_FAKE_SECONDS"] = "on", "8"
        from lib.broker import BrokerProcess
        cls.broker = BrokerProcess().start()
        cls.servers = []
        srv = ServerProcess(deep_merge(queue_config(cls.broker.url, ack_wait=cls.ACK_WAIT), {}), name=cls.__name__)
        srv.config["server"]["key"] = "TEST1"
        srv.config["services"] += [{"id": s, "enabled": True} for s in MODEL_SERVICES]
        srv.config["hardware"]["ram_gb"] = 256  # the services reserve 64 GB per job
        srv.write_config(srv.config)
        cls.servers.append(srv.start())
        cls.wait_broker(srv)
        cls.server, cls.api = srv, srv.client

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        for k, v in cls._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def setUp(self):
        offers = {o["service"]: o for o in self.api.get("/v1/capabilities")["offers"]}
        for s in MODEL_SERVICES:
            if not offers.get(s, {}).get("parallel"):
                self.skipTest(f"{s} not offered here: {offers.get(s, {}).get('reason')}")

    def render_procs(self, job_id):
        out = subprocess.run(["pgrep", "-f", f"runs/{job_id}"], capture_output=True, text=True).stdout
        return [p for p in out.split() if p]

    def test_g1_g2_kill_and_pull_mid_render(self):
        ack = self.api.queue_submit("act3.video_gen_open_source", {
            "model": "wan2.2-a14b", "kind": "t2v", "width": 320, "height": 176, "durationSeconds": 2,
            "prompt": "a red paper boat"}, max_attempts=1)
        job = ack["job_id"]
        self.wait_local(self.api, job, lambda j: j["caller_state"] == "RUNNING" and j.get("progress", 0) > 0.05)
        self.server.kill()                                     # G1: SIGKILL the controller mid-render
        self.assertTrue(self.render_procs(job), "the render died with the controller")
        original = FAKE_SCRIPT.read_text()
        try:
            FAKE_SCRIPT.write_text(original + "\nraise SystemExit('new code ran')\n")  # G2: a pull breaks the script
            self.server.start()
            self.wait_broker(self.server)
            done = self.api.queue_wait(job, timeout=60)
        finally:
            FAKE_SCRIPT.write_text(original)
        st = done["job_state"]
        self.assertEqual((st["state"], st["attempt"]), ("SUCCEEDED", 1), done)
        types = self.types(done)
        self.assertEqual(types.count("succeeded"), 1)
        self.assertNotIn("requeued", types)
        final = [e for e in done["events"] if e["type"] == "succeeded"][0]
        self.assertTrue(final["result"]["code_version"], final)  # the snapshot the output came from
        self.assertEqual(final["outputs"][0]["name"], "video")
        self.assertFalse(self.render_procs(job))

    def test_g5_cancel_mid_upscale(self):
        src = self.server.tmp / "source.mp4"
        src.write_bytes(b"\x00\x00\x00\x18ftypisomSOURCE")
        ack = self.api.queue_submit("act3.upscale_open_source", {
            "model": "seedvr2-standard", "width": 1280, "height": 720, "durationSeconds": 2},
            inputs=[{"name": "source_video", "path": str(src)}], max_attempts=1)
        job = ack["job_id"]
        self.wait_local(self.api, job, lambda j: j["caller_state"] == "RUNNING" and j.get("progress", 0) > 0.05)
        self.assertTrue(self.render_procs(job))
        t0 = time.time()
        self.api.post(f"/v1/queue/jobs/{job}/cancel")
        done = self.api.queue_wait(job, timeout=30)
        self.assertEqual(done["job_state"]["state"], "CANCELLED", done)
        self.assertLess(time.time() - t0, 30)
        time.sleep(0.5)
        self.assertFalse(self.render_procs(job), "the render is still running after cancel")
