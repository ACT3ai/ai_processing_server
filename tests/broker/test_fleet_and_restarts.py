"""Several servers on one broker, crashes and broker loss (pm/task_queue.mdx §9.4, §16, §21.2)."""

import time

from broker.base import BrokerTestCase
from lib.broker import NATS_SERVER, BrokerProcess, queue_config
from lib.client import APIError
from lib.harness import ServerProcess


class TwoServersShareOneQueue(BrokerTestCase):
    SERVERS = 2

    def test_each_job_runs_exactly_once(self):
        ids = [self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 300})["job_id"] for _ in range(8)]
        done = [self.api.queue_wait(i, timeout=60) for i in ids]
        ran_on = set()
        for d in done:
            self.assertEqual(d["job_state"]["state"], "SUCCEEDED")
            self.assertEqual(self.types(d).count("claimed"), 1, d)  # one claim, no redelivery
            ran_on.add(d["job_state"]["server"])
        self.assertEqual(ran_on, {"TEST1", "TEST2"})  # max_parallel 1 each: both had to take work
        # Each job exists in exactly one server's local table.
        for i, d in zip(ids, done):
            holders = []
            for s in self.servers:
                try:
                    s.client.job(i)
                    holders.append(s.config["server"]["key"])
                except APIError:
                    pass
            self.assertEqual(holders, [d["job_state"]["server"]])
        keys = {s["key"] for s in self.api.broker()["servers"]}
        self.assertEqual(keys, {"TEST1", "TEST2"})


class CrashedServerHandsOver(BrokerTestCase):
    """A killed worker's claim lapses after the ack wait and another worker finishes the job (attempt 2)."""
    SERVERS = 2
    ACK_WAIT = "4s"

    def test_ack_wait_takeover(self):
        a, b = self.servers
        b.client.post("/v1/drain")  # A takes the job
        ack = a.client.queue_submit("sys.compute", {"op": "sleep", "ms": 1500, "steps": 15}, max_attempts=3)
        self.wait_local(a.client, ack["job_id"], lambda j: j["caller_state"] == "RUNNING")
        a.kill()
        b.client.post("/v1/resume")
        t0 = time.time()
        done = b.client.queue_wait(ack["job_id"], timeout=30)
        self.assertEqual(done["job_state"]["state"], "SUCCEEDED")
        self.assertEqual((done["job_state"]["server"], done["job_state"]["attempt"]), ("TEST2", 2))
        self.assertLess(time.time() - t0, 15)


class RestartReturnsClaim(BrokerTestCase):
    """With a long ack wait, a restarted server returns its saved claim at once instead of after 60 s (§9.4)."""
    ACK_WAIT = "60s"

    def test_restart_requeues_saved_claim(self):
        ack = self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 1500, "steps": 15}, max_attempts=3)
        self.wait_local(self.api, ack["job_id"], lambda j: j["caller_state"] == "RUNNING" and "claim" in j)
        self.server.kill()
        t0 = time.time()
        self.server.start()
        self.wait_broker(self.server)
        done = self.api.queue_wait(ack["job_id"], timeout=30)
        self.assertLess(time.time() - t0, 20)  # far inside the 60 s ack wait
        self.assertEqual((done["job_state"]["state"], done["job_state"]["attempt"]), ("SUCCEEDED", 2))
        req = [e for e in done["events"] if e["type"] == "requeued"]
        self.assertTrue(req and "restarted" in req[0]["message"], done["events"])


class BrokerMissing(BrokerTestCase):
    """No broker at the configured URL: the server still starts, says broker_missing with the fix, and direct
    mode keeps working (§21.3 Q1). When the broker comes up later, the server connects on its own."""

    @classmethod
    def setUpClass(cls):
        cls.broker = BrokerProcess()  # not started yet
        srv = ServerProcess(queue_config(cls.broker.url), cls.SERVICES, name=cls.__name__)
        cls.servers = [srv.start()]
        cls.server, cls.api = srv, srv.client

    def test_broker_missing_then_connects(self):
        b = self.api.health()["broker"]
        self.assertIn(b["state"], ("broker_missing", "connecting"))
        self.wait_broker(self.server, "broker_missing")
        b = self.api.health()["broker"]
        self.assertIn("just broker-run", b["fix"])
        self.assertTrue(self.api.health()["ok"])
        with self.assertRaises(APIError) as e:
            self.api.queue_submit("sys.compute", {"op": "sleep", "ms": 1})
        self.assertEqual((e.exception.status, e.exception.code), (503, "broker_missing"))
        j = self.api.submit("sys.compute", {"op": "sleep", "ms": 5})  # direct mode is unaffected
        self.assertEqual(self.api.wait(j["id"])["state"], "completed")
        # Start the broker on the configured port: the server connects by itself (retries every 5 s).
        self.broker.start()
        self.wait_broker(self.server, "ok", timeout=20)
        _, done = self.run_queued({"op": "sleep", "ms": 5})
        self.assertEqual(done["job_state"]["server"], "TEST1")
