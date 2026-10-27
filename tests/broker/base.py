"""BrokerTestCase: one real nats-server plus one or more real servers per test class."""

import time
import unittest

from lib.broker import NATS_SERVER, BrokerProcess, queue_config
from lib.client import APIError
from lib.harness import ServerProcess, deep_merge


@unittest.skipUnless(NATS_SERVER, "nats-server not on PATH (brew install nats-server; see data/queue/broker.yaml)")
class BrokerTestCase(unittest.TestCase):
    SERVERS = 1         # how many servers share the broker
    ACK_WAIT = "60s"
    CONFIG = None       # extra this_server.yaml overrides for every server
    SERVICES = {"sys.compute": {"max_parallel": 1}}

    @classmethod
    def setUpClass(cls):
        cls.broker = BrokerProcess().start()
        cls.servers = []
        try:
            for i in range(cls.SERVERS):
                cfg = deep_merge(queue_config(cls.broker.url, ack_wait=cls.ACK_WAIT), cls.CONFIG or {})
                srv = ServerProcess(cfg, cls.SERVICES, name=f"{cls.__name__}_{i}")
                srv.config["server"]["key"] = f"TEST{i + 1}"
                srv.write_config(srv.config)
                cls.servers.append(srv.start())
                cls.wait_broker(srv)
        except Exception:
            cls.tearDownClass()
            raise
        cls.server = cls.servers[0]
        cls.api = cls.server.client

    @classmethod
    def tearDownClass(cls):
        for s in getattr(cls, "servers", []):
            s.cleanup()
        cls.broker.cleanup()

    @staticmethod
    def wait_broker(srv, state="ok", timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if srv.client.health()["broker"]["state"] == state:
                return
            time.sleep(0.1)
        raise AssertionError(f"broker state never became {state}: {srv.client.health()['broker']}\n{srv.stdout_tail()}")

    def run_queued(self, params, expect="SUCCEEDED", timeout=60, api=None, **fields):
        api = api or self.api
        ack = api.queue_submit("sys.compute", params, **fields)
        done = api.queue_wait(ack["job_id"], timeout=timeout)
        self.assertEqual(done["job_state"]["state"], expect, done)
        return ack["job_id"], done

    @staticmethod
    def wait_local(client, job_id, predicate, timeout=20):
        """Wait until the job is in this server's own table and predicate(job) holds (404 until it is pulled)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                j = client.job(job_id)
                if predicate(j):
                    return j
            except APIError as e:
                if e.status != 404:
                    raise
            time.sleep(0.03)
        raise AssertionError(f"job {job_id} never met the condition on this server")

    @staticmethod
    def types(done):
        return [e["type"] for e in sorted(done["events"], key=lambda e: e["seq"])]
