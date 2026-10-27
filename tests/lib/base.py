"""ServerTestCase: one real server per test class, plus helpers every test uses."""

import unittest

from .harness import ServerProcess
from .storage import FakeStorage


class ServerTestCase(unittest.TestCase):
    # Subclasses override these to shape the server under test.
    CONFIG = None     # deep-merged into harness.base_config
    SERVICES = None   # per-service settings, e.g. {"sys.compute": {"max_parallel": 2}}
    STORAGE = False   # start a FakeStorage (inputs and uploads)

    @classmethod
    def setUpClass(cls):
        cls.server = ServerProcess(cls.CONFIG, cls.SERVICES, name=cls.__name__)
        cls.storage = FakeStorage() if cls.STORAGE else None
        try:
            cls.server.start()
        except Exception:
            cls.server.cleanup()
            raise
        cls.api = cls.server.client

    @classmethod
    def tearDownClass(cls):
        if cls.storage:
            cls.storage.close()
        cls.server.cleanup()

    def run_job(self, service, params=None, timeout=60, expect="completed", **fields):
        """Submit, wait for a final state, and assert it (with the job log in the failure message)."""
        j = self.api.submit(service, params, **fields)
        done = self.api.wait(j["id"], timeout=timeout)
        if expect:
            self.assertEqual(done["state"], expect,
                             f"job {j['id']} ended {done['state']}: {done.get('error_kind')} {done.get('last_err')}\n"
                             f"--- job log ---\n{self.api.log(j['id'], tail=30)}")
        return done

    def assertOutputIntact(self, job, name):
        """Download a kept output and check its bytes against the size and sha256 the server reported."""
        from . import reference
        out = next(o for o in job["outputs"] if o["name"] == name)
        body, _ = self.api.output(job["id"], name)
        self.assertEqual(len(body), out["bytes"])
        self.assertEqual(reference.sha256(body), out["sha256"])
        return body
