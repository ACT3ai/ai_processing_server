"""The local API answers, protects itself with the bearer token, and describes the server truthfully."""

from lib.base import ServerTestCase
from lib.client import APIError


class HealthAndAuth(ServerTestCase):
    def test_health_needs_no_token(self):
        h = self.api.health()
        self.assertTrue(h["ok"])
        self.assertEqual(h["state"], "ONLINE")
        self.assertEqual(h["key"], "TEST1")
        self.assertGreater(h["pid"], 0)

    def test_routes_need_the_token(self):
        for path in ("/v1/server", "/v1/jobs", "/v1/capabilities", "/v1/queues"):
            with self.assertRaises(APIError) as e:
                self.api.get(path, token=False)
            self.assertEqual(e.exception.status, 401, path)
            self.assertEqual(e.exception.code, "unauthorized")
        with self.assertRaises(APIError) as e:
            self.api.get("/v1/server", token="wrong-token")
        self.assertEqual(e.exception.status, 401)

    def test_server_info(self):
        info = self.api.get("/v1/server")
        self.assertEqual(info["server"]["key"], "TEST1")
        self.assertEqual(info["limits"]["max_parallel_jobs"], 4)
        self.assertEqual(info["slots"]["cores_total"], min(8, info["slots"]["cores_total"]))
        self.assertIn("SUBMIT", info["transport"]["intake"])
        self.assertIn("NATS JetStream", info["transport"]["queue"])
        self.assertEqual(info["queue"]["role"], "standalone")
        self.assertEqual(info["broker"]["state"], "ok")
        self.assertNotIn("machine_id", str(info["server"]["bound_to"]))

    def test_hardware_min_rule(self):
        hw = self.api.get("/v1/hardware")
        det = hw["detected"]["cpu"]["logical_cores"]
        self.assertEqual(hw["effective"]["cpu"]["logical_cores"], min(det, 8))
        if det > 8:  # detected more than declared: offered the declared number, with an info drift
            self.assertTrue(any(d["field"] == "cpu.logical_cores" and d["level"] == "info" for d in hw["drift"]))

    def test_unknown_route_is_json_404(self):
        with self.assertRaises(APIError) as e:
            self.api.get("/v1/nope")
        self.assertEqual(e.exception.status, 404)
        self.assertEqual(e.exception.code, "not_found")

    def test_metrics_and_status_page(self):
        body, status, headers = self.api.request("GET", "/metrics", token=False, raw=True)
        self.assertEqual(status, 200)
        self.assertIn(b'aps_up{key="TEST1"', body)
        page, _, headers = self.api.request("GET", "/", token=False, raw=True)
        self.assertIn("text/html", headers["Content-Type"])
        self.assertIn(b"TEST1", page)

    def test_job_not_found(self):
        with self.assertRaises(APIError) as e:
            self.api.job("does-not-exist")
        self.assertEqual(e.exception.status, 404)

    def test_dns_rebinding_and_cross_site_calls_are_refused(self):
        import http.client
        for headers in ({"Host": "evil.example.com"}, {"Origin": "https://evil.example.com"}):
            c = http.client.HTTPConnection("127.0.0.1", self.server.port, timeout=5)
            c.request("GET", "/v1/health", headers=headers)
            r = c.getresponse()
            self.assertEqual(r.status, 403, headers)
            self.assertIn(b"forbidden_origin", r.read())
            c.close()
        c = http.client.HTTPConnection("127.0.0.1", self.server.port, timeout=5)
        c.request("GET", "/v1/health", headers={"Origin": f"http://localhost:{self.server.port}"})
        self.assertEqual(c.getresponse().status, 200)
