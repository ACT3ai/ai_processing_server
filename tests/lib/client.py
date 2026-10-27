"""A small client for the server's local API (pm/ai_processing_server.mdx §13). Standard library only."""

import json
import time
import urllib.error
import urllib.request

FINAL = ("completed", "archived", "cancelled")


class APIError(Exception):
    def __init__(self, status, body):
        self.status = status
        self.body = body
        try:
            self.json = json.loads(body)
        except ValueError:
            self.json = None
        super().__init__(f"HTTP {status}: {body[:300]}")

    @property
    def code(self):
        return (self.json or {}).get("error", {}).get("code")


class Client:
    def __init__(self, base, token):
        self.base = base.rstrip("/")
        self.token = token

    def request(self, method, path, body=None, token=True, raw=False, timeout=30):
        data = None
        headers = {}
        if body is not None:
            data = body if isinstance(body, bytes) else json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        if token:
            headers["Authorization"] = "Bearer " + (self.token if token is True else token)
        req = urllib.request.Request(self.base + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                payload = resp.read()
                if raw:
                    return payload, resp.status, dict(resp.headers)
                return json.loads(payload) if payload else None
        except urllib.error.HTTPError as e:
            raise APIError(e.code, e.read().decode(errors="replace")) from None

    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def post(self, path, body=None, **kw):
        return self.request("POST", path, body=body, **kw)

    # ---- convenience ----

    def health(self):
        return self.get("/v1/health", token=False, timeout=2)

    def submit(self, service, params=None, **fields):
        env = {"service": service, **fields}
        if params is not None:
            env["params"] = params
        return self.post("/v1/jobs", env)

    def job(self, job_id):
        return self.get(f"/v1/jobs/{job_id}")

    def jobs(self, **filters):
        q = "&".join(f"{k}={v}" for k, v in filters.items())
        return self.get("/v1/jobs" + ("?" + q if q else ""))["jobs"]

    def cancel(self, job_id):
        return self.post(f"/v1/jobs/{job_id}/cancel")

    def log(self, job_id, tail=None):
        path = f"/v1/jobs/{job_id}/log" + (f"?tail={tail}" if tail else "")
        body, _, _ = self.request("GET", path, raw=True)
        return body.decode(errors="replace")

    def output(self, job_id, name):
        body, _, headers = self.request("GET", f"/v1/jobs/{job_id}/outputs/{name}", raw=True)
        return body, headers

    def offers(self):
        return {o["service"]: o for o in self.get("/v1/capabilities")["offers"]}

    def wait(self, job_id, states=FINAL, timeout=60, interval=0.05, on_poll=None):
        """Poll until the job reaches one of states. on_poll(job) sees every snapshot (progress tracking)."""
        deadline = time.time() + timeout
        while True:
            j = self.job(job_id)
            if on_poll:
                on_poll(j)
            if j["state"] in states:
                return j
            if time.time() > deadline:
                raise TimeoutError(f"job {job_id} still {j['state']}/{j['stage']} after {timeout}s: {j.get('note')}")
            time.sleep(interval)

    # ---- task queue (pm/task_queue.mdx §13) ----

    def queue_submit(self, service, params=None, **fields):
        env = {"service": service, **fields}
        if params is not None:
            env["params"] = params
        return self.post("/v1/queue/jobs", env)

    def queue_job(self, job_id):
        return self.get(f"/v1/queue/jobs/{job_id}")

    def queue_cancel(self, job_id):
        return self.post(f"/v1/queue/jobs/{job_id}/cancel")

    def broker(self):
        return self.get("/v1/broker")

    def queue_wait(self, job_id, timeout=60, interval=0.1, final=("SUCCEEDED", "FAILED", "CANCELLED", "EXPIRED")):
        """Poll job_state until it is final (or one of `final`); returns {job_state, revision, events}."""
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                last = self.queue_job(job_id)
                if last["job_state"]["state"] in final:
                    return last
            except APIError as e:
                if e.status != 404:
                    raise
            time.sleep(interval)
        raise TimeoutError(f"queued job {job_id} not in {final} after {timeout}s: {last and last['job_state']}")

    def wait_until(self, job_id, predicate, timeout=30, interval=0.03):
        deadline = time.time() + timeout
        while time.time() < deadline:
            j = self.job(job_id)
            if predicate(j):
                return j
            time.sleep(interval)
        raise TimeoutError(f"condition not met for job {job_id} within {timeout}s")
