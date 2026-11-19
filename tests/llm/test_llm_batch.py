"""act3.llm_batch black-box: the LLM render task through the real binary, against a fake Ollama (always) and the real
local Ollama (when APS_TEST_OLLAMA_URL and APS_TEST_OLLAMA_MODEL are set, e.g. on BMS or a dev Mac running `ollama serve`).

Spec: ~/BGit/all/film/pm/subp/llm_gen_ai/mac_studio/llm_render_task.mdx §6-§8.
"""

import json
import os
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from lib.base import ServerTestCase


class FakeOllama:
    """A loopback Ollama: /api/version, /api/tags, /api/ps, /api/chat. Prompts containing 'FAIL' answer 500."""

    def __init__(self, models=("qwen3.8-flash-next:latest",), delay=0.05):
        self.models, self.delay, self.lock = list(models), delay, threading.Lock()
        self.inflight = self.max_inflight = self.calls = 0
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def send(self, code, obj):
                b = json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)

            def do_GET(self):
                if self.path == "/api/version":
                    return self.send(200, {"version": "0.40.16"})
                if self.path == "/api/tags":
                    return self.send(200, {"models": [{"name": m} for m in outer.models]})
                if self.path == "/api/ps":
                    return self.send(200, {"models": []})
                self.send(404, {"error": "not found"})

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])) or b"{}")
                last = body["messages"][-1]["content"]
                with outer.lock:
                    outer.calls += 1
                    outer.inflight += 1
                    outer.max_inflight = max(outer.max_inflight, outer.inflight)
                try:
                    if "single word OK" not in last:
                        time.sleep(outer.delay)
                    if "FAIL" in last:
                        return self.send(500, {"error": "llama runner process has terminated"})
                    self.send(200, {"model": body["model"], "message": {"role": "assistant", "content": "echo " + last},
                                    "done": True, "done_reason": "stop", "prompt_eval_count": 10, "eval_count": 5,
                                    "prompt_eval_duration": 10**7, "eval_duration": 5 * 10**7})
                finally:
                    with outer.lock:
                        outer.inflight -= 1

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()


class LLMBatchFake(ServerTestCase):
    SERVICES = {"act3.llm_batch": {"enabled": True}}

    @classmethod
    def setUpClass(cls):
        cls.ollama = FakeOllama()
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.ollama.close()

    def batch(self, n, fail_ids=(), **params):
        prompts = [{"prompt_id": f"p{i:03d}", "prompt": ("FAIL " if i in fail_ids else "") + f"question {i}"} for i in range(n)]
        p = {"models": ["qwen3.8-flash-next"], "ollama_url": self.ollama.url, "max_parallel": 6, "retries": 0,
             "stagger_ms": -1, "prompts": prompts}
        p.update(params)
        return p

    def report(self, job):
        body, _ = self.api.output(job["id"], "llm_report")
        return json.loads(body)

    def test_batch_of_40_six_in_flight(self):
        j = self.run_job("act3.llm_batch", self.batch(40), timeout=120)
        r = self.report(j)
        self.assertEqual((r["summary"]["ok"], r["summary"]["failed"]), (40, 0))
        self.assertEqual([s["name"] for s in r["stages"]], ["load_prompts", "preflight", "select_model", "warm", "prompts", "report"])
        self.assertLessEqual(self.ollama.max_inflight, 6)
        self.assertGreaterEqual(self.ollama.max_inflight, 4)
        res = json.loads(self.api.output(j["id"], "llm_results")[0])
        self.assertEqual(len(res["results"]), 40)
        self.assertEqual(j["result"]["ok"], 40)

    def test_one_prompt(self):
        j = self.run_job("act3.llm_batch", self.batch(1))
        self.assertEqual(self.report(j)["summary"]["ok"], 1)

    def test_partial_failure_is_reported_not_fatal(self):
        j = self.run_job("act3.llm_batch", self.batch(10, fail_ids=(3, 7)), timeout=120)
        r = self.report(j)
        self.assertEqual((r["summary"]["ok"], r["summary"]["failed"]), (8, 2))
        bad = [p for p in r["prompts"] if p["status"] == "failed"]
        self.assertTrue(all(p["attempts"][0]["diagnosis"]["answering"] for p in bad))
        self.assertEqual(r["stages"][4]["status"], "partial")

    def test_no_model_fails_infra_with_reason(self):
        # infra is retried (another attempt may land on a machine that has the model), then the job is archived
        j = self.run_job("act3.llm_batch", self.batch(2, models=["no-such-model"]), expect=None, timeout=120)
        self.assertIn(j["state"], ("failed", "archived"))
        self.assertEqual(j.get("error_kind"), "infra")
        self.assertIn("none of the asked models", j.get("last_err", ""))


@unittest.skipUnless(os.environ.get("APS_TEST_OLLAMA_URL") and os.environ.get("APS_TEST_OLLAMA_MODEL"),
                     "set APS_TEST_OLLAMA_URL and APS_TEST_OLLAMA_MODEL to run against a real Ollama")
class LLMBatchRealOllama(ServerTestCase):
    SERVICES = {"act3.llm_batch": {"enabled": True}}

    def test_six_real_prompts(self):
        prompts = [{"prompt_id": f"r{i}", "prompt": f"In one sentence, what is {i} times 7? Give the number."} for i in range(6)]
        p = {"models": [os.environ["APS_TEST_OLLAMA_MODEL"]], "ollama_url": os.environ["APS_TEST_OLLAMA_URL"],
             "max_parallel": 6, "num_ctx": 4096, "max_output_tokens": 256, "prompts": prompts}
        j = self.run_job("act3.llm_batch", p, timeout=900)
        r = json.loads(self.api.output(j["id"], "llm_report")[0])
        self.assertEqual(r["summary"]["ok"], 6, json.dumps(r["summary"]))
        print(f"\n  real Ollama: {r['model']} {r['summary']['wall_ms']} ms wall, "
              f"{r['summary']['aggregate_decode_tps']} tok/s aggregate")
