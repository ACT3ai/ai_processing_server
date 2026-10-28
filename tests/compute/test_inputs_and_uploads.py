"""Inputs are downloaded and verified; outputs go to presigned-style URLs; signatures never leak."""

import json
import random
import tempfile
from pathlib import Path

from lib import reference
from lib.base import ServerTestCase


def corpus(seed=7, words=60_000):
    rnd = random.Random(seed)
    vocab = ["shot", "scene", "camera", "light", "actor", "render", "frame", "dolly", "lens", "cut", "take", "Beat", "ACT3"]
    lines, line = [], []
    for _ in range(words):
        line.append(rnd.choice(vocab))
        if rnd.random() < 0.08:
            lines.append(" ".join(line))
            line = []
    lines.append(" ".join(line))
    return ("\n".join(lines) + "\n").encode()


class InputsAndUploads(ServerTestCase):
    STORAGE = True

    def test_file_stats_from_url(self):
        body = corpus()
        inp = self.storage.add_file("script.txt", body)
        j = self.run_job("sys.compute", {"op": "file_stats"}, inputs=[inp])
        want = reference.file_stats(body)
        got = {k: j["result"][k] for k in want}
        self.assertEqual(got, want)

    def test_sha256_mismatch_is_invalid_input(self):
        inp = self.storage.add_file("bad.txt", b"hello\n")
        inp["sha256"] = "0" * 64
        j = self.run_job("sys.compute", {"op": "file_stats"}, inputs=[inp], expect="archived")
        self.assertEqual(j["error_kind"], "invalid_input")
        self.assertIn("sha256 mismatch", j["last_err"])
        self.assertEqual(j["error_stage"], "STAGING")

    def test_missing_input_is_invalid_input(self):
        j = self.run_job("sys.compute", {"op": "file_stats"}, inputs=[{"name": "x.txt", "url": self.storage.base + "/files/missing"}],
                         expect="archived")
        self.assertEqual(j["error_kind"], "invalid_input")

    def test_transient_download_failure_is_retried_inside_the_stage(self):
        body = b"one two two\n"
        inp = self.storage.add_file("flaky.txt", body)
        self.storage.fail_next_gets(1)  # first GET answers 503, the stage retries after 1 s
        j = self.run_job("sys.compute", {"op": "file_stats"}, inputs=[inp])
        self.assertEqual(j["result"]["words"], 3)
        self.assertEqual(j["retried"], 0)  # stage retry, not a whole-job retry
        self.assertGreaterEqual(self.storage.gets.count("flaky.txt"), 2)

    def test_local_path_input_in_direct_mode(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "local.txt"
            p.write_bytes(b"alpha beta\ngamma\n")
            j = self.run_job("sys.compute", {"op": "file_stats"}, inputs=[{"name": "local.txt", "path": str(p)}])
        self.assertEqual((j["result"]["lines"], j["result"]["words"]), (2, 3))

    def test_output_upload_and_redaction(self):
        url = self.storage.upload_url("results/primes.json")
        j = self.run_job("sys.compute", {"op": "primes", "limit": 50_000},
                         outputs=[{"name": "result", "content_type": "application/json", "upload": {"method": "PUT", "url": url}}])
        up = self.storage.uploads["results/primes.json"]
        self.assertEqual(json.loads(up["body"]), j["result"])
        out = j["outputs"][0]
        self.assertEqual(out["sha256"], reference.sha256(up["body"]))
        self.assertEqual(out["bytes"], len(up["body"]))
        self.assertEqual(up["headers"].get("Content-Type"), "application/json")
        # The signed query string must not appear anywhere the API or the job log exposes.
        self.assertNotIn("s3cr3t", json.dumps(j))
        self.assertNotIn("s3cr3t", self.api.log(j["id"]))
        self.assertTrue(out["uploaded_to"].endswith("/bucket/results/primes.json"))
        self.assertNotIn("download", out)  # uploaded, not kept

    def test_input_names_must_be_unique(self):
        from lib.client import APIError
        a = self.storage.add_file("a.txt", b"a")
        with self.assertRaises(APIError) as e:
            self.api.submit("sys.compute", {"op": "file_stats"}, inputs=[a, a])
        self.assertEqual(e.exception.code, "invalid_input")
