"""act3.transcode, wtc.transcode and act3.video_merge with real video: inputs come over HTTP, outputs are uploaded,
and ffprobe on the uploaded bytes checks codec, pixel format, size and duration."""

import tempfile
import unittest
from pathlib import Path

from lib import reference
from lib.base import ServerTestCase


@unittest.skipUnless(reference.HAVE_FFMPEG, "ffmpeg/ffprobe not installed")
class FFmpegJobs(ServerTestCase):
    STORAGE = True

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.tmp = tempfile.TemporaryDirectory()
        d = Path(cls.tmp.name)
        cls.clip_a = reference.make_test_video(d / "a.mp4", 2.0)
        cls.clip_b = reference.make_test_video(d / "b.mp4", 1.0, freq=660)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()
        super().tearDownClass()

    def probe_upload(self, name):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "out.mp4"
            p.write_bytes(self.storage.uploads[name]["body"])
            return reference.ffprobe(p)

    def test_transcode_to_h264_and_scale(self):
        inp = self.storage.add_file("a.mp4", self.clip_a.read_bytes())
        progress = []
        j = self.api.submit("act3.transcode", {"height": 120, "crf": 28}, inputs=[inp],
                            outputs=[{"name": "video", "upload": {"method": "PUT", "url": self.storage.upload_url("out/a_120p.mp4")}}])
        done = self.api.wait(j["job_id"], timeout=120, on_poll=lambda s: progress.append(s["progress"]))
        self.assertEqual(done["state"], "SUCCEEDED", done.get("error_message"))
        self.assertEqual(progress, sorted(progress), "progress must never go backwards")
        info = self.probe_upload("out/a_120p.mp4")
        v = info["video"]
        self.assertEqual((v["codec_name"], v["pix_fmt"], v["width"], v["height"]), ("h264", "yuv420p", 160, 120))
        self.assertEqual(info["audio"]["codec_name"], "aac")
        self.assertAlmostEqual(info["duration"], 2.0, delta=0.15)
        r = done["result"]
        self.assertEqual(r["inputs"][0]["codec"], "mpeg4")  # really converted, not copied
        self.assertEqual(r["output"]["codec"], "h264")

    def test_wtc_transcode_keeps_output_on_server(self):
        inp = self.storage.add_file("b.mp4", self.clip_b.read_bytes())
        j = self.run_job("wtc.transcode", {}, inputs=[inp], timeout=120)
        body = self.assertOutputIntact(j, "video")
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "o.mp4"
            p.write_bytes(body)
            info = reference.ffprobe(p)
        self.assertEqual(info["video"]["codec_name"], "h264")  # never AV1 for X
        self.assertEqual((info["video"]["width"], info["video"]["height"]), (320, 240))

    def test_merge_concatenates_in_order(self):
        a = self.storage.add_file("m_a.mp4", self.clip_a.read_bytes())
        b = self.storage.add_file("m_b.mp4", self.clip_b.read_bytes())
        j = self.run_job("act3.video_merge", {}, inputs=[a, b], timeout=120,
                         outputs=[{"name": "video", "upload": {"method": "PUT", "url": self.storage.upload_url("scene/merged.mp4")}}])
        info = self.probe_upload("scene/merged.mp4")
        self.assertAlmostEqual(info["duration"], 3.0, delta=0.2)
        self.assertAlmostEqual(j["result"]["input_duration_s"], 3.0, delta=0.15)
        self.assertEqual(info["video"]["codec_name"], "h264")

    def test_merge_needs_two_inputs(self):
        a = self.storage.add_file("only.mp4", self.clip_b.read_bytes())
        j = self.run_job("act3.video_merge", {}, inputs=[a], expect="FAILED")
        self.assertEqual(j["error_kind"], "invalid_input")

    def test_not_a_video(self):
        bad = self.storage.add_file("notvideo.mp4", b"this is text, not a video\n")
        j = self.run_job("act3.transcode", {}, inputs=[bad], expect="FAILED", max_attempts=1)
        self.assertEqual(j["error_kind"], "invalid_input")
        self.assertIn("not a readable video", j["error_message"])
