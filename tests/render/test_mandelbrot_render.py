"""sys.render_test renders real frames: per-frame iteration counts are recomputed in Python and must match exactly,
the PNGs are real images of the right size, and (with ffmpeg) the MP4 is H.264 / yuv420p with every frame."""

import tempfile
import unittest
from pathlib import Path

from lib import reference
from lib.base import ServerTestCase

PARAMS = {"width": 64, "height": 48, "frames": 6, "max_iter": 120, "center_x": -0.7436438870371, "center_y": 0.1318259042053, "zoom": 2.0, "fps": 6}


class MandelbrotRender(ServerTestCase):
    def test_frames_match_reference(self):
        j = self.run_job("sys.render_test", {**PARAMS, "encode_video": False})
        r = j["result"]
        self.assertEqual(r["frames"], 6)
        self.assertFalse(r["video_encoded"])
        scales = reference.mandelbrot_scales(6, PARAMS["zoom"])
        total = 0
        for stat, scale in zip(r["frame_stats"], scales):
            want = reference.mandelbrot_frame(64, 48, 120, PARAMS["center_x"], PARAMS["center_y"], scale)
            self.assertEqual((stat["iter_sum"], stat["inside"]), (want["iter_sum"], want["inside"]), f"frame {stat['index']}")
            total += want["iter_sum"]
        self.assertEqual(r["total_iterations"], total)
        # Zooming in on the boundary needs more iterations per frame.
        self.assertGreater(r["frame_stats"][-1]["iter_sum"], r["frame_stats"][0]["iter_sum"])

    def test_png_outputs_are_real_images(self):
        j = self.run_job("sys.render_test", {**PARAMS, "encode_video": False})
        first = self.assertOutputIntact(j, "first_frame")
        last = self.assertOutputIntact(j, "last_frame")
        self.assertEqual(reference.png_size(first), (64, 48))
        self.assertEqual(reference.png_size(last), (64, 48))
        self.assertEqual(reference.sha256(first), j["result"]["frame_stats"][0]["png_sha256"])
        self.assertEqual(reference.sha256(last), j["result"]["frame_stats"][-1]["png_sha256"])
        self.assertNotEqual(first, last)

    @unittest.skipUnless(reference.HAVE_FFMPEG, "ffmpeg/ffprobe not installed")
    def test_video_encode(self):
        seen = []
        j = self.api.submit("sys.render_test", {**PARAMS, "frames": 12, "throttle_ms": 40, "encode_video": True})
        done = self.api.wait(j["id"], timeout=120, on_poll=lambda s: seen.append(s.get("step")))
        self.assertEqual(done["state"], "completed", done.get("last_err"))
        self.assertIn("render", seen)
        body = self.assertOutputIntact(done, "video")
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "render.mp4"
            p.write_bytes(body)
            info = reference.ffprobe(p)
        v = info["video"]
        self.assertEqual((v["codec_name"], v["pix_fmt"], v["width"], v["height"]), ("h264", "yuv420p", 64, 48))
        self.assertEqual(int(v["nb_read_frames"]), 12)
        self.assertAlmostEqual(info["duration"], 12 / 6, delta=0.2)

    def test_invalid_size_is_rejected(self):
        j = self.run_job("sys.render_test", {"width": 63, "height": 48}, expect="archived")
        self.assertEqual(j["error_kind"], "invalid_input")
        self.assertIn("even", j["last_err"])
