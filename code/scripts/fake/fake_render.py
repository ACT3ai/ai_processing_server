"""fake_render.py: behaves like a model render without one, for machines with no weights and for tests.

Used by the oss_video and oss_upscale runners when APS_FAKE_MODELS=on (never in production). Prints the same
ACT3_STAGE / ACT3_PROGRESS lines a real runner prints over --seconds, then writes --out: a real tiny H.264 MP4 when
ffmpeg is on PATH (a copy of --input for upscales), otherwise a placeholder file. A prompt containing FAKE_FAIL exits 1
with an out-of-memory line, so failure handling can be tested. Python 3 standard library only.
"""
import argparse
import os
import shutil
import subprocess
import sys
import time

a = argparse.ArgumentParser()
a.add_argument("--out", required=True)
a.add_argument("--input", default="")
a.add_argument("--prompt", default="")
a.add_argument("--seconds", type=float, default=4)
a.add_argument("--width", type=int, default=320)
a.add_argument("--height", type=int, default=180)
x = a.parse_args()

print("ACT3_STAGE loading", flush=True)
steps = 10
for i in range(1, steps + 1):
    if i == 2:
        print("ACT3_STAGE rendering", flush=True)
    time.sleep(x.seconds / steps)
    print(f"ACT3_PROGRESS {100 * i / steps:.1f}", flush=True)
if "FAKE_FAIL" in x.prompt:
    print("RuntimeError: MPS backend out of memory (FAKE)", file=sys.stderr, flush=True)
    sys.exit(1)
print("ACT3_STAGE encoding", flush=True)
os.makedirs(os.path.dirname(os.path.abspath(x.out)), exist_ok=True)
if x.input and os.path.exists(x.input):
    shutil.copyfile(x.input, x.out)
elif shutil.which("ffmpeg"):
    w, h = max(16, x.width // 2 * 2), max(16, x.height // 2 * 2)
    subprocess.run(["ffmpeg", "-y", "-v", "error", "-f", "lavfi", "-i", f"testsrc=size={w}x{h}:rate=16:duration=1",
                    "-c:v", "libx264", "-pix_fmt", "yuv420p", x.out], check=True)
else:
    with open(x.out, "wb") as f:
        f.write(b"\x00\x00\x00\x18ftypisomFAKE MP4")
print(f"wrote {x.out}", flush=True)
