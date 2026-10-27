"""Independent reimplementations of every calculation the server does, so tests check results, not just status.

None of this imports or calls the Go code. If the server and this file agree, the work was really done.
"""

import hashlib
import json
import shutil
import struct
import subprocess


def primes(limit):
    """Count, sum and largest prime <= limit (plain sieve of Eratosthenes)."""
    sieve = bytearray([1]) * (limit + 1)
    sieve[0:2] = b"\x00\x00"
    i = 2
    while i * i <= limit:
        if sieve[i]:
            sieve[i * i::i] = bytes(len(range(i * i, limit + 1, i)))
        i += 1
    count = total = largest = 0
    for n in range(limit + 1):
        if sieve[n]:
            count += 1
            total += n
            largest = n
    return {"count": count, "sum": total, "largest": largest}


def sha256_chain(seed, iterations):
    h = hashlib.sha256(seed.encode()).digest()
    for _ in range(iterations - 1):
        h = hashlib.sha256(h).digest()
    return h.hex()


def lcg_values(seed):
    """x = (1103515245·x + 12345) mod 2^31, value = x mod 101 − 50 (same generator as the compute runner)."""
    x = seed
    while True:
        x = (1103515245 * x + 12345) & 0x7FFFFFFF
        yield x % 101 - 50


def matrix(n, seed):
    g = lcg_values(seed)
    a = [[next(g) for _ in range(n)] for _ in range(n)]
    b = [[next(g) for _ in range(n)] for _ in range(n)]
    bt = list(zip(*b))
    trace = total = squares = 0
    for i in range(n):
        row = a[i]
        for j in range(n):
            v = sum(x * y for x, y in zip(row, bt[j]))
            total += v
            squares += v * v
            if i == j:
                trace += v
    return {"trace": trace, "sum": total, "sum_of_squares": squares}


def file_stats(body):
    words = body.split()  # ASCII whitespace, like the runner
    counts = {}
    for w in words:
        w = w.lower().decode()
        counts[w] = counts.get(w, 0) + 1
    top = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
    return {"lines": body.count(b"\n"), "words": len(words), "bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(), "top_words": [{"word": w, "count": c} for w, c in top]}


def mandelbrot_frame(w, h, max_iter, cx, cy, scale):
    """Iteration sum and inside-count of one frame, with the exact operation order of the render runner."""
    iter_sum = inside = 0
    for py in range(h):
        y0 = cy + ((py - h / 2) * scale) / w
        for px in range(w):
            x0 = cx + ((px - w / 2) * scale) / w
            zr = zi = 0.0
            i = 0
            while i < max_iter:
                zr2 = zr * zr
                zi2 = zi * zi
                if zr2 + zi2 > 4:
                    break
                zi = 2 * zr * zi + y0
                zr = zr2 - zi2 + x0
                i += 1
            iter_sum += i
            if i == max_iter:
                inside += 1
    return {"iter_sum": iter_sum, "inside": inside}


def mandelbrot_scales(frames, zoom):
    scale, out = 3.0, []
    for f in range(frames):
        if f > 0:
            scale = scale / zoom
        out.append(scale)
    return out


def png_size(body):
    """Width and height from a PNG's IHDR chunk."""
    assert body[:8] == b"\x89PNG\r\n\x1a\n", "not a PNG"
    return struct.unpack(">II", body[16:24])


def sha256(body):
    return hashlib.sha256(body).hexdigest()


# ---- media helpers (tests skip when ffmpeg is missing) ----

HAVE_FFMPEG = bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))


def make_test_video(path, seconds, size="320x240", rate=24, freq=440):
    """An MPEG-4 Part 2 + AAC clip with a test pattern and a tone, so transcoding to H.264 is real work."""
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                    "-f", "lavfi", "-i", f"testsrc2=size={size}:rate={rate}",
                    "-f", "lavfi", "-i", f"sine=frequency={freq}:sample_rate=48000",
                    "-t", str(seconds), "-c:v", "mpeg4", "-q:v", "5", "-c:a", "aac", "-shortest", str(path)],
                   check=True)
    return path


def ffprobe(path):
    out = subprocess.run(["ffprobe", "-v", "error", "-count_frames", "-show_entries",
                          "format=duration:stream=codec_type,codec_name,width,height,pix_fmt,nb_read_frames",
                          "-of", "json", str(path)], capture_output=True, text=True, check=True).stdout
    doc = json.loads(out)
    info = {"duration": float(doc["format"]["duration"])}
    for s in doc["streams"]:
        if s["codec_type"] == "video" and "video" not in info:
            info["video"] = s
        if s["codec_type"] == "audio" and "audio" not in info:
            info["audio"] = s
    return info
