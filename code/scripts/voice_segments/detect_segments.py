#!/usr/bin/env python3
"""
voice_segments — speech segment detection for ACT3 Voice Alignment (Full recordings).

Spec: ~/BGit/all/film/pm/subp/voice_acting/alignment.mdx (§3 contracts, §4 algorithm, §5 pick, §6 vendors).

One actor's full-length recording in (MP4, MOV, MP3, WAV, ... anything FFmpeg reads), two files out:
  <stem>.segments.json  — contract act3.voice_segments.v1: PRIMARY segments (spoken turns, split by >= primary_gap
                          of no dialogue) and SECONDARY segments (phrases inside a turn), seconds to 3 decimals.
  <stem>.envelope.json  — BBC audiowaveform JSON v2: one 8-bit min/max pair per 10 ms (the "volume height" file).

Engine: Silero VAD v5 (MIT, pip `silero-vad`) for the per-frame speech probability, then the same post-processing
as the browser engine (speechSegments.ts): hysteresis, run cleanup, bleed gate, grouping, padding, 1 ms refinement.
Without silero-vad installed it falls back to the energy engine (`act3-energy-v1`) and says so in `engine`.

Render-server task rules (ai_processing_server CLAUDE.md): seconds-long, idempotent (same input + params = same
output), reads nothing from the repo after start, so a git pull mid-task cannot break it.

Usage:
  python3 detect_segments.py <input> [--out DIR] [--primary-gap 0.6] [--secondary-gap 0.2] [--engine auto|silero|energy]
Prints the segments JSON path on success. Exit 2 = input unreadable.
"""
import argparse
import json
import math
import os
import subprocess
import sys

import numpy as np

FRAME = 0.01
DETECT_RATE = 16000


def decode(path, rate):
    """Mono float32 at `rate` through FFmpeg (the audio stream of a video file too)."""
    cmd = ["ffmpeg", "-v", "error", "-i", path, "-vn", "-ac", "1", "-ar", str(rate), "-f", "f32le", "-"]
    proc = subprocess.run(cmd, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.decode(errors="replace").strip() or "ffmpeg could not read the file")
    return np.frombuffer(proc.stdout, dtype=np.float32).copy()


def native_rate(path):
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "a:0", "-show_entries", "stream=sample_rate",
             "-of", "default=nw=1:nk=1", path], capture_output=True, text=True).stdout.strip()
        return int(out) if out else 48000
    except (OSError, ValueError):
        return 48000


def db(x):
    return 20 * np.log10(np.maximum(x, 1e-7))


def frame_levels(samples, rate):
    hop = int(round(rate * FRAME))
    count = int(math.ceil(len(samples) / hop))
    pad = np.concatenate([np.zeros(hop // 2, np.float32), samples, np.zeros(hop * 2, np.float32)])
    sq = np.cumsum(np.concatenate([[0.0], pad.astype(np.float64) ** 2]))
    starts = np.arange(count) * hop
    win = 2 * hop
    rms = np.sqrt((sq[starts + win] - sq[starts]) / win)
    return db(rms)


def silero_probs(samples16k, frames):
    """Silero VAD v5 speech probability per 32 ms window, spread onto 10 ms frames."""
    import torch
    from silero_vad import load_silero_vad

    model = load_silero_vad()
    win = 512  # 32 ms at 16 kHz
    probs = np.zeros(frames, np.float32)
    tensor = torch.from_numpy(samples16k)
    model.reset_states()
    for i in range(0, len(samples16k) - win + 1, win):
        p = float(model(tensor[i:i + win], DETECT_RATE).item())
        a = int(i / DETECT_RATE / FRAME)
        b = min(frames, int(math.ceil((i + win) / DETECT_RATE / FRAME)))
        probs[a:b] = np.maximum(probs[a:b], p)
    return probs


def merge(ranges, gap):
    out = []
    for r in ranges:
        if out and r[0] - out[-1][1] < gap:
            out[-1] = [out[-1][0], max(out[-1][1], r[1]), max(out[-1][2], r[2])]
        else:
            out.append(list(r))
    return out


def refine(samples, rate, t, off_db, side):
    """Move a boundary to the first / last 2 ms window above the leave threshold within +-20 ms."""
    win = max(1, int(round(rate * 0.002)))
    reach = int(round(rate * 0.02))
    centre = int(round(t * rate))
    rng = range(centre - reach, centre + reach + 1, win) if side == "start" else range(centre + reach, centre - reach - 1, -win)
    for i in rng:
        a, b = (i, i + win) if side == "start" else (i - win, i)
        a, b = max(0, a), min(len(samples), b)
        if b > a and db(np.sqrt(np.mean(samples[a:b].astype(np.float64) ** 2))) >= off_db:
            return i / rate
    return t


def envelope(samples, rate):
    per = max(1, int(round(rate * FRAME)))
    length = int(math.ceil(len(samples) / per))
    padded = np.concatenate([samples, np.zeros(length * per - len(samples), np.float32)]).reshape(length, per)
    lo = np.clip(np.round(padded.min(axis=1) * 128), -128, 0).astype(int)
    hi = np.clip(np.round(padded.max(axis=1) * 127), 0, 127).astype(int)
    data = np.empty(length * 2, int)
    data[0::2], data[1::2] = lo, hi
    return {"version": 2, "channels": 1, "sample_rate": rate, "samples_per_pixel": per, "bits": 8,
            "length": length, "data": data.tolist()}


def detect(path, primary_gap=0.6, secondary_gap=0.2, min_speech=0.1, fill_pause=0.08, pad=0.03, bleed_db=18.0,
           vad_on=0.5, vad_off=0.35, engine="auto"):
    rate = native_rate(path)
    samples = decode(path, rate)
    duration = len(samples) / rate
    levels = frame_levels(samples, rate)
    floor = float(np.percentile(levels, 10)) if len(levels) else -100.0
    speech = float(np.percentile(levels, 95)) if len(levels) else -100.0
    span = max(0.0, speech - floor)
    on_db = floor + max(9.0, 0.45 * span)
    off_db = floor + max(6.0, 0.30 * span)

    used = {"name": "act3-energy-v1", "version": "1", "post": "act3-segmenter-1"}
    score, on, off = levels, on_db, off_db
    if engine in ("auto", "silero"):
        try:
            score = silero_probs(decode(path, DETECT_RATE), len(levels))
            on, off = vad_on, vad_off
            used = {"name": "silero-vad", "version": "5", "post": "act3-segmenter-1"}
        except Exception as err:  # noqa: BLE001 - any failure falls back to the energy engine
            if engine == "silero":
                raise
            print(f"silero-vad unavailable ({err}); using the energy engine", file=sys.stderr)

    runs, start = [], None
    for f, v in enumerate(score):
        if start is None and v >= on:
            start = f
        elif start is not None and v < off:
            runs.append([start * FRAME, f * FRAME, float(levels[start:f + 1].max())])
            start = None
    if start is not None:
        runs.append([start * FRAME, len(score) * FRAME, float(levels[start:].max())])
    if span < 6:
        runs = []

    filled = merge(runs, fill_pause)
    kept = [r for r in filled if r[1] - r[0] >= min_speech and r[2] >= speech - bleed_db]
    secondary_raw = merge(kept, secondary_gap)
    primary_raw = merge(secondary_raw, primary_gap)

    def finish(ranges):
        out = []
        for i, r in enumerate(ranges):
            lo = (ranges[i - 1][1] + r[0]) / 2 if i else 0.0
            hi = (r[1] + ranges[i + 1][0]) / 2 if i < len(ranges) - 1 else duration
            s = max(lo, refine(samples, rate, r[0], off_db, "start") - pad)
            e = min(hi, refine(samples, rate, r[1], off_db, "end") + pad)
            out.append((round(s, 3), round(max(e, s + 0.01), 3), r[2]))
        return out

    prim = finish(primary_raw)
    sec = finish(secondary_raw)
    primary = [{"id": f"p{i + 1}", "start": s, "end": e, "duration": round(e - s, 3), "peak_db": round(pk, 1),
                "secondary": []} for i, (s, e, pk) in enumerate(prim)]
    secondary = []
    for i, (s, e, _pk) in enumerate(sec):
        mid = (s + e) / 2
        owner = next((p for p in primary if p["start"] <= mid <= p["end"]), None) or (
            min(primary, key=lambda p: abs((p["start"] + p["end"]) / 2 - mid)) if primary else None)
        sid = f"s{i + 1}"
        if owner:
            owner["secondary"].append(sid)
            s, e = max(s, owner["start"]), min(e, owner["end"])
        secondary.append({"id": sid, "primary": owner["id"] if owner else "", "start": s, "end": e,
                          "duration": round(e - s, 3)})

    result = {
        "schema": "act3.voice_segments.v1",
        "source": {"file": os.path.basename(path), "duration": round(duration, 3), "sample_rate": rate},
        "engine": used,
        "params": {"primary_gap": primary_gap, "secondary_gap": secondary_gap, "min_speech": min_speech, "pad": pad},
        "levels": {"noise_floor_db": round(floor, 1), "speech_db": round(speech, 1)},
        "primary": primary,
        "secondary": secondary,
    }
    return result, envelope(samples, rate)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input")
    ap.add_argument("--out", default=None, help="output dir (default: beside the input)")
    ap.add_argument("--primary-gap", type=float, default=0.6)
    ap.add_argument("--secondary-gap", type=float, default=0.2)
    ap.add_argument("--min-speech", type=float, default=0.1)
    ap.add_argument("--pad", type=float, default=0.03)
    ap.add_argument("--bleed-db", type=float, default=18.0)
    ap.add_argument("--engine", choices=["auto", "silero", "energy"], default="auto")
    args = ap.parse_args()
    try:
        segs, env = detect(args.input, args.primary_gap, args.secondary_gap, args.min_speech, pad=args.pad,
                           bleed_db=args.bleed_db, engine=args.engine)
    except RuntimeError as err:
        print(f"cannot read {args.input}: {err}", file=sys.stderr)
        sys.exit(2)
    out_dir = args.out or os.path.dirname(os.path.abspath(args.input))
    os.makedirs(out_dir, exist_ok=True)
    stem = os.path.splitext(os.path.basename(args.input))[0]
    seg_path = os.path.join(out_dir, f"{stem}.segments.json")
    env_path = os.path.join(out_dir, f"{stem}.envelope.json")
    for p, data in ((seg_path, segs), (env_path, env)):
        tmp = p + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(data, fh, indent=1 if p == seg_path else None)
        os.replace(tmp, p)
    print(seg_path)


if __name__ == "__main__":
    main()
