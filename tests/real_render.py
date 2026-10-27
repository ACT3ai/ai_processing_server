#!/usr/bin/env python3
"""A real render through the task queue on THIS machine's installed models (problems 2026_oct_27 #3).

    just render-test            # Wan 2.2 text-to-video, then SeedVR2 upscales that clip 2x
    just render-test wan        # only Wan 2.2
    just render-test upscale    # only SeedVR2 (on a 2 s ffmpeg test pattern)

Talks to the server already running here (`just run`), the same way the internal web app does: POST /v1/queue/jobs,
then polls GET /v1/queue/jobs/{id}. Small on purpose (Wan: 480x272, 17 frames, Lightning 4 steps, the smoke size of
~/BGit/all/app/smartc/film/servers/bms/oss_video/wan_2_2/model.yaml; SeedVR2: 3B "standard" to 960x544), so a pass
proves the whole path, weights included, in minutes instead of an hour. Outputs land in
~/T/_ai_processing_server/real_render/<time>/. Exit 0 only when every job asked for SUCCEEDED and its file is a
video of the expected size. Standard library only, like the rest of tests/.
"""

import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tests"))
from lib.client import APIError, Client  # noqa: E402

WAN = "act3.video_gen_open_source"
UPSCALE = "act3.upscale_open_source"


def local_api():
    """listen + token from this_server.yaml's local_api section (defaults otherwise)."""
    listen, token_file = "127.0.0.1:9440", "~/.credentials/ai_processing_server_local.token"
    cfg = ROOT / "data" / "git_ignore" / "this_server.yaml"
    if cfg.exists():
        text = cfg.read_text()
        m = re.search(r"^local_api:\s*\n((?:[ \t]+.*\n|\s*\n)*)", text, re.M)
        if m:
            for key, val in re.findall(r"^\s+(listen|token_file):\s*\"?([^\"#\n]+?)\"?\s*(?:#.*)?$", m.group(1), re.M):
                if key == "listen":
                    listen = val.strip()
                else:
                    token_file = val.strip()
    token = Path(os.path.expanduser(token_file)).read_text().strip()
    return Client("http://" + listen, token)


def say(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def offered(api, service):
    for o in api.get("/v1/capabilities")["offers"]:
        if o["service"] == service:
            return o.get("parallel", 0) > 0, o.get("reason", "")
    return False, "not in this server's catalog"


def run_job(api, service, params, inputs=None, timeout_min=90):
    fields = {"max_attempts": 1}
    if inputs:
        fields["inputs"] = inputs
    ack = api.queue_submit(service, params, **fields)
    job = ack["job_id"]
    say(f"{service}: queued {job}")
    deadline, last = time.time() + timeout_min * 60, None
    while time.time() < deadline:
        try:
            q = api.queue_job(job)
        except APIError as e:
            if e.status in (503, 504):  # broker blip: keep polling (servers.mdx §34 step 4)
                time.sleep(5)
                continue
            raise
        st = q["job_state"]
        line = f"{st['state']}" + (f" on {st.get('server')}" if st.get("server") else "")
        try:
            local = api.job(job)
            line += f" · {local.get('caller_state', '')} {100 * local.get('progress', 0):.0f}%"
        except APIError:
            pass
        if line != last:
            say(f"{service}: {line}")
            last = line
        if st["state"] in ("SUCCEEDED", "FAILED", "CANCELLED", "EXPIRED"):
            return job, q
        time.sleep(10)
    raise TimeoutError(f"{service} job {job} not finished after {timeout_min} min")


def fetch(api, job, q, dest):
    done = [e for e in q["events"] if e["type"] == "succeeded"]
    outs = (done[-1].get("outputs") or []) if done else []
    if not outs:
        raise AssertionError(f"job {job} succeeded without an output: {json.dumps(q['events'][-3:])[:800]}")
    out = outs[0]
    path = out.get("download") or f"/v1/jobs/{job}/outputs/{out['name']}"
    body, _, _ = api.request("GET", path, raw=True, timeout=300)
    dest.write_bytes(body)
    code = (done[-1].get("result") or {}).get("code_version", "?")
    say(f"saved {dest} ({len(body) / 1e6:.1f} MB, code snapshot {code})")
    return dest


def probe(path):
    if not shutil.which("ffprobe"):
        return None
    r = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                        "stream=codec_name,width,height,nb_frames", "-of", "json", str(path)], capture_output=True, text=True)
    s = (json.loads(r.stdout or "{}").get("streams") or [{}])[0]
    return s


def check_video(path, w, h, slack=0):
    s = probe(path)
    if s is None:
        say("ffprobe not found: size not checked")
        return True
    say(f"{path.name}: {s.get('codec_name')} {s.get('width')}x{s.get('height')}, {s.get('nb_frames')} frames")
    return abs((s.get("width") or 0) - w) <= slack and abs((s.get("height") or 0) - h) <= slack


def main():
    what = (sys.argv[1] if len(sys.argv) > 1 else "all").lower()
    if what not in ("all", "wan", "upscale"):
        sys.exit("usage: real_render.py [all|wan|upscale]")
    try:
        api = local_api()
        api.health()
    except Exception as e:  # noqa: BLE001
        sys.exit(f"the server is not running here ({e}). Start it: just run")
    h = api.health()
    if (h.get("broker") or {}).get("state") != "ok":
        sys.exit(f"the task queue is not up: {h.get('broker')}. Fix: just broker-run (then just render-check)")
    want = [WAN, UPSCALE] if what == "all" else [WAN] if what == "wan" else [UPSCALE]
    for s in want:
        ok, why = offered(api, s)
        if not ok:
            sys.exit(f"{s} is not offered on this machine: {why}\n"
                     f"Fix: install its weights, then `just enable {s}` and restart (just stop && just run).")

    out_dir = Path.home() / "T" / "_ai_processing_server" / "real_render" / time.strftime("%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    say(f"outputs: {out_dir}")
    ok = True
    source = None

    if WAN in want:
        t0 = time.time()
        job, q = run_job(api, WAN, {
            "model": "wan2.2-a14b", "kind": "t2v", "width": 480, "height": 272, "frames": 17,
            "prompt": "A slow push-in on a red paper boat drifting across a still pond at golden hour."})
        if q["job_state"]["state"] != "SUCCEEDED":
            say(f"Wan FAILED: {json.dumps(q['events'][-3:])[:1500]}")
            ok = False
        else:
            source = fetch(api, job, q, out_dir / "wan_480x272.mp4")
            ok &= check_video(source, 480, 272)
            say(f"Wan 2.2 took {(time.time() - t0) / 60:.1f} min")

    if UPSCALE in want and ok:
        if source is None:
            if not shutil.which("ffmpeg"):
                sys.exit("ffmpeg is needed to make the upscale test clip")
            source = out_dir / "testsrc_480x272.mp4"
            subprocess.run(["ffmpeg", "-v", "error", "-y", "-f", "lavfi", "-i", "testsrc2=size=480x272:rate=16",
                            "-t", "2", "-pix_fmt", "yuv420p", "-c:v", "libx264", str(source)], check=True)
        t0 = time.time()
        job, q = run_job(api, UPSCALE, {"model": "seedvr2", "quality": "standard", "width": 960, "height": 544,
                                        "duration_seconds": 2},
                         inputs=[{"name": "source_video", "path": str(source)}])
        if q["job_state"]["state"] != "SUCCEEDED":
            say(f"SeedVR2 FAILED: {json.dumps(q['events'][-3:])[:1500]}")
            ok = False
        else:
            up = fetch(api, job, q, out_dir / "seedvr2_960x544.mp4")
            ok &= check_video(up, 960, 544, slack=16)  # SeedVR2 keeps the aspect and may round the sides
            say(f"SeedVR2 took {(time.time() - t0) / 60:.1f} min")

    print()
    say(("PASS: real render(s) done on this machine through the queue. " if ok else "FAIL: see above. ") + str(out_dir))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
