# COPY of ~/BGit/all/film/code/subp/video_gen_ai/nvidia/h3_regenerate_2k/render_h3_regenerate.py, kept here so the task's code
# is frozen into each snapshot (pm/task_authoring.mdx R1, R2). Edit the source in the all repo, then run
# code/scripts/sync_from_all.sh. Run by the oss_regenerate runner (code/internal/runners/oss_regenerate.go).
"""render_h3_regenerate.py: regenerate ONE draft take at 768P / 1080P / 2K / 4K with NVIDIA Open-H3-Regenerate-2K on an
ACT3 render server (CUDA), through the official CLI (infer.py) or the ComfyUI port. Python 3 standard library only:
the model itself runs in its own process (infer.py in the model venv, or a ComfyUI server).

Called by the render server's task host (ai_processing_server runner oss_regenerate, or the internal web app's plugin
./h3-regenerate-local.ts, which builds this exact argv). Two checkpointed stages (h3_regenerate_2k.mdx §10.2):

  --stage refine   probe + re-check the draft (refusing a crop or a cut the caller did not accept), run the refine (CLI:
                   --save-latents; ComfyUI: the workflow's own Save Refined Latent), write the silent refined video to
                   --refined, <refined>.meta.json (timings, peak VRAM, Sana commit, weights revision) and a settings
                   stamp. A re-run with the same settings whose output and stamp are already there does nothing.
                   There is NO decode-only resume: a decode OOM re-runs the last pass once with the released VAE; the
                   saved latent is kept for a human to decode (the port's Decode Saved Latent workflow).
  --stage finish   trim the H3 frame-grid pad back to the draft's length, re-mux the draft's own audio (audio is never
                   regenerated, §2.5), encode H.264 yuv420p 24 fps +faststart, write --out atomically.
  --stage all      both, in one process (the internal web app's executor runs one argv).

Output grammar (the task host reads it): "ACT3_STAGE <name>", "ACT3_PROGRESS <0-100>" at least every 20 s even while
the model is silent (R6), "ACT3_NOTE <text>". Failures end with ONE line "ACT3_ERROR <kind>: <sentence>" on stderr and
exit code: 2 invalid_input, 3 infra (out of memory, disk), 4 license_not_cleared, 5 not_installed, 1 internal.

License gate (§12): output for --delivery customer is refused unless --license-cleared (Legal's call, not ours).

Spec: ~/BGit/all/film/pm/subp/video_gen_ai/nvidia/h3_regenerate_2k/h3_regenerate_2k.mdx (§3 inputs/outputs, §9 CLI and
ComfyUI, §10 integration). Prompt rules: ~/BGit/all/film/pm/subp/ai_prompts/video_gen_prompts/s/minimax/
open_h3_regenerate_2k/spec_open_h3_regenerate_2k.mdx.
"""
import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

EXIT = {"invalid_input": 2, "infra": 3, "license_not_cleared": 4, "not_installed": 5, "internal": 1}

# §3.2 output rungs (rows rendered, then cropped by the ComfyUI port: 1088 → 1080, 2176 → 2160)
TARGETS = {"768p": (1344, 768), "1080p": (1920, 1080), "2k": (2560, 1440), "4k": (3840, 2160)}
FPS = 24
CLI_WINDOW_FRAMES = 121  # §3.1: the CLI cuts or pads --input-video to 121 frames (5.04 s)
MAX_FRAMES = 360  # §3.2: 15 s in one pass
MIN_SHORT_SIDE = 256  # spec gap: no floor; 384P is the documented low end (§3.1)
HEARTBEAT_SECONDS = 20  # ai_processing_server task_authoring.mdx R6: progress at least every 30 s
MODEL_DIRS = [("models", "Open-H3-Regenerate-2K"), ("models", "minimax_h3", "Open-H3-Regenerate-2K")]  # §9.1, R4
REQUIRED_WEIGHTS = [  # §9.1 download.py layout; a partial download is a missing weight
    (("lora", "open_h3_regenerate_2k_lora.safetensors"), 10_000_000_000),
    (("lora", "sharp_curve_branches.safetensors"), 1_000_000_000),
    (("lora", "adaln_t_table.bin"), 1),
    (("vae_fast", "fast_h3_vae_noncausal_24l.safetensors"), 5_000_000_000),
    (("base_model", "model_index.json"), 1),
]

# Out of memory. A bare "Killed" is NOT here: it is an ordinary word in a prompt ("the man was killed at nine") and the
# tail can echo the prompt. The kernel's OOM kill shows as a line that is only "Killed", "Killed: 9", or exit -9/137.
RE_OOM = re.compile(
    r"out of memory|OutOfMemoryError|CUDA error: out of memory|cudaErrorMemoryAllocation|^\s*Killed(?::\s*9)?\s*$|"
    r"CUBLAS_STATUS_ALLOC_FAILED|cannot allocate memory",
    re.I | re.M,
)
RE_DISK = re.compile(r"no space left on device|ENOSPC|disk quota exceeded", re.I)
RE_MISSING_PKG = re.compile(r"ModuleNotFoundError|No module named|ImportError", re.I)
RE_MISSING_FILE = re.compile(r"(FileNotFoundError|No such file or directory).*(safetensors|weights|\.bin|\.pt|model_index)", re.I)
# The decode phase. infer.py's own stage text is unverified, so a bare "decod" (which also matches the startup line
# "Loading fast decoder weights") is never enough: a decode line must not be about loading, and the strong signals are
# the sampling loop reaching its last step, or this pass's latent appearing (--save-latents writes it before decode,
# §10.2 — unverified ordering, so it is one signal of three).
RE_DECODE_STAGE = re.compile(r"^(?!.*\b(?:load|loading|loaded|download|init\w*|prompt)\b).*\b(?:decod(?:e|es|ing)\b|vae[ _-]?decode)", re.I)
RE_NOT_SAMPLING = re.compile(r"load|download|shard|fetch|compil|warm", re.I)
RE_PCT = re.compile(r"(\d{1,3}(?:\.\d+)?)\s*%\s*\|")
RE_RATIO = re.compile(r"\b(?:step|steps|sampling|refine|denois\w*)\b[^0-9]{0,16}(\d+)\s*/\s*(\d+)", re.I)
RE_TQDM = re.compile(r"^\s*(\d+)/(\d+)\s*\[")


class Fail(Exception):
    def __init__(self, kind, message):
        super().__init__(message)
        self.kind = kind


def say(line):
    print(line, flush=True)


def stage(name):
    say(f"ACT3_STAGE {name}")


class Progress:
    """Maps the model's own progress into a band of the 0-100 bar and keeps a heartbeat going while it is silent."""

    def __init__(self):
        self.value = 0.0
        self.lo, self.hi = 0.0, 100.0
        self.note = ""
        self.last = 0.0
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.t0 = time.time()
        threading.Thread(target=self._beat, daemon=True).start()

    def band(self, lo, hi, note=""):
        with self.lock:
            self.lo, self.hi, self.note = lo, hi, note
        self.set(lo)

    def frac(self, f):
        with self.lock:
            v = self.lo + (self.hi - self.lo) * max(0.0, min(1.0, f))
        self.set(v)

    def set(self, v):
        with self.lock:
            self.value = max(self.value, min(100.0, v))  # never runs backwards
            self.last = time.time()
            v = self.value
        say(f"ACT3_PROGRESS {v:.1f}")

    def _beat(self):
        while not self.stop.wait(1.0):
            if time.time() - self.last >= HEARTBEAT_SECONDS:
                with self.lock:
                    v, note = self.value, self.note
                    self.last = time.time()
                say(f"ACT3_PROGRESS {v:.1f}")
                say(f"ACT3_NOTE {note or 'working'}: {int(time.time() - self.t0)} s elapsed")

    def close(self):
        self.stop.set()


# ── ffprobe / ffmpeg ──────────────────────────────────────────────────────────────────────────────────────────────


def tool(name):
    p = os.environ.get(f"H3R_{name.upper()}") or shutil.which(name)
    if not p:
        raise Fail("not_installed", f"{name} is not installed on this server (needed to probe and finish the take)")
    return p


def ratio(s):
    try:
        if "/" in s:
            a, b = s.split("/", 1)
            return float(a) / float(b) if float(b) else 0.0
        return float(s)
    except (TypeError, ValueError):
        return 0.0


def probe(path):
    """Width, height, fps, seconds, frames and whether it has audio — read from the file, never trusted from params."""
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        raise Fail("invalid_input", f"the draft video is missing or empty: {path}")
    out = subprocess.run(
        [tool("ffprobe"), "-v", "error", "-show_entries",
         "stream=codec_type,width,height,avg_frame_rate,r_frame_rate,nb_frames,duration:format=duration",
         "-of", "json", path],
        capture_output=True, text=True, timeout=120,
    )
    if out.returncode != 0:
        raise Fail("invalid_input", f"the draft is not a readable video: {out.stderr.strip()[-300:]}")
    try:
        doc = json.loads(out.stdout or "{}")
    except json.JSONDecodeError:
        raise Fail("invalid_input", "ffprobe returned no readable description of the draft")
    streams = doc.get("streams") or []
    video = next((s for s in streams if s.get("codec_type") == "video"), None)
    if not video:
        raise Fail("invalid_input", "the draft has no video stream")
    fps = ratio(video.get("avg_frame_rate") or "") or ratio(video.get("r_frame_rate") or "")
    secs = ratio(str(video.get("duration") or "")) or ratio(str((doc.get("format") or {}).get("duration") or ""))
    try:
        frames = int(video.get("nb_frames") or 0)
    except ValueError:
        frames = 0
    if frames <= 0 and secs > 0 and fps > 0:
        frames = round(secs * fps)
    if secs <= 0 and frames > 0 and fps > 0:
        secs = frames / fps
    return {
        "width": int(video.get("width") or 0),
        "height": int(video.get("height") or 0),
        "fps": fps,
        "seconds": secs,
        "frames": frames,
        "has_audio": any(s.get("codec_type") == "audio" for s in streams),
    }


def check_draft(info, backend, window_frames, allow_crop, allow_truncate=False):
    """§3.1 rules, re-checked on the real file (the job builder only saw what the caller claimed). Losing picture is
    REFUSED unless the caller accepted it (--allow-crop, --allow-truncate): never a silent truncation (§3.1, §3.2)."""
    w, h = info["width"], info["height"]
    if w <= 0 or h <= 0:
        raise Fail("invalid_input", "the draft's frame size could not be read")
    if h > w and not allow_crop:
        raise Fail("invalid_input", f"the draft is portrait ({w}x{h}); the refiner's grid is landscape and would crop most of the frame "
                   "(--allow-crop accepts it)")
    if h > w:
        say(f"ACT3_NOTE the draft is portrait ({w}x{h}); a landscape band of it is refined (--allow-crop)")
    if min(w, h) < MIN_SHORT_SIDE:
        raise Fail("invalid_input", f"the draft's short side is {min(w, h)} px; the refiner takes 384P-768P drafts")
    secs = info["seconds"]
    if secs <= 0:
        raise Fail("invalid_input", "the draft's duration could not be read")
    f24 = round(secs * FPS)
    cap = CLI_WINDOW_FRAMES if backend == "cli" else MAX_FRAMES
    window = min(window_frames, cap)
    if f24 > window + 1:  # one frame of rounding slack
        lost = (f24 - window) / FPS
        if not allow_truncate:
            raise Fail("invalid_input", f"the draft is {secs:.2f} s but this refine covers only the first {window / FPS:.2f} s "
                       f"({lost:.2f} s would be lost); re-plan with the real duration, use the ComfyUI backend (up to 15 s), "
                       "or accept the cut with --allow-truncate")
        say(f"ACT3_NOTE the draft is {secs:.2f} s; only the first {window / FPS:.2f} s are refined ({lost:.2f} s dropped, --allow-truncate)")
    if abs(info["fps"] - FPS) > 0.05:
        say(f"ACT3_NOTE the draft runs at {info['fps']:.3f} fps; the refiner resamples it to 24 fps")
    if min(w, h) > 768:
        say(f"ACT3_NOTE the draft ({w}x{h}) is above 768P; it is downscaled to the input size before the refine")


# ── installs ──────────────────────────────────────────────────────────────────────────────────────────────────────


def find_infer(home, sana):
    roots = [sana] if sana else []
    if home:
        roots.append(os.path.join(home, "sana"))
    for root in roots:
        for parts in MODEL_DIRS:
            d = os.path.join(root, *parts)
            if os.path.isfile(os.path.join(d, "infer.py")):
                return d
        if os.path.isfile(os.path.join(root, "infer.py")):
            return root
    raise Fail("not_installed", "infer.py not found: the NVlabs/Sana sol-engine checkout is not installed "
               f"(looked under {', '.join(roots) or 'nothing'}; install guide: README.md in this directory)")


def check_weights(weights):
    missing = []
    for parts, min_bytes in REQUIRED_WEIGHTS:
        p = os.path.join(weights, *parts)
        if not os.path.isfile(p) or os.path.getsize(p) < min_bytes:
            missing.append(p)
    if not os.path.isdir(os.path.join(weights, "latent_upsampler")):
        missing.append(os.path.join(weights, "latent_upsampler"))
    if missing:
        raise Fail("not_installed", "Open-H3-Regenerate-2K weights are missing or partial: " + ", ".join(missing)
                   + " (run download.py --weights ./weights; never download inside a running task)")


# ── the official CLI (§9.1) ───────────────────────────────────────────────────────────────────────────────────────


def sigma_flags(steps, start_sigma):
    """--steps picks the documented grid (3 from 0.909375, 2 from 0.7); any other start is passed explicitly."""
    doc = 0.7 if steps == 2 else 0.909375
    out = ["--steps", str(steps)]
    if start_sigma is not None and abs(start_sigma - doc) > 1e-6:
        out += ["--start-sigma", f"{start_sigma:g}"]
    return out


def latents_for(output):
    return re.sub(r"\.mp4$", "", output, flags=re.I) + ".latents.pt"


def infer_commands(x, infer_dir, refined, vae=None):
    """The infer.py argv list(s): one stage, or the two-stage ladder (768p + --save-latents, then --input-latents)."""
    py = x.infer_python or sys.executable
    infer = os.path.join(infer_dir, "infer.py")
    vae = vae or x.vae

    def common(resolution):
        a = ["--weights", x.weights, "--prompt", x.prompt_text, "--resolution", resolution]
        a += sigma_flags(x.steps, x.start_sigma)
        if x.fwd == "standard":
            a += ["--fwd", "standard"]
        if vae == "standard":
            a += ["--vae", "standard"]
        a += ["--vae-seed", str(x.seed), "--save-latents"]
        return a + shlex.split(x.extra_args or "")

    inp = ["--input-video", x.draft] + (["--input-size", "576p"] if x.input_size == "576p" else [])
    if not x.ladder or x.resolution == "768p":
        return [[py, infer] + inp + common(x.resolution) + ["--output", refined]]
    mid = re.sub(r"\.mp4$", "", refined, flags=re.I) + ".768p.mp4"
    return [
        [py, infer] + inp + common("768p") + ["--output", mid],
        [py, infer, "--input-latents", latents_for(mid)] + common(x.resolution) + ["--output", refined],
    ]


def run_logged(argv, prog, cwd=None, latents=None):
    """Run one model process, streaming its output, translating its progress; returns (code, tail, saw_decode).
    saw_decode: the sampling loop finished, a decode line (not a loading line) was printed, or this pass's latent was
    written while it ran — any of them means the refine itself fit and the failure came in decode."""
    tail = []
    saw_decode = False
    t_start = time.time()
    p = subprocess.Popen(argv, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
                         env={**os.environ, "PYTHONUNBUFFERED": "1"})
    for raw in p.stdout:
        line = raw.rstrip("\n")
        if not line:
            continue
        # tqdm redraws with \r: keep the last frame only
        line = line.split("\r")[-1]
        print(line, flush=True)
        tail.append(line)
        del tail[:-80]
        if not saw_decode and RE_DECODE_STAGE.search(line):
            saw_decode = True
            say("ACT3_NOTE decoding")
        m = RE_PCT.search(line) or RE_TQDM.search(line) or RE_RATIO.search(line)
        if m:
            f = float(m.group(1)) / 100 if m.re is RE_PCT else (int(m.group(1)) / int(m.group(2)) if int(m.group(2)) > 0 else 0.0)
            prog.frac(f)
            if f >= 0.999 and not RE_NOT_SAMPLING.search(line):
                saw_decode = True  # the last sampling step is done: what follows is decode
    code = p.wait()
    if latents and os.path.isfile(latents) and os.path.getmtime(latents) >= t_start - 1:
        saw_decode = True
    return code, "\n".join(tail), saw_decode


def scrub(text, prompt):
    """Remove the prompt (and lines that echo a piece of it) from model output before classifying a failure: a prompt
    can say "out of memory", "Killed" or "No such file" and must never decide what kind of failure this was."""
    text = text or ""
    prompt = (prompt or "").strip()
    if not prompt:
        return text
    for v in sorted({prompt, json.dumps(prompt)[1:-1], json.dumps(prompt, ensure_ascii=False)[1:-1]}, key=len, reverse=True):
        if v:
            text = text.replace(v, "<prompt>")
    # a line of the prompt echoed on its own (blank lines are dropped from the tail, so the whole prompt rarely matches):
    # even a short one like "Killed" must go
    prompt_lines = {re.sub(r"\s+", " ", ln).strip() for ln in prompt.splitlines() if ln.strip()}
    flat = re.sub(r"\s+", " ", prompt)
    chunks = {flat[i:i + 24] for i in range(0, len(flat) - 23, 8)} if len(flat) >= 24 else set()
    out = []
    for line in text.splitlines():
        lf = re.sub(r"\s+", " ", line).strip()
        echo = lf in prompt_lines or (len(lf) >= 24 and any(c in lf for c in chunks))
        out.append("<prompt echo>" if echo else line)
    return "\n".join(out)


def classify(code, tail, prompt=""):
    tail = scrub(tail, prompt)
    if RE_DISK.search(tail):
        return Fail("infra", "the render server ran out of disk")
    if RE_OOM.search(tail) or code in (-9, 137):
        return Fail("infra", "out of memory (the refine needs a bigger card, a lower target, or the released VAE)")
    if RE_MISSING_PKG.search(tail):
        return Fail("not_installed", "the model venv is missing a package: " + last_line(tail))
    if RE_MISSING_FILE.search(tail):
        return Fail("not_installed", "a model file is missing: " + last_line(tail))
    return Fail("internal", f"infer.py exited {code}: {last_line(tail)}")


def last_line(tail):
    for line in reversed(tail.splitlines()):
        if line.strip():
            return line.strip()[:400]
    return "no output"


def refine_cli(x, refined, prog):
    infer_dir = find_infer(x.home, x.sana)
    check_weights(x.weights)
    cmds = infer_commands(x, infer_dir, refined)
    x.infer_dir = infer_dir
    for i, argv in enumerate(cmds):
        lo, hi = 5 + 80 * i / len(cmds), 5 + 80 * (i + 1) / len(cmds)
        prog.band(lo, hi, f"refining (pass {i + 1} of {len(cmds)})")
        say(f"ACT3_NOTE infer.py pass {i + 1}/{len(cmds)}")
        out_i = argv[argv.index("--output") + 1]
        code, tail, saw_decode = run_logged(argv, prog, cwd=infer_dir, latents=latents_for(out_i))
        if code == 0:
            continue
        err = classify(code, tail, x.prompt_text)
        # §7.2: the fast decoder needs ~21 GB at 2K/5 s and 45 GB at 2K/15 s. One retry on the released decoder when
        # the out-of-memory came during decode (the refine itself fit). There is NO decode-only resume: whether
        # --input-latents on a refined latent decodes without refining again is unverified (README "Unverified"), so
        # the retry re-runs this whole pass; the saved latent stays for a human (the port's Decode Saved Latent).
        oom = err.kind == "infra" and "out of memory" in str(err)
        if oom and saw_decode and x.vae == "fast" and not x.no_vae_retry and i == len(cmds) - 1:
            say("ACT3_NOTE decode ran out of memory with the fast decoder; retrying once with the released VAE (--vae standard)")
            argv2 = infer_commands(x, infer_dir, refined, vae="standard")[-1]
            code, tail, _ = run_logged(argv2, prog, cwd=infer_dir)
            if code == 0:
                x.vae_used = "standard"
                continue
            err = classify(code, tail, x.prompt_text)
        raise err
    if not os.path.isfile(refined) or os.path.getsize(refined) == 0:
        raise Fail("internal", "infer.py exited 0 but wrote no video")


# ── the ComfyUI port (§9.2) ───────────────────────────────────────────────────────────────────────────────────────


def http_json(url, body=None, timeout=60):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"} if data else {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode() or "{}")


# §9.2 alternative LoRAs: the file each LoRA id reads (same table as h3-regenerate-job.ts LORAS)
LORA_FILES = {
    "r576": "open_h3_regenerate_2k_lora.safetensors",
    "r128": "minimax_h3_open_regenerate_2k_r128_comfyui_bf16.safetensors",
    "r384_kijai": "minimax_h3_open_regenerate_2k_refiner_rank_384_bf16.safetensors",
}
DOC_START_SIGMA = {3: 0.909375, 2: 0.7}  # §9.2 Sigmas node: the 3-step and 2-step grids


def _is_link(v):
    return isinstance(v, list) and len(v) == 2 and isinstance(v[0], (str, int))


def patch_workflow(wf, values):
    """Set inputs BY NAME in an API-format workflow ({node_id: {class_type, inputs, _meta}}). Node class names are not
    relied on (the port is days old, §11 R4). Returns (done, kinds): "node.input" it set, and which kinds of decision
    landed somewhere ("prompt", "video", "seed", "sigma", "max_frames", "input_size", "resolution", "audio", "lora")."""
    done = []
    kinds = set()
    for nid, node in wf.items():
        if not isinstance(node, dict) or not isinstance(node.get("inputs"), dict):
            continue
        ins = node["inputs"]
        cls = str(node.get("class_type", "")).lower()
        title = str((node.get("_meta") or {}).get("title", "")).lower()
        where = cls + " " + title

        def put(key, value, kind=None):
            ins[key] = value
            done.append(f"{nid}.{key}")
            if kind:
                kinds.add(kind)

        for key in list(ins.keys()):
            k = key.lower()
            if _is_link(ins[key]):  # a link to another node: never overwrite wiring
                continue
            if k in ("seed", "noise_seed"):
                put(key, values["seed"], "seed")
            elif k == "max_frames":
                put(key, values["max_frames"], "max_frames")
            elif k in ("start_sigma", "sigma_start", "denoise_start_sigma"):
                put(key, values["start_sigma"], "sigma")
            elif k == "steps" and "sigma" in where:
                put(key, values["steps"], "sigma")
            elif k in ("input_size", "draft_size", "input_resolution"):
                put(key, values["input_size"], "input_size")
            elif k in ("resolution", "target_resolution", "target", "output_resolution") and (
                    "upscale" in where or "regenerate" in where or "finish" in where or k != "resolution"):
                put(key, values["resolution"], "resolution")
            elif k in ("text", "prompt", "positive") and ("encode" in cls or "prompt" in title or "text" in cls):
                put(key, values["prompt"], "prompt")
            elif k in ("video", "video_path", "file", "filename", "path") and ("load" in cls or "draft" in cls or "draft" in title):
                put(key, values["video"], "video")
            elif k in ("audio_mode", "draft_audio", "audio_source"):
                put(key, values["audio"], "audio")
            elif k in ("lora_name", "lora", "lora_file") and "lora" in where and values.get("lora_file"):
                put(key, values["lora_file"], "lora")
            elif k == "filename_prefix":
                put(key, values["prefix"])
    return done, kinds


def wire_decoder(wf, decoder):
    """Apply the plan's decoder choice (§7.2, §9.2). "fast": the Fast VAE Decode node must be there. "standard": route a
    VAE into the Fast VAE Decode node's optional `vae` fallback input (or the workflow already decodes with ComfyUI's
    own VAE Decode). Returns a sentence for the log; raises not_installed when the workflow cannot express the choice
    (never runs a different decoder than the plan silently). Unverified: that a wired `vae` input makes the node
    decode with it, rather than only on its own OOM fallback — the port's README calls it "a fallback"."""
    nodes = {nid: n for nid, n in wf.items() if isinstance(n, dict) and isinstance(n.get("inputs"), dict)}
    def where(n):
        return (str(n.get("class_type", "")) + " " + str((n.get("_meta") or {}).get("title", ""))).lower()
    fast = [nid for nid, n in nodes.items() if "fast" in where(n) and "decode" in where(n)]
    plain = [nid for nid, n in nodes.items() if "vaedecode" in where(n).replace(" ", "") and nid not in fast]
    if decoder == "fast":
        if fast:
            return f"fast decoder: node {fast[0]}"
        raise Fail("not_installed", "the plan chose the fast H3 VAE decoder but the ComfyUI workflow has no Fast VAE Decode "
                   "node: export the port's Draft Video workflow in API format")
    if not fast:
        if plain:
            return f"released decoder: the workflow decodes with node {plain[0]}"
        raise Fail("not_installed", "the ComfyUI workflow has no VAE decode node at all")
    src = None
    for nid, n in nodes.items():
        if nid in fast:
            continue
        v = n["inputs"].get("vae")
        if _is_link(v):
            src = list(v)
            break
    if src is None:
        loaders = [nid for nid, n in nodes.items() if "vaeloader" in where(n).replace(" ", "")]
        if loaders:
            src = [loaders[0], 0]
    if src is None:
        raise Fail("not_installed", "the plan needs the released VAE decoder (the fast one does not fit here, §7.2), but the "
                   "ComfyUI workflow has no VAE to route into Fast VAE Decode's `vae` input: add a VAE loader to the workflow")
    for nid in fast:
        nodes[nid]["inputs"]["vae"] = src
        for key in list(nodes[nid]["inputs"].keys()):  # a toggle, when the node has one (unverified input names)
            if key.lower() in ("use_fallback", "force_vae", "use_vae", "fallback"):
                nodes[nid]["inputs"][key] = True
    return f"released decoder: VAE {src[0]} routed into Fast VAE Decode node(s) {', '.join(fast)}"


def required_kinds(x):
    """Which decisions MUST land in the workflow: anything that differs from the port's defaults (§9.2 "Defaults
    matching the official recipe": 3-step grid from 0.909375, 121 frames, input 768p, Draft Video → 2560x1440, silent
    audio, merged r576 LoRA). A decision the workflow cannot take fails the job instead of running another one."""
    need = {"prompt", "video", "seed"}
    start = x.start_sigma if x.start_sigma is not None else DOC_START_SIGMA.get(x.steps, 0.909375)
    if x.steps != 3 or abs(start - 0.909375) > 1e-6:
        need.add("sigma")
    if x.window_frames != CLI_WINDOW_FRAMES:
        need.add("max_frames")
    if x.input_size != "768p":
        need.add("input_size")
    if x.resolution != "2k":
        need.add("resolution")
    if x.model_audio == "draft":
        need.add("audio")
    if x.lora != "r576":
        need.add("lora")
    return need


def comfy_deadline(x):
    """How long a ComfyUI refine may take before we call it lost: ~20 min per 5 s at 2K (§7.4: 3-5 min measured, 4x
    margin), scaled by length and rung, 15 min to 6 h. $H3R_COMFY_DEADLINE_S overrides."""
    env = os.environ.get("H3R_COMFY_DEADLINE_S", "")
    try:
        if env and float(env) > 0:
            return float(env)
    except ValueError:
        pass
    rung = {"768p": 0.5, "1080p": 0.7, "2k": 1.0, "4k": 4.0}.get(x.resolution, 1.0)
    return max(15 * 60.0, min(6 * 3600.0, 20 * 60.0 * max(1.0, x.window_frames / CLI_WINDOW_FRAMES) * rung + 600))


def _queue_ids(q):
    ids = set()
    for key in ("queue_running", "queue_pending"):
        for item in (q or {}).get(key) or []:
            if isinstance(item, (list, tuple)) and len(item) > 1:
                ids.add(str(item[1]))
            elif isinstance(item, dict) and item.get("prompt_id"):
                ids.add(str(item["prompt_id"]))
    return ids


def refine_comfy(x, refined, prog):
    if not x.comfy_url:
        raise Fail("not_installed", "the ComfyUI backend needs --comfy-url (the ComfyUI server with the Open-H3 nodes)")
    if not x.comfy_workflow or not os.path.isfile(x.comfy_workflow):
        raise Fail("not_installed", "the ComfyUI backend needs --comfy-workflow: an API-format export of the port's "
                   "'Draft Video' example workflow")
    try:
        with open(x.comfy_workflow, encoding="utf-8") as fh:
            wf = json.load(fh)
    except (OSError, ValueError) as e:
        raise Fail("not_installed", f"the ComfyUI workflow is not readable JSON: {e}")
    if not isinstance(wf, dict) or not wf:
        raise Fail("not_installed", "the ComfyUI workflow is not an API-format export ({node_id: {class_type, inputs}})")
    if not x.comfy_input_dir or not os.path.isdir(x.comfy_input_dir):
        raise Fail("not_installed", "the ComfyUI backend needs --comfy-input-dir (ComfyUI's input/ folder) to hand it the draft")
    name = f"act3_h3r_{hashlib.sha256(os.path.abspath(x.draft).encode()).hexdigest()[:12]}{os.path.splitext(x.draft)[1] or '.mp4'}"
    shutil.copyfile(x.draft, os.path.join(x.comfy_input_dir, name))
    prefix = f"act3_h3r/{os.path.basename(os.path.dirname(os.path.abspath(refined)))}"
    done, kinds = patch_workflow(wf, {
        "seed": x.seed, "max_frames": x.window_frames,
        "start_sigma": x.start_sigma if x.start_sigma is not None else DOC_START_SIGMA.get(x.steps, 0.909375),
        "steps": x.steps, "input_size": x.input_size, "resolution": x.resolution, "prompt": x.prompt_text, "video": name,
        "audio": x.model_audio, "prefix": prefix, "lora_file": LORA_FILES.get(x.lora, ""),
    })
    missing = sorted(required_kinds(x) - kinds)
    if missing:
        raise Fail("not_installed", f"the ComfyUI workflow has no input for {', '.join(missing)}, which this job decided "
                   "(it would run a different job than planned): export the port's Draft Video workflow in API format")
    say("ACT3_NOTE ComfyUI inputs set: " + ", ".join(done))
    say("ACT3_NOTE " + wire_decoder(wf, x.vae))
    base = x.comfy_url.rstrip("/")
    try:
        r = http_json(f"{base}/prompt", {"prompt": wf, "client_id": str(uuid.uuid4())})
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        raise classify(1, body, x.prompt_text) if RE_OOM.search(scrub(body, x.prompt_text)) else \
            Fail("internal", f"ComfyUI refused the workflow: {e.code} {scrub(body, x.prompt_text)[:400]}")
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise Fail("infra", f"ComfyUI is not reachable at {base}: {e}")
    pid = r.get("prompt_id") if isinstance(r, dict) else None
    if not pid:
        raise Fail("internal", f"ComfyUI returned no prompt_id: {json.dumps(r)[:300]}")
    x.comfy_prompt_id = pid
    prog.band(5, 95, "ComfyUI refine")
    t0 = time.time()
    last_ok = t0
    deadline = t0 + comfy_deadline(x)
    poll = max(0.01, float(os.environ.get("H3R_COMFY_POLL_S", "2") or 2))
    lost = 0
    while True:
        time.sleep(poll)
        now = time.time()
        if now > deadline:
            raise Fail("infra", f"ComfyUI did not finish this refine within {int((deadline - t0) / 60)} min (prompt {pid}); "
                       "it may be stuck or far slower than measured")
        try:
            h = http_json(f"{base}/history/{urllib.parse.quote(str(pid))}")
            last_ok = now
        except (urllib.error.URLError, OSError, ValueError) as e:
            if now - last_ok > 600:
                raise Fail("infra", f"ComfyUI stopped answering for 10 min: {e}")
            continue
        entry = h.get(pid) if isinstance(h, dict) else None
        if not entry:
            # Not in history: queued, running — or lost (ComfyUI restarted, or the queue was cleared). /queue tells.
            try:
                ids = _queue_ids(http_json(f"{base}/queue"))
                lost = 0 if str(pid) in ids else lost + 1
            except (urllib.error.URLError, OSError, ValueError):
                pass
            if lost >= 3:
                raise Fail("infra", f"ComfyUI lost the refine (prompt {pid} is neither queued, running nor in history: "
                           "a ComfyUI restart or a cleared queue)")
            prog.frac(min(0.9, (now - t0) / 600))  # creep the bar so the job never looks hung
            continue
        status = entry.get("status") or {}
        if status.get("status_str") == "error":
            msgs = scrub(json.dumps(status.get("messages", [])), x.prompt_text)[-1500:]
            raise classify(1, msgs, x.prompt_text) if RE_OOM.search(msgs) else Fail("internal", f"ComfyUI run failed: {msgs[-400:]}")
        files = []
        for out in (entry.get("outputs") or {}).values():
            if not isinstance(out, dict):
                continue
            for key in ("videos", "gifs", "images", "files"):
                for f in out.get(key, []) or []:
                    if isinstance(f, dict) and str(f.get("filename", "")).lower().endswith((".mp4", ".mov", ".webm", ".mkv")):
                        files.append(f)
        if not files:
            if status.get("completed") is False:
                continue  # in history but still running (some ComfyUI versions write the entry early)
            raise Fail("internal", "ComfyUI finished without a video output")
        f = files[-1]
        q = urllib.parse.urlencode({"filename": f["filename"], "subfolder": f.get("subfolder", ""), "type": f.get("type", "output")})
        tmp = refined + ".part"
        try:
            with urllib.request.urlopen(f"{base}/view?{q}", timeout=600) as resp, open(tmp, "wb") as out:
                shutil.copyfileobj(resp, out)
        except (urllib.error.URLError, OSError) as e:
            if os.path.exists(tmp):
                os.remove(tmp)
            raise Fail("infra", f"downloading the refined video from ComfyUI failed: {e}")
        if os.path.getsize(tmp) == 0:
            os.remove(tmp)
            raise Fail("internal", "ComfyUI's refined video is empty")
        os.replace(tmp, refined)
        return


# ── finish: trim the pad, re-mux the draft audio (§2.5, §10.2 result) ────────────────────────────────────────────


def finish(x, refined, info, prog):
    if not os.path.isfile(refined) or os.path.getsize(refined) == 0:
        raise Fail("internal", f"the refined video is missing ({refined}): the refine step did not finish")
    stage("encoding")
    prog.band(97, 100, "encoding")
    secs = x.output_seconds if x.output_seconds and x.output_seconds > 0 else None
    window = x.window_frames / FPS
    if info and info["seconds"] > 0:
        secs = min(secs or info["seconds"], info["seconds"], window)
    mux = x.remux == "on" or (x.remux == "auto" and info and info["has_audio"])
    if x.remux == "on" and info and not info["has_audio"]:
        say("ACT3_NOTE the draft has no audio track: the master is silent")
        mux = False
    tw, th = TARGETS[x.resolution]
    argv = [tool("ffmpeg"), "-y", "-v", "error", "-i", refined]
    if mux:
        argv += ["-i", x.draft]
    argv += ["-map", "0:v:0"] + (["-map", "1:a:0?"] if mux else [])
    if secs:
        argv += ["-t", f"{secs:.3f}"]
    # crop to the exact rung (1088 → 1080, 2176 → 2160) and never upscale here: the model made the pixels
    argv += ["-vf", f"crop='min(iw,{tw})':'min(ih,{th})',fps={FPS},format=yuv420p",
             "-c:v", "libx264", "-preset", "medium", "-crf", "16", "-pix_fmt", "yuv420p", "-movflags", "+faststart"]
    if mux:
        argv += ["-c:a", "aac", "-b:a", "256k"]
    else:
        argv += ["-an"]
    os.makedirs(os.path.dirname(os.path.abspath(x.out)), exist_ok=True)
    tmp = x.out + ".part.mp4"
    out = subprocess.run(argv + [tmp], capture_output=True, text=True)
    if out.returncode != 0 or not os.path.isfile(tmp) or os.path.getsize(tmp) == 0:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise classify(out.returncode, out.stderr) if RE_DISK.search(out.stderr) else Fail("internal", f"writing the master failed: {out.stderr.strip()[-400:]}")
    os.replace(tmp, x.out)  # the task host never sees a half-written output
    prog.set(100)
    return {"seconds": secs, "audio": "draft" if mux else "none"}


# ── provenance (§10.2 result: timings, peak VRAM, code + weight versions) ─────────────────────────────────────────


class VramSampler:
    """Peak GPU memory while the refine runs, from nvidia-smi every 2 s ($H3R_NVIDIA_SMI overrides the binary). Reads
    the GPU in CUDA_VISIBLE_DEVICES when it is a plain index, else the largest reading of all GPUs. None = not known."""

    def __init__(self, every=2.0):
        self.peak_mib = None
        self.every = every
        self.stop = threading.Event()
        self.smi = os.environ.get("H3R_NVIDIA_SMI") or shutil.which("nvidia-smi")
        self.thread = threading.Thread(target=self._run, daemon=True) if self.smi else None
        if self.thread:
            self.thread.start()

    def _read(self):
        argv = [self.smi, "--query-gpu=memory.used", "--format=csv,noheader,nounits"]
        dev = os.environ.get("CUDA_VISIBLE_DEVICES", "").split(",")[0].strip()
        if dev.isdigit():
            argv += ["-i", dev]
        try:
            out = subprocess.run(argv, capture_output=True, text=True, timeout=10).stdout
            vals = [float(v) for v in re.findall(r"\d+(?:\.\d+)?", out)]
            return max(vals) if vals else None
        except (OSError, subprocess.SubprocessError, ValueError):
            return None

    def _run(self):
        while True:
            v = self._read()
            if v is not None:
                self.peak_mib = v if self.peak_mib is None else max(self.peak_mib, v)
            if self.stop.wait(self.every):
                return

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=15)  # the reading in flight (a refine can be shorter than one sample)
        return self.peak_mib


def sana_commit(home, sana):
    """The NVlabs/Sana commit infer.py came from (§10.2: pinned so a re-run is reproducible). None when unknown."""
    for root in [r for r in (sana, os.path.join(home, "sana") if home else "") if r]:
        if os.path.isdir(root):
            try:
                out = subprocess.run(["git", "-C", root, "rev-parse", "HEAD"], capture_output=True, text=True, timeout=20)
                if out.returncode == 0 and re.fullmatch(r"[0-9a-f]{7,64}", out.stdout.strip()):
                    return out.stdout.strip()
            except (OSError, subprocess.SubprocessError):
                pass
    return None


def weights_revision(weights):
    """The Hugging Face revision the weights were downloaded at. Unverified where download.py records it: a REVISION /
    revision.txt / .revision file, else huggingface_hub's local-dir metadata (first line = commit). None = unknown."""
    if not weights:
        return None
    for name in ("REVISION", "revision.txt", ".revision"):
        t = read_text(os.path.join(weights, name)).strip()
        if t:
            return t.splitlines()[0].strip()[:80]
    meta = os.path.join(weights, ".cache", "huggingface", "download", "lora", "open_h3_regenerate_2k_lora.safetensors.metadata")
    first = read_text(meta).strip().splitlines()
    return first[0].strip()[:80] if first else None


def infer_timings(outputs):
    """infer.py writes <output>.json with timings (§9.1 --output). Read it for every pass, both spellings."""
    found = []
    for out in outputs:
        for cand in (out + ".json", re.sub(r"\.mp4$", "", out, flags=re.I) + ".json"):
            if os.path.isfile(cand):
                try:
                    with open(cand, encoding="utf-8") as fh:
                        found.append({"output": os.path.basename(out), "timings": json.load(fh)})
                    break
                except (OSError, ValueError):
                    pass
    return found


# ── settings stamp (idempotent retries) ──────────────────────────────────────────────────────────────────────────


def settings_key(x):
    keep = {k: getattr(x, k) for k in ("backend", "resolution", "input_size", "steps", "start_sigma", "fwd", "vae", "seed",
                                       "window_frames", "model_audio", "lora", "ladder", "extra_args")}
    keep["prompt"] = x.prompt_text
    try:
        st = os.stat(x.draft)
        keep["draft"] = [os.path.basename(x.draft), st.st_size, int(st.st_mtime)]
    except OSError:
        keep["draft"] = None
    return hashlib.sha256(json.dumps(keep, sort_keys=True).encode()).hexdigest()


def read_text(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return ""


def main(argv=None):
    a = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    a.add_argument("--stage", choices=["refine", "finish", "all"], default="all")
    a.add_argument("--backend", choices=["cli", "comfyui"], default="cli")
    a.add_argument("--home", default="")
    a.add_argument("--sana", default="", help="NVlabs/Sana checkout (default <home>/sana)")
    a.add_argument("--infer-python", default="", help="python for infer.py (default: this interpreter)")
    a.add_argument("--weights", default="")
    a.add_argument("--draft", required=True)
    a.add_argument("--prompt", default="")
    a.add_argument("--prompt-file", default="")
    a.add_argument("--resolution", choices=list(TARGETS), default="2k")
    a.add_argument("--input-size", choices=["768p", "576p"], default="768p")
    a.add_argument("--steps", type=int, choices=[2, 3], default=2)
    a.add_argument("--start-sigma", type=float, default=None)
    a.add_argument("--fwd", choices=["standard", "sol_engine"], default="standard")
    a.add_argument("--vae", choices=["fast", "standard"], default="fast")
    a.add_argument("--no-vae-retry", action="store_true")
    a.add_argument("--seed", type=int, default=1766580932)
    a.add_argument("--window-frames", type=int, default=CLI_WINDOW_FRAMES)
    a.add_argument("--model-audio", choices=["draft", "silence"], default="silence")
    a.add_argument("--lora", default="r576")
    a.add_argument("--ladder", action="store_true")
    a.add_argument("--allow-crop", action="store_true")
    a.add_argument("--allow-truncate", action="store_true", help="accept refining only the first window of a longer draft")
    a.add_argument("--refined", default="")
    a.add_argument("--out", default="")
    a.add_argument("--remux", choices=["auto", "on", "off"], default="auto")
    a.add_argument("--output-seconds", type=float, default=0)
    a.add_argument("--delivery", choices=["internal", "customer"], default="customer")
    a.add_argument("--license-cleared", action="store_true")
    a.add_argument("--comfy-url", default="")
    a.add_argument("--comfy-workflow", default="")
    a.add_argument("--comfy-input-dir", default="")
    a.add_argument("--extra-args", default=os.environ.get("H3R_EXTRA_ARGS", ""), help="appended to every infer.py call (unverified flags)")
    a.add_argument("--dry-run", action="store_true", help="print the plan as JSON and exit")
    x = a.parse_args(argv)

    prog = Progress()
    try:
        # §12: the license gate fails closed
        if x.delivery == "customer" and not x.license_cleared:
            raise Fail("license_not_cleared", "Open-H3-Regenerate-2K output may not be delivered to a customer until Legal "
                       "clears the MiniMax H3 Community License (h3_regenerate_2k.mdx §12)")
        x.prompt_text = x.prompt
        if x.prompt_file:
            try:
                with open(x.prompt_file, encoding="utf-8") as fh:
                    x.prompt_text = fh.read()
            except OSError as e:
                raise Fail("invalid_input", f"the prompt file cannot be read: {e}")
        x.prompt_text = (x.prompt_text or "").replace("\r\n", "\n").strip()
        if not x.prompt_text:
            raise Fail("invalid_input", "the refine needs a prompt describing the draft (spec_open_h3_regenerate_2k.mdx §1)")
        if re.search(r"<\s*(Subject|Picture|Video|Audio)\s*\d+\s*>", x.prompt_text, re.I):
            raise Fail("invalid_input", "the prompt still has reference labels (<Picture N> …); convert it first (spec_open_h3_regenerate_2k.mdx §4)")
        if x.start_sigma is not None and not (0.0 < x.start_sigma <= 0.909375 + 1e-9):
            raise Fail("invalid_input", f"start sigma {x.start_sigma} is outside (0, 0.909375]")
        if not (1 <= x.window_frames <= MAX_FRAMES):
            raise Fail("invalid_input", f"window of {x.window_frames} frames is outside 1..{MAX_FRAMES}")
        if x.backend == "cli" and x.window_frames > CLI_WINDOW_FRAMES:
            raise Fail("invalid_input", f"the official CLI refines {CLI_WINDOW_FRAMES} frames; use the ComfyUI backend for longer drafts")
        if not x.weights and x.home:
            x.weights = os.path.join(x.home, "weights")
        refined = x.refined or os.path.join(os.path.dirname(os.path.abspath(x.out or x.draft)), "refined.mp4")
        if x.stage in ("finish", "all") and not x.out:
            raise Fail("invalid_input", "--out is required for the finish stage")

        if x.dry_run:
            plan = {"refined": refined, "out": x.out, "latents": latents_for(refined)}
            if x.backend == "cli":
                plan["infer"] = infer_commands(x, "<infer_dir>", refined)
            say(json.dumps(plan))
            return 0

        stage("probing")
        info = probe(x.draft)
        check_draft(info, x.backend, x.window_frames, x.allow_crop, x.allow_truncate)
        prog.set(2)

        if x.stage in ("refine", "all"):
            key = settings_key(x)
            stamp = refined + ".settings"
            if os.path.isfile(refined) and os.path.getsize(refined) > 0 and read_text(stamp).strip() == key:
                say("ACT3_NOTE the refined video for these exact settings is already there; not refining again")
            else:
                for p in (refined, stamp):
                    if os.path.exists(p):
                        os.remove(p)
                os.makedirs(os.path.dirname(os.path.abspath(refined)), exist_ok=True)
                stage("refining")
                t0 = time.time()
                x.vae_used, x.comfy_prompt_id, x.infer_dir = x.vae, None, None
                vram = VramSampler()
                try:
                    (refine_cli if x.backend == "cli" else refine_comfy)(x, refined, prog)
                finally:
                    peak = vram.close()
                elapsed = time.time() - t0
                passes = [c[c.index("--output") + 1] for c in infer_commands(x, "<d>", refined)] if x.backend == "cli" else []
                refine_meta = {
                    "backend": x.backend, "elapsed_s": round(elapsed, 1), "vae_used": x.vae_used,
                    "peak_vram_mib": peak, "sana_commit": sana_commit(x.home, x.sana) if x.backend == "cli" else None,
                    "weights_revision": weights_revision(x.weights) if x.backend == "cli" else None,
                    "infer_timings": infer_timings(passes), "comfy_prompt_id": x.comfy_prompt_id,
                    "latents": [latents_for(o) for o in passes if os.path.isfile(latents_for(o))],
                }
                with open(refined + ".meta.json", "w") as f:
                    json.dump(refine_meta, f, indent=2)
                with open(stamp, "w") as f:
                    f.write(key)
                kept = refine_meta["latents"]
                say(f"ACT3_NOTE refined in {elapsed:.0f} s" + (f"; latent kept at {kept[-1]} (decode it by hand if finish fails; "
                    "no automatic decode-only resume exists)" if kept else ""))
        if x.stage in ("finish", "all"):
            res = finish(x, refined, info, prog)
            meta = {
                "model": "open-h3-regenerate-2k", "backend": x.backend, "resolution": x.resolution, "steps": x.steps,
                "start_sigma": x.start_sigma, "seed": x.seed, "input_size": x.input_size, "fwd": x.fwd, "vae": x.vae,
                "draft": {k: info[k] for k in ("width", "height", "fps", "seconds", "has_audio")},
                "output_seconds": res["seconds"], "audio": res["audio"], "lora": x.lora,
                "refine": json.loads(read_text(refined + ".meta.json") or "null"),
                "license": {"delivery": x.delivery, "cleared": bool(x.license_cleared),
                            "notice": "MiniMax H3 Community License: internal use only until Legal clears it"},
            }
            with open(x.out + ".json", "w") as f:
                json.dump(meta, f, indent=2)
            say(f"wrote {x.out} ({os.path.getsize(x.out)} bytes)")
        return 0
    except Fail as e:
        print(f"ACT3_ERROR {e.kind}: {e}", file=sys.stderr, flush=True)
        return EXIT.get(e.kind, 1)
    except KeyboardInterrupt:
        print("ACT3_ERROR internal: interrupted", file=sys.stderr, flush=True)
        return 1
    finally:
        prog.close()


if __name__ == "__main__":
    sys.exit(main())
