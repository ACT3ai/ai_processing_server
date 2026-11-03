# COPY of ~/BGit/all/film/code/subp/video_gen_ai/wan/v2_2_a14b/local/render_wan.py, kept here so the task's code
# is frozen into each snapshot (pm/task_authoring.mdx R1, R2). Edit the source in the all repo, then run
# code/scripts/sync_from_all.sh. Run by the oss_video runner (code/internal/runners/oss_video.go).
"""render_wan.py: render ONE Wan 2.2 A14B job for an ACT3 render server (CUDA or MPS, BF16): text-to-video from the
prompt, or image-to-video from the shot's first frame (--image) when the I2V-A14B weights are installed.

Called by the render server's executor through the wan_2_2 local plugin (./wan-local.ts builds the exact argv).
Prints "ACT3_PROGRESS <0-100>" after every denoising step so the internal web app can draw a live bar, and
"ACT3_STAGE <name>" when the stage changes. Exit code 0 = the MP4 is at --out; anything else = failed (stderr tail
is the reason). Model facts: ~/BGit/all/film/pm/subp/video_gen_ai/wan/v2_2_a14b/wan_v2_2_a14b.mdx §10.7
(WanPipeline / WanImageToVideoPipeline, two experts switched by boundary_ratio from the config, 16 fps).

With --image but no I2V weights at --i2v-model, it renders text-to-video and prints "ACT3_NOTE first frame not used"
so the reason is in the job's log: a missing optional model never fails the job. A missing package (peft for the
LoRAs, imageio for an H.264 MP4) fails it at once, before the weights load.

The Lightning pair (lightx2v/Wan2.2-Lightning) is one LoRA per expert: high_noise_model.safetensors into
`transformer`, low_noise_model.safetensors into `transformer_2` (diffusers >= 0.35 `load_into_transformer_2`).
T2V and I2V have their own pair (--lightning, --lightning-i2v). When no pair is loaded and the plugin passed
--steps-full, that step count is used instead of the 4-step Lightning count.
"""
import argparse
import importlib.util
import os
import sys
import time

import numpy as np
import torch

a = argparse.ArgumentParser()
a.add_argument("--model", required=True)
a.add_argument("--out", required=True)
a.add_argument("--prompt", required=True)
a.add_argument("--negative", default="")
a.add_argument("--width", type=int, default=1280)
a.add_argument("--height", type=int, default=720)
a.add_argument("--frames", type=int, default=81)
a.add_argument("--fps", type=int, default=16)
a.add_argument("--steps", type=int, default=30)
a.add_argument("--steps-full", type=int, default=0)
a.add_argument("--seed", type=int, default=42)
a.add_argument("--device", choices=["cuda", "mps"], required=True)
a.add_argument("--lightning", default="")
a.add_argument("--image", default="")
a.add_argument("--i2v-model", default="")
a.add_argument("--lightning-i2v", default="")
x = a.parse_args()


def write_mp4(frames, path, fps):
    """The take as every browser plays it: H.264, yuv420p, index at the front (+faststart), so the first frame shows
    before the whole file has downloaded. diffusers' export_to_video left the index at the end. Exits non-zero
    (the job fails with this sentence) rather than leave a file the render server would upload as a broken take."""
    import imageio.v2 as imageio

    tmp = f"{path}.part.mp4"
    w = imageio.get_writer(
        tmp,
        fps=fps,
        codec="libx264",
        quality=None,
        pixelformat="yuv420p",
        macro_block_size=16,
        ffmpeg_params=["-crf", "16", "-preset", "medium", "-movflags", "+faststart"],
    )
    n = 0
    try:
        for f in frames:
            a = np.asarray(f)
            if a.dtype != np.uint8:
                a = (np.clip(a, 0.0, 1.0) * 255.0).round().astype(np.uint8)
            w.append_data(a)
            n += 1
    finally:
        w.close()
    if n == 0 or not os.path.exists(tmp) or os.path.getsize(tmp) == 0:
        sys.exit(f"writing the MP4 failed: {n} frame(s), {os.path.getsize(tmp) if os.path.exists(tmp) else 0} bytes")
    os.replace(tmp, path)  # the render server never sees a half-written output.mp4
    return n


def stage(name):
    print(f"ACT3_STAGE {name}", flush=True)


def lora_pair(folder):
    if not folder:
        return None
    high = os.path.join(folder, "high_noise_model.safetensors")
    low = os.path.join(folder, "low_noise_model.safetensors")
    return (high, low) if os.path.exists(high) and os.path.exists(low) else None


def first_frame(path, width, height):
    """The first frame cropped to the target aspect (centre) and sized to width x height."""
    from PIL import Image

    img = Image.open(path).convert("RGB")
    want = width / height
    w, h = img.size
    if w / h > want:
        nw = round(h * want)
        img = img.crop(((w - nw) // 2, 0, (w - nw) // 2 + nw, h))
    elif w / h < want:
        nh = round(w / want)
        img = img.crop((0, (h - nh) // 2, w, (h - nh) // 2 + nh))
    return img.resize((width, height), Image.LANCZOS)


if x.device == "mps" and not torch.backends.mps.is_available():
    sys.exit("PyTorch does not see Metal (MPS)")
if x.device == "cuda" and not torch.cuda.is_available():
    sys.exit("PyTorch does not see a CUDA GPU")

i2v = False
if x.image:
    if not os.path.exists(x.image):
        sys.exit(f"The first frame is missing: {x.image}")
    if x.i2v_model and os.path.exists(os.path.join(x.i2v_model, "model_index.json")):
        i2v = True
    else:
        print(
            "ACT3_NOTE first frame not used: the Wan2.2-I2V-A14B weights are not installed "
            f"({x.i2v_model or 'no --i2v-model'}); rendering text-to-video from the prompt",
            flush=True,
        )

pair_found = lora_pair(x.lightning_i2v if i2v else x.lightning)
# Fail in a second, before loading two 14B experts, when the venv lacks what this run needs. A job on 2026-10-27
# loaded both experts, then died at load_lora_weights because peft was missing (~/to/1_22.txt).
missing = [m for m in (["peft"] if pair_found else []) + ["imageio"] if importlib.util.find_spec(m) is None]
if missing:
    sys.exit(
        f"The Wan venv is missing {', '.join(missing)}: run `just build` in ~/BGit/all/app/code/film/servers "
        "(or `just venv` in ~/BGit/all/app/smartc/film/servers/bms/oss_video/wan_2_2/code)"
    )
# peft present is not enough: diffusers uses it only when USE_PEFT_BACKEND (peft and transformers new enough), and
# load_lora_weights raises "PEFT backend is required" otherwise. Check the exact flag it checks.
if pair_found:
    from diffusers.utils import USE_PEFT_BACKEND

    if not USE_PEFT_BACKEND:
        import importlib.metadata as md

        def ver(name):
            try:
                return md.version(name)
            except md.PackageNotFoundError:
                return "none"

        sys.exit(
            f"The Wan venv is missing a usable peft (diffusers USE_PEFT_BACKEND is False; peft {ver('peft')}, "
            f"transformers {ver('transformers')}): run `just venv` in "
            "~/BGit/all/app/smartc/film/servers/bms/oss_video/wan_2_2/code"
        )

t0 = time.time()
stage("loading")
if i2v:
    from diffusers import WanImageToVideoPipeline

    pipe = WanImageToVideoPipeline.from_pretrained(x.i2v_model, torch_dtype=torch.bfloat16)
    guidance, guidance_2 = 3.5, 3.5
else:
    from diffusers import WanPipeline

    pipe = WanPipeline.from_pretrained(x.model, torch_dtype=torch.bfloat16)
    guidance, guidance_2 = 4.0, 3.0
pair = pair_found
steps = x.steps
if pair:
    pipe.load_lora_weights(pair[0], adapter_name="lightning")
    pipe.load_lora_weights(pair[1], adapter_name="lightning_2", load_into_transformer_2=True)
    guidance, guidance_2 = 1.0, 1.0
    print("lightning LoRAs loaded", flush=True)
elif x.steps_full:
    steps = x.steps_full
if x.device == "cuda":
    pipe.enable_model_cpu_offload()  # two 14B experts do not both fit 32 GB
else:
    pipe.to("mps")
print(f"loaded {'i2v' if i2v else 't2v'} in {time.time() - t0:.0f} s, {steps} steps", flush=True)


def on_step(_pipe, i, _t, kwargs):
    print(f"ACT3_PROGRESS {100.0 * (i + 1) / steps:.1f}", flush=True)
    return kwargs


stage("rendering")
gen = torch.Generator(device="cpu").manual_seed(x.seed)
extra = {"image": first_frame(x.image, x.width, x.height)} if i2v else {}
frames = pipe(
    prompt=x.prompt,
    negative_prompt=x.negative or None,
    width=x.width,
    height=x.height,
    num_frames=x.frames,
    num_inference_steps=steps,
    guidance_scale=guidance,
    guidance_scale_2=guidance_2,
    generator=gen,
    callback_on_step_end=on_step,
    **extra,
).frames[0]
stage("encoding")
os.makedirs(os.path.dirname(os.path.abspath(x.out)), exist_ok=True)
n = write_mp4(frames, x.out, x.fps)
print(f"wrote {x.out} ({n} frames, {os.path.getsize(x.out)} bytes) in {time.time() - t0:.0f} s", flush=True)
