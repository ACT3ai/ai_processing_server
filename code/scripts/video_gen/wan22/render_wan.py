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

Wan2.2-TI2V-5B (ACT3_WAN_MODEL=wan2.2-ti2v-5b, for GPUs that cannot hold A14B) runs through the same script: its
model_index.json has one transformer (no boundary_ratio) and expand_timesteps, so it takes one guidance scale, a
float32 VAE (its model card), no Lightning, and image-to-video from the same folder (--i2v-model = --model).
On CUDA PyTorch is capped at 95% of the card, so an overflow fails here instead of spilling into system RAM (the
Windows driver's Sysmem Fallback froze the 5090). It offloads whole components (model offload) when the largest one
fits under that cap with 4 GiB to spare, and layer by layer (sequential offload, slower) when it does not or when a
render still runs out of GPU memory, so A14B renders on a 32 GB card and the 5B on an 8 GB one.
"""
import argparse
import importlib.util
import json
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


# ── ACT3 cancel mode (~/BGit/act3/ai_processing_server/pm/cancel.mdx §4.2) ───────────────────────────────────────────
# The render server's task host writes $ACT3_CANCEL_FILE when the job is cancelled. On Unix it also sends SIGINT, but on
# Windows a detached process cannot be sent a Ctrl-C, so the script watches the file itself: it prints
# "ACT3_STAGE cancelled" and raises KeyboardInterrupt in the main thread, which unwinds the finally blocks (free CUDA
# memory, stop a child server, close an ffmpeg pipe) inside the render type's grace. The task host kills the process
# tree when the grace runs out, and the controller removes the run directory with everything this script wrote.
ACT3_CANCEL_FILE = os.environ.get("ACT3_CANCEL_FILE", "")


def act3_cancelled():
    return bool(ACT3_CANCEL_FILE) and os.path.exists(ACT3_CANCEL_FILE)


def act3_watch_cancel(poll=1.0):
    if not ACT3_CANCEL_FILE:
        return
    import _thread
    import threading

    def watch():
        while not act3_cancelled():
            time.sleep(poll)
        print("ACT3_STAGE cancelled", flush=True)
        _thread.interrupt_main()

    threading.Thread(target=watch, name="act3-cancel", daemon=True).start()


def act3_exit_130_on_cancel():
    """A cancel that reaches the top of the script (finally blocks already ran) ends it with 130, the conventional
    "interrupted" code, and no traceback: a cancel is not a failure (ai_processing_server pm/cancel.mdx §4.2 C4)."""
    prev = sys.excepthook

    def hook(kind, value, tb):
        if issubclass(kind, KeyboardInterrupt) and act3_cancelled():
            print("ACT3_STAGE cancelled", flush=True)
            sys.stdout.flush()
            os._exit(130)
        prev(kind, value, tb)

    sys.excepthook = hook



act3_watch_cancel()  # before the model load: a cancel during a multi-minute load is graceful too
act3_exit_130_on_cancel()

def stage(name):
    print(f"ACT3_STAGE {name}", flush=True)


def lora_pair(folder):
    if not folder:
        return None
    high = os.path.join(folder, "high_noise_model.safetensors")
    low = os.path.join(folder, "low_noise_model.safetensors")
    return (high, low) if os.path.exists(high) and os.path.exists(low) else None


def model_config(folder):
    try:
        with open(os.path.join(folder, "model_index.json")) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


# PyTorch's share of the GPU on CUDA: the rest is the driver's, and an allocation past it fails instead of spilling
VRAM_FRACTION = 0.95


def largest_part_gib(p):
    """The biggest component's weights: model offload moves one component at a time onto the GPU."""
    sizes = [
        sum(t.numel() * t.element_size() for t in m.parameters())
        for m in p.components.values()
        if isinstance(m, torch.nn.Module)
    ]
    return max(sizes, default=0) / 2**30


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

cfg = model_config(x.i2v_model if i2v else x.model)
# A14B: two experts switched at boundary_ratio. TI2V-5B: one transformer, boundary_ratio null. No readable config:
# the A14B behaviour this script always had.
two_experts = cfg.get("boundary_ratio") is not None if cfg else True
pair_found = lora_pair(x.lightning_i2v if i2v else x.lightning) if two_experts else None
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
# Read each shard and let it go: with mmap, Windows keeps every fp32 shard of an expert mapped while its bf16 copy is
# built, and loading A14B's second expert nearly exhausted a 128 GB machine's RAM.
extra_load = {"disable_mmap": True}
if cfg.get("expand_timesteps"):
    from diffusers import AutoencoderKLWan

    extra_load["vae"] = AutoencoderKLWan.from_pretrained(
        x.i2v_model if i2v else x.model, subfolder="vae", torch_dtype=torch.float32
    )
if i2v:
    from diffusers import WanImageToVideoPipeline

    pipe = WanImageToVideoPipeline.from_pretrained(x.i2v_model, torch_dtype=torch.bfloat16, **extra_load)
    guidance, guidance_2 = 3.5, 3.5
else:
    from diffusers import WanPipeline

    pipe = WanPipeline.from_pretrained(x.model, torch_dtype=torch.bfloat16, **extra_load)
    guidance, guidance_2 = 4.0, 3.0
if not two_experts:
    guidance, guidance_2 = 5.0, None  # diffusers refuses guidance_scale_2 without a boundary_ratio
pair = pair_found
steps = x.steps
if pair:
    pipe.load_lora_weights(pair[0], adapter_name="lightning")
    pipe.load_lora_weights(pair[1], adapter_name="lightning_2", load_into_transformer_2=True)
    guidance, guidance_2 = 1.0, 1.0
    print("lightning LoRAs loaded", flush=True)
elif x.steps_full:
    steps = x.steps_full

layer_by_layer = False


def offload_layer_by_layer(why):
    global layer_by_layer
    print(f"ACT3_NOTE {why}: offloading layer by layer", flush=True)
    pipe.enable_sequential_cpu_offload()
    pipe.vae.enable_tiling()
    layer_by_layer = True


if x.device == "cuda":
    # Windows' driver puts a CUDA allocation that does not fit the card into system RAM instead of failing (Sysmem
    # Fallback), which can freeze the machine: capped, an overflow is an out-of-memory error here instead.
    torch.cuda.set_per_process_memory_fraction(VRAM_FRACTION)
    cap_gib = VRAM_FRACTION * torch.cuda.get_device_properties(0).total_memory / 2**30
    part_gib = largest_part_gib(pipe)
    if part_gib + 4 <= cap_gib:
        pipe.enable_model_cpu_offload()  # one part on the GPU at a time, with room for its activations
    else:
        offload_layer_by_layer(f"largest part {part_gib:.1f} GiB, GPU cap {cap_gib:.1f} GiB")
else:
    pipe.to("mps")
print(f"loaded {'i2v' if i2v else 't2v'} in {time.time() - t0:.0f} s, {steps} steps", flush=True)


def on_step(_pipe, i, _t, kwargs):
    print(f"ACT3_PROGRESS {100.0 * (i + 1) / steps:.1f}", flush=True)
    if act3_cancelled():  # stop between diffusion steps, never mid-kernel
        raise KeyboardInterrupt("cancelled")
    return kwargs


def render():
    return pipe(
        prompt=x.prompt,
        negative_prompt=x.negative or None,
        width=x.width,
        height=x.height,
        num_frames=x.frames,
        num_inference_steps=steps,
        guidance_scale=guidance,
        guidance_scale_2=guidance_2,
        generator=torch.Generator(device="cpu").manual_seed(x.seed),
        callback_on_step_end=on_step,
        **extra,
    ).frames[0]


stage("rendering")
extra = {"image": first_frame(x.image, x.width, x.height)} if i2v else {}
try:
    frames = render()
except torch.cuda.OutOfMemoryError:
    if x.device != "cuda" or layer_by_layer:
        raise
    pipe.remove_all_hooks()
    torch.cuda.empty_cache()
    offload_layer_by_layer("out of GPU memory with whole parts on the GPU")
    frames = render()
stage("encoding")
os.makedirs(os.path.dirname(os.path.abspath(x.out)), exist_ok=True)
n = write_mp4(frames, x.out, x.fps)
print(f"wrote {x.out} ({n} frames, {os.path.getsize(x.out)} bytes) in {time.time() - t0:.0f} s", flush=True)
