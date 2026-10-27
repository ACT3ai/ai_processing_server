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
so the reason is in the job's log: a missing optional model never fails the job.

The Lightning pair (lightx2v/Wan2.2-Lightning) is one LoRA per expert: high_noise_model.safetensors into
`transformer`, low_noise_model.safetensors into `transformer_2` (diffusers >= 0.35 `load_into_transformer_2`).
T2V and I2V have their own pair (--lightning, --lightning-i2v). When no pair is loaded and the plugin passed
--steps-full, that step count is used instead of the 4-step Lightning count.
"""
import argparse
import os
import sys
import time

import torch
from diffusers.utils import export_to_video

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

t0 = time.time()
stage("loading")
if i2v:
    from diffusers import WanImageToVideoPipeline

    pipe = WanImageToVideoPipeline.from_pretrained(x.i2v_model, torch_dtype=torch.bfloat16)
    guidance, guidance_2 = 3.5, 3.5
    pair = lora_pair(x.lightning_i2v)
else:
    from diffusers import WanPipeline

    pipe = WanPipeline.from_pretrained(x.model, torch_dtype=torch.bfloat16)
    guidance, guidance_2 = 4.0, 3.0
    pair = lora_pair(x.lightning)
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
export_to_video(frames, x.out, fps=x.fps)
print(f"wrote {x.out} in {time.time() - t0:.0f} s", flush=True)
