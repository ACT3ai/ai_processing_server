# COPY of ~/BGit/all/film/code/subp/video_gen_ai/kandinsky/video_pro_6_0/local/render_kandinsky.py, kept here so the task's code
# is frozen into each snapshot (pm/task_authoring.mdx R1, R2). Edit the source in the all repo, then run
# code/scripts/sync_from_all.sh. Run by the oss_video runner for Kandinsky 6.0 (code/internal/runners/oss_kandinsky.go).
"""render_kandinsky.py: render ONE Kandinsky 6.0 job (Lite-distill or Pro) for an ACT3 render server. INTERNAL ONLY.

Called by ai_processing_server's oss_video runner (and described by ./kandinsky-local.ts, which builds the same argv).
Generalizes the BMS smoke test (~/BGit/all/app/smartc/film/servers/bms/oss_video/kandinsky_6/code/smoke_kandinsky.py),
including its audio: the pipeline returns 44.1 kHz audio with the video, written to a WAV and muxed as AAC with ffmpeg
($ACT3_FFMPEG, else ffmpeg on PATH). Prints "ACT3_PROGRESS <pct>" per step and "ACT3_STAGE <name>".

On CUDA it offloads whole components (model offload) when the largest one fits the GPU with headroom, and layer by
layer (sequential offload) when it does not: Pro's 29B transformer does not fit the RTX 5090's 32 GB in BF16.
On MPS an unsupported op is a FINDING, not a bug: the stderr tail says which op.
Spec: ~/BGit/all/film/pm/subp/video_gen_ai/kandinsky/video_pro_6_0/kandinsky_video_pro_6_0.mdx §10.6, §12.3.
"""
import argparse
import os
import subprocess
import sys
import time
import wave

import torch
from diffusers import Kandinsky6TI2VAPipeline
from diffusers.utils import export_to_video, load_image

a = argparse.ArgumentParser()
a.add_argument("--model", required=True)
a.add_argument("--out", required=True)
a.add_argument("--prompt", required=True)
a.add_argument("--image", default="")
a.add_argument("--width", type=int, default=864)
a.add_argument("--height", type=int, default=480)
a.add_argument("--frames", type=int, default=121)
a.add_argument("--fps", type=int, default=24)
a.add_argument("--steps", type=int, default=10)
a.add_argument("--guidance", type=float, default=1.0)
a.add_argument("--seed", type=int, default=42)
a.add_argument("--device", choices=["cuda", "mps"], required=True)
x = a.parse_args()


def stage(name):
    print(f"ACT3_STAGE {name}", flush=True)


def largest_part_gib(p):
    """The biggest component's weights: model offload moves one component at a time onto the GPU."""
    sizes = [
        sum(t.numel() * t.element_size() for t in m.parameters())
        for m in p.components.values()
        if isinstance(m, torch.nn.Module)
    ]
    return max(sizes, default=0) / 2**30


if x.device == "mps" and not torch.backends.mps.is_available():
    sys.exit("PyTorch does not see Metal (MPS)")
if x.device == "cuda" and not torch.cuda.is_available():
    sys.exit("PyTorch does not see a CUDA GPU")
if x.image and not os.path.exists(x.image):
    sys.exit(f"The first frame is missing: {x.image}")

t0 = time.time()
stage("loading")
pipe = Kandinsky6TI2VAPipeline.from_pretrained(x.model, torch_dtype=torch.bfloat16)
if x.device == "cuda":
    gpu_gib = torch.cuda.get_device_properties(0).total_memory / 2**30
    part_gib = largest_part_gib(pipe)
    if part_gib + 3 <= gpu_gib:
        pipe.enable_model_cpu_offload()
    else:
        print(
            f"ACT3_NOTE low VRAM: offloading layer by layer (largest part {part_gib:.1f} GiB, GPU {gpu_gib:.1f} GiB)",
            flush=True,
        )
        pipe.enable_sequential_cpu_offload()
else:
    pipe.to("mps")
print(f"loaded in {time.time() - t0:.0f} s", flush=True)


def on_step(_pipe, i, _t, kwargs):
    print(f"ACT3_PROGRESS {100.0 * (i + 1) / x.steps:.1f}", flush=True)
    return kwargs


stage("rendering")
extra = {"image": load_image(x.image)} if x.image else {}
out = pipe(
    prompt=x.prompt,
    height=x.height,
    width=x.width,
    num_frames=x.frames,
    num_inference_steps=x.steps,
    guidance_scale=x.guidance,
    generator=torch.Generator(device="cpu").manual_seed(x.seed),
    callback_on_step_end=on_step,
    **extra,
)
frames = out.frames[0] if hasattr(out, "frames") else out[0]
audio = getattr(out, "audio", None)

stage("encoding")
os.makedirs(os.path.dirname(os.path.abspath(x.out)), exist_ok=True)
base = x.out[:-4] if x.out.endswith(".mp4") else x.out
silent = f"{base}.silent.mp4"
export_to_video(frames, silent, fps=x.fps)  # imageio + imageio-ffmpeg: H.264, which browsers play
if audio is None:
    os.replace(silent, x.out)  # the render server never sees a half-written output
else:
    pcm = (torch.as_tensor(audio[0]).float().cpu().clamp(-1, 1).reshape(-1) * 32767).short().numpy()
    wav = f"{base}.wav"
    with wave.open(wav, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(pipe.audio_sample_rate)
        w.writeframes(pcm.tobytes())
    muxed = f"{base}.muxed.mp4"
    ffmpeg = os.environ.get("ACT3_FFMPEG") or "ffmpeg"
    r = subprocess.run(
        [ffmpeg, "-y", "-loglevel", "error", "-i", silent, "-i", wav, "-c:v", "copy", "-c:a", "aac", "-shortest",
         "-movflags", "+faststart", muxed],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        sys.exit(f"muxing the audio failed: {(r.stderr or '').strip().splitlines()[-1:]}")
    os.replace(muxed, x.out)
    for f in (silent, wav):
        os.remove(f)
    print(f"audio {len(pcm) / pipe.audio_sample_rate:.1f} s at {pipe.audio_sample_rate} Hz", flush=True)
print(f"wrote {x.out} ({os.path.getsize(x.out)} bytes) in {time.time() - t0:.0f} s", flush=True)
