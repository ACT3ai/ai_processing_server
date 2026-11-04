# COPY of ~/BGit/all/film/code/subp/video_gen_ai/nvidia/h3_regenerate_2k/render_h3_generate.py, kept here so the task's code
# is frozen into each snapshot (pm/task_authoring.mdx R1, R2). Edit the source in the all repo, then run
# code/scripts/sync_from_all.sh. Run by the oss_video runner for MiniMax H3 (code/internal/runners/oss_h3.go); it imports render_h3_regenerate.py beside it.
"""render_h3_generate.py: render ONE MiniMax H3 clip (text or first frame to video, with audio) on an ACT3 render server,
through ComfyUI's native MiniMax-H3 nodes. Python 3 standard library only: the model runs in a private ComfyUI this
script starts on a free localhost port and stops when the render ends (the same install and plumbing as
./render_h3_regenerate.py, whose helpers it imports; ai_processing_server snapshots both files together).

Graph: UNETLoader (int8 FL2VA base) → MiniMaxH3SigmaShift → BasicGuider; CLIPLoader (Qwen3-VL, type minimax) +
VAELoader → MiniMaxH3ImageToVideo (prompt, size, length, optional first frame) → SamplerCustomAdvanced (euler, simple
schedule, CFG off: the base is guidance-distilled) → LTXVSeparateAVLatent → VAEDecode + VAEDecodeAudio → CreateVideo
→ SaveVideo. The clip is cut back to the requested length (H3 snaps frames up to its 17k+5 grid).

Output grammar as in ./render_h3_regenerate.py: "ACT3_STAGE", "ACT3_PROGRESS", "ACT3_NOTE", and one final
"ACT3_ERROR <kind>: <sentence>" with exit 2 invalid_input, 3 infra, 5 not_installed, 1 internal.
License: the MiniMax H3 Community License excludes the US from its free grant; ai_processing_server refuses customer
jobs for this model (internal testing only, model spec minimax_omni.mdx §10).
Spec: ~/BGit/all/film/pm/subp/video_gen_ai/minimax/fast_h3_omni/minimax_omni.mdx; prompt format:
~/BGit/all/film/pm/subp/ai_prompts/video_gen_prompts/s/minimax/spec_minimax.mdx.
"""
import argparse
import os
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from render_h3_regenerate import (  # noqa: E402
    COMFY_FILES, FPS, ComfyServer, Fail, Progress, comfy_dir, deadline_override, error_exit, node_option, run_workflow,
    say, setup_process, stage, tool,
)

MIN_FRAMES, MAX_FRAMES = 124, 362  # H3's trained range on its 17k+5 grid (5.2 s to 15 s)


def grid_frames(n):
    """H3's frame grid: 17k + 5, inside the trained range (a shorter ask renders 124 frames and is cut back)."""
    n = max(MIN_FRAMES, n)
    return n + (5 - n % 17) % 17


def workflow(x, files, image, prefix):
    i2v = {"clip": ["3", 0], "vae": ["4", 0], "prompt": x.prompt_text, "width": x.width, "height": x.height,
           "length": x.frames}
    wf = {
        "1": {"class_type": "UNETLoader", "inputs": {"unet_name": files["base_model"], "weight_dtype": "default"}},
        "2": {"class_type": "MiniMaxH3SigmaShift", "inputs": {"model": ["1", 0], "shift_video": x.shift_video, "shift_audio": x.shift_audio}},
        "3": {"class_type": "CLIPLoader", "inputs": {"clip_name": files["text_encoder"], "type": "minimax", "device": "default"}},
        "4": {"class_type": "VAELoader", "inputs": {"vae_name": files["video_vae"]}},
        "5": {"class_type": "VAELoader", "inputs": {"vae_name": files["audio_vae"]}},
        "7": {"class_type": "MiniMaxH3ImageToVideo", "inputs": i2v},
        "8": {"class_type": "RandomNoise", "inputs": {"noise_seed": x.seed}},
        "9": {"class_type": "BasicGuider", "inputs": {"model": ["2", 0], "conditioning": ["7", 0]}},
        "10": {"class_type": "KSamplerSelect", "inputs": {"sampler_name": "euler"}},
        "11": {"class_type": "BasicScheduler", "inputs": {"model": ["2", 0], "scheduler": "simple", "steps": x.steps, "denoise": 1.0}},
        "12": {"class_type": "SamplerCustomAdvanced", "inputs": {"noise": ["8", 0], "guider": ["9", 0], "sampler": ["10", 0],
                                                                 "sigmas": ["11", 0], "latent_image": ["7", 1]}},
        "13": {"class_type": "LTXVSeparateAVLatent", "inputs": {"av_latent": ["12", 0]}},
        "14": {"class_type": "VAEDecode", "inputs": {"samples": ["13", 0], "vae": ["4", 0]}},
        "15": {"class_type": "VAEDecodeAudio", "inputs": {"samples": ["13", 1], "vae": ["5", 0]}},
        "16": {"class_type": "CreateVideo", "inputs": {"images": ["14", 0], "fps": float(FPS), "audio": ["15", 0]}},
        "17": {"class_type": "SaveVideo", "inputs": {"video": ["16", 0], "filename_prefix": prefix, "format": "mp4",
                                                     "format.codec": "h264", "format.codec.encoding": "re-encode",
                                                     "format.codec.encoding.crf": 12.0}},
    }
    if image:
        wf["6"] = {"class_type": "LoadImage", "inputs": {"image": image}}
        i2v["first_frame"] = ["6", 0]
    return wf


def deadline(x):
    """~30 s a step for a 5 s 768p clip on a 32 GB card, scaled by length, with room for loading; 15 min to 6 h."""
    scale = (x.width * x.height) / (1344 * 768) * (x.frames / MIN_FRAMES)
    return deadline_override() or max(15 * 60.0, min(6 * 3600.0, 4 * (30 * x.steps * scale) + 600))


def cut(src, out, seconds):
    tmp = out + ".part.mp4"
    r = subprocess.run([tool("ffmpeg"), "-y", "-v", "error", "-i", src, "-t", f"{seconds:.3f}", "-map", "0:v:0", "-map", "0:a:0?",
                        "-c:v", "libx264", "-preset", "medium", "-crf", "16", "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "192k",
                        "-movflags", "+faststart", tmp], capture_output=True, text=True)
    if r.returncode != 0 or not os.path.isfile(tmp) or os.path.getsize(tmp) == 0:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise Fail("internal", f"writing the clip failed: {r.stderr.strip()[-400:]}")
    os.replace(tmp, out)  # the task host never sees a half-written output


def main(argv=None):
    a = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    a.add_argument("--home", required=True, help="the install: <home>/ComfyUI")
    a.add_argument("--out", required=True)
    a.add_argument("--prompt", default="")
    a.add_argument("--prompt-file", default="")
    a.add_argument("--image", default="", help="first frame (image to video); omitted = text to video")
    a.add_argument("--width", type=int, default=1344)
    a.add_argument("--height", type=int, default=768)
    a.add_argument("--seconds", type=float, default=5.0)
    a.add_argument("--steps", type=int, default=20)
    a.add_argument("--seed", type=int, default=42)
    a.add_argument("--shift-video", type=float, default=12.0)
    a.add_argument("--shift-audio", type=float, default=3.0)
    a.add_argument("--comfy-home", default="")
    a.add_argument("--comfy-python", default="")
    x = a.parse_args(argv)
    prog = Progress()
    try:
        x.prompt_text = x.prompt
        if x.prompt_file:
            try:
                with open(x.prompt_file, encoding="utf-8") as fh:
                    x.prompt_text = fh.read()
            except OSError as e:
                raise Fail("invalid_input", f"the prompt file cannot be read: {e}")
        x.prompt_text = (x.prompt_text or "").replace("\r\n", "\n").strip()
        if not x.prompt_text:
            raise Fail("invalid_input", "H3 needs a prompt")
        if x.image and not os.path.isfile(x.image):
            raise Fail("invalid_input", f"the first frame is missing: {x.image}")
        if x.width % 32 or x.height % 32 or min(x.width, x.height) < 256:
            raise Fail("invalid_input", f"{x.width}x{x.height}: H3 sides are multiples of 32, at least 256")
        if not 0 < x.seconds <= MAX_FRAMES / FPS + 0.01:
            raise Fail("invalid_input", f"{x.seconds} s: one H3 clip is at most {MAX_FRAMES / FPS:.1f} s")
        if not 1 <= x.steps <= 100:
            raise Fail("invalid_input", f"{x.steps} steps is outside 1..100")
        if not 0 <= x.seed < 2 ** 64:
            raise Fail("invalid_input", f"seed {x.seed} is outside 0..2^64-1")
        x.frames = min(MAX_FRAMES, grid_frames(round(x.seconds * FPS)))
        root = comfy_dir(x, olm=False)
        stage("loading")
        # scratch beside the task's working dir, never beside the output
        with ComfyServer(x, root, os.path.join(os.getcwd(), "comfy"), prog) as srv:
            prog.band(1, 5, "starting ComfyUI")
            srv.wait_ready()
            files = {k: node_option(srv.base, node, field, COMFY_FILES[k]) for k, node, field in (
                ("base_model", "UNETLoader", "unet_name"), ("text_encoder", "CLIPLoader", "clip_name"),
                ("video_vae", "VAELoader", "vae_name"), ("audio_vae", "VAELoader", "vae_name"))}
            image = ""
            if x.image:
                image = "first_frame" + (os.path.splitext(x.image)[1] or ".png")
                shutil.copyfile(x.image, os.path.join(srv.dirs["input"], image))
            say(f"ACT3_NOTE {'image' if image else 'text'} to video: {x.width}x{x.height}, {x.frames} frames, {x.steps} steps")
            stage("rendering")
            prog.band(5, 95, "rendering")
            t0 = time.time()
            clip = run_workflow(srv, workflow(x, files, image, "act3_h3/clip"), x, deadline(x))
            stage("encoding")
            prog.band(95, 100, "encoding")
            os.makedirs(os.path.dirname(os.path.abspath(x.out)), exist_ok=True)
            cut(clip, x.out, x.seconds)
        prog.set(100)
        say(f"wrote {x.out} ({os.path.getsize(x.out)} bytes) in {time.time() - t0:.0f} s")
        return 0
    except (Exception, KeyboardInterrupt) as e:
        return error_exit(e)
    finally:
        prog.close()


if __name__ == "__main__":
    setup_process()
    sys.exit(main())
