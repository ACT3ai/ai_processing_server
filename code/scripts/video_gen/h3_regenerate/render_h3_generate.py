# COPY of ~/BGit/all/film/code/subp/video_gen_ai/nvidia/h3_regenerate_2k/render_h3_generate.py, kept here so the task's code
# is frozen into each snapshot (pm/task_authoring.mdx R1, R2). Edit the source in the all repo, then run
# code/scripts/sync_from_all.sh. Run by the oss_video runner for MiniMax H3 (code/internal/runners/oss_h3.go); it imports render_h3_regenerate.py beside it.
"""render_h3_generate.py: render ONE MiniMax H3 clip (video with audio) on an ACT3 render server,
through ComfyUI's native MiniMax-H3 nodes. Python 3 standard library only: the model runs in a private ComfyUI this
script starts on a free localhost port and stops when the render ends (the same install and plumbing as
./render_h3_regenerate.py, whose helpers it imports; ai_processing_server snapshots both files together).

Two tasks (spec_minimax.mdx §1):
* T2VA / I2VA / FL2VA: the FL2VA base and MiniMaxH3ImageToVideo with an optional first and last frame.
* Ref2VA, the Seedance-like path: any --ref-image (`<Picture N>` in order, first frame included) or --ref-audio
  (`<Audio N>`, voice timbre or a line to re-perform) switches to the Ref2VA base and MiniMaxH3ReferenceToVideo; the
  prompt is the six-section Ref2VA form that names those labels. At most 9 images and 3 audio clips (15 s in all);
  audio is never the only reference.
Both: MiniMaxH3SigmaShift → BasicGuider → SamplerCustomAdvanced (euler, simple schedule, CFG off) →
LTXVSeparateAVLatent → VAEDecode + VAEDecodeAudio → CreateVideo → SaveVideo, then the clip is cut back to the requested
length (H3 snaps frames up to its 17k+5 grid).

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
REF2VA_BASE = "minimax_h3_ref2va_pruned_int8_convrot.safetensors"
MAX_REF_IMAGES, MAX_REF_AUDIO, MAX_REF_AUDIO_SECONDS = 9, 3, 15.0


def grid_frames(n):
    """H3's frame grid: 17k + 5, inside the trained range (a shorter ask renders 124 frames and is cut back)."""
    n = max(MIN_FRAMES, n)
    return n + (5 - n % 17) % 17


def workflow(x, files, prefix, image="", last="", refs=(), audios=()):
    """The API graph: Ref2VA when any reference is given, else FL2VA with the optional first / last frame."""
    cond = {"clip": ["3", 0], "vae": ["4", 0], "prompt": x.prompt_text, "width": x.width, "height": x.height,
            "length": x.frames}
    wf = {
        "1": {"class_type": "UNETLoader", "inputs": {"unet_name": files["ref2va_model" if refs or audios else "base_model"],
                                                     "weight_dtype": "default"}},
        "2": {"class_type": "MiniMaxH3SigmaShift", "inputs": {"model": ["1", 0], "shift_video": x.shift_video, "shift_audio": x.shift_audio}},
        "3": {"class_type": "CLIPLoader", "inputs": {"clip_name": files["text_encoder"], "type": "minimax", "device": "default"}},
        "4": {"class_type": "VAELoader", "inputs": {"vae_name": files["video_vae"]}},
        "5": {"class_type": "VAELoader", "inputs": {"vae_name": files["audio_vae"]}},
        "7": {"class_type": "MiniMaxH3ImageToVideo", "inputs": cond},
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
    if refs or audios:
        wf["7"]["class_type"] = "MiniMaxH3ReferenceToVideo"
        cond.update({"audio_vae": ["5", 0], "ref_image_size": "match"})
        for i, name in enumerate(refs):
            wf[f"20{i}"] = {"class_type": "LoadImage", "inputs": {"image": name}}
            cond[f"ref_images.ref_image_{i}"] = [f"20{i}", 0]
        for j, name in enumerate(audios):
            wf[f"30{j}"] = {"class_type": "LoadAudio", "inputs": {"audio": name}}
            cond[f"ref_audios.ref_audio_{j}"] = [f"30{j}", 0]
        return wf
    for nid, key, name in (("6", "first_frame", image), ("18", "last_frame", last)):
        if name:
            wf[nid] = {"class_type": "LoadImage", "inputs": {"image": name}}
            cond[key] = [nid, 0]
    return wf


def media_seconds(path):
    r = subprocess.run([tool("ffprobe"), "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", path],
                       capture_output=True, text=True)
    try:
        return float(r.stdout.strip())
    except ValueError:
        raise Fail("invalid_input", f"the reference audio cannot be read: {os.path.basename(path)}")


def stage_audio(srcs, input_dir):
    """Each reference audio as a WAV in ComfyUI's input dir, cut so all of them fit H3's 15 s total."""
    names, left = [], MAX_REF_AUDIO_SECONDS
    for j, src in enumerate(srcs):
        secs = media_seconds(src)
        if left < 1.0:
            say(f"ACT3_NOTE reference audio {j + 1} dropped: the {MAX_REF_AUDIO_SECONDS:.0f} s reference budget is used up")
            break
        keep = min(secs, left)
        if keep < secs:
            say(f"ACT3_NOTE reference audio {j + 1} cut from {secs:.1f} s to {keep:.1f} s (H3 takes {MAX_REF_AUDIO_SECONDS:.0f} s of reference audio in all)")
        name = f"ref_audio_{j}.wav"
        r = subprocess.run([tool("ffmpeg"), "-y", "-v", "error", "-i", src, "-t", f"{keep:.3f}", "-vn", "-ac", "2", "-ar", "48000",
                            os.path.join(input_dir, name)], capture_output=True, text=True)
        if r.returncode != 0:
            raise Fail("invalid_input", f"reference audio {j + 1} cannot be decoded: {r.stderr.strip()[-300:]}")
        names.append(name)
        left -= keep
    return names


def stage_image(src, input_dir, name):
    name += os.path.splitext(src)[1].lower() or ".png"
    shutil.copyfile(src, os.path.join(input_dir, name))
    return name


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
    a.add_argument("--last-frame", default="", help="last frame (FL2VA)")
    a.add_argument("--ref-image", action="append", default=[], help="Ref2VA <Picture N>, in order (repeat)")
    a.add_argument("--ref-audio", action="append", default=[], help="Ref2VA <Audio N>, in order (repeat)")
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
        for f in [x.image, x.last_frame, *x.ref_image, *x.ref_audio]:
            if f and not os.path.isfile(f):
                raise Fail("invalid_input", f"an input is missing: {f}")
        ref2va = bool(x.ref_image or x.ref_audio)
        if ref2va and (x.image or x.last_frame):
            raise Fail("invalid_input", "Ref2VA takes its frames as --ref-image <Picture N> lines, not --image / --last-frame")
        if len(x.ref_image) > MAX_REF_IMAGES or len(x.ref_audio) > MAX_REF_AUDIO:
            raise Fail("invalid_input", f"H3 takes at most {MAX_REF_IMAGES} reference images and {MAX_REF_AUDIO} reference audio clips")
        if x.ref_audio and not x.ref_image:
            raise Fail("invalid_input", "H3 never takes audio as the only reference: add an image")
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
                ("text_encoder", "CLIPLoader", "clip_name"), ("video_vae", "VAELoader", "vae_name"),
                ("audio_vae", "VAELoader", "vae_name"))}
            if ref2va:
                files["ref2va_model"] = node_option(srv.base, "UNETLoader", "unet_name", REF2VA_BASE)
            else:
                files["base_model"] = node_option(srv.base, "UNETLoader", "unet_name", COMFY_FILES["base_model"])
            inp = srv.dirs["input"]
            image = stage_image(x.image, inp, "first_frame") if x.image else ""
            last = stage_image(x.last_frame, inp, "last_frame") if x.last_frame else ""
            refs = [stage_image(f, inp, f"ref_image_{i}") for i, f in enumerate(x.ref_image)]
            audios = stage_audio(x.ref_audio, inp)
            task = (f"Ref2VA ({len(refs)} image(s), {len(audios)} audio)" if ref2va else
                    "FL2VA" if last else "I2VA" if image else "T2VA")
            say(f"ACT3_NOTE {task}: {x.width}x{x.height}, {x.frames} frames, {x.steps} steps")
            stage("rendering")
            prog.band(5, 95, "rendering")
            t0 = time.time()
            clip = run_workflow(srv, workflow(x, files, "act3_h3/clip", image, last, refs, audios), x, deadline(x))
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
