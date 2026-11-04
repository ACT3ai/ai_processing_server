# COPY of ~/BGit/all/app/smartc/film/servers/bms/oss_video/flashvsr/code/run_flashvsr.py, kept here so the task's code
# is frozen into each snapshot (pm/task_authoring.mdx R1, R2). Edit the source in the all repo, then run
# code/scripts/sync_from_all.sh. Run by the oss_upscale runner for FlashVSR (code/internal/runners/oss_flashvsr.go).
#!/usr/bin/env python3
"""run_flashvsr.py: FlashVSR v1.1 (Tiny, streaming) on Apple Silicon (MPS) or NVIDIA (CUDA), with no custom kernels.

The ACT3 port (install step "port", bms/oss_video/flashvsr); ai_processing_server's oss_upscale runner runs a frozen
copy. Upstream OpenImagingLab/FlashVSR is used unchanged, at the commit recorded in instance/storage/install/port.yaml;
this file only swaps what needs a custom build or is CUDA-only, so the same file runs on a Mac and on the RTX 5090
without compiling mit-han-lab's Block-Sparse-Attention:

  * block_sparse_attn (mit-han-lab, CUDA build) -> the SAME 128-token block mask, in torch: each query block gathers
    only the key blocks its mask row selects (index_select), rows bucketed by count, then batched
    scaled_dot_product_attention. Exact same output as the dense masked form (FLASHVSR_ATTN=dense), ~2.8x faster.
  * float64 / complex128 (rope, timestep embedding: Metal has no float64) -> float32 / complex64.
  * on MPS: "cuda" device strings and torch.cuda.empty_cache -> mps.
The device is CUDA when PyTorch sees one, else MPS; FLASHVSR_DEVICE=cuda|mps forces it.

Unlike upstream's example (which centre-crops to a multiple of 128), the input is upscaled bicubic, edge-padded to a
multiple of 128, run, and cropped back, so the output is exactly scale x the input, every frame, original fps and
audio.

  run_flashvsr.py --input in.mp4 --output out.mp4 [--scale 4] [--seed 0] [--sparse-ratio 2.0] [--local-range 11]

Progress lines "ACT3_PROGRESS <pct>" are read by the render server (local-plugin.ts parseCommonProgress).
Env: ACT3_OSS_HOME (default ~/act3_render/flashvsr), ACT3_FFMPEG / ACT3_FFPROBE, FLASHVSR_ATTN = sparse (default) | dense,
FLASHVSR_ATTN_CHUNK_ELEMS (dense: mask elements per SDPA call), FLASHVSR_ATTN_BLOCKS (sparse: key blocks per batch).
"""
import argparse
import math
import os
import shutil
import subprocess
import sys
import time
import types

os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")

import numpy as np
import torch
import torch.nn.functional as F

HOME = os.environ.get("ACT3_OSS_HOME") or os.path.expanduser("~/act3_render/flashvsr")
REPO = os.path.join(HOME, "FlashVSR")
EXAMPLES = os.path.join(REPO, "examples", "WanVSR")
WEIGHTS = os.path.join(HOME, "models", "FlashVSR-v1.1")
BLOCK = 128  # Block-Sparse-Attention's block size (wan_video_dit.py windows are 2x8x8 = 128 tokens)
ATTN = os.environ.get("FLASHVSR_ATTN", "sparse")
CHUNK_ELEMS = int(os.environ.get("FLASHVSR_ATTN_CHUNK_ELEMS", str(1 << 28)))
GATHER_BLOCKS = int(os.environ.get("FLASHVSR_ATTN_BLOCKS", str(1 << 15)))
BUCKET = 16  # rows are padded to a multiple of 16 key blocks so few batch shapes are needed


def progress(pct, text=""):
    print(f"ACT3_PROGRESS {pct:.1f} {text}".rstrip(), flush=True)


# ---------------------------------------------------------------------------------------------------------------
# 1. The CUDA-only kernel, replaced before upstream imports it.
# ---------------------------------------------------------------------------------------------------------------
def _dense(qh, kh, vh, mask, scale):
    """Reference form: the block mask expanded to tokens, chunked over query blocks."""
    Sq, Sk, H = qh.shape[2], kh.shape[2], qh.shape[1]
    kmask = mask.repeat_interleave(BLOCK, dim=2)[:, :, :Sk]  # H nq Sk
    step = max(1, CHUNK_ELEMS // (H * BLOCK * Sk))
    out = torch.empty_like(qh)
    for i in range(0, mask.shape[1], step):
        q0, q1 = i * BLOCK, min((i + step) * BLOCK, Sq)
        m = kmask[:, i:i + step].repeat_interleave(BLOCK, dim=1)[:, : q1 - q0]
        out[:, :, q0:q1] = F.scaled_dot_product_attention(qh[:, :, q0:q1], kh, vh, attn_mask=m.unsqueeze(0), scale=scale)
    return out


def _sparse(qh, kh, vh, mask, scale):
    """Only the selected key blocks: per (head, query block) row, gather its key blocks and attend to them."""
    _, H, Sq, D = qh.shape
    Sk = kh.shape[2]
    nq, nk = mask.shape[1], mask.shape[2]
    qb = F.pad(qh[0], (0, 0, 0, nq * BLOCK - Sq)).reshape(H * nq, BLOCK, D)
    kf = F.pad(kh[0], (0, 0, 0, nk * BLOCK - Sk)).reshape(H * nk, BLOCK * D)
    vf = F.pad(vh[0], (0, 0, 0, nk * BLOCK - Sk)).reshape(H * nk, BLOCK * D)
    tok_ok = torch.arange(nk * BLOCK, device=qh.device).reshape(nk, BLOCK) < Sk  # the last key block may be short
    count = mask.sum(-1).reshape(-1)                                                   # H*nq
    order = torch.sort(mask.to(torch.int8), dim=-1, descending=True, stable=True).indices.reshape(H * nq, nk)
    padded = ((count + BUCKET - 1) // BUCKET * BUCKET).clamp(min=BUCKET, max=nk)
    head_base = (torch.arange(H * nq, device=qh.device) // nq) * nk
    out = torch.empty_like(qb)
    for K in torch.unique(padded).tolist():
        rows = torch.nonzero(padded == K).squeeze(1)
        for s in range(0, rows.numel(), max(1, GATHER_BLOCKS // K)):
            r = rows[s:s + max(1, GATHER_BLOCKS // K)]
            kidx = order[r, :K]                                                        # n K
            flat = (head_base[r, None] + kidx).reshape(-1)
            kk = kf.index_select(0, flat).reshape(-1, 1, K * BLOCK, D)
            vv = vf.index_select(0, flat).reshape(-1, 1, K * BLOCK, D)
            ok = (torch.arange(K, device=qh.device)[None, :] < count[r, None])[:, :, None] & tok_ok[kidx]
            out[r] = F.scaled_dot_product_attention(qb[r].unsqueeze(1), kk, vv, attn_mask=ok.reshape(-1, 1, 1, K * BLOCK),
                                                    scale=scale).squeeze(1)
    return out.reshape(H, nq * BLOCK, D)[:, :Sq].unsqueeze(0)


def block_sparse_attn_func(q, k, v, cu_seqlens_q, cu_seqlens_k, head_mask_type, streaming_info, base_blockmask,
                           max_seqlen_q_, max_seqlen_k_, p_dropout, deterministic=False, softmax_scale=None,
                           is_causal=False, exact_streaming=False, return_attn_probs=False):
    """q, k, v: (S, H, D) for one sequence. base_blockmask: (1, H, ceil(Sq/128), ceil(Sk/128)) bool."""
    qh, kh, vh = (t.permute(1, 0, 2).unsqueeze(0) for t in (q, k, v))  # 1 H S D
    mask = base_blockmask[0].to(device=q.device, dtype=torch.bool)    # H nq nk
    out = (_dense if ATTN == "dense" else _sparse)(qh, kh, vh, mask, softmax_scale)
    return out[0].permute(1, 0, 2)


sys.modules["block_sparse_attn"] = types.SimpleNamespace(block_sparse_attn_func=block_sparse_attn_func)


def _no_download(*a, **k):
    raise RuntimeError("run_flashvsr.py never downloads: weights come from the pinned HF repo via `just weights`")


# Upstream's auto-downloader imports modelscope unconditionally; we never download, so it is not installed.
sys.modules["modelscope"] = types.SimpleNamespace(snapshot_download=_no_download)

# ---------------------------------------------------------------------------------------------------------------
# 2. Upstream, with its float64 and cuda assumptions patched.
# ---------------------------------------------------------------------------------------------------------------
sys.path[:0] = [REPO, EXAMPLES]
DEVICE = os.environ.get("FLASHVSR_DEVICE") or ("cuda" if torch.cuda.is_available() else "mps")
DTYPE = torch.bfloat16
if DEVICE == "mps":
    torch.cuda.empty_cache = lambda: torch.mps.empty_cache()
FFMPEG = os.environ.get("ACT3_FFMPEG") or "ffmpeg"
FFPROBE = os.environ.get("ACT3_FFPROBE") or "ffprobe"

import diffsynth.models.wan_video_dit as dit  # noqa: E402


def sinusoidal_embedding_1d(dim, position):
    sinusoid = torch.outer(position.type(torch.float32), torch.pow(
        10000, -torch.arange(dim // 2, dtype=torch.float32, device=position.device).div(dim // 2)))
    x = torch.cat([torch.cos(sinusoid), torch.sin(sinusoid)], dim=1)
    return x.to(position.dtype)


def precompute_freqs_cis(dim, end=1024, theta=10000.0):
    freqs = 1.0 / (theta ** (torch.arange(0, dim, 2)[: (dim // 2)].float() / dim))
    freqs = torch.outer(torch.arange(end, device=freqs.device).float(), freqs)
    return torch.polar(torch.ones_like(freqs), freqs)  # complex64


def rope_apply(x, freqs, num_heads):
    x = dit.rearrange(x, "b s (n d) -> b s n d", n=num_heads)
    x_out = torch.view_as_complex(x.to(torch.float32).reshape(x.shape[0], x.shape[1], x.shape[2], -1, 2))
    x_out = torch.view_as_real(x_out * freqs.to(torch.complex64)).flatten(2)
    return x_out.to(x.dtype)


dit.sinusoidal_embedding_1d = sinusoidal_embedding_1d
dit.precompute_freqs_cis = precompute_freqs_cis
dit.rope_apply = rope_apply

import diffsynth.pipelines.flashvsr_tiny as tiny  # noqa: E402

tiny.sinusoidal_embedding_1d = sinusoidal_embedding_1d

from diffsynth import ModelManager, FlashVSRTinyPipeline  # noqa: E402
from utils.utils import Causal_LQ4x_Proj  # noqa: E402
from utils.TCDecoder import build_tcdecoder  # noqa: E402


def init_pipeline():
    mm = ModelManager(torch_dtype=DTYPE, device="cpu")
    mm.load_models([os.path.join(WEIGHTS, "diffusion_pytorch_model_streaming_dmd.safetensors")])
    pipe = FlashVSRTinyPipeline.from_model_manager(mm, device=DEVICE)
    proj = Causal_LQ4x_Proj(in_dim=3, out_dim=1536, layer_num=1)
    proj.load_state_dict(torch.load(os.path.join(WEIGHTS, "LQ_proj_in.ckpt"), map_location="cpu"), strict=True)
    pipe.denoising_model().LQ_proj_in = proj.to(DEVICE, dtype=DTYPE)
    pipe.TCDecoder = build_tcdecoder(new_channels=[512, 256, 128, 128], device=DEVICE, dtype=DTYPE,
                                     new_latent_channels=16 + 768)
    pipe.TCDecoder.load_state_dict(torch.load(os.path.join(WEIGHTS, "TCDecoder.ckpt"), map_location="cpu"),
                                   strict=False)
    pipe.TCDecoder.to(DEVICE, dtype=DTYPE)
    pipe.to(DEVICE)
    # A GPU under 16 GB cannot hold the DiT, VAE and decoder at once: keep the DiT's weights in RAM, streamed per layer
    small = DEVICE == "cuda" and torch.cuda.get_device_properties(0).total_memory < 16 * 2**30
    pipe.enable_vram_management(num_persistent_param_in_dit=0 if small else None)
    ctx = torch.load(os.path.join(EXAMPLES, "prompt_tensor", "posi_prompt.pth"), map_location="cpu")
    pipe.init_cross_kv(context_tensor=ctx)
    pipe.load_models_to_device(["dit", "vae"])
    return pipe


# ---------------------------------------------------------------------------------------------------------------
# 3. Video in and out (ffmpeg), exact scale, no crop.
# ---------------------------------------------------------------------------------------------------------------
def probe(path):
    out = subprocess.run([FFPROBE, "-v", "error", "-select_streams", "v:0", "-show_entries",
                          "stream=width,height,r_frame_rate", "-of", "csv=p=0", path],
                         capture_output=True, text=True, check=True).stdout.strip().split(",")
    w, h = int(out[0]), int(out[1])
    num, den = (out[2].split("/") + ["1"])[:2]
    has_audio = bool(subprocess.run([FFPROBE, "-v", "error", "-select_streams", "a", "-show_entries",
                                     "stream=index", "-of", "csv=p=0", path],
                                    capture_output=True, text=True).stdout.strip())
    return w, h, f"{num}/{den}", float(num) / float(den or 1), has_audio


def read_frames(path, w, h):
    raw = subprocess.run([FFMPEG, "-v", "error", "-i", path, "-f", "rawvideo", "-pix_fmt", "rgb24", "-"],
                         capture_output=True, check=True).stdout
    return np.frombuffer(raw, np.uint8).reshape(-1, h, w, 3)


def largest_8n1_leq(n):
    return 0 if n < 1 else ((n - 1) // 8) * 8 + 1


def prepare(frames, scale):
    n, h0, w0, _ = frames.shape
    sW, sH = 2 * round(w0 * scale / 2), 2 * round(h0 * scale / 2)  # even: libx264 yuv420p refuses odd sides
    tW, tH = math.ceil(sW / BLOCK) * BLOCK, math.ceil(sH / BLOCK) * BLOCK
    idx = list(range(n)) + [n - 1] * 4
    Fn = largest_8n1_leq(len(idx))
    while Fn - 4 < n:  # never drop real frames: pad the tail until 8n+1 covers them all
        idx.append(n - 1)
        Fn = largest_8n1_leq(len(idx))
    idx = idx[:Fn]
    out = torch.empty((1, 3, Fn, tH, tW), dtype=DTYPE, device=DEVICE)
    for j, i in enumerate(idx):
        t = torch.from_numpy(frames[i]).to(DEVICE).permute(2, 0, 1).unsqueeze(0).float() / 127.5 - 1.0
        t = F.interpolate(t, size=(sH, sW), mode="bicubic", align_corners=False).clamp(-1, 1)
        t = F.pad(t, (0, tW - sW, 0, tH - sH), mode="replicate")
        out[0, :, j] = t[0].to(DTYPE)
    return out, Fn, tH, tW, sH, sW


def write_video(frames_u8, path, rate, src, has_audio):
    n, h, w, _ = frames_u8.shape
    tmp = path + ".noaudio.mp4"
    p = subprocess.Popen([FFMPEG, "-v", "error", "-y", "-f", "rawvideo", "-pix_fmt", "rgb24", "-s", f"{w}x{h}",
                          "-r", rate, "-i", "-", "-c:v", "libx264", "-crf", "16", "-preset", "medium",
                          "-pix_fmt", "yuv420p", "-movflags", "+faststart", tmp], stdin=subprocess.PIPE)
    p.stdin.write(np.ascontiguousarray(frames_u8).tobytes())
    p.stdin.close()
    if p.wait() != 0:
        raise RuntimeError("ffmpeg encode failed")
    if has_audio:
        subprocess.run([FFMPEG, "-v", "error", "-y", "-i", tmp, "-i", src, "-map", "0:v", "-map", "1:a?",
                        "-c:v", "copy", "-c:a", "aac", "-shortest", path], check=True)
        os.remove(tmp)
    else:
        shutil.move(tmp, path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--scale", type=float, default=4.0)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--sparse-ratio", type=float, default=2.0)  # upstream: 1.5 faster, 2.0 more stable
    ap.add_argument("--local-range", type=int, default=11)      # upstream: 9 sharper, 11 more stable
    a = ap.parse_args()
    if DEVICE == "cuda" and not torch.cuda.is_available():
        sys.exit("PyTorch does not see a CUDA GPU")
    if DEVICE == "mps" and not torch.backends.mps.is_available():
        sys.exit("PyTorch sees neither a CUDA GPU nor Metal (MPS)")
    print(f"device {DEVICE}", flush=True)
    t0 = time.time()
    w0, h0, rate, fps, has_audio = probe(a.input)
    frames = read_frames(a.input, w0, h0)
    n = frames.shape[0]
    if n < 1:
        sys.exit(f"No frames in {a.input}")
    print(f"input {w0}x{h0} {n} frames @ {fps:.3f} fps, audio={has_audio}", flush=True)
    progress(2, "loading models")
    pipe = init_pipeline()
    progress(10, "models loaded")
    LQ, Fn, tH, tW, sH, sW = prepare(frames, a.scale)
    print(f"target {sW}x{sH} (padded {tW}x{tH}), {Fn} frames in", flush=True)

    total = (Fn - 1) // 8 - 2
    real_tqdm = tiny.tqdm

    def tq(it, *args, **kw):
        for i, x in enumerate(it):
            progress(10 + 80 * i / max(1, total), f"chunk {i + 1}/{total}")
            yield x
    tiny.tqdm = tq
    try:
        video = pipe(prompt="", negative_prompt="", cfg_scale=1.0, num_inference_steps=1, seed=a.seed,
                     LQ_video=LQ, num_frames=Fn, height=tH, width=tW, is_full_block=False, if_buffer=True,
                     topk_ratio=a.sparse_ratio * 768 * 1280 / (tH * tW), kv_ratio=3.0,
                     local_range=a.local_range, color_fix=True)
    finally:
        tiny.tqdm = real_tqdm
    progress(92, "encoding")
    video = video[0] if video.dim() == 5 else video  # C T H W
    out = ((video.float().clamp(-1, 1) + 1) * 127.5).round().to(torch.uint8)
    out = out[:, :n, :sH, :sW].permute(1, 2, 3, 0).cpu().numpy()
    if out.shape[0] < n:
        print(f"note: model returned {out.shape[0]} of {n} frames; last frame repeated", flush=True)
        out = np.concatenate([out, np.repeat(out[-1:], n - out.shape[0], 0)])
    os.makedirs(os.path.dirname(os.path.abspath(a.output)), exist_ok=True)
    write_video(out, a.output, rate, a.input, has_audio)
    progress(100, "done")
    print(f"done {a.output} {sW}x{sH} {n} frames in {time.time() - t0:.1f} s", flush=True)


if __name__ == "__main__":
    main()
