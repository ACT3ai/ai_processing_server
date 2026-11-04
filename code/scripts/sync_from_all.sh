#!/bin/sh
# Refresh the task scripts this repo runs from their sources in the ACT3 all repo (pm/task_authoring.mdx R2).
# The copies are what each snapshot freezes, so a running task never sees a later edit. Usage, from anywhere:
#   ~/BGit/act3/ai_processing_server/code/scripts/sync_from_all.sh [ALL_REPO]   (default ~/BGit/all)
set -eu
ALL="${1:-$HOME/BGit/all}"
HERE="$(cd "$(dirname "$0")" && pwd)"
copy() { # copy <source relative to ALL> <dest relative to HERE> [runner that runs it]
	src="$ALL/$1"; dst="$HERE/$2"; runner="${3:-the oss_video runner (code/internal/runners/oss_video.go)}"
	[ -f "$src" ] || { echo "missing: $src" >&2; exit 1; }
	mkdir -p "$(dirname "$dst")"
	{
		echo "# COPY of ~/BGit/all/$1, kept here so the task's code"
		echo "# is frozen into each snapshot (pm/task_authoring.mdx R1, R2). Edit the source in the all repo, then run"
		echo "# code/scripts/sync_from_all.sh. Run by $runner."
		cat "$src"
	} > "$dst.tmp" && mv "$dst.tmp" "$dst"
	echo "synced $2"
}
copy film/code/subp/video_gen_ai/wan/v2_2_a14b/local/render_wan.py video_gen/wan22/render_wan.py
copy film/code/subp/video_gen_ai/kandinsky/video_pro_6_0/local/render_kandinsky.py video_gen/kandinsky6/render_kandinsky.py \
	"the oss_video runner for Kandinsky 6.0 (code/internal/runners/oss_kandinsky.go)"
copy app/smartc/film/servers/bms/oss_video/flashvsr/code/run_flashvsr.py upscale/flashvsr/run_flashvsr.py \
	"the oss_upscale runner for FlashVSR (code/internal/runners/oss_flashvsr.go)"
copy film/code/subp/video_gen_ai/nvidia/h3_regenerate_2k/render_h3_regenerate.py video_gen/h3_regenerate/render_h3_regenerate.py \
	"the oss_regenerate runner (code/internal/runners/oss_regenerate.go)"
