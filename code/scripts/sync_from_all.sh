#!/bin/sh
# Refresh the task scripts this repo runs from their sources in the ACT3 all repo (pm/task_authoring.mdx R2).
# The copies are what each snapshot freezes, so a running task never sees a later edit. Usage, from anywhere:
#   ~/BGit/act3/ai_processing_server/code/scripts/sync_from_all.sh [ALL_REPO]   (default ~/BGit/all)
set -eu
ALL="${1:-$HOME/BGit/all}"
HERE="$(cd "$(dirname "$0")" && pwd)"
copy() { # copy <source relative to ALL> <dest relative to HERE>
	src="$ALL/$1"; dst="$HERE/$2"
	[ -f "$src" ] || { echo "missing: $src" >&2; exit 1; }
	mkdir -p "$(dirname "$dst")"
	{
		echo "# COPY of ~/BGit/all/$1, kept here so the task's code"
		echo "# is frozen into each snapshot (pm/task_authoring.mdx R1, R2). Edit the source in the all repo, then run"
		echo "# code/scripts/sync_from_all.sh. Run by the oss_video runner (code/internal/runners/oss_video.go)."
		cat "$src"
	} > "$dst.tmp" && mv "$dst.tmp" "$dst"
	echo "synced $2"
}
copy film/code/subp/video_gen_ai/wan/v2_2_a14b/local/render_wan.py video_gen/wan22/render_wan.py
