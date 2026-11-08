package jobs

import (
	"fmt"
	"strings"
	"time"
)

// StopReport is what CANCEL MODE did to stop one job's render and clean up after it (pm/cancel.mdx §6). It is kept
// on the job record (`stop`), carried in the job's final `cancelled` event (note + result), and printed by the
// `cancel` and `queue show` subcommands, so an operator — and ACT3 production — can see that a cancel really
// stopped the work, how, and how long it took.
type StopReport struct {
	// Profile is the render type's cancel profile (runners/cancel.go): seedvr2, flashvsr, wan_2_2, ltx_2_5,
	// kandinsky_6, minimax_h3, open_h3_regenerate, ffmpeg, fake, ...
	Profile string `json:"profile"`
	// Stage is where the job was when the cancel reached it: staging, running, uploading.
	Stage string `json:"stage,omitempty"`
	// Step is the task host step that was running (a segmented upscale: "upscale 3/7").
	Step       string `json:"step,omitempty"`
	StepsDone  int    `json:"steps_done,omitempty"`
	StepsTotal int    `json:"steps_total,omitempty"`

	AskedAt   time.Time `json:"asked_at"`             // the controller wrote the cancel
	SeenAt    time.Time `json:"seen_at,omitempty"`    // the task host saw it
	StoppedAt time.Time `json:"stopped_at,omitempty"` // the last process of the tree was gone
	Seconds   float64   `json:"seconds"`              // AskedAt → StoppedAt

	GraceSeconds float64 `json:"grace_seconds,omitempty"`
	// Graceful: the task stopped on the interrupt / cancel file within its grace (its finally blocks ran).
	Graceful bool `json:"graceful"`
	// Forced: the grace ran out and the task host killed the tree.
	Forced bool `json:"forced,omitempty"`
	// Escalated: the task host did not finish the stop itself, so the controller killed its process groups.
	Escalated bool `json:"escalated,omitempty"`

	// TreeVerified: every process group the task ran was checked and is gone.
	TreeVerified bool `json:"tree_verified"`
	// GPU is the VRAM check: "released" (nvidia-smi lists none of the tree's processes), "held" (still listed at
	// the end of the wait), "not_checked" (no NVIDIA GPU / no nvidia-smi: Apple unified memory is freed with the
	// process), or "not_used".
	GPU string `json:"gpu"`

	ScratchBytesRemoved int64 `json:"scratch_bytes_removed,omitempty"`
	// RunDirKept: the run directory was NOT removed because the stop could not be verified (the sweep retries).
	RunDirKept bool `json:"run_dir_kept,omitempty"`

	// Problems lists anything that went wrong while stopping (an escalation, a group that would not die, a GPU
	// still held). Empty on a clean stop.
	Problems []string `json:"problems,omitempty"`
}

// Clean is a stop that was verified with nothing left behind.
func (r *StopReport) Clean() bool {
	return r != nil && r.TreeVerified && len(r.Problems) == 0 && r.GPU != "held"
}

// Summary is one sentence for logs, the cancelled event's note, and the CLI.
func (r *StopReport) Summary() string {
	if r == nil {
		return ""
	}
	how := "gracefully"
	switch {
	case r.Escalated:
		how = "after the controller killed it"
	case r.Forced:
		how = fmt.Sprintf("by force after its %.0f s grace", r.GraceSeconds)
	case !r.Graceful:
		how = "before it started rendering"
	}
	parts := []string{fmt.Sprintf("%s stopped %s in %.1f s", orDash(r.Profile), how, r.Seconds)}
	if r.Step != "" {
		parts = append(parts, "during "+r.Step)
	}
	if r.TreeVerified {
		parts = append(parts, "no process left")
	} else {
		parts = append(parts, "processes NOT verified gone")
	}
	switch r.GPU {
	case "released":
		parts = append(parts, "GPU memory released")
	case "held":
		parts = append(parts, "GPU memory STILL HELD")
	}
	if r.ScratchBytesRemoved > 0 {
		parts = append(parts, fmt.Sprintf("%.1f MB of scratch removed", float64(r.ScratchBytesRemoved)/1e6))
	}
	if r.RunDirKept {
		parts = append(parts, "run directory kept for the sweep")
	}
	s := strings.Join(parts, ", ")
	if len(r.Problems) > 0 {
		s += " (problems: " + strings.Join(r.Problems, "; ") + ")"
	}
	return s
}

func orDash(s string) string {
	if s == "" {
		return "the job"
	}
	return s
}
