package procs

import "time"

type procRef struct {
	pid int
	at  time.Time
}

// Windows: the step's Job Object holds every descendant; no snapshot is needed.
func descendants(int) []procRef { return nil }
func killEscaped([]procRef) int { return 0 }
