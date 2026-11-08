package procs

import "time"

// Proc is one process by identity.
type Proc struct {
	PID       int
	StartedAt time.Time
}

// Descendants lists root's live descendants by parentage, each with its OS start time (nil on Windows, where the
// step's Job Object holds them). The controller uses it while escalating against a wedged task host.
func Descendants(root int) []Proc {
	var out []Proc
	for _, r := range descendants(root) {
		out = append(out, Proc{PID: r.pid, StartedAt: r.at})
	}
	return out
}
