//go:build !windows

package procs

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// procRef is one process by identity (pid + OS start time).
type procRef struct {
	pid int
	at  time.Time
}

// descendants lists every live descendant of root by parentage (ps -A -o pid=,ppid=, the same on Linux and macOS),
// with each one's start time. Taken when a cancel arrives, BEFORE anything is signalled: a child that left the
// process group (setsid: a private model server, a daemon) is still a descendant then, but is reparented to init once
// its parent dies, and could not be found afterwards (pm/cancel.mdx §5.4).
func descendants(root int) []procRef {
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	out, err := exec.CommandContext(ctx, "ps", "-A", "-o", "pid=,ppid=").Output()
	if err != nil {
		return nil
	}
	kids := map[int][]int{}
	for _, l := range strings.Split(string(out), "\n") {
		f := strings.Fields(l)
		if len(f) != 2 {
			continue
		}
		pid, e1 := strconv.Atoi(f[0])
		ppid, e2 := strconv.Atoi(f[1])
		if e1 == nil && e2 == nil {
			kids[ppid] = append(kids[ppid], pid)
		}
	}
	var refs []procRef
	queue := append([]int(nil), kids[root]...)
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		if at, ok := StartTime(p); ok {
			refs = append(refs, procRef{pid: p, at: at})
		}
		queue = append(queue, kids[p]...)
	}
	return refs
}

// killEscaped kills, after the group is gone, every snapshot descendant that is still the same process (it left the
// group). It returns how many it had to kill.
func killEscaped(snap []procRef) int {
	n := 0
	for _, r := range snap {
		if SameProcess(r.pid, r.at) {
			killPID(r.pid)
			n++
		}
	}
	return n
}
