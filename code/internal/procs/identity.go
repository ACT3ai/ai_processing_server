package procs

import "time"

// Process identity for CANCEL MODE (pm/cancel.mdx §5.3, §5.4). A pid alone is not an identity: the OS reuses pids, so
// a step or task host that died hours ago may share its pid with an unrelated process now. Before the controller
// kills or counts a process, it checks that the process now holding the pid started when ours did.

// StartTolerance is how far a process's OS start time may be from the time we recorded for it (the recording happens
// just after the start, and macOS reports start times to the second).
var StartTolerance = 10 * time.Second

// StartTime is when the process pid started, by the OS's own account; ok=false when there is no such process or the
// OS will not say.
func StartTime(pid int) (time.Time, bool) {
	if pid <= 0 {
		return time.Time{}, false
	}
	return startTime(pid)
}

// SameProcess reports whether pid is alive AND is the process we recorded as started at `at`.
func SameProcess(pid int, at time.Time) bool {
	st, ok := StartTime(pid)
	if !ok {
		return false
	}
	d := st.Sub(at)
	return d > -StartTolerance && d < StartTolerance
}

// OurGroup reports whether the process group pgid (Unix) / process tree root (Windows) that we recorded as started at
// `at` still has a live process. A group whose leader is gone but whose members live is still ours: the OS never
// reuses a pid while it is the id of a live process group. A live leader that started at another time is a
// reused pid, not ours.
func OurGroup(pgid int, at time.Time) bool {
	if !groupAlive(pgid) {
		return false
	}
	st, ok := StartTime(pgid)
	if !ok {
		return true // leaderless group: ours
	}
	d := st.Sub(at)
	return d > -StartTolerance && d < StartTolerance
}

// FindByEnv lists the live processes whose environment contains key=value. Every step of a task runs with
// ACT3_RUN_DIR=<its run directory>, and children inherit it, so this finds a child that left the process group
// (setsid, a daemonising server) too. Linux reads /proc/<pid>/environ, macOS `ps -E`; Windows returns nil (a Job
// Object contains the whole tree there instead).
func FindByEnv(key, value string) []int {
	if key == "" || value == "" {
		return nil
	}
	return findByEnv(key + "=" + value)
}

// KillPID kills one process at once.
func KillPID(pid int) {
	if pid > 1 {
		killPID(pid)
	}
}
