package procs

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func startTime(pid int) (time.Time, bool) {
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	out, err := exec.CommandContext(ctx, "ps", "-o", "lstart=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("Mon Jan _2 15:04:05 2006", strings.Join(strings.Fields(string(out)), " "), time.Local)
	if err != nil {
		// lstart pads the day with a space; Fields+Join turns "Nov  8" into "Nov 8"
		t, err = time.ParseInLocation("Mon Jan 2 15:04:05 2006", strings.Join(strings.Fields(string(out)), " "), time.Local)
		if err != nil {
			return time.Time{}, false
		}
	}
	return t, true
}

// findByEnv: `ps -E` prints a process's environment after its command (only for this user's processes).
func findByEnv(kv string) []int {
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()
	out, err := exec.CommandContext(ctx, "ps", "-A", "-E", "-ww", "-o", "pid=,command=").Output()
	if err != nil {
		return nil
	}
	var pids []int
	for _, l := range strings.Split(string(out), "\n") {
		l = strings.TrimSpace(l)
		if !strings.Contains(l, " "+kv+" ") && !strings.HasSuffix(l, " "+kv) {
			continue
		}
		f := strings.Fields(l)
		if pid, err := strconv.Atoi(f[0]); err == nil && pid != os.Getpid() {
			pids = append(pids, pid)
		}
	}
	return pids
}

func killPID(pid int) { _ = syscall.Kill(pid, syscall.SIGKILL) }
