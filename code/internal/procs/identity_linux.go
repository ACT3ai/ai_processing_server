package procs

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// clkTck: USER_HZ, 100 on every Linux we run on.
const clkTck = 100

func bootTime() (time.Time, bool) {
	b, err := os.ReadFile("/proc/stat")
	if err != nil {
		return time.Time{}, false
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "btime ") {
			n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(l, "btime ")), 10, 64)
			if err == nil {
				return time.Unix(n, 0), true
			}
		}
	}
	return time.Time{}, false
}

func startTime(pid int) (time.Time, bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return time.Time{}, false
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')') // the command name may hold spaces and parentheses
	if i < 0 {
		return time.Time{}, false
	}
	f := strings.Fields(s[i+1:])
	if len(f) < 20 { // field 22 overall is f[19] after "pid (comm)"
		return time.Time{}, false
	}
	ticks, err := strconv.ParseInt(f[19], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	boot, ok := bootTime()
	if !ok {
		return time.Time{}, false
	}
	return boot.Add(time.Duration(ticks) * time.Second / clkTck), true
}

func findByEnv(kv string) []int {
	want := []byte(kv)
	dirs, _ := filepath.Glob("/proc/[0-9]*")
	var out []int
	for _, d := range dirs {
		b, err := os.ReadFile(d + "/environ")
		if err != nil {
			continue
		}
		for _, e := range bytes.Split(b, []byte{0}) {
			if bytes.Equal(e, want) {
				if pid, err := strconv.Atoi(filepath.Base(d)); err == nil && pid != os.Getpid() {
					out = append(out, pid)
				}
				break
			}
		}
	}
	return out
}

func killPID(pid int) { _ = syscall.Kill(pid, syscall.SIGKILL) }
