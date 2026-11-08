package procs

import (
	"strconv"
	"time"

	"golang.org/x/sys/windows"
)

func startTime(pid int) (time.Time, bool) {
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return time.Time{}, false
	}
	defer windows.CloseHandle(h)
	var c, e, k, u windows.Filetime
	if windows.GetProcessTimes(h, &c, &e, &k, &u) != nil {
		return time.Time{}, false
	}
	var code uint32
	if windows.GetExitCodeProcess(h, &code) == nil && code != 259 { // 259 = STILL_ACTIVE
		return time.Time{}, false
	}
	return time.Unix(0, c.Nanoseconds()), true
}

func findByEnv(string) []int { return nil } // the step's Job Object holds the whole tree instead

func killPID(pid int) { taskkill("/F", "/PID", strconv.Itoa(pid)) }
