//go:build windows

package taskhost

import (
	"fmt"
	"syscall"
	"time"
)

var getTickCount64 = syscall.NewLazyDLL("kernel32.dll").NewProc("GetTickCount64")

// BootID names this boot of the machine: the boot time to the minute (now minus the uptime). A pid recorded under
// another boot id belongs to a process that is gone, whatever process holds that number now.
func BootID() string {
	ms, _, _ := getTickCount64.Call()
	if ms == 0 {
		return ""
	}
	boot := time.Now().Add(-time.Duration(ms) * time.Millisecond).UTC().Round(time.Minute)
	return fmt.Sprintf("boot-%d", boot.Unix())
}

const stillActive = 259

// pidAlive reports whether a process with this pid exists.
func pidAlive(pid int) bool {
	h, err := syscall.OpenProcess(0x1000 /* PROCESS_QUERY_LIMITED_INFORMATION */, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(h)
	var code uint32
	if err := syscall.GetExitCodeProcess(h, &code); err != nil {
		return true // it exists; we just cannot read it
	}
	return code == stillActive
}
