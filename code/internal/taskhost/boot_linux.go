//go:build linux

package taskhost

import (
	"os"
	"strings"
)

// BootID names this boot of the machine: a pid recorded under another boot id belongs to a process that is gone,
// whatever process holds that number now.
func BootID() string {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
