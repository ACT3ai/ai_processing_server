//go:build darwin

package taskhost

import "syscall"

// BootID names this boot of the machine (kern.bootsessionuuid): a pid recorded under another boot id belongs to a
// process that is gone, whatever process holds that number now.
func BootID() string {
	v, err := syscall.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return ""
	}
	return v
}
