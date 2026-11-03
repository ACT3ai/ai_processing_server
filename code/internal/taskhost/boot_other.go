//go:build !linux && !darwin && !windows

package taskhost

// BootID is unknown on this OS: liveness falls back to the pid and the heartbeat age.
func BootID() string { return "" }
