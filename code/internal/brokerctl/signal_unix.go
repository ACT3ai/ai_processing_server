//go:build !windows

package brokerctl

import (
	"strconv"
	"syscall"
)

// signalReload sends SIGHUP: nats-server re-reads its config without dropping connections.
func signalReload(pid string) error {
	n, err := strconv.Atoi(pid)
	if err != nil {
		return err
	}
	return syscall.Kill(n, syscall.SIGHUP)
}
