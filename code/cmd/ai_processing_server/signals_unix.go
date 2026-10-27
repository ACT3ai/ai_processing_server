//go:build !windows

package main

import (
	"os"
	"syscall"
)

var (
	stopSignals  = []os.Signal{syscall.SIGINT, syscall.SIGTERM}
	reloadSignal = os.Signal(syscall.SIGHUP)
)

func alive(p *os.Process) bool { return p.Signal(syscall.Signal(0)) == nil }

func terminate(_ paths, p *os.Process) error { return p.Signal(syscall.SIGTERM) }
