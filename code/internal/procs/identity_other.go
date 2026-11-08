//go:build !linux && !darwin && !windows

package procs

import "time"

func startTime(int) (time.Time, bool) { return time.Time{}, false }
func findByEnv(string) []int          { return nil }
func killPID(int)                     {}
