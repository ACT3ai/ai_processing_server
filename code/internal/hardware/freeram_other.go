//go:build !linux && !darwin && !windows

package hardware

func freeRAM() (float64, float64, bool) { return 0, 0, false }
