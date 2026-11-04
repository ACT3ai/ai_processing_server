package hardware

import "os"

func freeRAM() (float64, float64, bool) {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, false
	}
	avail, total := ParseMeminfoAvailableGB(string(b))
	return avail, total, total > 0
}
