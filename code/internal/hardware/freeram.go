package hardware

import (
	"regexp"
	"strconv"
	"strings"
)

// FreeRAM is how much RAM a new allocation can still get without the machine swapping, and the total, in GiB.
// ok is false when this OS cannot say. Read once a second by the task host's low-memory guard.
func FreeRAM() (freeGB, totalGB float64, ok bool) { return freeRAM() }

// ParseMeminfoAvailableGB reads MemAvailable and MemTotal from /proc/meminfo.
func ParseMeminfoAvailableGB(out string) (availGB, totalGB float64) {
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		kb, _ := strconv.ParseFloat(f[1], 64)
		switch f[0] {
		case "MemAvailable:":
			availGB = kb / (1 << 20)
		case "MemTotal:":
			totalGB = kb / (1 << 20)
		}
	}
	return
}

var (
	reVMStatPage  = regexp.MustCompile(`page size of (\d+) bytes`)
	reVMStatCount = regexp.MustCompile(`(?m)^Pages (free|inactive|speculative|purgeable):\s+(\d+)\.`)
)

// ParseVMStatFreeGB reads macOS `vm_stat`: free, inactive, speculative and purgeable pages are what the kernel hands
// out before it compresses or swaps.
func ParseVMStatFreeGB(out string) float64 {
	m := reVMStatPage.FindStringSubmatch(out)
	if m == nil {
		return 0
	}
	page, _ := strconv.ParseFloat(m[1], 64)
	var pages float64
	for _, c := range reVMStatCount.FindAllStringSubmatch(out, -1) {
		n, _ := strconv.ParseFloat(c[2], 64)
		pages += n
	}
	return pages * page / (1 << 30)
}
