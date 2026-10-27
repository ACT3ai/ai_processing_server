package hardware

import (
	"context"
	"os"
	"strings"
)

func detectOS(_ context.Context, f *Facts) {
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		f.CPU.Model = ParseCPUInfoModel(string(b))
	} else {
		f.Errors = append(f.Errors, "cpuinfo: "+err.Error())
	}
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		f.RAMGB = ParseMeminfoGB(string(b))
	} else {
		f.Errors = append(f.Errors, "meminfo: "+err.Error())
	}
	if b, err := os.ReadFile("/etc/machine-id"); err == nil {
		f.MachineID = strings.TrimSpace(string(b))
	}
}

func readLoad(_ context.Context) float64 {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return 0
	}
	return ParseLoadAvg(string(b))
}
