package hardware

import (
	"context"
	"regexp"
	"strconv"
	"strings"
)

var ioregUUID = regexp.MustCompile(`"IOPlatformUUID" = "([^"]+)"`)

func detectOS(ctx context.Context, f *Facts) {
	sysctlInt := func(name string) int {
		out, err := run(ctx, "sysctl", "-n", name)
		if err != nil {
			return 0
		}
		v, _ := strconv.Atoi(strings.TrimSpace(out))
		return v
	}
	if out, err := run(ctx, "sysctl", "-n", "machdep.cpu.brand_string"); err == nil {
		f.CPU.Model = strings.TrimSpace(out)
	} else {
		f.Errors = append(f.Errors, "sysctl cpu: "+err.Error())
	}
	if v := sysctlInt("hw.physicalcpu"); v > 0 {
		f.CPU.PhysicalCores = v
	}
	if v := sysctlInt("hw.logicalcpu"); v > 0 {
		f.CPU.LogicalCores = v
	}
	f.CPU.PerformanceCores = sysctlInt("hw.perflevel0.physicalcpu")
	f.CPU.EfficiencyCores = sysctlInt("hw.perflevel1.physicalcpu")
	if out, err := run(ctx, "sysctl", "-n", "hw.memsize"); err == nil {
		b, _ := strconv.ParseFloat(strings.TrimSpace(out), 64)
		f.RAMGB = round1(b / (1 << 30))
	}
	if out, err := runSlow(ctx, "system_profiler", "SPDisplaysDataType", "-json"); err == nil {
		f.GPUs = ParseSystemProfilerDisplays([]byte(out), f.RAMGB)
	} else {
		f.Errors = append(f.Errors, "system_profiler: "+err.Error())
	}
	if out, err := run(ctx, "ioreg", "-rd1", "-c", "IOPlatformExpertDevice"); err == nil {
		if m := ioregUUID.FindStringSubmatch(out); m != nil {
			f.MachineID = m[1]
		}
	}
}

func readLoad(ctx context.Context) float64 {
	out, err := run(ctx, "sysctl", "-n", "vm.loadavg")
	if err != nil {
		return 0
	}
	return ParseLoadAvg(out)
}
