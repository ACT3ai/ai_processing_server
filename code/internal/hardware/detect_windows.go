package hardware

import (
	"context"
	"strings"
	"syscall"
	"unsafe"
)

type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

func detectOS(ctx context.Context, f *Facts) {
	k32 := syscall.NewLazyDLL("kernel32.dll")
	var m memoryStatusEx
	m.Length = uint32(unsafe.Sizeof(m))
	if r, _, err := k32.NewProc("GlobalMemoryStatusEx").Call(uintptr(unsafe.Pointer(&m))); r != 0 {
		f.RAMGB = round1(float64(m.TotalPhys) / (1 << 30))
	} else {
		f.Errors = append(f.Errors, "GlobalMemoryStatusEx: "+err.Error())
	}
	if out, err := run(ctx, "reg", "query", `HKLM\HARDWARE\DESCRIPTION\System\CentralProcessor\0`, "/v", "ProcessorNameString"); err == nil {
		f.CPU.Model = lastRegField(out)
	}
	if out, err := run(ctx, "reg", "query", `HKLM\SOFTWARE\Microsoft\Cryptography`, "/v", "MachineGuid"); err == nil {
		f.MachineID = lastRegField(out)
	}
}

// lastRegField returns the value column of `reg query` output ("    Name    REG_SZ    value").
func lastRegField(out string) string {
	for _, line := range strings.Split(out, "\n") {
		if i := strings.Index(line, "REG_SZ"); i >= 0 {
			return strings.TrimSpace(line[i+len("REG_SZ"):])
		}
	}
	return ""
}

func readLoad(_ context.Context) float64 { return 0 }
