package hardware

import (
	"os"
	"syscall"
	"unsafe"
)

func diskUsage(path string) (Disk, error) {
	os.MkdirAll(path, 0o755)
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return Disk{}, err
	}
	var free, total, totalFree uint64
	proc := syscall.NewLazyDLL("kernel32.dll").NewProc("GetDiskFreeSpaceExW")
	if r, _, err := proc.Call(uintptr(unsafe.Pointer(p)), uintptr(unsafe.Pointer(&free)),
		uintptr(unsafe.Pointer(&total)), uintptr(unsafe.Pointer(&totalFree))); r == 0 {
		return Disk{}, err
	}
	return Disk{Path: path, TotalGB: round1(float64(total) / (1 << 30)), FreeGB: round1(float64(free) / (1 << 30)), Kind: "unknown"}, nil
}
