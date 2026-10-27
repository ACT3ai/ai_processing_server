//go:build !windows

package hardware

import (
	"os"
	"syscall"
)

func diskUsage(path string) (Disk, error) {
	os.MkdirAll(path, 0o755)
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return Disk{}, err
	}
	bs := float64(st.Bsize)
	return Disk{
		Path:    path,
		TotalGB: round1(float64(st.Blocks) * bs / (1 << 30)),
		FreeGB:  round1(float64(st.Bavail) * bs / (1 << 30)),
		Kind:    "unknown",
	}, nil
}
