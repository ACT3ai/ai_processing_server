package hardware

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"
)

var darwinTotal = sync.OnceValue(func() float64 {
	out, err := run(context.Background(), "sysctl", "-n", "hw.memsize")
	if err != nil {
		return 0
	}
	b, _ := strconv.ParseFloat(strings.TrimSpace(out), 64)
	return b / (1 << 30)
})

func freeRAM() (float64, float64, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := run(ctx, "vm_stat")
	total := darwinTotal()
	if err != nil || total == 0 {
		return 0, 0, false
	}
	return ParseVMStatFreeGB(out), total, true
}
