package procs

import (
	"bufio"
	"slices"
	"strings"
	"testing"
)

// A tqdm bar redraws with '\r': each redraw is its own line, so its progress is read as it happens.
func TestScanLinesOrCR(t *testing.T) {
	sc := bufio.NewScanner(strings.NewReader("load\r\nEuler:  0%|  | 0/4\rEuler: 50%|##| 2/4\rEuler: 100%|####| 4/4\ndone"))
	sc.Split(scanLinesOrCR)
	var got []string
	for sc.Scan() {
		got = append(got, sc.Text())
	}
	want := []string{"load", "Euler:  0%|  | 0/4", "Euler: 50%|##| 2/4", "Euler: 100%|####| 4/4", "done"}
	if !slices.Equal(got, want) {
		t.Fatalf("lines = %q, want %q", got, want)
	}
}
