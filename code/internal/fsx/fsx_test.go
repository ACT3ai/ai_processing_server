package fsx

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Concurrent writers to one name never leave a torn file or a stray temp file behind.
func TestWriteAtomicNoTornFileAndNoTempLeft(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "heartbeat.json")
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := strings.Repeat(string(rune('a'+i)), 64<<10)
			for range 20 {
				if err := WriteAtomic(p, []byte(body), 0o600); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	b, err := os.ReadFile(p)
	if err != nil || len(b) != 64<<10 || strings.Count(string(b), string(b[0])) != len(b) {
		t.Fatalf("torn file: %d bytes, err %v", len(b), err)
	}
	ents, _ := os.ReadDir(dir)
	if len(ents) != 1 {
		t.Fatalf("temp files left behind: %v", ents)
	}
	if err := WriteDurable(filepath.Join(dir, "j.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(filepath.Join(dir, "j.json")); fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", fi.Mode())
	}
}
