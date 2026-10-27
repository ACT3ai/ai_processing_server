package config

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/hardware"
)

func catalog(t *testing.T) *Catalog {
	t.Helper()
	c, err := LoadCatalog(filepath.Join("..", "..", "..", "data"))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func canRunAll(ServiceDef, hardware.Facts) (bool, string) { return true, "" }

var laptop = hardware.Facts{OS: "darwin", Arch: "arm64", Hostname: "Bry-MacBook-Pro-2025b.local", MachineID: "M1",
	CPU: hardware.CPU{Model: "Apple M4 Max", PhysicalCores: 16, LogicalCores: 16}, RAMGB: 128,
	GPUs: []hardware.GPU{{Index: 0, Vendor: "apple", Name: "Apple M4 Max", UnifiedMemoryGB: 128, Compute: "metal"}}}

// P3: init creates the file once and never changes a byte of it afterwards.
func TestInitNeverOverwrites(t *testing.T) {
	cat := catalog(t)
	path := filepath.Join(t.TempDir(), "this_server.yaml")
	res, err := InitThisServer(InitOptions{Path: path, Catalog: cat, Facts: laptop, Version: "test", CanRun: canRunAll})
	if err != nil || !res.Created {
		t.Fatalf("first init: created=%v err=%v", res.Created, err)
	}
	before, _ := os.ReadFile(path)
	st1, _ := os.Stat(path)
	if st1.Mode().Perm() != 0o600 {
		t.Errorf("mode %v, want 0600", st1.Mode().Perm())
	}
	time.Sleep(20 * time.Millisecond)
	other := laptop
	other.RAMGB = 64
	for i := 0; i < 2; i++ {
		res, err = InitThisServer(InitOptions{Path: path, Catalog: cat, Facts: other, Version: "test2", CanRun: canRunAll})
		if err != nil || res.Created || res.Wrote != "" {
			t.Fatalf("init #%d touched the file: %+v %v", i+2, res, err)
		}
	}
	after, _ := os.ReadFile(path)
	st2, _ := os.Stat(path)
	if !bytes.Equal(before, after) || !st1.ModTime().Equal(st2.ModTime()) {
		t.Fatal("this_server.yaml changed on a second init")
	}
	ts, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if ts.Server.Key != "BLAP" || ts.Server.BoundTo.Hostname != laptop.Hostname {
		t.Fatalf("known-server match failed: %+v", ts.Server)
	}
	if errs, _ := Validate(ts, cat); len(errs) > 0 {
		t.Fatalf("generated file invalid: %v", errs)
	}
}

func TestWriteNewKeepsIdentityAndOriginal(t *testing.T) {
	cat := catalog(t)
	path := filepath.Join(t.TempDir(), "this_server.yaml")
	InitThisServer(InitOptions{Path: path, Catalog: cat, Facts: laptop, Version: "t", CanRun: canRunAll})
	before, _ := os.ReadFile(path)
	res, err := InitThisServer(InitOptions{Path: path, Catalog: cat, Facts: laptop, Version: "t", CanRun: canRunAll, WriteNew: true})
	if err != nil || res.Wrote != path+".new" {
		t.Fatalf("write-new: %+v %v", res, err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("--write-new modified this_server.yaml")
	}
}

func TestLegacyFileIsLeftAlone(t *testing.T) {
	cat := catalog(t)
	path := filepath.Join(t.TempDir(), "this_server.yaml")
	legacy := []byte("this_server:\n  name: \"old probe\"\n")
	os.WriteFile(path, legacy, 0o644)
	if _, err := Load(path); !errors.Is(err, ErrLegacy) {
		t.Fatalf("want ErrLegacy, got %v", err)
	}
	if _, err := InitThisServer(InitOptions{Path: path, Catalog: cat, Facts: laptop, Version: "t", CanRun: canRunAll}); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(legacy, after) {
		t.Fatal("legacy file modified")
	}
}

func TestUnknownMachineGetsChangeMe(t *testing.T) {
	cat := catalog(t)
	f := laptop
	f.Hostname = "some-other-box"
	_, ts, err := Generate(cat, f, "t", canRunAll, nil)
	if err != nil {
		t.Fatal(err)
	}
	errs, _ := Validate(ts, cat)
	if ts.Server.Key != "CHANGE_ME" || len(errs) == 0 {
		t.Fatalf("unknown machine must get CHANGE_ME and fail validation: key=%s errs=%v", ts.Server.Key, errs)
	}
}

func TestValidateRejectsBadValues(t *testing.T) {
	cat := catalog(t)
	_, ts, _ := Generate(cat, laptop, "t", canRunAll, nil)
	ts.Limits.MaxParallelJobs = 0
	ts.Services = append(ts.Services, ServiceSetting{ID: "sys.compute", LocalBias: 99})
	errs, _ := Validate(ts, cat)
	if len(errs) < 2 {
		t.Fatalf("want errors for max_parallel_jobs, duplicate and bias, got %v", errs)
	}
}
