package hardware

import "testing"

func TestParseNvidiaSMI(t *testing.T) {
	out := "0, GPU-3c5e1111, NVIDIA GeForce RTX 5090, 32607, 581.57, 12.0\n1, GPU-aaaa, NVIDIA GeForce RTX 4080, 16376, 581.57, 8.9\n"
	g := ParseNvidiaSMI(out)
	if len(g) != 2 {
		t.Fatalf("want 2 GPUs, got %d", len(g))
	}
	if g[0].Name != "NVIDIA GeForce RTX 5090" || g[0].VRAMGB != 31.8 || g[0].Compute != "cuda" || g[0].ComputeVersion != "12.0" {
		t.Fatalf("bad gpu 0: %+v", g[0])
	}
	if g[1].Index != 1 || g[1].VRAMGB != 16 {
		t.Fatalf("bad gpu 1: %+v", g[1])
	}
	if len(ParseNvidiaSMI("NVIDIA-SMI has failed because it couldn't communicate with the NVIDIA driver.")) != 0 {
		t.Fatal("a broken nvidia-smi must yield no GPUs, not garbage")
	}
}

func TestParseSystemProfiler(t *testing.T) {
	out := `{"SPDisplaysDataType":[{"sppci_model":"Apple M4 Max","sppci_cores":"40","spdisplays_vendor":"sppci_vendor_Apple","spdisplays_mtlgpufamilysupport":"spdisplays_metal4"}]}`
	g := ParseSystemProfilerDisplays([]byte(out), 128)
	if len(g) != 1 || g[0].Vendor != "apple" || g[0].Compute != "metal" || g[0].Cores != 40 || g[0].UnifiedMemoryGB != 128 {
		t.Fatalf("bad apple gpu: %+v", g)
	}
}

func TestVersions(t *testing.T) {
	if v := ParseVersion("ffmpeg version 9.0.2 Copyright (c) 2000-2026"); v != "9.0.2" {
		t.Fatalf("got %q", v)
	}
	if v := ParseVersion("Blender 3.6.2\n\tbuild date: x"); v != "3.6.2" {
		t.Fatalf("got %q", v)
	}
	cases := []struct {
		have, c string
		ok      bool
	}{{"9.0.2", ">=6.0", true}, {"3.6.2", ">=4.2", false}, {"4.2", ">=4.2", true}, {"N-1234-gabc", ">=6.0", true}, {"1.0", "", true}}
	for _, c := range cases {
		if SatisfiesVersion(c.have, c.c) != c.ok {
			t.Errorf("SatisfiesVersion(%q, %q) != %v", c.have, c.c, c.ok)
		}
	}
}

func TestEffectiveMinRule(t *testing.T) {
	det := Facts{CPU: CPU{LogicalCores: 32}, RAMGB: 128, GPUs: []GPU{{Index: 0, Name: "RTX 5090", VRAMGB: 32}}}
	// Owner lowers RAM and declares a second GPU that is not there.
	dec := &Facts{CPU: CPU{LogicalCores: 64}, RAMGB: 64, GPUs: []GPU{{Index: 0, VRAMGB: 32}, {Index: 1, Name: "RTX 4090", VRAMGB: 24}}}
	eff, drift := Effective(det, dec)
	if eff.RAMGB != 64 || eff.CPU.LogicalCores != 32 || len(eff.GPUs) != 1 {
		t.Fatalf("effective wrong: ram %v cores %v gpus %d", eff.RAMGB, eff.CPU.LogicalCores, len(eff.GPUs))
	}
	var errs int
	for _, d := range drift {
		if d.Level == "error" {
			errs++
		}
	}
	if errs != 2 { // cores raised above detected, GPU 1 missing
		t.Fatalf("want 2 error drifts, got %+v", drift)
	}
}
