package config

import (
	"strings"
	"testing"
)

// $ACT3_WAN_MODEL sizes the Wan offer: TI2V-5B fits an 8 GB card, an unknown name refuses to load.
func TestServiceModelPicksItsNeeds(t *testing.T) {
	const id = "act3.video_gen_open_source"
	c, err := LoadCatalog("../../../data")
	if err != nil {
		t.Fatal(err)
	}
	if s := c.Services[id]; s.Requires.MinVRAMGB != 24 || s.ResourcesPerJob.RAMGB != 64 {
		t.Fatalf("default: %+v", s)
	}
	t.Setenv("ACT3_WAN_MODEL", "wan2.2-ti2v-5b")
	c, err = LoadCatalog("../../../data")
	if err != nil {
		t.Fatal(err)
	}
	if s := c.Services[id]; s.Requires.MinVRAMGB != 6 || s.ResourcesPerJob.RAMGB != 48 || s.ResourcesPerJob.CPUCores != 4 {
		t.Fatalf("5B: %+v", s)
	}
	t.Setenv("ACT3_WAN_MODEL", "wan2.2-a14b")
	if c, err = LoadCatalog("../../../data"); err != nil || c.Services[id].Requires.MinVRAMGB != 24 {
		t.Fatalf("a14b named: %v", err)
	}
	t.Setenv("ACT3_WAN_MODEL", "wan2.2-typo")
	if _, err = LoadCatalog("../../../data"); err == nil || !strings.Contains(err.Error(), "wan2.2-ti2v-5b") {
		t.Fatalf("typo: %v", err)
	}
}
