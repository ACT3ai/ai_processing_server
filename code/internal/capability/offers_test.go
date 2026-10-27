package capability

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
)

func setup(t *testing.T, f hardware.Facts, ids ...string) Input {
	t.Helper()
	cat, err := config.LoadCatalog(filepath.Join("..", "..", "..", "data"))
	if err != nil {
		t.Fatal(err)
	}
	ts := &config.ThisServer{Limits: config.DefaultLimits()}
	for _, id := range ids {
		ts.Services = append(ts.Services, config.ServiceSetting{ID: id, Enabled: true})
	}
	return Input{Catalog: cat, Server: ts, Facts: f, Runners: map[string]bool{"compute": true, "render": true, "ffmpeg": true, "comfy": true}}
}

func find(offers []Offer, id string) Offer {
	for _, o := range offers {
		if o.Service == id {
			return o
		}
	}
	return Offer{}
}

var ff = map[string]*hardware.Engine{"ffmpeg": {Version: "7.1"}, "ffprobe": {Version: "7.1"}, "comfyui": {Version: "0.3"}}

func TestB5090OffersComfy(t *testing.T) {
	f := hardware.Facts{CPU: hardware.CPU{LogicalCores: 32}, RAMGB: 128, Engines: ff,
		GPUs: []hardware.GPU{{Index: 0, Vendor: "nvidia", VRAMGB: 32, Compute: "cuda"}}}
	o := find(ComputeOffers(setup(t, f, "act3.comfy_on_prem", "act3.transcode")), "act3.comfy_on_prem")
	if o.Parallel != 1 || len(o.GPUs) != 1 {
		t.Fatalf("5090 should offer one comfy job on GPU 0: %+v", o)
	}
}

func TestMacHasNoCUDA(t *testing.T) {
	f := hardware.Facts{CPU: hardware.CPU{LogicalCores: 16}, RAMGB: 128, Engines: ff,
		GPUs: []hardware.GPU{{Index: 0, Vendor: "apple", UnifiedMemoryGB: 128, Compute: "metal"}}}
	o := find(ComputeOffers(setup(t, f, "act3.comfy_on_prem")), "act3.comfy_on_prem")
	if o.Parallel != 0 || !strings.Contains(o.Reason, "cuda") {
		t.Fatalf("mac must not offer a CUDA service, with a reason: %+v", o)
	}
}

func TestTwoCardsAreNeverSummed(t *testing.T) {
	// BPC: two 16 GB cards. A 24 GB service must not fit by adding them up.
	f := hardware.Facts{CPU: hardware.CPU{LogicalCores: 24}, RAMGB: 128, Engines: ff, GPUs: []hardware.GPU{
		{Index: 0, Vendor: "nvidia", VRAMGB: 16, Compute: "cuda"}, {Index: 1, Vendor: "nvidia", VRAMGB: 16, Compute: "cuda"}}}
	o := find(ComputeOffers(setup(t, f, "act3.video_gen_open_source")), "act3.video_gen_open_source")
	if o.Parallel != 0 {
		t.Fatalf("2×16 GB must not run a 24 GB job: %+v", o)
	}
}

func TestOwnerFenceAndCaps(t *testing.T) {
	f := hardware.Facts{CPU: hardware.CPU{LogicalCores: 10}, RAMGB: 64, Engines: ff}
	in := setup(t, f, "sys.compute")
	in.Server.Services = append(in.Server.Services, config.ServiceSetting{ID: "act3.transcode", Enabled: false})
	offers := ComputeOffers(in)
	if o := find(offers, "act3.transcode"); o.Parallel != 0 || o.Reason != "off in this_server.yaml" {
		t.Fatalf("off service offered: %+v", o)
	}
	// 10 cores × 80% = 8 usable, 1 per job, catalog default 8, machine cap 4 → 4.
	if o := find(offers, "sys.compute"); o.Parallel != 4 {
		t.Fatalf("want 4 (max_parallel_jobs cap), got %+v", o)
	}
	in.Server.Services[0].MaxParallel = 2
	if o := find(ComputeOffers(in), "sys.compute"); o.Parallel != 2 {
		t.Fatalf("owner max_parallel ignored: %+v", o)
	}
}

func TestMissingEngineReason(t *testing.T) {
	f := hardware.Facts{CPU: hardware.CPU{LogicalCores: 8}, RAMGB: 64, Engines: map[string]*hardware.Engine{}}
	o := find(ComputeOffers(setup(t, f, "act3.transcode")), "act3.transcode")
	if o.Parallel != 0 || !strings.Contains(o.Reason, "ffmpeg") {
		t.Fatalf("missing ffmpeg must say so: %+v", o)
	}
}
