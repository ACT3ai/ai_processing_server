// Package capability turns hardware facts and the owner's allow-list into offers: for each service, can this
// machine run it, on which GPUs, how many at once, and why not when it cannot (spec §7.3).
package capability

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/hardware"
)

// Offer is one service's answer. Reason is "ok" when Parallel > 0, otherwise a sentence a person can act on.
type Offer struct {
	Service   string           `json:"service"`
	Platform  string           `json:"platform"`
	Title     string           `json:"title"`
	Runner    string           `json:"runner"`
	Enabled   bool             `json:"enabled"`
	Parallel  int              `json:"parallel"`
	GPUs      []int            `json:"gpus,omitempty"`
	Resources config.Resources `json:"resources_per_job"`
	LocalBias int              `json:"local_bias"`
	Reason    string           `json:"reason"`
}

// Input is everything ComputeOffers needs; effective hardware already has min(detected, declared) applied.
type Input struct {
	Catalog *config.Catalog
	Server  *config.ThisServer
	Facts   hardware.Facts
	Runners map[string]bool // runner names built into this binary
}

// CanRun checks hard hardware and engine requirements only (used by init to propose the service list).
func CanRun(def config.ServiceDef, f hardware.Facts) (bool, string) {
	names := make([]string, 0, len(def.Requires.Engines))
	for name := range def.Requires.Engines {
		names = append(names, name)
	}
	slices.Sort(names) // deterministic reasons
	for _, name := range names {
		constraint := def.Requires.Engines[name]
		want := strings.TrimSpace(name + " " + constraint)
		e := f.Engines[name]
		if e == nil {
			return false, fmt.Sprintf("needs %s, found none", want)
		}
		if !hardware.SatisfiesVersion(e.Version, constraint) {
			return false, fmt.Sprintf("needs %s, found %s", want, e.Version)
		}
	}
	if def.Requires.GPU == "required" {
		if _, why := gpuCandidates(def, f, nil, 0, 0); why != "" {
			return false, why
		}
	}
	return true, ""
}

// ComputeOffers returns one offer per catalog service, sorted by id.
func ComputeOffers(in Input) []Offer {
	var out []Offer
	l := in.Server.Limits
	for _, id := range in.Catalog.IDs() {
		def := in.Catalog.Services[id]
		o := Offer{Service: id, Platform: def.Platform, Title: def.Title, Runner: def.Runner, Resources: def.ResourcesPerJob}
		set := in.Server.Service(id)
		switch {
		case set == nil:
			o.Reason = "not listed in this_server.yaml (new services are opt-in)"
			if ok, why := CanRun(def, in.Facts); !ok {
				o.Reason = "not listed in this_server.yaml: " + why
			}
		case !set.Enabled:
			o.Reason = "off in this_server.yaml"
		case !in.Runners[def.Runner]:
			o.Enabled = true
			o.Reason = fmt.Sprintf("runner %q is not built into this version yet", def.Runner)
		default:
			o.Enabled = true
			o.LocalBias = set.LocalBias
			o.Parallel, o.GPUs, o.Reason = parallelFor(def, in.Facts, set, l)
		}
		if o.Parallel > 0 {
			o.Reason = "ok"
		}
		out = append(out, o)
	}
	return out
}

func parallelFor(def config.ServiceDef, f hardware.Facts, set *config.ServiceSetting, l config.Limits) (int, []int, string) {
	if ok, why := CanRun(def, f); !ok {
		return 0, nil, why
	}
	res := def.ResourcesPerJob
	usableCores := int(math.Floor(float64(f.CPU.LogicalCores) * float64(100-l.CPUReservePct) / 100))
	if usableCores < 1 {
		usableCores = 1
	}
	if res.CPUCores > usableCores {
		return 0, nil, fmt.Sprintf("needs %d CPU cores per job; %d usable after the %d%% reserve", res.CPUCores, usableCores, l.CPUReservePct)
	}
	parallel := usableCores / res.CPUCores
	usableRAM := f.RAMGB - l.RAMReserveGB
	if res.RAMGB > 0 {
		if usableRAM < res.RAMGB {
			return 0, nil, fmt.Sprintf("needs %.1f GB RAM per job; %.1f GB usable after the %.0f GB reserve", res.RAMGB, usableRAM, l.RAMReserveGB)
		}
		parallel = min(parallel, int(usableRAM/res.RAMGB))
	}

	var gpus []int
	if def.Requires.GPU == "required" {
		cands, why := gpuCandidates(def, f, set.GPUs, l.VRAMHeadroomGB, l.RAMReserveGB)
		if why != "" {
			return 0, nil, why
		}
		gpuParallel := 0
		for _, c := range cands {
			gpus = append(gpus, c.index)
			gpuParallel += c.fits
		}
		parallel = min(parallel, gpuParallel)
	}
	if set.MaxParallel > 0 {
		parallel = min(parallel, set.MaxParallel)
	}
	if def.MaxParallelDefault > 0 && set.MaxParallel == 0 {
		parallel = min(parallel, def.MaxParallelDefault)
	}
	parallel = min(parallel, l.MaxParallelJobs)
	if parallel < 1 {
		return 0, nil, "no capacity after limits"
	}
	return parallel, gpus, ""
}

type gpuCand struct {
	index int
	fits  int // jobs of this service that fit on the card at once
}

// gpuCandidates applies the VRAM / unified-memory fit per card. Cards are never summed (BPC rule, spec §9.3).
func gpuCandidates(def config.ServiceDef, f hardware.Facts, allowed []int, headroom, ramReserve float64) ([]gpuCand, string) {
	if len(f.GPUs) == 0 {
		return nil, "needs a GPU; none detected"
	}
	var cands []gpuCand
	best := 0.0
	for _, g := range f.GPUs {
		if len(allowed) > 0 && !slices.Contains(allowed, g.Index) {
			continue
		}
		if len(def.Requires.Compute) > 0 && !slices.Contains(def.Requires.Compute, g.Compute) {
			continue
		}
		var usable, need float64
		if g.VRAMGB > 0 {
			usable = g.VRAMGB - headroom
			need = max(def.Requires.MinVRAMGB, def.ResourcesPerJob.VRAMGB)
		} else {
			usable = g.UnifiedMemoryGB - ramReserve
			need = max(def.Requires.MinUnifiedMemGB, def.ResourcesPerJob.VRAMGB)
		}
		best = max(best, usable)
		if usable < need || usable <= 0 {
			continue
		}
		fits := 1
		if def.ResourcesPerJob.VRAMGB > 0 {
			fits = max(1, int(usable/def.ResourcesPerJob.VRAMGB))
		}
		cands = append(cands, gpuCand{g.Index, fits})
	}
	if len(cands) == 0 {
		need := def.Requires.MinVRAMGB
		kind := "VRAM"
		if len(f.GPUs) > 0 && f.GPUs[0].VRAMGB == 0 {
			need, kind = def.Requires.MinUnifiedMemGB, "unified memory"
		}
		if len(def.Requires.Compute) > 0 {
			return nil, fmt.Sprintf("needs a %v GPU with %.0f GB %s; best usable here is %.0f GB", def.Requires.Compute, need, kind, best)
		}
		return nil, fmt.Sprintf("needs %.0f GB %s; best usable here is %.0f GB", need, kind, best)
	}
	return cands, ""
}
