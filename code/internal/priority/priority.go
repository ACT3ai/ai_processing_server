// Package priority maps a job's 0..300 priority to a band and orders the work consumers a worker pulls from
// (pm/task_queue.mdx §14). Phase Q1 pulls bands strictly in order (p0 first); the weighted deficit round robin over
// bands and callers, and aging, are phase Q2 (§14.4).
package priority

import (
	"sort"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

// Clamp keeps a priority on the shared 0..300 scale (parent spec §10.2).
func Clamp(p int) int { return min(max(p, 0), 300) }

// BandFor returns the first band whose min_priority the priority reaches. bands are most urgent first.
func BandFor(bands []config.Band, p int) string {
	for _, b := range bands {
		if p >= b.MinPriority {
			return b.Name
		}
	}
	return bands[len(bands)-1].Name
}

// CapBand lowers band to the caller's ceiling (§14.3): a caller with max_band p1 asking for p0 gets p1.
func CapBand(bands []config.Band, band, maxBand string) (string, bool) {
	idx := func(n string) int {
		for i, b := range bands {
			if b.Name == n {
				return i
			}
		}
		return 0
	}
	if maxBand == "" || idx(band) >= idx(maxBand) {
		return band, false
	}
	return maxBand, true
}

// Candidate is one service this worker could start a job of right now.
type Candidate struct {
	Service   string
	LocalBias int
}

// PullOrder lists the consumers to try, most urgent first: band by band; inside a band, callers in the given order
// (the local caller first when callers.local.mode is first); inside a caller, services by the owner's local_bias
// then by id (§14.4 steps 1 to 3, without DRR).
func PullOrder(bands []config.Band, callers []string, cands []Candidate) []queue.ConsumerRef {
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].LocalBias != cands[j].LocalBias {
			return cands[i].LocalBias > cands[j].LocalBias
		}
		return cands[i].Service < cands[j].Service
	})
	var out []queue.ConsumerRef
	for _, b := range bands {
		for _, c := range callers {
			for _, s := range cands {
				out = append(out, queue.ConsumerRef{Band: b.Name, Caller: c, Service: s.Service})
			}
		}
	}
	return out
}
