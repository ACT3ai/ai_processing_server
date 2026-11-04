package scheduler

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ACT3ai/ai_processing_server/internal/queue"
)

// The runner reads params.model; the model fence checks the envelope's. A caller that sets only the envelope's model
// used to get the runner's default model (a FlashVSR job ran as SeedVR2 on PC-10, 2026-11-04).
func TestEnvelopeModelReachesTheRunner(t *testing.T) {
	model := func(params string, envModel string) (string, error) {
		local, err := toLocalEnvelope(&queue.Envelope{JobID: "j", Caller: "local", Service: "s", Model: envModel, Params: json.RawMessage(params)})
		if err != nil {
			return "", err
		}
		var p struct {
			Model  string `json:"model"`
			Factor int    `json:"factor"`
		}
		_ = json.Unmarshal(local.Params, &p)
		if p.Factor != 4 && strings.Contains(params, "factor") {
			t.Fatalf("other params lost: %s", local.Params)
		}
		return p.Model, nil
	}
	if m, err := model(`{"factor":4}`, "flashvsr-1.1"); err != nil || m != "flashvsr-1.1" {
		t.Fatalf("envelope model not passed on: %q %v", m, err)
	}
	if m, err := model(``, "ltx-2.5"); err != nil || m != "ltx-2.5" {
		t.Fatalf("no params: %q %v", m, err)
	}
	if m, err := model(`{"model":"wan2.2-a14b","factor":4}`, "wan2.2-a14b"); err != nil || m != "wan2.2-a14b" {
		t.Fatalf("same model: %q %v", m, err)
	}
	if m, err := model(`{"model":"seedvr2-best"}`, ""); err != nil || m != "seedvr2-best" {
		t.Fatalf("no envelope model keeps the params': %q %v", m, err)
	}
	if _, err := model(`{"model":"seedvr2-best"}`, "seedvr2-standard"); err == nil || !strings.Contains(err.Error(), "disagrees") {
		t.Fatalf("a params model the fence never saw must be refused: %v", err)
	}
}
