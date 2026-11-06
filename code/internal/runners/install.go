package runners

import (
	"fmt"
	"path/filepath"
	"strings"
)

// InstallProblems names, per runner, why its model is not installed on this machine ("" or absent = ready).
// Capability offers read it, so a machine whose owner turned a model service on before installing the weights
// offers nothing for it instead of claiming fleet jobs and failing them with "not installed" (problems
// 2026_oct_27 #7). With APS_FAKE_MODELS=on every runner counts as installed.
func InstallProblems() map[string]string {
	m := map[string]string{}
	if FakeModels() {
		return m
	}
	// A runner with several models is offered when any of them is installed: film/servers verifies each model on its
	// own, and a job naming one that is missing fails its Check with "not installed".
	if p := firstProblem(wanInstallProblem(wanMachineModel()), ltxInstallProblem(), kandinskyInstallProblem("lite"), kandinskyInstallProblem("pro"), h3InstallProblem()); p != "" {
		m[OSSVideo{}.Name()] = p
	}
	if p := firstProblem(seedvr2InstallProblem(seedvr2Qualities["standard"].dit), flashvsrInstallProblem()); p != "" {
		m[OSSUpscale{}.Name()] = p
	}
	if p := regenInstallProblem(); p != "" {
		m[OSSRegenerate{}.Name()] = p
	}
	return m
}

// wanInstallProblem checks what every job of one Wan 2.2 model needs: the venv and that model's diffusers weights,
// image-to-video included (shots almost always carry a first frame).
func wanInstallProblem(model string) string {
	m, ok := wanModels[model]
	if !ok {
		return fmt.Sprintf("Wan model %q (ACT3_WAN_MODEL) is not one this server renders: %s", model, strings.Join(wanModelIDs(), ", "))
	}
	home := wanHome()
	need := []string{venvPython(home), filepath.Join(home, "models", m.dir, "model_index.json")}
	if m.i2vDir != "" {
		need = append(need, filepath.Join(home, "models", m.i2vDir, "model_index.json"))
	}
	for _, f := range need {
		if !fileAtLeast(f, 1) {
			return fmt.Sprintf("Wan 2.2 is not installed on this server: missing %s (install guide: ~/BGit/all/app/smartc/film/servers/bms/oss_video/wan_2_2/)", f)
		}
	}
	return ""
}

// seedvr2InstallProblem checks the repo, its venv, the VAE and the DiT weight a job of one quality needs.
func seedvr2InstallProblem(dit string) string {
	home := seedvr2Home()
	repo := filepath.Join(home, "repo")
	models := filepath.Join(home, "models", "SEEDVR2")
	for _, f := range []string{venvPython(repo), filepath.Join(repo, "inference_cli.py")} {
		if !fileAtLeast(f, 1) {
			return fmt.Sprintf("SeedVR2 is not installed on this server: missing %s (install: ~/BGit/all/film/code/subp/upscaling/open_source/seed_vr2_mac_studio/install_mac.sh)", f)
		}
	}
	for _, w := range []string{seedvr2VAE, dit} {
		if !fileAtLeast(filepath.Join(models, w), 100<<20) {
			return fmt.Sprintf("SeedVR2 is not installed for this quality: weight %s is missing or partial in %s", w, models)
		}
	}
	return ""
}

// firstProblem is "" when any check passed (that model is installed), else the first check's problem.
func firstProblem(problems ...string) string {
	for _, p := range problems {
		if p == "" {
			return ""
		}
	}
	return problems[0]
}
