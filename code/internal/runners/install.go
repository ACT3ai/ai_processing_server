package runners

import (
	"fmt"
	"path/filepath"
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
	if p := wanInstallProblem(); p != "" {
		m[OSSVideo{}.Name()] = p
	}
	if p := seedvr2InstallProblem(seedvr2Qualities["standard"].dit); p != "" {
		m[OSSUpscale{}.Name()] = p
	}
	return m
}

// wanInstallProblem checks what every Wan 2.2 job needs: the venv and the T2V diffusers weights.
func wanInstallProblem() string {
	home := wanHome()
	for _, f := range []string{venvPython(home), filepath.Join(home, "models", "Wan2.2-T2V-A14B-Diffusers", "model_index.json")} {
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
			return fmt.Sprintf("SeedVR2 weight %s is missing or partial in %s", w, models)
		}
	}
	return ""
}
