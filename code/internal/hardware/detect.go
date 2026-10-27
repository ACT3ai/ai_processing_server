package hardware

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	cmdTimeout = 5 * time.Second
	urlTimeout = 2 * time.Second
)

// Options control detection.
type Options struct {
	// WorkDir is the disk whose free space matters most (paths.work_dir). Always probed.
	WorkDir string
	// EnginePaths overrides where to look for an engine (this_server.yaml `engines:`), name → path or URL.
	EnginePaths map[string]string
}

// Detect probes the machine. It never fails as a whole: probe errors are collected in Facts.Errors.
func Detect(ctx context.Context, opts Options) Facts {
	f := Facts{OS: runtime.GOOS, Arch: runtime.GOARCH, Engines: map[string]*Engine{}}
	f.Hostname, _ = os.Hostname()
	f.CPU.LogicalCores = runtime.NumCPU()
	f.CPU.PhysicalCores = runtime.NumCPU()

	detectOS(ctx, &f) // per-OS: CPU model, cores, RAM, GPUs, machine id

	if gpus := detectNvidia(ctx, &f); len(gpus) > 0 {
		// NVIDIA is authoritative for CUDA cards; keep any Apple entry (cannot coexist in practice).
		var keep []GPU
		for _, g := range f.GPUs {
			if g.Vendor != "nvidia" {
				keep = append(keep, g)
			}
		}
		f.GPUs = append(keep, gpus...)
	}

	dir := opts.WorkDir
	if dir == "" {
		dir, _ = os.Getwd()
	}
	if d, err := diskUsage(dir); err == nil {
		f.Disks = append(f.Disks, d)
	} else {
		f.Errors = append(f.Errors, "disk: "+err.Error())
	}

	detectEngines(ctx, &f, opts.EnginePaths)
	return f
}

func detectNvidia(ctx context.Context, f *Facts) []GPU {
	smi, err := exec.LookPath("nvidia-smi")
	if err != nil {
		return nil
	}
	out, err := run(ctx, smi, "--query-gpu=index,uuid,name,memory.total,driver_version,compute_cap",
		"--format=csv,noheader,nounits")
	if err != nil {
		f.Errors = append(f.Errors, "nvidia-smi: "+err.Error())
		return nil
	}
	return ParseNvidiaSMI(out)
}

// engineProbe describes how to find one engine and read its version (spec §7.2).
type engineProbe struct {
	name     string
	binaries []string
	extra    map[string][]string // GOOS → common install paths
	args     []string
	url      string // HTTP engines: health URL
}

var engineProbes = []engineProbe{
	{name: "ffmpeg", binaries: []string{"ffmpeg"}, args: []string{"-version"}},
	{name: "ffprobe", binaries: []string{"ffprobe"}, args: []string{"-version"}},
	{name: "blender", binaries: []string{"blender"}, args: []string{"--version"}, extra: map[string][]string{
		"darwin":  {"/Applications/Blender.app/Contents/MacOS/Blender"},
		"windows": {`C:\Program Files\Blender Foundation\Blender 4.5\blender.exe`, `C:\Program Files\Blender Foundation\Blender 4.2\blender.exe`},
	}},
	{name: "python", binaries: []string{"python3", "python"}, args: []string{"--version"}},
	{name: "docker", binaries: []string{"docker"}, args: []string{"--version"}},
	{name: "comfyui", url: "http://127.0.0.1:8188/system_stats"},
}

func detectEngines(ctx context.Context, f *Facts, overrides map[string]string) {
	for _, p := range engineProbes {
		if p.url != "" {
			u := p.url
			if o := overrides[p.name]; o != "" {
				u = strings.TrimRight(o, "/") + "/system_stats"
			}
			f.Engines[p.name] = probeURL(ctx, u)
			continue
		}
		path := findBinary(p, overrides[p.name])
		if path == "" {
			f.Engines[p.name] = nil
			continue
		}
		out, err := run(ctx, path, p.args...)
		if err != nil {
			f.Errors = append(f.Errors, fmt.Sprintf("%s --version: %v", p.name, err))
			f.Engines[p.name] = nil
			continue
		}
		f.Engines[p.name] = &Engine{Path: path, Version: ParseVersion(out)}
		if p.name == "ffmpeg" {
			if enc, err := run(ctx, path, "-hide_banner", "-encoders"); err == nil {
				addEncoders(f, ParseEncoders(enc))
			}
		}
	}
}

func addEncoders(f *Facts, enc []string) {
	for i := range f.GPUs {
		for _, e := range enc {
			v := f.GPUs[i].Vendor
			if (v == "nvidia" && strings.Contains(e, "nvenc")) || (v == "apple" && strings.Contains(e, "videotoolbox")) ||
				(v == "intel" && strings.Contains(e, "qsv")) || (v == "amd" && strings.Contains(e, "amf")) {
				f.GPUs[i].Encoders = append(f.GPUs[i].Encoders, e)
			}
		}
	}
}

func findBinary(p engineProbe, override string) string {
	if override != "" {
		if _, err := os.Stat(expandHome(override)); err == nil {
			return expandHome(override)
		}
	}
	for _, b := range p.binaries {
		if path, err := exec.LookPath(b); err == nil {
			return path
		}
	}
	for _, c := range p.extra[runtime.GOOS] {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}

func probeURL(ctx context.Context, u string) *Engine {
	ctx, cancel := context.WithTimeout(ctx, urlTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	return &Engine{URL: strings.TrimSuffix(u, "/system_stats"), Version: "unknown"}
}

// run executes a probe command with a timeout and empty stdin, returning combined output.
func run(ctx context.Context, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, cmdTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	if ctx.Err() != nil {
		return out.String(), fmt.Errorf("timed out after %s", cmdTimeout)
	}
	return out.String(), err
}

func expandHome(p string) string {
	if strings.HasPrefix(p, "~/") || p == "~" {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, strings.TrimPrefix(p, "~"))
	}
	return p
}
