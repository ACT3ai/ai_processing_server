// Package hardware detects what this machine is: OS, CPU, RAM, disks, GPUs and installed engines (spec §7).
// Every probe has a timeout and never needs admin rights. A failing probe records an error and the rest continue.
package hardware

import (
	"fmt"
	"sort"
)

// Facts is everything detected about the machine. It is also the shape of the `hardware:` block in
// this_server.yaml (the owner's declared view), so effective = min(detected, declared) works field by field.
type Facts struct {
	OS       string             `yaml:"os" json:"os"`
	Arch     string             `yaml:"arch" json:"arch"`
	CPU      CPU                `yaml:"cpu" json:"cpu"`
	RAMGB    float64            `yaml:"ram_gb" json:"ram_gb"`
	Disks    []Disk             `yaml:"disks,omitempty" json:"disks"`
	GPUs     []GPU              `yaml:"gpus,omitempty" json:"gpus"`
	Engines  map[string]*Engine `yaml:"engines,omitempty" json:"engines"`
	Hostname string             `yaml:"-" json:"hostname"`
	// MachineID is IOPlatformUUID (macOS), MachineGuid (Windows) or /etc/machine-id (Linux).
	MachineID string   `yaml:"-" json:"machine_id"`
	Errors    []string `yaml:"-" json:"errors,omitempty"`
}

type CPU struct {
	Model            string `yaml:"model" json:"model"`
	PhysicalCores    int    `yaml:"physical_cores" json:"physical_cores"`
	LogicalCores     int    `yaml:"logical_cores" json:"logical_cores"`
	PerformanceCores int    `yaml:"performance_cores,omitempty" json:"performance_cores,omitempty"`
	EfficiencyCores  int    `yaml:"efficiency_cores,omitempty" json:"efficiency_cores,omitempty"`
}

type Disk struct {
	Path    string  `yaml:"path" json:"path"`
	TotalGB float64 `yaml:"total_gb" json:"total_gb"`
	FreeGB  float64 `yaml:"free_gb" json:"free_gb"`
	Kind    string  `yaml:"kind" json:"kind"` // nvme | ssd | hdd | network | unknown
}

type GPU struct {
	Index           int      `yaml:"index" json:"index"`
	Vendor          string   `yaml:"vendor" json:"vendor"` // nvidia | apple | amd | intel
	Name            string   `yaml:"name" json:"name"`
	UUID            string   `yaml:"uuid,omitempty" json:"uuid,omitempty"`
	VRAMGB          float64  `yaml:"vram_gb,omitempty" json:"vram_gb"`
	UnifiedMemoryGB float64  `yaml:"unified_memory_gb,omitempty" json:"unified_memory_gb,omitempty"`
	Cores           int      `yaml:"cores,omitempty" json:"cores,omitempty"`
	Compute         string   `yaml:"compute" json:"compute"` // cuda | metal | rocm | none
	ComputeVersion  string   `yaml:"compute_version,omitempty" json:"compute_version,omitempty"`
	Driver          string   `yaml:"driver,omitempty" json:"driver,omitempty"`
	Encoders        []string `yaml:"encoders,omitempty" json:"encoders,omitempty"`
}

// UsableMemoryGB is VRAM for a discrete card, or unified memory for Apple silicon.
func (g GPU) UsableMemoryGB() float64 {
	if g.VRAMGB > 0 {
		return g.VRAMGB
	}
	return g.UnifiedMemoryGB
}

// Engine is one installed tool. nil in Facts.Engines means "probed, not found".
type Engine struct {
	Path    string `yaml:"path,omitempty" json:"path,omitempty"`
	URL     string `yaml:"url,omitempty" json:"url,omitempty"`
	Version string `yaml:"version" json:"version"`
}

// Drift is one difference between what this_server.yaml declares and what was detected (spec §7.4).
type Drift struct {
	Field    string `json:"field"`
	Declared string `json:"declared"`
	Detected string `json:"detected"`
	Level    string `json:"level"` // error: declared but missing · info: detected more than declared
	Message  string `json:"message"`
}

// Effective applies spec §7.4: detection is truth for what exists, the file is truth for what is offered,
// every number is min(detected, declared), and a missing declared value means "use detected".
func Effective(detected Facts, declared *Facts) (Facts, []Drift) {
	eff := detected
	eff.Disks = append([]Disk(nil), detected.Disks...)
	eff.GPUs = append([]GPU(nil), detected.GPUs...)
	if declared == nil {
		return eff, nil
	}
	var drift []Drift
	minInt := func(field string, det int, dec *int) {
		if *dec <= 0 || *dec == det {
			*dec = det
			return
		}
		if *dec > det {
			drift = append(drift, Drift{field, fmt.Sprint(*dec), fmt.Sprint(det), "error",
				fmt.Sprintf("%s: this_server.yaml declares %d, detected %d; using %d", field, *dec, det, det)})
			*dec = det
		} else {
			drift = append(drift, Drift{field, fmt.Sprint(*dec), fmt.Sprint(det), "info",
				fmt.Sprintf("%s: detected %d, offering the declared %d (run `just reinit` to offer more)", field, det, *dec)})
		}
	}
	minFloat := func(field string, det float64, dec *float64) {
		if *dec <= 0 || *dec == det {
			*dec = det
			return
		}
		level, msg := "info", fmt.Sprintf("%s: detected %.1f, offering the declared %.1f", field, det, *dec)
		if *dec > det {
			level, msg = "error", fmt.Sprintf("%s: this_server.yaml declares %.1f, detected %.1f; using %.1f", field, *dec, det, det)
			*dec = det
		}
		drift = append(drift, Drift{field, fmt.Sprintf("%.1f", *dec), fmt.Sprintf("%.1f", det), level, msg})
	}

	logical := declared.CPU.LogicalCores
	minInt("cpu.logical_cores", detected.CPU.LogicalCores, &logical)
	eff.CPU.LogicalCores = logical
	ram := declared.RAMGB
	minFloat("ram_gb", detected.RAMGB, &ram)
	eff.RAMGB = ram

	if len(declared.GPUs) > 0 {
		byIndex := map[int]GPU{}
		for _, g := range detected.GPUs {
			byIndex[g.Index] = g
		}
		var gpus []GPU
		for _, d := range declared.GPUs {
			det, ok := byIndex[d.Index]
			if !ok {
				drift = append(drift, Drift{fmt.Sprintf("gpus[%d]", d.Index), d.Name, "missing", "error",
					fmt.Sprintf("GPU %d (%s) missing: declared in this_server.yaml, not detected", d.Index, d.Name)})
				continue
			}
			if d.VRAMGB > 0 && d.VRAMGB < det.VRAMGB {
				det.VRAMGB = d.VRAMGB
			}
			if d.UnifiedMemoryGB > 0 && d.UnifiedMemoryGB < det.UnifiedMemoryGB {
				det.UnifiedMemoryGB = d.UnifiedMemoryGB
			}
			gpus = append(gpus, det)
			delete(byIndex, d.Index)
		}
		for idx, g := range byIndex {
			drift = append(drift, Drift{fmt.Sprintf("gpus[%d]", idx), "absent", g.Name, "info",
				fmt.Sprintf("GPU %d (%s) detected but not in this_server.yaml; not offered until you run `just reinit`", idx, g.Name)})
		}
		sort.Slice(gpus, func(i, j int) bool { return gpus[i].Index < gpus[j].Index })
		eff.GPUs = gpus
	}
	return eff, drift
}
