package hardware

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

// Parsers are pure functions over recorded tool output, so they are unit tested with fixtures and no GPU.

// ParseNvidiaSMI reads
// `nvidia-smi --query-gpu=index,uuid,name,memory.total,driver_version,compute_cap --format=csv,noheader,nounits`.
func ParseNvidiaSMI(out string) []GPU {
	var gpus []GPU
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Split(line, ",")
		if len(f) < 6 {
			continue
		}
		for i := range f {
			f[i] = strings.TrimSpace(f[i])
		}
		idx, err := strconv.Atoi(f[0])
		if err != nil {
			continue
		}
		mib, _ := strconv.ParseFloat(f[3], 64)
		gpus = append(gpus, GPU{
			Index: idx, Vendor: "nvidia", UUID: f[1], Name: f[2],
			VRAMGB: round1(mib / 1024), Driver: f[4], Compute: "cuda", ComputeVersion: f[5],
		})
	}
	return gpus
}

// ParseSystemProfilerDisplays reads `system_profiler SPDisplaysDataType -json` (Apple silicon GPU).
func ParseSystemProfilerDisplays(out []byte, unifiedGB float64) []GPU {
	var doc struct {
		Displays []struct {
			Model  string `json:"sppci_model"`
			Cores  string `json:"sppci_cores"`
			Vendor string `json:"spdisplays_vendor"`
			Metal  string `json:"spdisplays_mtlgpufamilysupport"`
		} `json:"SPDisplaysDataType"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		return nil
	}
	var gpus []GPU
	for i, d := range doc.Displays {
		g := GPU{Index: i, Name: d.Model, Compute: "none"}
		g.Cores, _ = strconv.Atoi(d.Cores)
		v := strings.ToLower(d.Vendor + " " + d.Model)
		switch {
		case strings.Contains(v, "apple"):
			g.Vendor, g.Compute, g.UnifiedMemoryGB = "apple", "metal", unifiedGB
			g.ComputeVersion = strings.TrimPrefix(d.Metal, "spdisplays_")
		case strings.Contains(v, "nvidia"):
			g.Vendor = "nvidia"
		case strings.Contains(v, "amd"), strings.Contains(v, "ati"):
			g.Vendor = "amd"
		case strings.Contains(v, "intel"):
			g.Vendor = "intel"
		}
		gpus = append(gpus, g)
	}
	return gpus
}

var versionRe = regexp.MustCompile(`(\d+(?:\.\d+)+)`)

// ParseVersion pulls the first dotted version from a `--version` line ("ffmpeg version 9.0.2 Copyright" → "9.0.2").
func ParseVersion(out string) string {
	first := strings.SplitN(strings.TrimSpace(out), "\n", 2)[0]
	if m := versionRe.FindStringSubmatch(first); m != nil {
		return m[1]
	}
	return strings.TrimSpace(first)
}

// ParseEncoders keeps the hardware video encoders from `ffmpeg -hide_banner -encoders`.
func ParseEncoders(out string) []string {
	var enc []string
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || !strings.HasPrefix(f[0], "V") {
			continue
		}
		name := f[1]
		for _, hw := range []string{"_nvenc", "_videotoolbox", "_qsv", "_amf", "_vaapi"} {
			if strings.HasSuffix(name, hw) {
				enc = append(enc, name)
			}
		}
	}
	return enc
}

// ParseMeminfoGB reads MemTotal from /proc/meminfo.
func ParseMeminfoGB(out string) float64 {
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				kb, _ := strconv.ParseFloat(f[1], 64)
				return round1(kb / 1024 / 1024)
			}
		}
	}
	return 0
}

// ParseCPUInfoModel reads the first "model name" from /proc/cpuinfo.
func ParseCPUInfoModel(out string) string {
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "model name") {
			if i := strings.Index(line, ":"); i >= 0 {
				return strings.TrimSpace(line[i+1:])
			}
		}
	}
	return ""
}

// ParseLoadAvg reads the 1-minute load from `sysctl -n vm.loadavg` ("{ 2.10 1.98 1.80 }") or /proc/loadavg.
func ParseLoadAvg(out string) float64 {
	f := strings.Fields(strings.Trim(strings.TrimSpace(out), "{}"))
	if len(f) == 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(f[0], 64)
	return v
}

// CompareVersions returns -1, 0 or 1 comparing dotted numeric versions. Non-numeric parts compare as 0.
func CompareVersions(a, b string) int {
	pa, pb := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(pa) || i < len(pb); i++ {
		var x, y int
		if i < len(pa) {
			x, _ = strconv.Atoi(pa[i])
		}
		if i < len(pb) {
			y, _ = strconv.Atoi(pb[i])
		}
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}

// SatisfiesVersion checks a catalog constraint such as ">=6.0" or "" (any). An unparsable installed
// version (a git build of ffmpeg) is treated as satisfying, because refusing a working tool is worse.
func SatisfiesVersion(have, constraint string) bool {
	constraint = strings.TrimSpace(constraint)
	if constraint == "" || constraint == "*" {
		return true
	}
	if !versionRe.MatchString(have) {
		return true
	}
	have = versionRe.FindString(have)
	for _, op := range []string{">=", "<=", ">", "<", "="} {
		if strings.HasPrefix(constraint, op) {
			c := CompareVersions(have, strings.TrimSpace(constraint[len(op):]))
			switch op {
			case ">=":
				return c >= 0
			case "<=":
				return c <= 0
			case ">":
				return c > 0
			case "<":
				return c < 0
			default:
				return c == 0
			}
		}
	}
	return CompareVersions(have, constraint) >= 0
}

func round1(v float64) float64 { return float64(int64(v*10+0.5)) / 10 }
