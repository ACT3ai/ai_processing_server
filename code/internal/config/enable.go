package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

// SetEnabled is `ai_processing_server enable|disable <item>...`: the owner's own switch for this_server.yaml
// (problems 2026_oct_27 #2). An item is a service id from data/services, or "queue" (queue.enabled). It edits the
// file as text, so every comment and hand edit stays where it was; a service missing from the file (a service added
// after init wrote it) is added at the top of `services:`; a file without a `queue:` section gets the template's.
// The old file is kept as this_server.yaml.bak-<time>, and the result must parse or nothing is written.
// init still never rewrites this_server.yaml (spec §6.1, P3): this runs only when the owner asks for it.
func SetEnabled(path string, cat *Catalog, items []string, on bool) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	text := string(b)
	var did []string
	for _, item := range items {
		var msg string
		if item == "queue" {
			text, msg, err = setQueue(text, cat.Template, on)
		} else {
			def, ok := cat.Services[item]
			if !ok {
				return nil, fmt.Errorf("%s is neither \"queue\" nor a service in data/services (%s)", item, strings.Join(cat.IDs(), ", "))
			}
			text, msg, err = setService(text, def, on)
		}
		if err != nil {
			return nil, err
		}
		did = append(did, msg)
	}
	if text == string(b) {
		return did, nil
	}
	if _, err := Parse([]byte(text), path+" (after the edit)"); err != nil {
		return nil, fmt.Errorf("edit not written, the result would not load: %w", err)
	}
	bak := path + ".bak-" + time.Now().Format("20060102-150405")
	if err := os.WriteFile(bak, b, 0o600); err != nil {
		return nil, err
	}
	if err := writeAtomic(path, []byte(text), false); err != nil {
		return nil, err
	}
	return append(did, "previous file kept as "+bak), nil
}

var topKey = regexp.MustCompile(`^[A-Za-z_]`)

// block returns the [start, end) line range of top-level key `key:` (end = next top-level key), or -1.
func block(lines []string, key string) (int, int) {
	start := -1
	for i, l := range lines {
		if start < 0 {
			if strings.HasPrefix(l, key+":") {
				start = i
			}
			continue
		}
		if topKey.MatchString(l) {
			return start, i
		}
	}
	return start, len(lines)
}

func setService(text string, def ServiceDef, on bool) (string, string, error) {
	lines := strings.Split(text, "\n")
	s, e := block(lines, "services")
	if s < 0 {
		return "", "", fmt.Errorf("this_server.yaml has no services: section")
	}
	idLine := regexp.MustCompile(`^(\s*)- id:\s*` + regexp.QuoteMeta(def.ID) + `\s*(#.*)?$`)
	enabledLine := regexp.MustCompile(`^(\s*)enabled:\s*(true|false)\b(.*)$`)
	for i := s + 1; i < e; i++ {
		m := idLine.FindStringSubmatch(lines[i])
		if m == nil {
			continue
		}
		for j := i + 1; j < e; j++ {
			if strings.HasPrefix(strings.TrimSpace(lines[j]), "- ") || topKey.MatchString(lines[j]) {
				break
			}
			if em := enabledLine.FindStringSubmatch(lines[j]); em != nil {
				if em[2] == fmt.Sprint(on) {
					return text, fmt.Sprintf("%s already enabled: %t", def.ID, on), nil
				}
				lines[j] = fmt.Sprintf("%senabled: %t%s", em[1], on, em[3])
				return strings.Join(lines, "\n"), fmt.Sprintf("%s: enabled: %t", def.ID, on), nil
			}
		}
		ind := m[1] + "  "
		lines = append(lines[:i+1], append([]string{fmt.Sprintf("%senabled: %t", ind, on)}, lines[i+1:]...)...)
		return strings.Join(lines, "\n"), fmt.Sprintf("%s: enabled: %t (line added)", def.ID, on), nil
	}
	if !on {
		return text, def.ID + " is not listed, which already means off", nil
	}
	head := lines[s]
	if strings.TrimSpace(strings.SplitN(head, "#", 2)[0]) == "services: []" {
		head = "services:"
	}
	add := []string{head, "  - id: " + def.ID, "    enabled: true"}
	if def.Title != "" {
		add = append(add, "    # "+def.Title)
	}
	lines = append(lines[:s], append(add, lines[s+1:]...)...)
	return strings.Join(lines, "\n"), def.ID + ": added to services, enabled: true", nil
}

func setQueue(text, template string, on bool) (string, string, error) {
	lines := strings.Split(text, "\n")
	s, e := block(lines, "queue")
	if s >= 0 {
		enabledLine := regexp.MustCompile(`^(\s+)enabled:\s*(true|false)\b(.*)$`)
		for i := s + 1; i < e; i++ {
			if m := enabledLine.FindStringSubmatch(lines[i]); m != nil {
				if m[2] == fmt.Sprint(on) {
					return text, fmt.Sprintf("queue already enabled: %t", on), nil
				}
				lines[i] = fmt.Sprintf("%senabled: %t%s", m[1], on, m[3])
				return strings.Join(lines, "\n"), fmt.Sprintf("queue: enabled: %t", on), nil
			}
		}
		lines = append(lines[:s+1], append([]string{fmt.Sprintf("  enabled: %t", on)}, lines[s+1:]...)...)
		return strings.Join(lines, "\n"), fmt.Sprintf("queue: enabled: %t (line added)", on), nil
	}
	if !on {
		return text, "no queue: section, which already means off", nil
	}
	tl := strings.Split(template, "\n")
	ts, te := block(tl, "queue")
	if ts < 0 {
		return "", "", fmt.Errorf("data/templates/this_server.template.yaml has no queue: section to copy")
	}
	sec := strings.Join(tl[ts:te], "\n")
	sec = strings.Replace(sec, "enabled: false", "enabled: true", 1)
	text = strings.TrimRight(text, "\n") + "\n\n# Added by `ai_processing_server enable queue` " + time.Now().Format("2006-01-02") +
		" (pm/task_queue.mdx §8.2, D-014).\n" + strings.TrimRight(sec, "\n") + "\n"
	return text, "queue: section added from the template, enabled: true", nil
}
