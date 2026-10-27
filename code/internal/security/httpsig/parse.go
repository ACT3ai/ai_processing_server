package httpsig

import (
	"encoding/base64"
	"strconv"
	"strings"
)

// A minimal RFC 8941 structured-field reader: enough for Signature-Input, Signature and Content-Digest.

// splitTop splits s on sep where sep is not inside quotes or parentheses.
func splitTop(s string, sep byte) []string {
	var out []string
	depth, quoted, start := 0, false, 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case quoted && c == '\\':
			i++
		case c == '"':
			quoted = !quoted
		case !quoted && c == '(':
			depth++
		case !quoted && c == ')':
			depth--
		case !quoted && depth == 0 && c == sep:
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}

// dictMember returns the raw value of key in a dictionary header ("sig1=(...);created=1, sig2=...").
func dictMember(header, key string) (string, bool) {
	for _, m := range splitTop(header, ',') {
		k, v, ok := strings.Cut(strings.TrimSpace(m), "=")
		if ok && k == key {
			return strings.TrimSpace(v), true
		}
	}
	return "", false
}

// byteSeq decodes ":base64:".
func byteSeq(v string) ([]byte, error) {
	v = strings.TrimSpace(v)
	if len(v) < 2 || v[0] != ':' || v[len(v)-1] != ':' {
		return nil, errf("signature_invalid", "not a byte sequence")
	}
	return base64.StdEncoding.DecodeString(v[1 : len(v)-1])
}

// parseInput parses an inner list with parameters: ("@method" "signature";req;key="sig1");created=1;keyid="k".
func parseInput(v string) ([]Component, Params, error) {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, "(") {
		return nil, nil, errf("signature_invalid", "Signature-Input member is not an inner list")
	}
	end := -1
	quoted := false
	for i := 1; i < len(v); i++ {
		if v[i] == '\\' && quoted {
			i++
			continue
		}
		if v[i] == '"' {
			quoted = !quoted
		} else if v[i] == ')' && !quoted {
			end = i
			break
		}
	}
	if end < 0 {
		return nil, nil, errf("signature_invalid", "unterminated inner list")
	}
	var comps []Component
	for _, item := range strings.Fields(v[1:end]) {
		parts := splitTop(item, ';')
		name, err := strconv.Unquote(parts[0])
		if err != nil {
			return nil, nil, errf("signature_invalid", "component %s is not a quoted string", parts[0])
		}
		c := Component{Name: name}
		ps, err := parseParams(parts[1:])
		if err != nil {
			return nil, nil, err
		}
		for _, p := range ps {
			switch {
			case p.Name == "req":
				c.Req = true
			case p.Name == "key" && !p.IsInt:
				c.Key = p.Str
			default:
				return nil, nil, errf("signature_invalid", "component parameter %s is not in the profile", p.Name)
			}
		}
		comps = append(comps, c)
	}
	rest := v[end+1:]
	if rest == "" {
		return comps, nil, nil
	}
	if rest[0] != ';' {
		return nil, nil, errf("signature_invalid", "junk after inner list")
	}
	params, err := parseParams(splitTop(rest[1:], ';'))
	return comps, params, err
}

// parseParams parses name, name=123 or name="str" items (a bare name is boolean true, kept as Int 1).
func parseParams(items []string) (Params, error) {
	var out Params
	for _, it := range items {
		it = strings.TrimSpace(it)
		if it == "" {
			continue
		}
		name, val, has := strings.Cut(it, "=")
		switch {
		case !has:
			out = append(out, Param{Name: name, Int: 1, IsInt: true})
		case strings.HasPrefix(val, `"`):
			s, err := strconv.Unquote(val)
			if err != nil {
				return nil, errf("signature_invalid", "parameter %s: bad string", name)
			}
			out = append(out, Param{Name: name, Str: s})
		default:
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return nil, errf("signature_invalid", "parameter %s: not an integer", name)
			}
			out = append(out, Param{Name: name, Int: n, IsInt: true})
		}
	}
	return out, nil
}
