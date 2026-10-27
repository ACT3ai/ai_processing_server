// Package redact keeps secrets out of every log line, job log and error.err entry (pm/security.mdx §14).
package redact

import (
	"net/url"
	"regexp"
	"strings"
)

var (
	// secretKey matches attribute names whose values are secret. The public kid, server_key and fingerprint are
	// allowed through (checked first).
	secretKey  = regexp.MustCompile(`(?i)token|secret|passw(or)?d|authorization|api_?key|private|signature|cookie|credential|(^|_)key$`)
	publicKey  = regexp.MustCompile(`(?i)^(kid|keyid|server_key|fingerprint|key)$`)
	pemBlock   = regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(-----END [A-Z ]*PRIVATE KEY-----|$)`)
	enrollCode = regexp.MustCompile(`\bENR(-[A-Z0-9]{4}){3,}\b`)
	bearer     = regexp.MustCompile(`(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{8,}`)
	urlInText  = regexp.MustCompile(`https?://[^\s"'<>]+`)
)

// Value redacts val when key names a secret, and otherwise scrubs secrets that appear inside the text.
func Value(key, val string) string {
	if !publicKey.MatchString(key) && secretKey.MatchString(key) {
		return "[redacted]"
	}
	return Text(val)
}

// Text strips PEM private keys, enrollment codes, bearer tokens and the query strings of URLs (presigned
// signatures) from free text.
func Text(s string) string {
	if !strings.ContainsAny(s, "-:B") && !strings.Contains(s, "bearer") { // fast path: nothing that could match
		return s
	}
	s = pemBlock.ReplaceAllString(s, "[redacted private key]")
	s = enrollCode.ReplaceAllString(s, "ENR-[redacted]")
	s = bearer.ReplaceAllString(s, "Bearer [redacted]")
	if strings.Contains(s, "://") {
		s = urlInText.ReplaceAllStringFunc(s, URL)
	}
	return s
}

// URL returns scheme://host/path for an http(s) URL with a query string or user info, and anything else unchanged.
func URL(s string) string {
	if !strings.HasPrefix(s, "http://") && !strings.HasPrefix(s, "https://") {
		return s
	}
	u, err := url.Parse(s)
	if err != nil || (u.RawQuery == "" && u.User == nil) {
		return s
	}
	u.RawQuery = ""
	u.User = nil
	return u.String()
}
