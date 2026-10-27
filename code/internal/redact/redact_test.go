package redact

import (
	"strings"
	"testing"
)

// security.mdx §19.1: a line with a presigned URL, a PEM block, an enrollment code and a bearer token comes out clean.
func TestTextIsClean(t *testing.T) {
	in := "PUT https://b.s3.amazonaws.com/o/x.mp4?X-Amz-Signature=abc123&X-Amz-Credential=AKIA failed; " +
		"key -----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIJ+DYvh6SEqVTm50\n-----END PRIVATE KEY----- " +
		"code ENR-7KQ2-H9XW-4MZD-PT3A header Authorization: Bearer abcdefghijklmnop"
	out := Text(in)
	for _, bad := range []string{"X-Amz-Signature", "AKIA", "MC4CAQ", "7KQ2", "abcdefghijklmnop"} {
		if strings.Contains(out, bad) {
			t.Fatalf("%q survived: %s", bad, out)
		}
	}
	if !strings.Contains(out, "https://b.s3.amazonaws.com/o/x.mp4") {
		t.Fatalf("host and path lost: %s", out)
	}
}

func TestValueByKey(t *testing.T) {
	for _, k := range []string{"token", "api_key", "Authorization", "password", "private_key", "signature", "session_cookie"} {
		if Value(k, "s3cret") != "[redacted]" {
			t.Fatalf("%s not redacted", k)
		}
	}
	for _, k := range []string{"kid", "server_key", "fingerprint", "key", "job_id", "service"} {
		if Value(k, "B5090") != "B5090" {
			t.Fatalf("%s wrongly redacted", k)
		}
	}
}
