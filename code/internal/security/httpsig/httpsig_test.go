package httpsig

import (
	"bytes"
	"crypto/ed25519"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/security/keys"
)

// RFC 9421 Appendix B.1.4 test-key-ed25519 and the B.2.6 request.
const rfcKey = `-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIJ+DYvh6SEqVTm50DFtMDoQikTmiCqirVv9mWG9qfSnF
-----END PRIVATE KEY-----
`

func TestRFC9421B26(t *testing.T) {
	k, err := keys.ParsePrivatePEM([]byte(rfcKey), "rfc")
	if err != nil {
		t.Fatal(err)
	}
	r, _ := http.NewRequest("POST", "https://example.com/foo?param=Value&Pet=dog", strings.NewReader(`{"hello": "world"}`))
	r.Host = "example.com"
	r.Header.Set("Date", "Tue, 20 Apr 2021 02:07:55 GMT")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Content-Length", "18")
	comps := []Component{C("date"), C("@method"), C("@path"), C("@authority"), C("content-type"), C("content-length")}
	params := Params{{Name: "created", Int: 1618884473, IsInt: true}, {Name: "keyid", Str: "test-key-ed25519"}}
	base, err := SignatureBase(RequestMessage(r, ""), comps, params)
	if err != nil {
		t.Fatal(err)
	}
	wantBase := `"date": Tue, 20 Apr 2021 02:07:55 GMT
"@method": POST
"@path": /foo
"@authority": example.com
"content-type": application/json
"content-length": 18
"@signature-params": ("date" "@method" "@path" "@authority" "content-type" "content-length");created=1618884473;keyid="test-key-ed25519"`
	if base != wantBase {
		t.Fatalf("signature base:\n%s\nwant:\n%s", base, wantBase)
	}
	_, sig, _ := Sign(RequestMessage(r, ""), comps, params, k.Private)
	if want := ":wqcAqbmYJ2ji2glfAMaRy4gruYYnx2nEFN2HN6jrnDnQCK1u02Gb04v9EDgwUPiu4A0w6vuQv5lIp5WPpBKRCw==:"; sig != want {
		t.Fatalf("signature %s\nwant      %s", sig, want)
	}
}

// RFC 9530 §B.1 / apis.mdx §5.2: sha-256 of {"hello": "world"}.
func TestContentDigest(t *testing.T) {
	body := []byte(`{"hello": "world"}`)
	if got := ContentDigest(body); got != "sha-256=:X48E9qOokqqrvdts8nOJRJN3OWDUoyWxBf7kbu9DBPE=:" {
		t.Fatal(got)
	}
	if err := CheckContentDigest("sha-512=:WZDPaVn/7XgHaAy8pmojAkGWoRx2UFChF41A2svX+TaPm+AbwAgBWnrIiYllu7BNNyealdVLvRwEmTHWXvJwew==:", body); err != nil {
		t.Fatal(err)
	}
	if err := CheckContentDigest(ContentDigest(body), []byte(`{"hello": "World"}`)); err == nil {
		t.Fatal("changed body passed")
	}
}

type fixture struct {
	server, platform *keys.Key
	now              time.Time
}

func newFixture(t *testing.T) fixture {
	gen := func() *keys.Key {
		r, err := keys.InitKey(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return r.Key
	}
	return fixture{server: gen(), platform: gen(), now: time.Unix(1793120550, 0)}
}

func (f fixture) request(t *testing.T, body string) (*http.Request, []byte) {
	r, _ := http.NewRequest("POST", "https://api.act3ai.com/processing/v1/jobs/7c0e/complete", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Server-Key", "B5090")
	r.Header.Set("Idempotency-Key", "0192d4c1-7a3e-7c11-9d2b-5f0e1a2b3c4d")
	s := &Signer{Key: f.server.Private, KeyID: f.server.KID, Now: func() time.Time { return f.now }}
	if err := s.SignRequest(r, []byte(body)); err != nil {
		t.Fatal(err)
	}
	return r, []byte(body)
}

func (f fixture) verifier(replay *ReplayCache) *Verifier {
	return &Verifier{Authority: "api.act3ai.com", Replay: replay, Now: func() time.Time { return f.now.Add(2 * time.Second) },
		Keys: func(kid string) (ed25519.PublicKey, string, error) {
			switch kid {
			case f.server.KID:
				return f.server.Public, "B5090", nil
			case f.platform.KID:
				return f.platform.Public, "", nil
			}
			return nil, "", nil
		}}
}

func code(err error) string {
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	return ""
}

func TestRequestRoundTripAndRejections(t *testing.T) {
	f := newFixture(t)
	r, body := f.request(t, `{"lease_id":"L1"}`)
	if !strings.Contains(r.Header.Get("Signature-Input"), `"idempotency-key"`) {
		t.Fatal("idempotency-key not covered")
	}
	replay := NewReplayCache()
	if _, err := f.verifier(replay).VerifyRequest(r, body); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	if _, err := f.verifier(replay).VerifyRequest(r, body); code(err) != "replay" {
		t.Fatalf("replay: %v", err)
	}

	cases := map[string]struct {
		mut  func(r *http.Request, b *[]byte, v *Verifier)
		want string
	}{
		"changed body":      {func(_ *http.Request, b *[]byte, _ *Verifier) { *b = []byte(`{"lease_id":"L2"}`) }, "digest_mismatch"},
		"changed path":      {func(r *http.Request, _ *[]byte, _ *Verifier) { r.URL.Path = "/processing/v1/jobs/other/complete" }, "signature_invalid"},
		"staging authority": {func(_ *http.Request, _ *[]byte, v *Verifier) { v.Authority = "staging.act3ai.com" }, "signature_invalid"},
		"other server key":  {func(r *http.Request, _ *[]byte, _ *Verifier) { r.Header.Set("X-Server-Key", "BPC") }, "signature_invalid"},
		"61 s old": {func(_ *http.Request, _ *[]byte, v *Verifier) {
			v.Now = func() time.Time { return f.now.Add(61 * time.Second) }
		}, "signature_expired"},
		"unknown kid": {func(_ *http.Request, _ *[]byte, v *Verifier) {
			v.Keys = func(string) (ed25519.PublicKey, string, error) { return nil, "", nil }
		}, "key_unknown"},
		"hmac alg":            {func(r *http.Request, _ *[]byte, _ *Verifier) { swapIn(r, `alg="ed25519"`, `alg="hmac-sha256"`) }, "signature_invalid"},
		"wrong tag":           {func(r *http.Request, _ *[]byte, _ *Verifier) { swapIn(r, `tag="act3-processing-v1"`, `tag="other"`) }, "signature_invalid"},
		"digest not covered":  {func(r *http.Request, _ *[]byte, _ *Verifier) { swapIn(r, `"content-digest" `, ``) }, "signature_invalid"},
		"no signature at all": {func(r *http.Request, _ *[]byte, _ *Verifier) { r.Header.Del("Signature") }, "signature_invalid"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r, body := f.request(t, `{"lease_id":"L1"}`)
			v := f.verifier(nil)
			tc.mut(r, &body, v)
			if _, err := v.VerifyRequest(r, body); code(err) != tc.want {
				t.Fatalf("got %v, want %s", err, tc.want)
			}
		})
	}
}

func swapIn(r *http.Request, old, new string) {
	r.Header.Set("Signature-Input", strings.Replace(r.Header.Get("Signature-Input"), old, new, 1))
}

func TestResponseBoundToRequest(t *testing.T) {
	f := newFixture(t)
	req, _ := f.request(t, `{"offers":[]}`)
	platform := &Signer{Key: f.platform.Private, KeyID: f.platform.KID, Now: func() time.Time { return f.now }}
	body := []byte(`{"jobs":[]}`)
	h := http.Header{"Content-Type": {"application/json"}}
	if err := platform.SignResponse(h, 200, body, req); err != nil {
		t.Fatal(err)
	}
	resp := &http.Response{StatusCode: 200, Header: h}
	v := f.verifier(nil)
	if _, err := v.VerifyResponse(resp, body, req); err != nil {
		t.Fatalf("valid response rejected: %v", err)
	}
	// A man in the middle that rewrites the job list.
	if _, err := v.VerifyResponse(resp, []byte(`{"jobs":[{"job_id":"evil"}]}`), req); code(err) != "digest_mismatch" {
		t.Fatalf("tampered body: %v", err)
	}
	// Yesterday's response replayed to today's request.
	other, _ := f.request(t, `{"offers":[1]}`)
	if _, err := v.VerifyResponse(resp, body, other); code(err) != "signature_invalid" {
		t.Fatalf("response replayed to another request: %v", err)
	}
	// A response signed by the server's own key (not a pinned platform key) is still verified by key, so the
	// caller must pin only platform keys; here the fixture knows both, so check an unsigned response instead.
	resp2 := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}}
	if _, err := v.VerifyResponse(resp2, body, req); code(err) != "signature_invalid" {
		t.Fatalf("unsigned response: %v", err)
	}
	if !bytes.Contains([]byte(h.Get("Signature-Input")), []byte(`"signature";req;key="sig1"`)) {
		t.Fatal("response does not cover the request signature")
	}
}
