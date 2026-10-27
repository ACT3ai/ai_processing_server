package httpsig

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"slices"
	"sync"
	"time"
)

// RequestComponents is the profile's covered list for a request (apis.mdx §5.1). Body and idempotency components
// are added only when the request has them.
func RequestComponents(r *http.Request, hasBody bool) []Component {
	c := []Component{C("@method"), C("@authority"), C("@path"), C("@query")}
	if hasBody {
		c = append(c, C("content-digest"), C("content-type"))
	}
	c = append(c, C("x-server-key"))
	if r.Header.Get("Idempotency-Key") != "" {
		c = append(c, C("idempotency-key"))
	}
	return c
}

// ResponseComponents is the platform's covered list for a response (apis.mdx §5.4).
func ResponseComponents(hasBody bool) []Component {
	c := []Component{C("@status")}
	if hasBody {
		c = append(c, C("content-digest"), C("content-type"))
	}
	return append(c, ReqSignature(Label))
}

// Signer signs requests (server side) or responses (platform side, and the fake platform in tests).
type Signer struct {
	Key       ed25519.PrivateKey
	KeyID     string
	Authority string // the platform's public authority, when it differs from the URL host
	Now       func() time.Time
}

func (s *Signer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// SignRequest adds Content-Digest (when body is non-empty), Signature-Input and Signature. A retry must call it
// again: every signature gets a fresh created time and nonce (apis.mdx §4.5).
func (s *Signer) SignRequest(r *http.Request, body []byte) error {
	if len(body) > 0 {
		r.Header.Set("Content-Digest", ContentDigest(body))
	}
	nonce := make([]byte, 16)
	rand.Read(nonce)
	created := s.now().Unix()
	params := Params{
		{Name: "created", Int: created, IsInt: true},
		{Name: "expires", Int: created + 60, IsInt: true},
		{Name: "nonce", Str: base64.RawURLEncoding.EncodeToString(nonce)},
		{Name: "keyid", Str: s.KeyID},
		{Name: "alg", Str: AlgEd25519},
		{Name: "tag", Str: TagRequest},
	}
	in, sig, err := Sign(RequestMessage(r, s.Authority), RequestComponents(r, len(body) > 0), params, s.Key)
	if err != nil {
		return err
	}
	r.Header.Set("Signature-Input", Label+"="+in)
	r.Header.Set("Signature", Label+"="+sig)
	return nil
}

// SignResponse signs a response header set for status and body, bound to the request's signature.
func (s *Signer) SignResponse(h http.Header, status int, body []byte, req *http.Request) error {
	if len(body) > 0 {
		h.Set("Content-Digest", ContentDigest(body))
	}
	params := Params{
		{Name: "created", Int: s.now().Unix(), IsInt: true},
		{Name: "keyid", Str: s.KeyID},
		{Name: "alg", Str: AlgEd25519},
		{Name: "tag", Str: TagResponse},
	}
	m := &Message{Status: status, Header: h, Request: RequestMessage(req, s.Authority)}
	in, sig, err := Sign(m, ResponseComponents(len(body) > 0), params, s.Key)
	if err != nil {
		return err
	}
	h.Set("Signature-Input", Label+"="+in)
	h.Set("Signature", Label+"="+sig)
	return nil
}

// KeyFunc returns the public key for a kid, and the server key it belongs to (empty for platform keys).
type KeyFunc func(kid string) (ed25519.PublicKey, string, error)

// Verifier checks signatures against known public keys.
type Verifier struct {
	Keys      KeyFunc
	Authority string        // configured public authority (requests); empty = the request's Host
	Window    time.Duration // allowed |now − created|; default 60 s for requests, 300 s for responses
	Replay    *ReplayCache  // requests only; nil disables the nonce check
	Now       func() time.Time
}

func (v *Verifier) now() time.Time {
	if v.Now != nil {
		return v.Now()
	}
	return time.Now()
}

// Verified describes an accepted signature.
type Verified struct {
	KeyID     string
	ServerKey string
	Nonce     string
	Covered   []Component
}

// VerifyRequest is the platform algorithm of apis.mdx §5.3 (steps 1–11). body is the exact bytes received.
func (v *Verifier) VerifyRequest(r *http.Request, body []byte) (*Verified, error) {
	return v.verify(RequestMessage(r, v.Authority), body, TagRequest, true, RequestComponents(r, len(body) > 0), 60*time.Second,
		r.Header.Get("X-Server-Key"))
}

// VerifyResponse checks a platform response against pinned keys and binds it to req (apis.mdx §5.4).
func (v *Verifier) VerifyResponse(resp *http.Response, body []byte, req *http.Request) (*Verified, error) {
	m := &Message{Status: resp.StatusCode, Header: resp.Header, Request: RequestMessage(req, v.Authority)}
	return v.verify(m, body, TagResponse, false, ResponseComponents(len(body) > 0), 300*time.Second, "")
}

func (v *Verifier) verify(m *Message, body []byte, tag string, isRequest bool, required []Component, window time.Duration, serverKey string) (*Verified, error) {
	rawIn, ok := dictMember(m.Header.Get("Signature-Input"), Label)
	if !ok {
		return nil, errf("signature_invalid", "no signature labelled %s", Label)
	}
	rawSig, ok := dictMember(m.Header.Get("Signature"), Label)
	if !ok {
		return nil, errf("signature_invalid", "Signature header has no %s", Label)
	}
	comps, params, err := parseInput(rawIn)
	if err != nil {
		return nil, err
	}
	if t, _ := params.Get("tag"); t != tag {
		return nil, errf("signature_invalid", "tag %q, want %q", t, tag)
	}
	if a, _ := params.Get("alg"); a != AlgEd25519 {
		return nil, errf("signature_invalid", "alg %q is not allowed", a)
	}
	created, okC := params.GetInt("created")
	kid, okK := params.Get("keyid")
	nonce, okN := params.Get("nonce")
	expires, okE := params.GetInt("expires")
	if !okC || !okK || (isRequest && (!okN || !okE)) {
		return nil, errf("signature_invalid", "created, keyid (and on requests expires, nonce) are required")
	}
	if v.Window > 0 {
		window = v.Window
	}
	now := v.now()
	if d := now.Sub(time.Unix(created, 0)); d > window || d < -window || (okE && !now.Before(time.Unix(expires, 0))) {
		return nil, errf("signature_expired", "created %d is outside the %s window", created, window)
	}
	for _, c := range required {
		if !slices.Contains(comps, c) {
			return nil, errf("signature_invalid", "component %s must be covered", c)
		}
	}
	pub, owner, err := v.Keys(kid)
	if err != nil || pub == nil {
		return nil, errf("key_unknown", "key %s is not known", kid)
	}
	if isRequest && owner != serverKey {
		return nil, errf("signature_invalid", "key does not belong to X-Server-Key")
	}
	base, err := SignatureBase(m, comps, params)
	if err != nil {
		return nil, err
	}
	sig, err := byteSeq(rawSig)
	if err != nil || !ed25519.Verify(pub, []byte(base), sig) {
		return nil, errf("signature_invalid", "signature verification failed")
	}
	if len(body) > 0 {
		if err := CheckContentDigest(m.Header.Get("Content-Digest"), body); err != nil {
			return nil, err
		}
	}
	if isRequest && v.Replay != nil && !v.Replay.Add(kid+"\x00"+nonce, now) {
		return nil, errf("replay", "nonce already used")
	}
	return &Verified{KeyID: kid, ServerKey: owner, Nonce: nonce, Covered: comps}, nil
}

// ReplayCache remembers (kid, nonce) pairs for 5 minutes (apis.mdx §5.3 step 11). In memory: enough for the fake
// platform in tests and for a single-process WTC backend; ACT3 uses Redis.
type ReplayCache struct {
	mu   sync.Mutex
	seen map[string]time.Time
	TTL  time.Duration
}

func NewReplayCache() *ReplayCache {
	return &ReplayCache{seen: map[string]time.Time{}, TTL: 5 * time.Minute}
}

// Add records id and reports false if it was already present.
func (c *ReplayCache) Add(id string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.seen) > 10000 {
		for k, t := range c.seen {
			if now.Sub(t) > c.TTL {
				delete(c.seen, k)
			}
		}
	}
	if t, ok := c.seen[id]; ok && now.Sub(t) <= c.TTL {
		return false
	}
	c.seen[id] = now
	return true
}
