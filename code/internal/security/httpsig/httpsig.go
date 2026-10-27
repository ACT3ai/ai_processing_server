// Package httpsig implements the RFC 9421 HTTP Message Signatures profile of pm/apis.mdx §5, with RFC 9530
// Content-Digest. The server signs every request to a platform with its Ed25519 key and verifies the platform's
// signature on every response, bound to the request's own signature through the "signature";req component.
//
// This is the small in-repo implementation security.mdx §3.4 allows: only what the profile uses (derived
// components @method @authority @path @query @status, header fields, ;req and ;key parameters, Ed25519), tested
// against the RFC 9421 Appendix B.2.6 vector.
package httpsig

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
)

// Profile constants (apis.mdx §5.1, §5.4).
const (
	Label       = "sig1"
	TagRequest  = "act3-processing-v1"
	TagResponse = "act3-processing-v1-resp"
	AlgEd25519  = "ed25519"
)

// Error is a verification failure with the stable code of apis.mdx §12.
type Error struct {
	Code   string // signature_invalid | signature_expired | key_unknown | replay | digest_mismatch
	Detail string
}

func (e *Error) Error() string { return e.Code + ": " + e.Detail }

func errf(code, format string, a ...any) *Error {
	return &Error{Code: code, Detail: fmt.Sprintf(format, a...)}
}

// Component is one covered component: a derived name (@method), a header field (content-digest), or
// "signature";req;key="sig1" (the request's signature, covered by a response).
type Component struct {
	Name string
	Req  bool
	Key  string
}

// C is shorthand for a plain component.
func C(name string) Component { return Component{Name: name} }

// ReqSignature is the component that binds a response to the request's signature labelled label.
func ReqSignature(label string) Component { return Component{Name: "signature", Req: true, Key: label} }

func (c Component) String() string {
	s := strconv.Quote(c.Name)
	if c.Req {
		s += ";req"
	}
	if c.Key != "" {
		s += ";key=" + strconv.Quote(c.Key)
	}
	return s
}

// Param is one signature parameter (created, expires, nonce, keyid, alg, tag), kept in order.
type Param struct {
	Name  string
	Str   string
	Int   int64
	IsInt bool
}

// Params is an ordered parameter list.
type Params []Param

func (p Params) String() string {
	var b strings.Builder
	for _, x := range p {
		b.WriteString(";" + x.Name)
		if x.IsInt {
			b.WriteString("=" + strconv.FormatInt(x.Int, 10))
		} else {
			b.WriteString("=" + strconv.Quote(x.Str))
		}
	}
	return b.String()
}

// Get returns a string parameter.
func (p Params) Get(name string) (string, bool) {
	for _, x := range p {
		if x.Name == name && !x.IsInt {
			return x.Str, true
		}
	}
	return "", false
}

// GetInt returns an integer parameter.
func (p Params) GetInt(name string) (int64, bool) {
	for _, x := range p {
		if x.Name == name && x.IsInt {
			return x.Int, true
		}
	}
	return 0, false
}

// Message is the part of a request or response that components are read from.
type Message struct {
	Method    string
	Authority string
	Path      string
	Query     string // without the leading "?"
	Status    int    // responses only
	Header    http.Header
	Request   *Message // for ;req components on a response
}

// RequestMessage builds a Message from a request. authority overrides the Host (the platform compares with its
// configured public hostname, not the proxied Host, apis.mdx §5.3 step 8).
func RequestMessage(r *http.Request, authority string) *Message {
	if authority == "" {
		authority = r.Host
		if authority == "" {
			authority = r.URL.Host
		}
	}
	path := r.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	return &Message{Method: r.Method, Authority: normAuthority(authority, r.URL.Scheme), Path: path, Query: r.URL.RawQuery, Header: r.Header}
}

// normAuthority lowercases the host and drops a default port (RFC 9421 §2.2.3).
func normAuthority(a, scheme string) string {
	a = strings.ToLower(a)
	if h, p, err := net.SplitHostPort(a); err == nil && ((p == "443" && scheme != "http") || (p == "80" && scheme == "http")) {
		return h
	}
	return a
}

func (m *Message) value(c Component) (string, error) {
	if c.Req {
		if m.Request == nil {
			return "", errf("signature_invalid", "component %s needs the request", c)
		}
		if c.Name == "signature" && c.Key != "" {
			v, ok := dictMember(m.Request.Header.Get("Signature"), c.Key)
			if !ok {
				return "", errf("signature_invalid", "request has no signature %q", c.Key)
			}
			return v, nil
		}
		return m.Request.value(Component{Name: c.Name})
	}
	switch c.Name {
	case "@method":
		return strings.ToUpper(m.Method), nil
	case "@authority":
		return m.Authority, nil
	case "@path":
		return m.Path, nil
	case "@query":
		return "?" + m.Query, nil
	case "@status":
		if m.Status == 0 {
			return "", errf("signature_invalid", "@status is only defined on a response")
		}
		return strconv.Itoa(m.Status), nil
	}
	if strings.HasPrefix(c.Name, "@") {
		return "", errf("signature_invalid", "derived component %s is not in the profile", c.Name)
	}
	vals := m.Header.Values(c.Name)
	if len(vals) == 0 {
		return "", errf("signature_invalid", "covered header %s is missing", c.Name)
	}
	for i := range vals {
		vals[i] = strings.TrimSpace(vals[i])
	}
	return strings.Join(vals, ", "), nil
}

// SignatureBase builds the exact bytes that are signed (RFC 9421 §2.5).
func SignatureBase(m *Message, comps []Component, params Params) (string, error) {
	var b strings.Builder
	ids := make([]string, len(comps))
	for i, c := range comps {
		v, err := m.value(c)
		if err != nil {
			return "", err
		}
		if strings.ContainsAny(v, "\r\n") {
			return "", errf("signature_invalid", "component %s contains a newline", c)
		}
		ids[i] = c.String()
		fmt.Fprintf(&b, "%s: %s\n", ids[i], v)
	}
	fmt.Fprintf(&b, "\"@signature-params\": %s", SignatureParams(comps, params))
	return b.String(), nil
}

// SignatureParams is the inner list plus parameters, the value of the Signature-Input member.
func SignatureParams(comps []Component, params Params) string {
	ids := make([]string, len(comps))
	for i, c := range comps {
		ids[i] = c.String()
	}
	return "(" + strings.Join(ids, " ") + ")" + params.String()
}

// Sign signs m and returns the Signature-Input and Signature member values (without the label).
func Sign(m *Message, comps []Component, params Params, key ed25519.PrivateKey) (input, sig string, err error) {
	base, err := SignatureBase(m, comps, params)
	if err != nil {
		return "", "", err
	}
	return SignatureParams(comps, params), ":" + base64.StdEncoding.EncodeToString(ed25519.Sign(key, []byte(base))) + ":", nil
}

// ContentDigest returns the RFC 9530 Content-Digest header value for body (sha-256).
func ContentDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha-256=:" + base64.StdEncoding.EncodeToString(sum[:]) + ":"
}

// CheckContentDigest verifies a Content-Digest header (sha-256 or sha-512) against body.
func CheckContentDigest(header string, body []byte) error {
	if header == "" {
		return errf("digest_mismatch", "Content-Digest is missing")
	}
	for _, member := range splitTop(header, ',') {
		name, val, ok := strings.Cut(strings.TrimSpace(member), "=")
		if !ok {
			continue
		}
		want, err := byteSeq(val)
		if err != nil {
			continue
		}
		var got []byte
		switch name {
		case "sha-256":
			s := sha256.Sum256(body)
			got = s[:]
		case "sha-512":
			s := sha512.Sum512(body)
			got = s[:]
		default:
			continue
		}
		if subtle.ConstantTimeCompare(got, want) != 1 {
			return errf("digest_mismatch", "Content-Digest %s does not match the body", name)
		}
		return nil
	}
	return errf("digest_mismatch", "Content-Digest has no sha-256 or sha-512 member")
}
