package keys

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// JWK is a public Ed25519 key in RFC 7517 / RFC 8037 form, plus our act3:* members (security.mdx §5.1).
// It has no "d" member, so it can never carry a private key.
type JWK struct {
	Kty       string `json:"kty"`
	Crv       string `json:"crv"`
	X         string `json:"x"`
	KID       string `json:"kid"`
	Use       string `json:"use,omitempty"`
	Alg       string `json:"alg,omitempty"`
	ServerKey string `json:"act3:server_key,omitempty"`
	CreatedAt string `json:"act3:created_at,omitempty"`
	Status    string `json:"act3:status,omitempty"` // current | next | retired
}

// JWKS is a JWK Set.
type JWKS struct {
	Keys []JWK `json:"keys"`
}

// Thumbprint is the RFC 7638 SHA-256 thumbprint of an Ed25519 public key, base64url without padding. The members
// are the required ones (crv, kty, x) in lexicographic order with no whitespace.
func Thumbprint(pub ed25519.PublicKey) string {
	canon := `{"crv":"Ed25519","kty":"OKP","x":"` + base64.RawURLEncoding.EncodeToString(pub) + `"}`
	sum := sha256.Sum256([]byte(canon))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// PublicJWK returns the key's public JWK, marked current.
func (k *Key) PublicJWK(serverKey string, created time.Time) JWK {
	return JWK{Kty: "OKP", Crv: "Ed25519", X: base64.RawURLEncoding.EncodeToString(k.Public), KID: k.KID,
		Use: "sig", Alg: "EdDSA", ServerKey: serverKey, CreatedAt: created.Format(time.RFC3339), Status: "current"}
}

// PublicKey decodes and checks a JWK: Ed25519 only, the kid must be its thumbprint, and a private member is refused.
func (j JWK) PublicKey() (ed25519.PublicKey, error) {
	if j.Kty != "OKP" || j.Crv != "Ed25519" {
		return nil, fmt.Errorf("jwk %s: only OKP / Ed25519 is supported", j.KID)
	}
	b, err := base64.RawURLEncoding.DecodeString(j.X)
	if err != nil || len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("jwk %s: bad x", j.KID)
	}
	pub := ed25519.PublicKey(b)
	if j.KID != "" && j.KID != Thumbprint(pub) {
		return nil, fmt.Errorf("jwk %s: kid is not the RFC 7638 thumbprint of the key", j.KID)
	}
	return pub, nil
}

// ReadJWKS reads a JWK Set file and refuses any key with a private "d" member.
func ReadJWKS(path string) (*JWKS, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseJWKS(b, path)
}

// ParseJWKS parses a JWK Set; name is used in errors.
func ParseJWKS(b []byte, name string) (*JWKS, error) {
	var raw struct {
		Keys []map[string]any `json:"keys"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	for _, k := range raw.Keys {
		if _, ok := k["d"]; ok {
			return nil, fmt.Errorf("%s holds a PRIVATE key (a \"d\" member); remove it and treat that key as exposed", name)
		}
	}
	var set JWKS
	if err := json.Unmarshal(b, &set); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return &set, nil
}

// Find returns the key with kid.
func (s *JWKS) Find(kid string) (JWK, bool) {
	for _, k := range s.Keys {
		if k.KID == kid {
			return k, true
		}
	}
	return JWK{}, false
}

// WritePublicJWKS writes (or updates) path so it lists k as current. A key already listed keeps its entry
// (created_at is not rewritten); other keys in the file are kept as they are, so rotation history survives.
func WritePublicJWKS(path string, k *Key, serverKey string, now time.Time) (bool, error) {
	set := &JWKS{}
	if old, err := ReadJWKS(path); err == nil {
		set = old
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if _, ok := set.Find(k.KID); ok {
		return false, nil
	}
	set.Keys = append(set.Keys, k.PublicJWK(serverKey, now))
	b, err := json.MarshalIndent(set, "", "  ")
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return false, err
	}
	return true, os.Rename(tmp, path)
}
