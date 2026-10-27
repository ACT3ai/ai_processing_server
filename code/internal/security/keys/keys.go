// Package keys owns the server identity key: one Ed25519 key pair per server, generated on the machine, the private
// half never leaving it (pm/security.mdx §4). InitKey is the only function that creates the private key file, and
// it never overwrites one. The public half is published as a JWK Set (§5, jwks.go).
package keys

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

// File names inside the key directory (security.mdx §4.2).
const (
	PrivateFile = "server_identity.ed25519.pem"
	PublicFile  = "server_identity.jwks.json"
)

// Key is a loaded server identity key. The private key stays in memory only; nothing in this package prints it.
type Key struct {
	Private ed25519.PrivateKey
	Public  ed25519.PublicKey
	KID     string // RFC 7638 thumbprint, base64url
	Path    string
}

// Fingerprint is the human form of the kid: "SHA256:" plus the first 16 characters in groups of four.
func (k *Key) Fingerprint() string { return Fingerprint(k.KID) }

// Fingerprint formats a kid for people to compare over the phone (security.mdx §0 vocabulary).
func Fingerprint(kid string) string {
	s := kid
	if len(s) > 16 {
		s = s[:16]
	}
	out := "SHA256:"
	for i := 0; i < len(s); i += 4 {
		if i > 0 {
			out += "-"
		}
		out += s[i:min(i+4, len(s))]
	}
	return out
}

// InitResult says what InitKey did.
type InitResult struct {
	Created bool
	Key     *Key
}

// InitKey creates the private key in dir when none exists (mode 0600 in a 0700 directory, written atomically and
// create-only) and otherwise only loads and checks the existing one. It never replaces a key.
func InitKey(dir string) (InitResult, error) {
	path := filepath.Join(dir, PrivateFile)
	if _, err := os.Stat(path); err == nil {
		k, err := LoadKey(path)
		return InitResult{Key: k}, err
	} else if !errors.Is(err, fs.ErrNotExist) {
		return InitResult{}, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return InitResult{}, err
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return InitResult{}, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return InitResult{}, err
	}
	b := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := writeCreateOnly(path, b); err != nil {
		return InitResult{}, err
	}
	return InitResult{Created: true, Key: &Key{Private: priv, Public: pub, KID: Thumbprint(pub), Path: path}}, nil
}

// LoadKey reads a PKCS #8 PEM Ed25519 private key and refuses one that other users can read (security.mdx §4.2).
func LoadKey(path string) (*Key, error) {
	if err := CheckPermissions(path); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParsePrivatePEM(b, path)
}

// ParsePrivatePEM parses a PKCS #8 PEM Ed25519 private key; name is used in errors.
func ParsePrivatePEM(b []byte, name string) (*Key, error) {
	blk, _ := pem.Decode(b)
	if blk == nil || blk.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("%s: not a PKCS #8 PEM private key", name)
	}
	k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	priv, ok := k.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s: not an Ed25519 key", name)
	}
	pub := priv.Public().(ed25519.PublicKey)
	return &Key{Private: priv, Public: pub, KID: Thumbprint(pub), Path: name}, nil
}

// CheckPermissions refuses a key file readable by group or others (the OpenSSH rule). Windows ACLs are checked by
// the owner for now (security.mdx §4.2 lists the ACL check as to do).
func CheckPermissions(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if runtime.GOOS != "windows" && st.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%s is readable by other users (mode %o); run chmod 600 on it", path, st.Mode().Perm())
	}
	return nil
}

// writeCreateOnly writes path atomically with mode 0600 and fails if it already exists, even in a race.
func writeCreateOnly(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(b); err == nil {
		err = f.Sync()
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Link(tmp, path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("%s appeared while the key was being created; not overwriting it", path)
		}
		return err
	}
	return nil
}
