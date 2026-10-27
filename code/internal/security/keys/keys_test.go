package keys

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// RFC 8037 Appendix A.3: the thumbprint of the example Ed25519 key.
func TestThumbprintRFC8037(t *testing.T) {
	x, _ := base64.RawURLEncoding.DecodeString("11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo")
	if got, want := Thumbprint(ed25519.PublicKey(x)), "kPrK_qmxVWaYVA9wwBF6Iuo3vVzz7TxHCTwXBygrS4k"; got != want {
		t.Fatalf("thumbprint %s, want %s", got, want)
	}
}

func TestInitKeyNeverOverwrites(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "keys")
	r1, err := InitKey(dir)
	if err != nil || !r1.Created {
		t.Fatalf("first init: %v created=%v", err, r1.Created)
	}
	path := filepath.Join(dir, PrivateFile)
	st1, _ := os.Stat(path)
	b1, _ := os.ReadFile(path)
	if runtime.GOOS != "windows" && st1.Mode().Perm() != 0o600 {
		t.Fatalf("mode %o, want 600", st1.Mode().Perm())
	}
	time.Sleep(20 * time.Millisecond)
	r2, err := InitKey(dir)
	if err != nil || r2.Created {
		t.Fatalf("second init: %v created=%v", err, r2.Created)
	}
	st2, _ := os.Stat(path)
	b2, _ := os.ReadFile(path)
	if !bytes.Equal(b1, b2) || !st1.ModTime().Equal(st2.ModTime()) {
		t.Fatal("InitKey changed an existing key")
	}
	if r1.Key.KID != r2.Key.KID {
		t.Fatal("kid changed")
	}
	if !strings.HasPrefix(r1.Key.Fingerprint(), "SHA256:") || len(r1.Key.Fingerprint()) != len("SHA256:")+19 {
		t.Fatalf("fingerprint %q", r1.Key.Fingerprint())
	}
}

func TestLoadRefusesWorldReadableKey(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX modes")
	}
	dir := t.TempDir()
	r, _ := InitKey(dir)
	os.Chmod(r.Key.Path, 0o644)
	if _, err := LoadKey(r.Key.Path); err == nil {
		t.Fatal("a 0644 key loaded")
	}
}

func TestJWKSWriteAndPrivateMemberRefused(t *testing.T) {
	dir := t.TempDir()
	r, _ := InitKey(filepath.Join(dir, "k"))
	path := filepath.Join(dir, "pub", PublicFile)
	wrote, err := WritePublicJWKS(path, r.Key, "TEST1", time.Now())
	if err != nil || !wrote {
		t.Fatalf("write: %v %v", err, wrote)
	}
	if wrote, _ := WritePublicJWKS(path, r.Key, "TEST1", time.Now()); wrote {
		t.Fatal("second write changed the file")
	}
	set, err := ReadJWKS(path)
	if err != nil || len(set.Keys) != 1 {
		t.Fatalf("read: %v %+v", err, set)
	}
	pub, err := set.Keys[0].PublicKey()
	if err != nil || !pub.Equal(r.Key.Public) {
		t.Fatalf("public key round trip: %v", err)
	}
	if b, _ := os.ReadFile(path); bytes.Contains(b, []byte(`"d"`)) {
		t.Fatal("public JWKS contains a private member")
	}
	if _, err := ParseJWKS([]byte(`{"keys":[{"kty":"OKP","crv":"Ed25519","x":"AA","d":"secret"}]}`), "x"); err == nil {
		t.Fatal("a JWK with d was accepted")
	}
	bad := set.Keys[0]
	bad.KID = "not-the-thumbprint"
	if _, err := bad.PublicKey(); err == nil {
		t.Fatal("kid mismatch accepted")
	}
}
