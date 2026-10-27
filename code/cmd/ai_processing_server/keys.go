package main

// The server identity key on the command line (pm/security.mdx §4, §5.1): `init` and `keys init` create it once,
// `keys show` prints only its public half, `run` and `validate` load and check it. Nothing here prints the private key.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/ACT3ai/ai_processing_server/internal/config"
	"github.com/ACT3ai/ai_processing_server/internal/security/keys"
)

// keyDir is security.key_dir, or data/git_ignore/keys when it is not set.
func keyDir(p paths, ts *config.ThisServer) string {
	if ts != nil && ts.Security.KeyDir != "" {
		return config.ExpandHome(ts.Security.KeyDir)
	}
	return filepath.Join(p.State, "keys")
}

// publishDir is servers/{servers_dir}/keys when this server is checked into the repo, else "" (security.mdx §5.1,
// §5.2). servers_dir comes from this_server.yaml, or from known_servers.yaml for files made before the field existed.
func publishDir(p paths, ts *config.ThisServer) string {
	dir := ts.Server.ServersDir
	if dir == "" {
		if cat, err := config.LoadCatalog(p.Data); err == nil {
			for _, k := range cat.Known.Servers {
				if k.Key == ts.Server.Key {
					dir = k.ServersDir
				}
			}
		}
	}
	if dir == "" || dir != filepath.Base(dir) {
		return ""
	}
	servers := filepath.Join(p.Root, "servers", dir)
	if st, err := os.Stat(servers); err != nil || !st.IsDir() {
		return ""
	}
	return filepath.Join(servers, "keys")
}

// loadServerKey loads the private key; the error wraps fs.ErrNotExist when there is none yet.
func loadServerKey(p paths, ts *config.ThisServer) (*keys.Key, error) {
	return keys.LoadKey(filepath.Join(keyDir(p, ts), keys.PrivateFile))
}

// setupKey creates the key if missing (never replacing one) and publishes its public JWK Set beside it and, for a
// checked-in server, in servers/{Server}/keys/. It prints what it did.
func setupKey(p paths, ts *config.ThisServer) error {
	if !config.ValidKey(ts.Server.Key) {
		fmt.Println("server identity key: skipped until server.key is set")
		return nil
	}
	res, err := keys.InitKey(keyDir(p, ts))
	if err != nil {
		return fmt.Errorf("server identity key: %w", err)
	}
	k := res.Key
	verb := "existing"
	if res.Created {
		verb = "created"
	}
	fmt.Printf("server identity key: %s %s (private, mode 0600, stays on this machine)\n", verb, k.Path)
	fmt.Printf("  fingerprint %s  kid %s\n", k.Fingerprint(), k.KID)

	targets := []string{filepath.Join(filepath.Dir(k.Path), keys.PublicFile)}
	pub := publishDir(p, ts)
	if pub != "" {
		targets = append(targets, filepath.Join(pub, keys.PublicFile))
	}
	for i, path := range targets {
		msg, err := publish(path, k, ts.Server.Key)
		if err != nil {
			return fmt.Errorf("public key %s: %w", path, err)
		}
		if i == 1 && msg == "wrote" {
			msg += ": commit it and open a pull request; the merge is the approval (pm/security.mdx §5.1)"
		}
		fmt.Printf("  public key %s: %s\n", rel(p, path), msg)
	}
	if pub == "" {
		fmt.Println("  not checked in (no servers/ directory for " + ts.Server.Key + "): enroll with a one-time code (pm/security.mdx §5.2)")
	}
	return nil
}

// publish writes k into the JWK Set at path, unless that file already lists other keys but not this one: then it
// is left alone, because the machine's key changed and the old one must be retired or revoked by a person (§10).
func publish(path string, k *keys.Key, serverKey string) (string, error) {
	set, err := keys.ReadJWKS(path)
	if err == nil {
		if _, ok := set.Find(k.KID); ok {
			return "up to date", nil
		}
		if len(set.Keys) > 0 {
			return fmt.Sprintf("NOT UPDATED: it lists %d other key(s) but not this machine's %s. If this machine's key "+
				"was replaced, mark the old key retired and revoke it on the platforms (pm/security.mdx §10.4), then "+
				"add this one", len(set.Keys), k.Fingerprint()), nil
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	if _, err := keys.WritePublicJWKS(path, k, serverKey, time.Now()); err != nil {
		return "", err
	}
	return "wrote", nil
}

func rel(p paths, path string) string {
	if r, err := filepath.Rel(p.Root, path); err == nil && filepath.IsLocal(r) {
		return r
	}
	return path
}

// cmdKeys is `ai_processing_server keys init|show`.
func cmdKeys(p paths, args []string) error {
	ts, err := config.Load(p.Config)
	if err != nil {
		return fmt.Errorf("%w (the key's location comes from this_server.yaml; run `just build` first)", err)
	}
	sub := "show"
	if len(args) > 0 {
		sub = args[0]
	}
	switch sub {
	case "init":
		return setupKey(p, ts)
	case "show":
		k, err := loadServerKey(p, ts)
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("no server identity key in %s: run `ai_processing_server keys init`", keyDir(p, ts))
		}
		if err != nil {
			return err
		}
		jwk := k.PublicJWK(ts.Server.Key, time.Now())
		jwk.CreatedAt = "" // not known from the private key alone; the published JWK Set records it
		if p.JSONOut {
			return json.NewEncoder(os.Stdout).Encode(map[string]any{"server_key": ts.Server.Key, "kid": k.KID,
				"fingerprint": k.Fingerprint(), "private_key_file": k.Path, "public_jwk": jwk})
		}
		b, _ := json.MarshalIndent(jwk, "", "  ")
		fmt.Printf("server %s\nfingerprint %s\nkid %s\nprivate key file %s (contents never printed)\n", ts.Server.Key,
			k.Fingerprint(), k.KID, k.Path)
		if pub := publishDir(p, ts); pub != "" {
			fmt.Println("published in", rel(p, filepath.Join(pub, keys.PublicFile)))
		}
		fmt.Printf("public JWK:\n%s\n", b)
		return nil
	default:
		return fmt.Errorf("usage: ai_processing_server keys [init|show]")
	}
}
