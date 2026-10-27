# Bryan_Mac_Studio (BMS): server identity key

Public keys only (pm/security.mdx §5.1). The private key never leaves the Mac Studio.

| Field | Value |
| --- | --- |
| Server key | `BMS` |
| Fingerprint | `SHA256:Pyaz-gkZk-HvFC-kAQd` |
| kid (RFC 7638) | `PyazgkZkHvFCkAQdUN6UzdjSIdX34ZxKkQQvPYg108w` |
| Algorithm | Ed25519 (`EdDSA`) |
| Created | 2026-10-27 by `just build` (`ai_processing_server init`) on the machine |
| Private key | `~/repos/act3ai-credentials/keys/ai_processing_server/BMS/server_identity.ed25519.pem`, mode 0600, git-ignored by that repo's `/keys/` rule, not backed up |
| Approval | the merge of the pull request that adds this file to `main` |

Check it on the machine with `./ai_processing_server keys show`; the fingerprint must match the table above.
