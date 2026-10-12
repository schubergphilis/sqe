**STATUS 2026-09-10 (e): `deny.toml` gets a `[licenses]` allow list; the ~600 `error[rejected]` lines per CI test job are gone (`ci/deny-license-allowlist`).**

The aikido test gate runs `cargo deny check` (non-blocking) when a
`deny.toml` exists. Ours had only `[advisories]`, and cargo-deny 0.20 with
no `[licenses]` section rejects every license as "not explicitly allowed":
`licenses FAILED` with about 600 rejections on every job this week (!977
622, !980 617, !982 605, !983 620), including `MIT OR Apache-2.0`. Pure
noise that would also have hidden a real finding. The allow list is the
intersection of the platform policy (`aikido/policy/license-policy.json`,
what the blocking SBOM job enforces) with the sixteen expressions the
graph carries (`cargo deny --all-features list -l license`); `r-efi`'s
`MIT OR Apache-2.0 OR LGPL-2.1-or-later` passes on its MIT branch.
Local `cargo deny --all-features check licenses`: `licenses ok`. Also
today: three CI-timing flakes fixed (!984 config env lock, !985 hive TTL
reload + streaming idle-timeout margins), none related to the sqlite-rs
or ring work.
