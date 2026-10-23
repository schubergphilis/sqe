**STATUS 2026-10-23: aikido gates accept ctutils 0.4.5 (`fix/aikido-ctutils-evidence`).**

ctutils 0.4.5 (RUSTSEC-2026-0385, bumped in !1514) was published on
2026-10-23, before deps.dev indexed it, so `aikido-license-policy` and
`aikido-reputation` failed on main and on every MR.

- License: aikido !241 adds reviewed evidence for `pkg:cargo/ctutils@0.4.*`
  (Apache-2.0 / MIT). This branch bumps the aikido pins once it merges.
- Reputation: `.asset` waives `reputation-unresolved` for `cargo/ctutils`
  until 2026-11-06.

**NEXT:** after aikido !241 merges, bump the 5 include refs and
`AIKIDO_SHARED_REF` to its merge commit; drop the `.asset` waiver once
deps.dev lists ctutils 0.4.5 (the job reports a waiver that matches nothing).
