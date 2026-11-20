**STATUS 2026-11-20: `aikido-sast` red on the catalog-scope test key (`fix/sast-test-key`).**

The v0.55.2 tag pipeline failed `aikido-sast` on one high finding,
`detected-private-key` at `crates/sqe-coordinator/tests/it/catalog_scope_e2e.rs:37`
(added with #908). The key is the published ES256 test pair the bearer
provider's unit tests also use. A `nosemgrep` with the full rule id now sits on
the line before the match.

**NEXT:** confirm `aikido-sast` is green on this MR's pipeline.
