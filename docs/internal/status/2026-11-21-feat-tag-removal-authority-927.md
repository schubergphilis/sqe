# 2026-11-21 feat/tag-removal-authority-927

Issue #927 (mirror sqe#19): a steward allowed to apply a tag could also
remove one, which takes a mask off a column. OpenSpec change
`tag-removal-authority`.

## What changed

- `access_control.tag_removal = same | remove_tag | admin` (default `same`,
  the old behaviour). Tag enums moved to `sqe-core/src/config/tags.rs`.
- `TablePrivilege::RemoveTag` (`remove_tag`); Ranger `all`, `alter` and
  `apply_tag` do not imply it.
- Removals gated: column `UNSET TAGS`, `UNSET MASKING POLICY`, raw
  `sqe.column-tags` writes and CREATE OR REPLACE that drop a key (re-checked
  under the commit lock via `TagWriteAuth { changed, removed }`), object
  `UNSET TAGS` (table/schema: `remove_tag`; catalog: admin).
- Authorised removals logged (user, statement, table, columns).
- Quickstart bootstrap adds `remove_tag` to the `hive` servicedef; tutorial
  and `sqe.toml.example` document the setting.

## Gate

- `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --features test-sqlite --test it grant_introspection_gate_test` -> 51 passed
- `cargo test -p sqe-write --lib tag_source_impl` -> 42 passed; ranger_store and config unit tests pass
- `cargo clippy -p sqe-core -p sqe-policy-api -p sqe-policy -p sqe-write -p sqe-coordinator --all-targets -- -D warnings` -> 0

## Left open

- The live Ranger e2e (`make test-access-control`) was not run for this MR.

**NEXT:** #920 (sqe#22), then #919 catalog-scoped tag keys.
