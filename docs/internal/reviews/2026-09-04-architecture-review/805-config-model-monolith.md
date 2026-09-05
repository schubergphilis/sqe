# 805: One 9,900-line config file carries 39 config structs, two auth models and the production validator

- Scope: sqe-core (`config.rs`)
- Severity: medium
- Category: maintainability
- Effort: M
- Status: open

## Problem

`crates/sqe-core/src/config.rs` is 9,904 lines (June 2026: 6,030). It holds
39 `pub struct *Config` with about 338 public fields, the TOML loader, the
env override function (see 802), `validate()`, the production-mode rules,
`Debug` impls that redact secrets (`config.rs:1082`), and the doc comments
that are the only operator reference for most knobs.

Two generations of the auth model still coexist in `AuthConfig`:
`config.rs:1872` `pub keycloak_url: String`, `:1875` `pub realm: String`, and
`:1908` `pub providers: Vec<AuthProviderConfig>`. The legacy single-catalog
block coexists with the named map the same way (`config.rs:22` `pub catalog`,
`:30` `pub catalogs: HashMap<..>`), with a doc comment explaining that "a
placeholder REST URL satisfies the deserializer".

Production-mode rules are spread through the file as individual `if
self.coordinator.production_mode && ...` checks (e.g. `config.rs:4733`) and
as doc-comment promises on unrelated structs (`:179`, `:482`, `:2236`,
`:3270`, `:3367`, `:3797-3816`). There is no single list of what production
mode enforces; the Helm `values-production.yaml` reproduces the list by hand.

Validation runs at startup (`crates/sqe-coordinator/src/bin/sqe_server.rs:833`,
`crates/sqe-worker/src/main.rs:47`) but per-subsystem configs also validate
at use (`crates/sqe-spill/src/store_s3.rs:213,229`), so an invalid spill
config is only discovered at the first spill.

## Why it matters

- Every config change is a merge conflict magnet in one file; the file is
  edited by most MRs that add a feature.
- The dual auth and dual catalog models mean two code paths through
  `sqe-auth` and `sqe-catalog` bootstrap, both of which must be tested.
- Production-mode guarantees cannot be audited from one place, which is
  what a security reviewer will ask for.

## Proposed change

1. Convert to a directory module `sqe-core/src/config/` with one file per
   section (`coordinator.rs`, `worker.rs`, `auth.rs`, `catalog.rs`,
   `storage.rs`, `policy.rs`, `metrics.rs`, `maintenance.rs`, `hive.rs`,
   `load.rs`, `validate.rs`, `production.rs`). Pure move first, no
   behaviour change.
2. Collect all production checks into `production.rs` as a table of
   `(name, predicate, message)` and have `validate()` iterate it. Export the
   table to `docs/site/book/src/operations/production-guide.md` via a
   test that compares the doc list to the table.
3. Remove the legacy flat auth fields: at load time, if `keycloak_url` is
   set, synthesize a single `providers` entry and `warn!`; delete the
   fields one release later. Same for `[catalog]` into `catalogs["default"]`.
4. Call every subsystem `validate()` from `SqeConfig::validate()` so
   nothing is deferred to first use.

## Acceptance criteria

- No file under `crates/sqe-core/src/config/` exceeds 1,500 lines.
- `grep -c 'production_mode &&' crates/sqe-core/src/config/` is 0 outside `production.rs`.
- A TOML with an invalid `[spill.s3]` block fails at `sqe-server` startup, not at first spill.
- Loading a TOML with only legacy `keycloak_url` produces one provider and one warning.

## Related

- 802 (env override surface).
- June 2026 codebase analysis, High item 13 and Medium items 15, 16, Improvement 2 (all still open).
- Overlaps 103 (sqe-core view); this file adds the production-rule scattering and deferred subsystem validation.
