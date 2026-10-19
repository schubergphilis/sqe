# CLAUDE.md

**SQE (Sovereign Query Engine)** -- a distributed SQL query engine for Apache Iceberg on DataFusion + iceberg-rust, querying via the Polaris REST Catalog. One binary, `sqe-server --mode coordinator|worker`.

Many design notes under `docs/site/book/src/design-notes/` are history, not the current implementation: OPA/Cedar policy engines, a Ballista-derived scheduler and a stage-based shuffle were designed but never shipped or were removed. Trust the code and the book's architecture pages (`docs/site/book/src/architecture/`) over them.

## Architecture and design decisions

- **Distributed scan only**: workers scan and stream Arrow back; every join, aggregate and sort runs on the coordinator. There is no shuffle on any query path; workers answer `DoExchange` with `Unimplemented`
- **No service account**: every query runs as the authenticated user (OIDC password grant -> bearer token passthrough to Polaris/S3)
- **Policy enforcement by LogicalPlan rewriting *before* DataFusion optimization** (row filters, column masks, column restriction). Backends: Ranger, Polaris grants, in-memory, passthrough. The optimizer can push user predicates through row filters but not through masked columns. A predicate that can fail (arithmetic, functions, lossy casts; see `crates/sqe-policy/src/leakproof.rs`) would run on hidden rows and leak them through its error, so when a plan holds one, row-filtered scans are read through `CASE WHEN <row filter> THEN col END` instead. DELETE / UPDATE / MERGE always read the target through the same guard (`policy_projection_sql`, `merge_sql::target_policy_select`), with the row-filter verdict and raw values carried in internal `__pol_` / `__sqe_raw_` aliases
- **No information leakage**: restricted columns are nullified in place (the name stays in the schema, every value is NULL, never an error), row filters are transparent -- PostgreSQL RLS model. `information_schema.columns` therefore lists restricted column names; only `SHOW STATS` drops them (#665)
- **Parser**: wrap sqlparser-rs, don't fork; SQE statements are pre-parsed in `sqe-sql`. Policies are authored with `CREATE [OR REPLACE] POLICY` / `DROP POLICY`, or `GRANT SELECT ... MASKED WITH (...) / ROWS WHERE` (one item per grantee in Ranger's single mask/filter policy per resource; `sqe-sql/src/grant_policy.rs`), and stored in Ranger
- **dbt**: native dbt-sqe adapter over ADBC Flight SQL, not the Trino compat layer

## Docs

- `docs/site/` -- published (book -> docs.getsqe.com, ebook, blog). Must stay publish-clean (no secrets or PII); run `make leak-scan` before publishing
- `docs/internal/` -- working history (specs, plans, reviews, status). Never published
- `docs/evidence/` -- generated artifacts (benchmark charts, perf JSON)

## Build loop

Rebuild time here is dominated by the 24-crate workspace chain, not by rustc: an edit in `sqe-core` recompiles ~15 crates (~1.5–3 min). Keep one feature set and one target dir.

```bash
cargo check-server   # type-check sqe-server only; fastest feedback
cargo dev            # sqe-server + sqe-cli; the same command `make dev` runs
cargo sweep -t 2     # reclaim old artifacts when target/ grows past ~50 GB
```

- **Don't mix feature sets.** `cargo build`, `make dev` (`--no-default-features`, two binaries) and `cargo clippy --all-features` each keep a separate copy of the whole dependency graph. A 2026-10-19 inspection found 86 fingerprints for `sqe-core` and a 128 GB `target/`; alternate between them and every build is a partial cold build.
- **`incremental = false`** in `[profile.dev]`/`[profile.test]`: measured 2× faster (`sqe-core` edit 2m50 → 1m21) and it frees ~21 GB. Use `CARGO_INCREMENTAL=1` for a one-crate loop if you want it back.
- **sccache stays off locally** (`.cargo/config.toml`): it misses on the crates that actually change. It only pays off in CI.
- **One builder per tree.** Concurrent cargo runs (another session, `cargo audit`, a dep bump landing) serialise on the build lock and invalidate fingerprints; a dependency refresh costs one full rebuild per session.

## Integration tests

```bash
# Local only; CI has no equivalent job, so a green pipeline says nothing
# about them. Brings up its own Polaris + RustFS stack on fixed ports.
make test-integration                             # full suite
make test-integration FILTER=test_ctas_roundtrip  # one test
make test-distributed                             # coordinator + 2 workers
make test-integration-down                        # tear the stack down
```

## Git workflow

- **Never push directly to main** -- feature branch + merge request. `origin` is GitLab; use `glab`, not `gh`. The GitHub remote is a read-only mirror
- Branch prefixes: `feat/`, `fix/`, `refactor/`, `docs/`, `test/`. One logical change per MR
- No git worktrees -- use simple branches
- Edit source files with the Edit tool, not sed or Python scripts

## After completing work

Before committing a feature or fix, update:

1. **`README.md`** -- the roadmap checklist (the live roadmap)
2. **`docs/internal/status/YYYY-MM-DD-<branch-slug>.md`** -- new file with a `**NEXT:**` line (template in `docs/internal/status/README.md`). Never edit `nextsteps.md`: it is only a pointer, and a shared line there made every pair of open MRs conflict
3. **`changelog.d/<branch-slug>.md`** -- the changelog entry (format in `changelog.d/README.md`). Never edit `CHANGELOG.md` in an MR; `scripts/release.sh` folds the fragments in
4. **`openspec/changes/*/tasks.md`** -- check off completed tasks
5. **`benchmarks/results/`** -- commit benchmark JSON reports (see the `benchmarks` skill)
