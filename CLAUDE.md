# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**SQE (Sovereign Query Engine)** -- A Rust-based distributed SQL query engine for Apache Iceberg tables. Built on DataFusion + iceberg-rust for querying via Polaris REST Catalog, with OIDC auth passthrough and Ranger / Polaris-grants / in-memory / passthrough policy enforcement.

This repository contains the full engine implementation. Licensed under Apache 2.0.

The shipped long-running binary is `sqe-server` (`--mode coordinator` or `--mode worker`), built from `crates/sqe-server`, which is the only crate depending on both the `sqe-coordinator` and `sqe-worker` libraries. `sqe-worker` is the standalone worker with the same bootstrap. OPA and Cedar were the original design and were not built.

## Architecture

- **Coordinator**: SQL parsing, auth, policy enforcement (plan rewriting), optimization, distributed scheduling. Run as `sqe-server --mode coordinator`.
- **Workers**: Stateless DataFusion executors receiving secured `ScanTask`s (file list + projection + predicate + limit) and user bearer tokens. The executed topology is a **distributed scan**: workers scan and stream Arrow back, and every join, aggregate and sort runs on the coordinator. There is no stage-based shuffle on any query path -- the `DoExchange` shuffle intake was deleted in 2026-09 (it was feature-gated and no build enabled it), and workers answer `DoExchange` with `Unimplemented`. `sqe-planner` holds the scan-task model, morsel planning and five coordinator optimizer rules; its stage/shuffle planner was deleted (#551)
- **Auth model**: No service account -- every query runs as the authenticated user via OIDC password grant -> bearer token passthrough to Polaris/S3
- **Security**: Policy enforcement via LogicalPlan rewriting *before* DataFusion optimization (row filters, column masks, column restriction). Pluggable backend: Ranger, Polaris grants, in-memory, or passthrough

## Key Design Decisions

- **Parser extension strategy**: Wrap sqlparser-rs, don't fork. Standard GRANT/REVOKE parsed normally, then post-parse transform detects `MASKED WITH`/`ROWS WHERE` extensions and converts to custom `PolicyStatement` AST nodes
- **Plan rewriting before optimization**: Security filters injected above TableScan; DataFusion optimizer can push user predicates through row filters but not through masked columns. A predicate that can fail (arithmetic, functions, lossy casts; see `sqe-policy/src/leakproof.rs`) would run on hidden rows and leak them through its error, so when a plan holds one, row-filtered scans are read through `CASE WHEN <row filter> THEN col END` instead. DELETE / UPDATE / MERGE always read the target through the same guard (`policy_projection_sql`, `merge_sql::target_policy_select`), with the row-filter verdict and raw values carried in internal `__pol_` / `__sqe_raw_` aliases
- **No information leakage**: Restricted columns are nullified in place (the name stays in the schema, every value is NULL, never an error), row filters are transparent, masked columns block predicate pushdown on raw values -- follows PostgreSQL RLS model. `information_schema.columns` therefore lists restricted column names; only `SHOW STATS` drops them (#665)
- **Write path**: Merge-on-Read with position deletes first (simpler); compaction added later
- **dbt compatibility**: Native dbt-sqe Python adapter over ADBC Flight SQL (Path A), not Trino compat layer

## Documentation Structure

Design docs use the **openspec** format with three tiers per phase:

- `proposal.md` -- Summary, motivation, what changes, success criteria, rollback strategy
- `design.md` -- Architecture diagrams, Rust trait definitions, data flows, key design decisions
- `tasks.md` -- Numbered task checklist broken into sub-phases
- `specs/` -- GIVEN/WHEN/THEN requirement scenarios per domain (e.g., `sql-extensions/spec.md`, `security-policy/spec.md`)

Key docs:
- `docs/site/book/src/design-notes/datafusion-architecture.md` -- Overall SQE architecture, component breakdown, tech choices, implementation phases
- `docs/internal/process/openspec.md` -- Phase 5 policy SQL extensions (parser, policy store, plan rewriter, coordinator integration)
- `docs/site/book/src/design-notes/dbt-sqe.md` -- Phase 2c dbt compatibility (write path, information_schema, dbt-sqe adapter)

The `docs/` tree splits into three zones:

- `docs/site/` -- published content (book -> docs.getsqe.com; plus ebook, blog, compare). The book at `docs/site/book/` is the canonical reference, and design history is published under `docs/site/book/src/design-notes/`.
- `docs/internal/` -- working history (specs, plans, reviews, audit, prompts, process). Never published.
- `docs/evidence/` -- generated data artifacts (benchmark charts, perf explains, matrix/perf JSON).

Invariant: `docs/site/` must be publish-clean (no secrets or PII). `make leak-scan` enforces it; run it before publishing.

## Roadmap

The live roadmap and its status live in `README.md`; `nextsteps.md` carries the current pointer. Distribution uses a bespoke scheduler; Ballista was evaluated and rejected (see `docs/site/book/src/design-notes/ballista-evaluation-learnings.md`).

## Common Commands

```bash
# Integration tests run locally only; CI has no equivalent job, so a green
# pipeline says nothing about them. Brings up its own Polaris + RustFS stack
# and preflights the fixed ports.
make test-integration                             # full suite
make test-integration FILTER=test_ctas_roundtrip  # one test
make test-distributed                             # coordinator + 2 workers
make test-integration-down                        # tear the stack down
```

## Git Workflow

- **Never push directly to main** -- all changes go through feature branches + merge requests. `origin` is GitLab; use `glab`, not `gh`. The GitHub remote is a read-only mirror.
- Workflow: `git checkout -b feat/<name>` -> commit -> `git push -u origin feat/<name>` -> open a merge request
- No git worktrees -- use simple branches
- Branch naming: `feat/`, `fix/`, `refactor/`, `docs/`, `test/` prefixes
- Keep PRs focused -- one logical change per PR, not mega-branches
- Edit source files with the Edit tool, not sed or Python scripts

## After Completing Work

When finishing a feature, bugfix, or any implementation task, **always update these files** before committing:

1. **`README.md`** -- Update the roadmap checklist (mark items done, add new items)
2. **`nextsteps.md`** -- Update status line, mark completed steps, shift "NEXT" pointer
3. **`openspec/changes/*/tasks.md`** -- Check off completed tasks (`- [ ]` -> `- [x]`)
4. **`benchmarks/results/`** -- Commit benchmark JSON reports for historical tracking (the `benchmarks` skill has the run and compare procedure)

This ensures the project state is always visible to anyone reading the repo.
