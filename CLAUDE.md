# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**SQE (Sovereign Query Engine)** -- A Rust-based distributed SQL query engine for Apache Iceberg tables. Built on DataFusion + iceberg-rust for querying via Polaris REST Catalog, with OIDC auth passthrough and Ranger / Polaris-grants / in-memory / passthrough policy enforcement.

This repository contains the full engine implementation. Licensed under Apache 2.0.

The shipped long-running binary is `sqe-server` (`--mode coordinator` or `--mode worker`). `sqe-worker` is the standalone worker with the same bootstrap. OPA and Cedar were the original design and were not built.

## Architecture

```
Client (Flight SQL, Trino HTTP, MCP, Quack/DuckDB RPC, web UI)
  -> sqe-server (coordinator)
  -> Workers (DataFusion)
  -> Iceberg (Polaris + S3)
```

- **Coordinator**: SQL parsing, auth, policy enforcement (plan rewriting), optimization, distributed scheduling. Run as `sqe-server --mode coordinator`.
- **Workers**: Stateless DataFusion executors receiving secured plan fragments + user bearer tokens
- **Auth model**: No service account -- every query runs as the authenticated user via OIDC password grant -> bearer token passthrough to Polaris/S3
- **Security**: Policy enforcement via LogicalPlan rewriting *before* DataFusion optimization (row filters, column masks, column restriction). Pluggable backend: Ranger, Polaris grants, in-memory, or passthrough

## Crate Structure

| Crate | Purpose |
|---|---|
| `sqe-core` | Shared types, config, errors |
| `sqe-sql` | SQL parser, statement classifier, GRANT/REVOKE |
| `sqe-auth` | Pluggable auth chain (10 providers), token cache |
| `sqe-catalog` | Iceberg REST client, caching, scan execution |
| `sqe-policy` | Policy enforcement (passthrough, in-memory, Apache Ranger) |
| `sqe-planner` | Plan splitting, star-schema reorder, join strategy |
| `sqe-coordinator` | Flight SQL server, query handler, Trino HTTP (`sqe-server` binary) |
| `sqe-worker` | Stateless DataFusion executor |
| `sqe-cli` | Interactive SQL client (cluster + embedded modes) |
| `sqe-metrics` | Prometheus, OpenTelemetry, audit logger |
| `sqe-lineage` | OpenLineage 2-0-2 emitter; column-level lineage |
| `sqe-trino-compat` | Trino wire protocol |
| `sqe-trino-functions` | Trino-compatible scalar UDFs for DataFusion |
| `sqe-mcp` | Model Context Protocol endpoint (OAuth resource server, tools) |
| `sqe-compaction` | Compaction / rewrite-data-files |
| `sqe-spill` | Spill substrate |
| `sqe-quack-wire` | Pure-Rust port of DuckDB's BinarySerializer (Quack RPC) |
| `sqe-quack-server` | Quack RPC server; accepts DuckDB clients over HTTP |
| `sqe-quack-client` | Client side of the DuckDB Quack RPC |
| `sqe-bench` | Benchmark suite |

## Key Design Decisions

- **Parser extension strategy**: Wrap sqlparser-rs, don't fork. Standard GRANT/REVOKE parsed normally, then post-parse transform detects `MASKED WITH`/`ROWS WHERE` extensions and converts to custom `PolicyStatement` AST nodes
- **Plan rewriting before optimization**: Security filters injected above TableScan; DataFusion optimizer can push user predicates through row filters but not through masked columns
- **No information leakage**: Denied columns are invisible (not errors), row filters are transparent, masked columns block predicate pushdown on raw values -- follows PostgreSQL RLS model
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

1. **Phase 1** -- Single-node: DataFusion + iceberg-rust + OIDC auth + Flight SQL
2. **Phase 2** -- Views, INSERT INTO, manifest caching, audit logging
3. **Phase 2c** -- dbt compatibility: write path (CTAS, MERGE, DELETE), information_schema, dbt-sqe adapter
4. **Phase 3** -- Distributed execution: bespoke scheduler + stateless DataFusion workers (Ballista was evaluated and wound down; see `docs/site/book/src/design-notes/ballista-evaluation-learnings.md`)
5. **Phase 4** -- Production hardening: metrics, benchmarks, Helm, Trino compat
6. **Phase 5** -- Security: Ranger / Polaris grants / in-memory policy engine, GRANT/REVOKE SQL, column masks, row filters

## Tech Stack

- **Language**: Rust (engine), Python (dbt adapter)
- **Query engine**: Apache DataFusion
- **Distribution**: bespoke scheduler + stateless DataFusion workers over Arrow Flight
- **Table format**: Apache Iceberg v3 via iceberg-rust 0.8.0+
- **Catalog**: Apache Polaris (Iceberg REST)
- **Auth**: OIDC (any provider: Keycloak, Auth0, Okta, etc.)
- **Policy**: Apache Ranger, Polaris grants, in-memory, or passthrough (OPA/Cedar were designed, not built)
- **Wire protocol**: Arrow Flight SQL (primary), Trino HTTP, MCP, Quack/DuckDB RPC, web UI
- **Storage**: S3-compatible (AWS S3, Ceph, Garage, R2, etc.)
- **Policy cache**: moka (async TTL cache)
- **Deployment**: Kubernetes (Helm)
- **License**: Apache 2.0

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

## After Completing Work

When finishing a feature, bugfix, or any implementation task, **always update these files** before committing:

1. **`README.md`** -- Update the roadmap checklist (mark items done, add new items)
2. **`nextsteps.md`** -- Update status line, mark completed steps, shift "NEXT" pointer
3. **`openspec/changes/*/tasks.md`** -- Check off completed tasks (`- [ ]` -> `- [x]`)
4. **`benchmarks/results/`** -- Commit benchmark JSON reports for historical tracking (the `benchmarks` skill has the run and compare procedure)

This ensures the project state is always visible to anyone reading the repo.

