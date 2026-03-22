# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**SQE (Sovereign Query Engine)** — A Rust-based distributed SQL query engine replacing patched Trino (DCAF branch). Built on DataFusion + iceberg-rust for querying Apache Iceberg tables via Polaris REST Catalog, with OIDC auth passthrough and OPA/Cedar-based fine-grained security.

This repository contains the full engine implementation across 10 crates.

## Architecture

```
Client (JDBC/Flight SQL) → Coordinator → Workers (DataFusion) → Iceberg (Polaris + S3)
```

- **Coordinator**: SQL parsing, auth, policy enforcement (plan rewriting), optimization, distributed scheduling
- **Workers**: Stateless DataFusion executors receiving secured plan fragments + user bearer tokens
- **Auth model**: No service account — every query runs as the authenticated user via OIDC password grant → bearer token passthrough to Polaris/S3
- **Security**: Policy enforcement via LogicalPlan rewriting *before* DataFusion optimization (row filters, column masks, column restriction). Pluggable backend: OPA, Cedar, or passthrough

## Planned Crate Structure

| Crate | Purpose |
|---|---|
| `sqe-core` | Shared types, config, errors |
| `sqe-sql` | Extended SQL parser (sqlparser-rs), custom AST for GRANT/REVOKE/SHOW GRANTS/SHOW EFFECTIVE POLICY |
| `sqe-auth` | OIDC password grant, session manager, JWT validation |
| `sqe-policy` | PolicyEnforcer trait, PolicyStore trait (OPA/Cedar/InMemory), PlanRewriter, policy cache (moka) |
| `sqe-catalog` | Iceberg REST catalog client (wraps iceberg-rust), information_schema virtual providers |
| `sqe-planner` | LogicalPlan → PhysicalPlan, partition-aware splitting |
| `sqe-coordinator` | Scheduler, Flight SQL server, session management, statement routing |
| `sqe-worker` | Executor, DataFusion runtime, Flight client |
| `sqe-trino-compat` | Optional Trino wire protocol adapter |
| `sqe-metrics` | Prometheus exporter, OTel integration |

## Key Design Decisions

- **Parser extension strategy**: Wrap sqlparser-rs, don't fork. Standard GRANT/REVOKE parsed normally, then post-parse transform detects `MASKED WITH`/`ROWS WHERE` extensions and converts to custom `PolicyStatement` AST nodes
- **Plan rewriting before optimization**: Security filters injected above TableScan; DataFusion optimizer can push user predicates through row filters but not through masked columns
- **No information leakage**: Denied columns are invisible (not errors), row filters are transparent, masked columns block predicate pushdown on raw values — follows PostgreSQL RLS model
- **Write path**: Merge-on-Read with position deletes first (simpler); compaction added later
- **dbt compatibility**: Native dbt-sqe Python adapter over ADBC Flight SQL (Path A), not Trino compat layer

## Documentation Structure

Design docs use the **openspec** format with three tiers per phase:

- `proposal.md` — Summary, motivation, what changes, success criteria, rollback strategy
- `design.md` — Architecture diagrams, Rust trait definitions, data flows, key design decisions
- `tasks.md` — Numbered task checklist broken into sub-phases
- `specs/` — GIVEN/WHEN/THEN requirement scenarios per domain (e.g., `sql-extensions/spec.md`, `security-policy/spec.md`)

Key docs:
- `docs/datafusion-architecture.md` — Overall SQE architecture, component breakdown, tech choices, implementation phases
- `docs/openspec.md` — Phase 5 policy SQL extensions (parser, policy store, plan rewriter, coordinator integration)
- `docs/dbt-sqe.md` — Phase 2c dbt compatibility (write path, information_schema, dbt-sqe adapter)
- `docs/polciy-extend.md` — Duplicate of openspec.md (same content)

## Implementation Phases

1. **Phase 1** — Single-node: DataFusion + iceberg-rust + OIDC auth + Flight SQL
2. **Phase 2** — Views, INSERT INTO, manifest caching, audit logging
3. **Phase 2c** — dbt compatibility: write path (CTAS, MERGE, DELETE), information_schema, dbt-sqe adapter
4. **Phase 3** — Distributed execution: Ballista-derived scheduler + workers
5. **Phase 4** — Production hardening: metrics, benchmarks, Helm, Trino compat
6. **Phase 5** — Security: OPA/Cedar policy engine, GRANT/REVOKE SQL, column masks, row filters

## Tech Stack

- **Language**: Rust (engine), Python (dbt adapter)
- **Query engine**: Apache DataFusion
- **Distribution**: Ballista (forked)
- **Table format**: Apache Iceberg v3 via iceberg-rust 0.8.0+
- **Catalog**: Apache Polaris (Iceberg REST)
- **Auth**: OIDC (any provider: Keycloak, Auth0, Okta, etc.)
- **Policy**: OPA (Rego) or Cedar (pluggable)
- **Wire protocol**: Arrow Flight SQL (primary), Trino HTTP (optional compat)
- **Storage**: S3-compatible (AWS S3, Ceph, Garage, R2, etc.)
- **Policy cache**: moka (async TTL cache)
- **Deployment**: Kubernetes (Helm)

## Common Commands

```bash
# Build
cargo build --all

# Unit tests
cargo test --all

# Clippy (strict)
cargo clippy --all-targets --all-features -- -D warnings

# Integration tests (requires running quickstart stack: Polaris + S3-compatible storage)
scripts/integration-test.sh

# Security advisory scan
cargo audit
```

## Git Workflow

- All changes go through branches + GitLab MRs (never push directly to main)
- Remote: `origin` = GitLab (`sbp.gitlab.schubergphilis.com`)
- Use `glab` CLI for MR creation
