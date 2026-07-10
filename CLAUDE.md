# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**SQE (Sovereign Query Engine)** -- A Rust-based distributed SQL query engine for Apache Iceberg tables. Built on DataFusion + iceberg-rust for querying via Polaris REST Catalog, with OIDC auth passthrough and OPA/Cedar-based fine-grained security.

This repository contains the full engine implementation across 10 crates. Licensed under Apache 2.0.

## Architecture

```
Client (JDBC/Flight SQL) -> Coordinator -> Workers (DataFusion) -> Iceberg (Polaris + S3)
```

- **Coordinator**: SQL parsing, auth, policy enforcement (plan rewriting), optimization, distributed scheduling
- **Workers**: Stateless DataFusion executors receiving secured plan fragments + user bearer tokens
- **Auth model**: No service account -- every query runs as the authenticated user via OIDC password grant -> bearer token passthrough to Polaris/S3
- **Security**: Policy enforcement via LogicalPlan rewriting *before* DataFusion optimization (row filters, column masks, column restriction). Pluggable backend: OPA, Cedar, or passthrough

## Crate Structure

| Crate | Purpose |
|---|---|
| `sqe-core` | Shared types, config, errors |
| `sqe-sql` | Extended SQL parser (sqlparser-rs), custom AST for GRANT/REVOKE/SHOW GRANTS/SHOW EFFECTIVE POLICY |
| `sqe-auth` | OIDC password grant, session manager, JWT validation |
| `sqe-policy` | PolicyEnforcer trait, PolicyStore trait (OPA/Cedar/InMemory), PlanRewriter, policy cache (moka) |
| `sqe-catalog` | Iceberg REST catalog client (wraps iceberg-rust), information_schema virtual providers |
| `sqe-planner` | LogicalPlan -> PhysicalPlan, partition-aware splitting |
| `sqe-coordinator` | Scheduler, Flight SQL server, session management, statement routing |
| `sqe-worker` | Executor, DataFusion runtime, Flight client |
| `sqe-trino-compat` | Optional Trino wire protocol adapter |
| `sqe-metrics` | Prometheus exporter, OTel integration |

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

Invariant: `docs/site/` must be publish-clean (no secrets). A later task adds a leak-scan to enforce it.

## Implementation Phases

1. **Phase 1** -- Single-node: DataFusion + iceberg-rust + OIDC auth + Flight SQL
2. **Phase 2** -- Views, INSERT INTO, manifest caching, audit logging
3. **Phase 2c** -- dbt compatibility: write path (CTAS, MERGE, DELETE), information_schema, dbt-sqe adapter
4. **Phase 3** -- Distributed execution: bespoke scheduler + stateless DataFusion workers (Ballista was evaluated and wound down; see `docs/site/book/src/design-notes/ballista-evaluation-learnings.md`)
5. **Phase 4** -- Production hardening: metrics, benchmarks, Helm, Trino compat
6. **Phase 5** -- Security: OPA/Cedar policy engine, GRANT/REVOKE SQL, column masks, row filters

## Tech Stack

- **Language**: Rust (engine), Python (dbt adapter)
- **Query engine**: Apache DataFusion
- **Distribution**: bespoke scheduler + stateless DataFusion workers over Arrow Flight
- **Table format**: Apache Iceberg v3 via iceberg-rust 0.8.0+
- **Catalog**: Apache Polaris (Iceberg REST)
- **Auth**: OIDC (any provider: Keycloak, Auth0, Okta, etc.)
- **Policy**: OPA (Rego) or Cedar (pluggable)
- **Wire protocol**: Arrow Flight SQL (primary), Trino HTTP (optional compat)
- **Storage**: S3-compatible (AWS S3, Ceph, Garage, R2, etc.)
- **Policy cache**: moka (async TTL cache)
- **Deployment**: Kubernetes (Helm)
- **License**: Apache 2.0

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

- **NEVER push directly to main** -- all changes go through feature branches + pull requests
- Workflow: `git checkout -b feat/<name>` -> commit -> `git push -u origin feat/<name>` -> open a pull request
- No git worktrees -- use simple branches
- Branch naming: `feat/`, `fix/`, `refactor/`, `docs/`, `test/` prefixes
- Keep PRs focused -- one logical change per PR, not mega-branches

## Benchmarks

Benchmark JSON results in `benchmarks/results/` are **committed to the repo** for historical comparison. After running benchmarks (TPC-H, TPC-DS, SSB, etc.):

1. **Commit the JSON report** -- `git add benchmarks/results/*.json` -- these files track performance over time
2. **Compare against baselines** -- Use historical results to detect regressions before merging
3. **Key baselines** to compare against:
   - `tpch-sf1-flight-2026-04-02T14:16:27.json` -- single-node baseline (22/22, 37.5s)
   - `tpch-sf1-flight-2026-04-06T20:57:10.json` -- distributed baseline (22/22, 12.0s, 3.1x faster)
4. **Run benchmarks after performance-sensitive changes** -- spill, scan planning, I/O pipeline, distributed execution

```bash
# Quick smoke test: TPC-H SF1 single-node
BENCH_SCALE=1 ./scripts/benchmark-test.sh tpch

# Iterating on engine code between runs: dev-release profile (release
# opt-level, no LTO, incremental) rebuilds in a fraction of release time.
# Committed baseline numbers must still come from PROFILE=release.
PROFILE=dev-release BENCH_SCALE=1 ./scripts/benchmark-test.sh tpch

# Distributed test: coordinator + 2 workers (heavy; brings up its own stack)
scripts/test.sh scenario distributed
```

## After Completing Work

When finishing a feature, bugfix, or any implementation task, **always update these files** before committing:

1. **`README.md`** -- Update the roadmap checklist (mark items done, add new items)
2. **`nextsteps.md`** -- Update status line, mark completed steps, shift "NEXT" pointer
3. **`openspec/changes/*/tasks.md`** -- Check off completed tasks (`- [ ]` -> `- [x]`)
4. **`benchmarks/results/`** -- Commit benchmark JSON reports for historical tracking

This ensures the project state is always visible to anyone reading the repo.

## Writing Style (Ebook, Blog, Docs)

All publications (ebook chapters in `docs/site/ebook/chapters/`, blog posts in `docs/site/blog/`, and documentation) MUST follow Jacob's voice from `docs/site/ebook/voice.md`. Key rules:

### Forbidden Characters
- **NEVER use emdash** (`—` U+2014). Replace with periods, commas, colons, or restructured sentences.
- **NEVER use endash** (`–` U+2013). Use a hyphen (`-`) or rewrite.
- **NEVER use Unicode arrows** (`→` `←` `▶`). Use `->` in code blocks, "becomes" or "leads to" in prose.

### Forbidden Words and Phrases (AI tells)
- Never: "delve", "leverage", "utilize", "facilitate", "comprehensive", "robust", "cutting-edge", "game-changer", "paradigm shift", "synergy"
- Never: "it's worth noting", "importantly", "notably", "interestingly", "in conclusion"
- Never: "This approach ensures", "This enables", "This allows for"
- Never start a sentence with "This" referring to the previous sentence. Name the subject.

### Forbidden Patterns
- No rhetorical questions as transitions ("But what about X?" "So how does Y work?")
- No trailing summaries that repeat what was just said
- No exclamation marks
- No emoji in prose (OK in terminal output examples)

### Required Style
- **Short sentences carry the weight.** They land the point.
- Longer sentences do the explaining. Three clauses maximum.
- Alternate between short and long. Rhythm matters.
- Paragraphs: 3-5 sentences. Earned single-sentence paragraphs for emphasis.
- Use "we" for team decisions, "I" sparingly (only in ch17 retrospective).
- Present tense for how things work. Past tense for narrative.
- Direct and opinionated. No hedging. State the tradeoff, pick a side.
- Code examples must compile or be clearly marked as pseudocode.

### Before Committing Any Publication
Run this check: `grep -rn '—' docs/site/ebook/chapters/ docs/site/blog/` — must return zero hits in prose (OK in frontmatter `description` fields and inside code/tree-diagram blocks).
