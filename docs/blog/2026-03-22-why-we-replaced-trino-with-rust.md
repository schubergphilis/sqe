---
title: "We Replaced Our Trino Fork with a Rust SQL Engine"
description: "How we went from maintaining a 2M-line Java fork to shipping a 50MB binary that runs every query as the authenticated user."
pubDate: "2026-03-22"
author: "Jacob Verhoeks"
tags:
  - "rust"
  - "datafusion"
  - "trino"
  - "architecture"
---



*How we went from maintaining a 2M-line Java fork to shipping a 50MB binary that runs every query as the authenticated user.*

---

## The problem we were solving

We run a sovereign data platform. Every customer gets their own Iceberg lakehouse on S3, managed through Apache Polaris as the REST catalog, with Keycloak handling identity. The architecture is clean. Until you need a query engine.

Trino was the obvious choice. Battle-tested, speaks SQL, has an Iceberg connector. But Trino was designed for a world where a service account connects to everything on behalf of users. Our world is different. Every query must run as the authenticated user, with their bearer token passed through to the catalog and storage layer. No god-mode service account. No shared credentials.

So we forked Trino. We built a custom Keycloak SPI, patched the Iceberg connector to pass bearer tokens, and maintained what we called the "DCAF branch." It worked. But every Trino release meant rebasing our patches, re-running compatibility tests, and hoping nothing broke in the 2 million lines of Java we didn't write.

Then came the questions we couldn't answer with patches:

- **How do we inject row-level security filters into query plans before optimisation?** Trino's access control is a gate. It either allows or denies. We needed plan rewriting: transparent row filters, column masking, predicate pushdown that respects security boundaries.
- **How do we run this in Kubernetes without 8GB per worker?** Trino's JVM overhead means large heaps, stop-the-world GC pauses, and 10-30 second cold starts. Not great for autoscaling.
- **How do we stop maintaining someone else's codebase?** Every quarter, the rebase got harder. Features we didn't use created merge conflicts with features we did.

We needed to stop patching and start building.

---

## Why DataFusion

We evaluated DataFusion, DuckDB, and Velox. DataFusion won for one reason: it gives you the query plan as a first-class data structure you can inspect, rewrite, and extend before the optimiser touches it.

This is exactly what we needed for security. Our architecture injects policy filters (row filters, column masks) into the `LogicalPlan` between parsing and optimisation. DataFusion's optimizer then pushes user predicates through our security filters where safe, but can never push predicates through masked columns. The PostgreSQL RLS model, implemented at the query engine level.

DataFusion also gave us:

- **Arrow-native execution.** Columnar data, zero-copy where possible, no serialisation between plan nodes. The same Arrow RecordBatches flow from Iceberg through the engine to the client.
- **Extensible physical plans.** Custom `ExecutionPlan` nodes for Iceberg scans, distributed fragment shipping, and credential injection. All within DataFusion's framework, not fighting it.
- **A Rust ecosystem.** One language from query parsing to S3 I/O. No JNI bridges, no serialisation boundaries between Java and native code.

The key insight: DataFusion is a library, not a server. We compose the query engine we need rather than configuring the one someone else built.

---

## The architecture: bearer token passthrough

This is the design that made everything click.

```
Client (JDBC / Flight SQL / HTTP)
        |
        v
   Coordinator
   |  - OIDC password grant (Keycloak, Auth0, any provider)
   |  - Policy enforcement (plan rewriting)
   |  - Fragment scheduling
   |
   +---> Polaris REST Catalog  (Authorization: Bearer {user_token})
   |         |
   |         +---> S3 credential vending (per-user, per-table)
   |
   +---> Workers (plan fragments + credentials via Flight metadata)
            |
            +---> S3 (with user-scoped credentials)
```

**No service account anywhere.** When you run `SELECT * FROM sales.orders`, the coordinator authenticates you via OIDC, gets your bearer token, and passes it to Polaris. Polaris checks your permissions and vends S3 credentials scoped to exactly the tables you're allowed to see. Workers receive plan fragments with those credentials attached. They never see anyone else's data.

Token refresh happens in the background. The coordinator tracks token lifetimes and refreshes them before expiry. For long-running distributed queries, refreshed credentials are pushed to workers via Flight metadata. If a token can't be refreshed, the session expires cleanly.

This is what "sovereign" means in practice. The user's identity follows the query from parsing to storage I/O.

---

## What iceberg-rust gives us

We use [iceberg-rust](https://github.com/apache/iceberg-rust) (v0.9) for all Iceberg operations. This matters more than you'd think.

Trino's Iceberg connector is a Java library that speaks Iceberg via the Java SDK, serialises data through Hive/Parquet readers, and bridges everything through JNI when you want native performance. It works, but it's layers on layers.

iceberg-rust is a native Rust implementation of the Iceberg spec. It reads Iceberg metadata, Parquet data files, and speaks the REST catalog protocol. All in the same language as our query engine. DataFusion's `TableProvider` trait wraps `IcebergTableProvider` directly. No serialisation boundary. No JNI. No class-loading surprises.

The practical result:

- **Metadata caching** with 30-second TTL, built on moka's async cache
- **Credential vending** extracted from Polaris `loadTable` responses, cached per (session, table)
- **Predicate pushdown** from DataFusion filters into Iceberg scan predicates, including LIMIT, LIKE, Boolean, and Timestamp pushdown available since DataFusion 52
- **Manifest-aware fragment splitting** for distributed execution: the coordinator reads manifest files and groups data files into fragments assigned to workers

---

## Distributed execution

Single-node was the starting point. Distributed execution was the goal.

The coordinator splits physical plans into fragments based on Iceberg manifest groups. Each fragment is a self-contained unit: a serialised plan (via DataFusion's proto codec) plus the S3 credentials needed to read the data files. Fragments are dispatched to workers via Arrow Flight `do_exchange`, and results stream back as Arrow RecordBatches.

Workers are stateless. They receive a fragment, build a local DataFusion `SessionContext` with the provided credentials, execute, and stream results. They don't know about each other. They don't share state. They heartbeat to the coordinator every 5 seconds, and the coordinator uses a weighted scheduler to balance load.

If a worker dies, the coordinator re-assigns its fragments to another worker or falls back to local execution. For small queries (few manifests, few workers available), the coordinator skips distribution entirely and runs locally. This adaptive decision happens automatically based on table size and cluster state.

---

## What works today

As of March 2026, the core engine is production-functional.

**SQL**: Full ANSI SQL via DataFusion. Window functions (LEAD, LAG, PARTITION BY), CTEs, correlated subqueries, all join types, aggregates with HAVING, GROUPING SETS, UNION ALL, INTERSECT, EXCEPT.

**DDL/DML**: CREATE TABLE AS SELECT, INSERT INTO SELECT, CREATE/DROP VIEW, DROP TABLE, ALTER TABLE RENAME. Write path produces Parquet via Iceberg's append snapshots.

**Protocols**: Arrow Flight SQL as the primary protocol (gRPC, binary Arrow), plus a Trino HTTP compatibility layer for dashboard migration. The Trino compat speaks enough of the Trino wire protocol that existing Trino JDBC clients connect without changes.

**Auth and Security**: OIDC password grant with any compliant provider, background token refresh, TLS support (optional mTLS), per-user and global rate limiting, query timeouts with per-role overrides, session lifecycle management (idle + absolute timeouts), structured audit logging with query hashing.

**Observability**: OpenTelemetry traces propagated from coordinator to workers, Prometheus metrics (query counts, durations, rows, fragments), health endpoints (`/healthz`, `/readyz`), JSON audit log with session tracking.

**Distributed**: Fragment scheduler with load weighting, worker heartbeats, credential refresh push, failure handling with retry and local fallback, configurable memory limits with spill-to-disk.

---

## What we can't do yet (and why)

**DELETE FROM and MERGE INTO** are blocked on iceberg-rust's Merge-on-Read implementation (position delete files). This is tracked in iceberg-rust Epic #2186, estimated Q3 2026. Without row-level deletes, we can't support dbt's incremental materializations. We can do full-table overwrites via `INSERT OVERWRITE` semantics, but that's not the same thing.

**The OPA/Cedar policy engine** is designed (the `PolicyEnforcer` trait and `PlanRewriter` are in place with a passthrough implementation), but the actual policy backends aren't wired yet. This is by design. We're waiting for the Polaris OPA SPI refactor (PR #3999) to stabilise before building against it.

**Time travel** (`SELECT * FROM table FOR SYSTEM_TIME AS OF '2026-01-01'`) is supported by iceberg-rust but not yet wired into our SQL parser.

These are real gaps. We chose to ship what works rather than wait for completeness.

---

## The numbers

| Metric | Trino (DCAF fork) | SQE |
|--------|-------------------|-----|
| Binary size | ~1.2 GB (with plugins) | ~50 MB |
| Cold start | 10-30 seconds | <1 second |
| Minimum memory | 2 GB (coordinator) | ~100 MB |
| Worker memory | 8 GB+ recommended | Configurable, 512 MB viable |
| Codebase maintained | ~2M lines (fork) | ~5K lines (purpose-built) |
| Language | Java 21 | Rust |
| GC pauses | Yes (G1GC) | No (no GC) |
| Auth model | Service account + patches | Native bearer token passthrough |

These numbers matter for Kubernetes. A 50MB container that starts in under a second and runs in 512MB means you can autoscale workers aggressively. No warm-up. No GC tuning. No heap sizing ceremonies.

---

## The roadmap

We think in steps, not sprints.

| Step | Status | What |
|------|--------|------|
| Core engine | Done (99/103) | Distributed SQL, write path, Trino compat, observability |
| Security hardening | Done (51/51) | TLS, rate limiting, timeouts, audit, error sanitisation |
| Pluggable auth | Next | Bearer token, API key, mTLS, anonymous providers |
| Pluggable catalogs | Next | AWS Glue, Nessie, Hive Metastore, storage-only |
| Policy engine | Q3 2026 | OPA/Cedar row filters, column masks, GRANT/REVOKE SQL |
| Semantic AI layer | Future | RDF/SPARQL on Iceberg, vector search, agent interfaces |

Steps 4 and 5 (pluggable auth and catalogs) can run in parallel. Step 6 (semantic layer) is fully additive. New crates, no existing code broken.

The blocked items (DELETE/MERGE, OPA integration) are waiting on upstream, not on us. When iceberg-rust lands Merge-on-Read, we wire it in. When Polaris stabilises its OPA SPI, we build against it. Until then, we ship what we have.

---

## Why this works

Three things made this project viable.

**DataFusion is a library, not a framework.** We compose the pieces we need. When we needed security plan rewriting, we added an optimizer rule. When we needed distributed execution, we added a custom physical plan codec. We never fought the framework. We extended it.

**iceberg-rust is native, not a bridge.** No JNI. No serialisation boundaries. The same Arrow types flow from Iceberg metadata through the query engine to the Flight SQL client. When iceberg-rust adds a feature, we get it directly.

**Bearer token passthrough is an architecture, not a feature.** It's not something we bolted on. Every component, from session management to credential vending to worker dispatch, was built around the principle that the user's identity follows the query. This simplifies everything downstream: no shared credential stores, no privilege escalation risks, no "who was this query actually running as?" questions.

We went from maintaining a Trino fork to shipping a purpose-built engine in Rust. Less code, faster execution, stronger security, easier to operate. The bet on DataFusion + iceberg-rust paid off.

---

*SQE is open-sourced under Apache 2.0.*
