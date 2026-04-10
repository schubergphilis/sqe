# Sovereign by Design

**Building a Distributed SQL Engine in Rust — From DataFusion to Production**

*Jacob Verhoeks, 2026*

---

## The Thesis

You don't need Trino. You don't need Spark. You don't need a managed query service with a bill you can't predict and an auth model you can't control. What you need is a query engine that runs *as the user*, talks to *your* catalog, reads *your* storage, and enforces *your* policies — with nothing in between.

This book is the story of building that engine — and the journey that led to it.

---

## The Author's Journey

This book doesn't start with code. It starts with years of hitting walls in the data platform space — documented in real time on [dev.to/jverhoeks](https://dev.to/jverhoeks):

**2022–2023: The Vendor Lock-in Years**
- AWS Glue custom connectors and their secret management pain
- Snowflake as the "easy" answer that creates dependency
- The first realisation: your data platform shouldn't require a vendor's permission to query your own data

**2024: The Iceberg Awakening**
- Glue Iceberg REST API + PyIceberg — discovering the open table format
- Unity Catalog's Iceberg REST API — seeing Databricks hedge toward openness
- Snowflake + Iceberg tables — watching the proprietary vendors adopt the open format
- Cross-cloud bridging (Glue ↔ Snowflake) — understanding why a *central catalog* matters

**2025: Toward Independence**
- DuckDB + S3 + Iceberg REST API — a query engine that doesn't need a cluster
- IOMete — cloud-independent Spark, but still Spark
- Software supply chain security — understanding the risks of deep dependency trees
- The question: *what if the query engine was also independent?*

**2026: Building It — 316 commits in 15 days**
- Agentic AI development workflows — using Claude Code to build SQE itself
- Spec-driven engineering — the OpenSpec format and *The Art of Agents*
- SQE: from initial commit to distributed execution with concurrent load testing

The git log tells the real story — and the book follows it:

```
Mar 14  — Initial commit: architecture docs + core engine spec
Mar 14  — All 6 crates scaffolded: core, auth, policy, sql, catalog, coordinator
Mar 14  — First Flight SQL integration test passes (auth + Iceberg query via Polaris)
Mar 15  — Write path: CTAS, INSERT INTO, DROP TABLE tested
Mar 15  — Distributed execution: ScanTask protocol, worker, DistributedScanExec
Mar 16  — Prometheus metrics, audit logging, Trino HTTP compat, information_schema
Mar 17  — Docker, Helm chart, mdBook docs, CLI
Mar 18  — Lightweight test stack: Polaris in-memory + RustFS (no AWS needed)
Mar 19  — 37 integration tests: views, joins, aggregations, EXPLAIN
Mar 21  — Benchmark suite: TPC-H (22), SSB (13), TPC-DS (99), ClickBench (43) queries
Mar 22  — TPC-E, TPC-BB, TPC-C generators + benchmark runner
Mar 24  — Weighted fragment scheduler, OTel trace propagation, heartbeats
Mar 25  — Query history, result cache, system.runtime.* virtual tables
Mar 27  — Distributed docker-compose: coordinator + 2 workers
Mar 28  — Distributed execution wired into query pipeline
Mar 29  — Concurrent client load test, schema projection fix
Mar 30  — dbt hardening: file collision fix, 17 Trino function aliases, structured error codes
Apr 07  — Distributed benchmarks: TPC-H 22/22, TPC-DS 98/99, 2.4x avg speedup
Apr 08  — OSS release: Apache 2.0 license, AUDIT.md, v0.15.0
Apr 09  — Trino compat blitz: 63% to 95% in one day (70+ UDFs, time travel, 6 metadata TVFs)
Apr 10  — Streaming writes, sort order safety, IN-subquery rewrite, Trino comparison benchmarks
```

Each chapter in this book connects to this journey. The callout boxes reference specific commits, articles, and experiences.

---

## Structure

The book follows the arc of building SQE from zero to production across **five parts** and **18 chapters**, mirroring the actual journey. Each chapter pairs narrative (the *why*) with implementation (the *how*) and connects to spec-driven engineering principles from *The Art of Agents*.

**Estimated total: 360-400 pages**

---

## Part I: Why Build (Chapters 0-2) — ~70 pages

The journey from managed services to the decision to build your own engine.

| Ch | Title | Pages | Topics |
|----|-------|-------|--------|
| 0 | **Preface: The Sovereignty Thesis** | 12 | Why we built SQE. The cost of dependency. What sovereignty means in data infrastructure. The author's journey from Glue/Snowflake to independence. Connection to spec-driven design (*Art of Agents*, Ch 1). |
| 1 | **The Catalog Wars** | 28 | From Hive Metastore to REST catalog. Glue as catalog. Unity Catalog vs Polaris. Why Iceberg REST is the HTTP of data catalogs — a standard interface, not a product. The centrality of the catalog in modern data architecture. Why Polaris wins for sovereignty. |
| 2 | **Tables Made of Files** | 28 | Apache Iceberg from first principles. Metadata tree, manifests, partition evolution. The PyIceberg experiments (Glue REST, Unity REST). iceberg-rust 0.8 — what works, what we had to work around. Why Rust Iceberg changes the game vs Python Iceberg. |

**Art of Agents crossover:** Chapter 0 maps to *Laying Plans* (Five Constants). Chapter 1 maps to *Terrain* — the catalog landscape shapes every decision downstream.

**dev.to connections:**
- Ch 1: "Unity Catalog Iceberg Rest Api and PyIceberg", "Glue Iceberg Rest Api and PyIceberg", "Bridging Clouds"
- Ch 2: "Duckberg!", "DuckDB S3 Tables with Iceberg using Iceberg Rest API", "Collibra Protect, Snowflake and Iceberg tables"

---

## Part II: First Query (Chapters 3-6) — ~90 pages

Building the engine. Getting a SQL query to return results from an Iceberg table via Polaris, authenticated as the actual user.

| Ch | Title | Pages | Topics |
|----|-------|-------|--------|
| 3 | **The Engine You Already Have** | 24 | DataFusion as a library, not a service. The power of `SessionContext` — one line to get a SQL engine. How a SQL string becomes Arrow batches. Why Rust. The catalog-storage-compute separation. Comparison to DuckDB (embedded) and Spark (distributed). DataFusion's extensibility model. |
| 4 | **You Are the Query** | 20 | OIDC password grant flow. Why no service account. Bearer token passthrough to Polaris and S3. The `SqeSessionContext` — one per user, one per query. Session lifecycle. Why this model is impossible to retrofit into Trino. |
| 5 | **Speaking Arrow** | 18 | Arrow Flight SQL protocol. Why not REST, why not JDBC directly. The `FlightSqlService` trait. Handshake → GetFlightInfo → DoGet pipeline. Connection from DBeaver, Python, Rust. The wire protocol as user experience. |
| 6 | **The Catalog Is the API** | 24 | information_schema as a virtual provider. system.runtime.* tables. Namespace resolution. How dbt discovers your warehouse. The dbt-sqe adapter: Python + ADBC Flight SQL. Making `dbt run` work on the first try. |

**Art of Agents crossover:** Chapter 3 maps to *Energy* (Ch 5: Tool Design) — DataFusion is the tool. Chapter 4 maps to *Protocol* — auth shapes everything.

---

## Part III: Making It Real (Chapters 7-10) — ~90 pages

Features that turn a prototype into something teams trust with production data.

| Ch | Title | Pages | Topics |
|----|-------|-------|--------|
| 7 | **The Write Path** | 28 | INSERT INTO, CTAS, MERGE, DELETE via Copy-on-Write (rewrite_files). Streaming writes: why `df.collect()` kills you at scale and how `df.execute_stream()` fixes it. Merge-on-Read with position deletes (PositionDeleteFileWriter + FastAppendAction auto-routing by DataContentType). The IN (subquery) workaround for UPDATE/DELETE. Iceberg commit protocol. Conflict resolution. Why compaction comes later. |
| 8 | **What You Can't See Can't Hurt You** | 22 | Policy-as-plan-rewriting. Row filters injected above TableScan. Column masks that block predicate pushdown. The PostgreSQL RLS model applied to DataFusion LogicalPlans. OPA and Cedar as pluggable backends. The connection to Collibra Protect / governance platforms. |
| 9 | **Observability Without Surprise** | 18 | Prometheus metrics on every query stage. OpenTelemetry traces from SQL parse to Arrow batch. Health endpoints. What to alert on. The dashboard that paged oncall at 3am. |
| 10 | **Configuration Is the Product** | 22 | TOML config design. The move from hardcoded to fully configurable. Environment variable overlay. Feature toggles that aren't flags. The journey from "works on my machine" to "works in any environment". Twelve-Factor applied to a query engine. Plugin points: custom catalog, custom policy backend, custom auth provider. |

**Art of Agents crossover:** Chapter 8 maps to *Tactical Dispositions* (defence through schema). Chapter 10 maps to *Attack by Stratagem* (composability) — configuration as the composition surface.

---

## Part IV: Going Distributed (Chapters 11-14) — ~90 pages

Splitting the engine across coordinator and workers. Ballista heritage, custom scheduling, and the failure modes nobody warns you about.

| Ch | Title | Pages | Topics |
|----|-------|-------|--------|
| 11 | **Why Distribute at All** | 18 | When single-node stops being enough. The scan bottleneck. Partition-level parallelism. Amdahl's Law for query engines. The decision framework: data volume, concurrency, query complexity. When to stay single-node. |
| 12 | **Standing on Ballista's Shoulders** | 24 | Apache Ballista: what it provides and where we diverged. The protobuf codec for plan fragments. Worker registration and heartbeat. What we kept (serialisation, execution model) and what we replaced (scheduler, auth, config). Why forking was the right call. |
| 13 | **The Coordinator and the Worker** | 26 | Coordinator: plan splitting, fragment assignment, session management. Worker: receiving fragments, executing with the *user's* token, streaming results back via Flight. The trust boundary between coordinator and worker. Memory limits, spill to disk, resource management. |
| 14 | **Failure Is a Feature** | 22 | Worker crash, network partition, slow worker, coordinator restart. Fragment retry semantics. The load test with 50 concurrent clients that broke everything. What we fixed and what we accepted. Designing for recovery, not prevention. |

**Art of Agents crossover:** Chapter 11 maps to *Waging War* (distribution has a cost). Chapter 12 maps to *Manoeuvring* (adaptive orchestration). Chapter 14 maps to *Nine Situations* (failure modes).

---

## Part V: Production (Chapters 15-17) — ~60 pages

Shipping it. Running it. Keeping it running.

| Ch | Title | Pages | Topics |
|----|-------|-------|--------|
| 15 | **Deploying Sovereignty** | 22 | Docker multi-stage build (2.3GB → 47MB). Helm chart: coordinator Deployment + worker StatefulSet. Rolling upgrades with zero query interruption. Resource requests that make sense. The Kubernetes topology for a sovereign engine. |
| 16 | **Benchmarks Don't Lie (But They Mislead)** | 22 | TPC-H, TPC-DS, SSB, TPC-C, TPC-E, TPC-BB on Iceberg. SQE vs Trino on same hardware via `--compare-trino` flag (identical queries, same Polaris catalog, diff results). Where SQE wins (scan, auth overhead, cold start, memory). The sort order correctness trap: why trusting Iceberg sort metadata can silently corrupt results. The benchmark that mattered: 50 dbt models, nightly batch, wall-clock time. |
| 17 | **What We'd Do Differently** | 22 | The AI-assisted build: honest assessment — what the AI did well (implementation, debugging, test generation), what it didn't (architecture, security trade-offs). The human reviewed every big turn. Decisions we'd change. What Rust taught us (including: compile times at scale are real). The open-source goal. Where this goes next. Build-vs-buy honest accounting. |

**Art of Agents crossover:** Chapter 17 maps to *Use of Spies* (Ch 13: Feedback Loops) — the retrospective that closes the build cycle.

---

## Appendices — ~35 pages

| App | Title | Pages | Content |
|-----|-------|-------|---------|
| A | **Crate Map** | 6 | All 10 crates, responsibilities, dependency graph, public API surface |
| B | **Iceberg REST Catalog Comparison** | 8 | Polaris vs Unity vs Gravitino vs Nessie: features, auth models, deployment, Iceberg REST compliance |
| C | **SQL Compatibility Matrix** | 10 | SQE vs Trino: ~95% coverage across 13 categories. 70+ UDFs, engine-level features (USE, SHOW CREATE TABLE, TRUNCATE, TRY, time travel). The full `docs/trino-compatibility.md` with exact gap reasons. How we went from 63% to 95% in two days. |
| D | **Flight SQL Client Cookbook** | 6 | Connection recipes for Python, Java, Go, Rust, DBeaver, dbt |
| E | **The OpenSpec That Started It All** | 6 | The original proposal annotated with what changed and why |
| F | **Art of Agents Quick Reference** | 3 | The Five Constants, Promote/Pivot/Compost, and how they applied |

---

## Implementation Status

The book is honest about what's built vs what's planned. Chapters cover both.

| Chapter | Status |
|---------|--------|
| Ch 0-2 (Why Build, Catalogs, Iceberg) | Written from experience — articles, experiments, decisions |
| Ch 3-5 (DataFusion, Auth, Flight SQL) | **Implemented** — core engine works |
| Ch 6 (Catalog/dbt) | **Implemented** — information_schema, dbt-sqe adapter (table/view/incremental/seed), 6 metadata TVFs |
| Ch 7 (Write Path) | **Implemented** — INSERT, CTAS (streaming), DELETE/UPDATE/MERGE (CoW + MoR), time travel, IN-subquery rewrite |
| Ch 8 (Security/Policy) | **Implemented** — PolicyPlanRewriter, OPA backend, policy cache, SQL extensions |
| Ch 9 (Observability) | **Implemented** — Prometheus + health endpoints |
| Ch 10 (Configuration) | **Implemented** — TOML config, env overlay, trait-based plugin points |
| Ch 11-14 (Distributed) | **Implemented** — coordinator/worker/Ballista fork working |
| Ch 15-16 (Deploy/Bench) | **Implemented** — Docker + Helm + TPC-H |
| Ch 17 (Retrospective) | Written as the book is completed |

Chapters about future phases are written as design narratives — "here's what we designed, here's the trait, here's why, here's what's next" — not as implementation guides for code that doesn't exist yet.

---

## Running Code

This book's running code *is* the SQE repository itself. Each chapter references specific commits, crates, and tests. Tagged commits mark the engine at each stage:

```
git checkout book/ch03-first-query
git checkout book/ch07-write-path
git checkout book/ch12-ballista
git checkout book/ch15-helm-deploy
```

---

## Voice and Narrative Flow

**Who the author is:** A principal engineer who's been solving complex problems since childhood. Not a manager. Not a joke teller. The drive is the puzzle — figuring things out, making them work, understanding why they break.

**The voice:** A veteran engineer talking to a peer over coffee. Direct, practical, occasionally dry. No forced humour, no motivational speeches. When something is interesting, the writing shows it through the details, not through exclamation marks. When something went wrong, it says so plainly.

**The underlying rhythm:** Each chapter follows a natural problem-solving arc, but never rigidly or formulaically. The reader should feel the shape without seeing the scaffolding:

- Open with what's in the way — the constraint, the gap, the thing that doesn't work yet
- Walk through the thinking — approaches considered, trade-offs weighed, paths explored
- Show the attempts — including dead ends (AI lets you try three paths in the time one used to take)
- Land on what worked — the code, the trait, the config, the architecture
- Leave the reader with what matters — the insight that applies beyond this specific problem

This isn't a rigid 5-section formula. Some chapters are mostly "the thinking" (Chapter 1: The Catalog Wars). Some are mostly "the attempts" (Chapter 14: Failure Is a Feature). The arc is felt, not announced. The reader stays because each chapter is a puzzle being solved, and puzzles are inherently interesting when the stakes are real.

**The AI angle, naturally:** The AI assistant shows up where it naturally fits — "we tried three approaches to the schema projection bug, the AI found the fix in DataFusion's internals" — not as a thesis statement in every chapter. The human makes the decisions. The AI makes the pace possible.

**Forbidden:**
- Forced jokes or puns
- Motivational language ("you can do it!", "the future is bright")
- Breathless excitement ("amazing!", "incredible!")
- Apologising for complexity ("this might seem complicated, but...")
- The word "journey" used unironically more than twice in the whole book

**Allowed:**
- Dry observations that land because they're true
- Admitting when something was harder than expected
- Admitting when something was easier than expected
- Saying "I don't know" or "we got lucky"
- Technical detail that would bore a manager but fascinate an engineer

## Callout System

Following *The Art of Agents* pattern, adapted for the problem-solving voice:

| Callout | Purpose |
|---------|---------|
| `.sovereignty` | Core sovereignty principle — why this choice protects independence |
| `.datafusion` | DataFusion deep dive — trait implementations, optimizer rules, internals |
| `.iceberg` | Iceberg/catalog deep dive — REST protocol, manifest mechanics, Polaris specifics |
| `.deadend` | A path we tried that didn't work — and why it was still worth trying |
| `.fieldreport` | Real production story — the outage, the migration, the benchmark |
| `.antipattern` | What not to do — common mistakes when building on DataFusion/Iceberg |
| `.artofagents` | Cross-reference to *The Art of Agents* principles |
| `.devto` | Connection to published dev.to article — "I wrote about this at the time..." |

---

## Page Budget

| Section | Pages |
|---------|-------|
| Front matter (preface, TOC) | 14 |
| Part I: Why Build (Ch 0-2) | 68 |
| Part II: First Query (Ch 3-6) | 86 |
| Part III: Making It Real (Ch 7-10) | 87 |
| Part IV: Going Distributed (Ch 11-14) | 90 |
| Part V: Production (Ch 15-17) | 60 |
| Appendices (A-F) | 35 |
| **Total** | **~440** |

---

## Target Audience

| Segment | Why they'd read this |
|---------|---------------------|
| **DataFusion developers** | Practical guide to building production systems on DataFusion |
| **Iceberg ecosystem engineers** | REST catalog deep dive, iceberg-rust in production |
| **Rust systems programmers** | Real Rust project: async, Arrow, gRPC, distributed systems |
| **Platform engineers** | Blueprint for sovereign data infrastructure |
| **Data engineers leaving Snowflake/Databricks** | The "what comes after managed services" answer |
| **Technical leaders** | Build-vs-buy decision framework for query infrastructure |

**Realistic sales estimate:** 3,000–5,000 copies (technical niche, high relevance as Iceberg adoption accelerates)
