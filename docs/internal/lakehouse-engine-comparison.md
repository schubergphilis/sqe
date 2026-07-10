# SQE vs the Lakehouse Query Engines: Comparative Analysis and Improvement Roadmap

Internal competitive research. Date: 2026-06-21.

Baseline engines: Trino/Presto, Apache Spark with native accelerators (Comet, Gluten/Velox, Databricks Photon), StarRocks, ClickHouse, Dremio. DataFusion and DuckDB referenced as the foundational vectorized layer.

This document combines an external, source-verified research pass (28 sources, 25 adversarially-verified claims) with SQE's own benchmark history and code state. External claims carry citations. Statements about SQE's current behavior come from our own benchmark memory and code, and are marked as such.

## How to read this

The research evidence is strong on performance (Dimension 1) and client compatibility (Dimension 2), where primary sources and papers exist. It is thinner on security/governance (Dimension 3) and architecture/DX (Dimension 4), where the public literature is mostly vendor docs. For those two we lean on what we already know about SQE's code and the peer engines' documented behavior, and we flag the spots that need a dedicated follow-up research pass.

---

## Executive summary

SQE sits on DataFusion, so its execution ceiling is set by DataFusion's plan-time partition model, not by the morsel-driven runtime work-stealing model that HyPer, Umbra, and DuckDB use to hit near-linear many-core scaling. That single architectural fact explains most of what we see at SF10 and above.

Our own clean-rig verdict already shows the shape of it. SQE wins every SF1 suite. At SF10 it wins TPC-DS by 1.22x but trails TPC-H at 0.86x and SSB at 0.5x. That is a real scaling crossover, not a contention artifact. The research points at the levers that close it.

Three findings matter most:

1. **Predicate transfer is the flagship performance lever.** Generalizing Bloom-join sideways information passing across the whole join graph beats one-hop Bloom join 3.3x and Yannakakis 4.8x on TPC-H. It attacks our documented SF10-SF100 join-explosion gaps directly.
2. **We already have dynamic filters; the next step is multi-join.** DataFusion ships dynamic filters (TopK and hash-join SIP). We already hit and fixed a per-batch rebuild bug in them (the snapshot-cache fix). The win now is extending one-hop filtering to full predicate transfer.
3. **The compatibility bets are sound and the next moves are proven.** Flight SQL primary is the right columnar choice. A Postgres wire front-end plus ADBC/JDBC drivers is the well-trodden path to drop-in client compatibility, validated by StarRocks and Duckgres.

The rest of this document works through each dimension and ends with a single prioritized roadmap.

---

## Dimension 1: Performance and execution

### Where SQE stands

SQE is columnar and vectorized for free, because DataFusion is built on Arrow. That is the correct base. The peer accelerators confirm it: Photon deliberately chose vectorized-interpreted execution over code generation because it is easier to build, debug, and observe, and specialization closes most of the codegen gap [Photon SIGMOD 2022]. Comet, which like SQE delegates to DataFusion, gets ~2.4x over vanilla Spark on single-node TPC-H versus Velox-based Gluten's ~2.8x [Comet docs]. The Rust/DataFusion lineage trails the C++/Velox lineage today, but the gap is closing and the base model is the same.

Our own numbers: SQE wins all SF1 suites on a clean dedicated rig (TPC-H 3.2x, SSB 2.3x, TPC-DS 3.7x vs the comparison engine). At SF10 the picture inverts on join-heavy work: TPC-DS still wins 1.22x, but TPC-H drops to 0.86x and SSB to 0.5x. The scan-parallelism gap (#131) is fixed: configurable intra-file row-group splitting (`task_split_target_size`, 32MB) took SSB SF1 from 0.64x to 1.45x. The remaining SF10 gap is join behavior, not scan.

### What the leading engines do better

**Morsel-driven scheduling.** The structural difference. In morsel-driven parallelism, small input fragments (morsels) are scheduled to worker threads at runtime via work-stealing, and the degree of parallelism is not baked into the plan; it changes elastically during execution [Leis et al., SIGMOD 2014]. HyPer reached an average speedup of over 30x on 32 cores on TPC-H and SSB at SF100, where static plan-driven engines like Vectorwise often stay under 10x [same]. DataFusion fixes parallelism at plan time via `target_partitions`. That is exactly the model morsel-driven scheduling replaces. Caveat: HyPer is a single-node in-memory NUMA engine, so 30x is aspirational framing, not a like-for-like target for a distributed Iceberg-over-S3 engine.

**Predicate transfer.** The highest-leverage join technique available. It generalizes Bloom join from a single join to arbitrary multi-join graphs, propagating Bloom-filter predicates across the join graph to replace Yannakakis semi-joins. On TPC-H it beats Bloom join 3.3x on average (up to 61x) and Yannakakis 4.8x on average (up to 47x) [Yang et al., CIDR 2024]. This is the textbook fix for the join-explosion we see at SF10.

**Batch-level runtime adaptivity.** Photon's defining technique: per-micro-batch fast paths for no-NULLs, no-inactive-rows, all-ASCII, and sparse-batch compaction [Photon SIGMOD 2022]. It matters precisely because lakehouse statistics are often absent, which is SQE's exact situation reading Iceberg-over-S3.

### What SQE already has (from our code and benchmarks)

- **Dynamic filters are live.** DataFusion's TopK-to-scan filtering (over 10x on LIMIT-ORDER-BY) and hash-join SIP (5x alone, ~22-25x with late materialization) are in our DataFusion base [DataFusion dynamic-filters blog]. We already found and fixed a real bug here: per-batch `DynamicFilterPhysicalExpr::current()` was rebuilding a ~300K-node CASE-of-InLists every batch on partitioned joins, causing q12/q17/q10/q20 explosions; the fix caches the first sealed snapshot. So we are past "turn it on" and into "make it robust and extend it."
- **Bloom-on-write exists but is bench-unused.** We build Bloom filters on write but the benchmark tables do not exercise them. That is a self-inflicted blind spot, not a missing feature.
- **Partition-on-write is the strongest unused lever.** Bench tables are unpartitioned, and CTAS `PARTITIONED BY (month(col))` parse-fails on Hive col-defs. Partition pruning is the biggest pruning win we are not using.
- **Sort-on-write OOMs instead of spilling.** `ExternalSorterMerge` has `can_spill=false`, so sort-on-write CTAS at SF10 hard-OOMs. Graceful spill degradation is missing.
- **q95-class self-joins are single-threaded.** CollectLeft self-join has no probe-side parallelism. Config levers make it worse and regress TPC-H. This needs the probe-side parallelism work, a #131 follow-up.

### Recommendations (Dimension 1)

1. **Validate dynamic filters survive our plan rewriting.** Our policy layer rewrites the LogicalPlan before optimization. Confirm dynamic filters and SIP are not blocked by the security rewrite, and benchmark them on the SF10-SF100 rigs. Low-medium effort, high value, and partly a verification task.
2. **Implement multi-join predicate transfer.** This is the flagship. Build Bloom-filter SIP across the join graph, above or within DataFusion. High effort, very high impact (3.3-4.8x on join-heavy TPC-H), and it directly targets the SF10 crossover.
3. **Close the easy pruning gaps first.** Fix CTAS `PARTITIONED BY (transform(col))` parsing so bench and real tables get partition pruning. Wire Bloom-on-write into the benchmark tables so the filters we already build actually get used. These are cheap relative to predicate transfer and may recover a chunk of the SF10 gap on their own.
4. **Make sort-on-write spill instead of OOM.** Graceful degradation, so partitioned/sorted CTAS at SF10 does not need a memory-limit bump to survive.
5. **Evaluate morsel-style work-stealing inside workers.** The big, uncertain bet. DataFusion's plan-time partition model is not trivially replaceable, so this is very high effort and partly upstream-dependent. Worth a spike and an upstream conversation, not a near-term commitment.

---

## Dimension 2: Client and SQL compatibility

### Where SQE stands

Flight SQL primary is the right call, and the research backs it. Flight SQL is Arrow-columnar plus Flight RPC, giving JDBC/ODBC-like function while keeping data columnar end-to-end and avoiding the double row-to-column transposition that row-based JDBC/ODBC forces [Arrow Flight SQL blog]. ADBC sits above as a client API spec, so a database can support ADBC even without Flight SQL [ADBC FAQ]. That gives us a clean driver story. We also already ship a Trino HTTP compat layer (`sqe-trino-compat`) and a Trino-functions crate, so we have a working precedent for adding wire-protocol front-doors.

Our known dialect gaps are documented: DataFusion lacks Trino's standalone `year()`/`month()`/`day_of_week()`, which broke dbt models. ClickBench q28's `regexp_replace` backreference is a dialect difference, not a bug. These are the long tail of "be compatible," and they are the part that makes BI tools and ORMs actually work.

### What the leading engines do, and the proven patterns

- **StarRocks added Flight SQL (v3.5.1+)** for full DDL/DML/DQL over Python ADBC and Java JDBC drivers, keeping data columnar end-to-end versus the row-based MySQL/JDBC path [StarRocks docs]. This validates our primary-protocol choice from a peer that also kept a MySQL wire protocol for compatibility.
- **Duckgres puts a PostgreSQL wire front-end over DuckDB**, so any Postgres client (psql, pgAdmin, lib/pq, psycopg2) and BI tools connect unchanged [Duckgres]. Note Duckgres transpiles Postgres SQL to DuckDB SQL; it is not pure passthrough. That distinction matters for us.

### The auth fit nobody else has

SQE's per-user OIDC password grant is an unusually clean match for the Postgres and MySQL wire protocols. Both authenticate with username and password at the handshake. SQE already turns a password into an OIDC token via the password grant. So a Postgres client's `PasswordMessage` maps directly onto our existing flow, with no bearer-token juggling. That is a genuine advantage over engines that bolt a wire protocol onto a service-account model.

### Recommendations (Dimension 2)

1. **Ship ADBC and Arrow Flight SQL JDBC drivers and validate the columnar end-to-end path.** Lowest-effort high-value compatibility win. It makes BI and dbt drop-in over the protocol we already lead with. Low-medium effort, high value.
2. **Add a PostgreSQL wire front-end with pg_catalog / information_schema emulation.** This is the drop-in-for-Postgres-clients move. The wire framing is the small part (use the mature `pgwire` crate); the catalog emulation that makes DBeaver/psql/Tableau introspection work is the larger part. Prefer Postgres over MySQL: bigger tool ecosystem, more mature Rust crate, well-documented `pg_catalog` prior art. Medium-high effort, high value.
3. **Gate every front-door behind a Cargo feature.** Trino, Quack, and the future pg/mysql crates should be optional. Today the Trino and Quack stacks compile unconditionally into the coordinator. Postgres support drags in `pgwire` plus a TLS tree; do not tax every build with it. This is a build-time and modularity win as much as a compat one.
4. **Keep chipping at the dialect tail.** The Trino-functions work is the model. Publishing it (see Dimension 4) doubles as compatibility coverage and open-source reach.

---

## Dimension 3: Security and governance

Research evidence here was thin (mostly vendor docs), so this section leans on SQE's own design and the peer engines' documented behavior. Treat the comparative claims as directional and worth a dedicated follow-up pass.

### Where SQE stands, and where it is genuinely ahead

SQE enforces policy by rewriting the LogicalPlan before DataFusion optimization: row filters, column masks, and column restriction injected above the TableScan, following the PostgreSQL RLS model (denied columns invisible, masked columns block predicate pushdown on raw values). The pluggable backend (OPA, Cedar, passthrough) is a real architectural strength. Trino added OPA access control relatively recently [Trino OPA docs, 2024]; Dremio added row- and column-level controls as a product feature [Dremio blog]; Ranger does row filtering and column masking for Hive via policies [Ranger wiki]. SQE's plan-rewrite approach is in the same class and the per-user OIDC passthrough model is stronger on sovereignty than the service-account norm.

Two things are worth stating plainly because they are easy to overclaim:

- **The OIDC passthrough model is the differentiator.** Every query runs as the authenticated user with their own bearer token to Polaris and S3. There is no service account holding the keys to everything. For EU digital sovereignty and least-privilege, that is a stronger story than the service-account model most engines use. It is also what makes the Postgres wire front-end auth so clean.
- **We do not have Lake Formation-style fine-grained enforcement at the storage layer.** SQE reads S3 directly with caller credentials; LF only gates table/DB permission level. The "fine-grained LF grants" framing is not backed by SQE behavior. Keep the claims honest.

### Audit and OCSF

The OCSF audit logging work (on `feat/ocsf-audit-logging`) is the right direction and aligns SQE with an open standard [OCSF schema]. The model, identity capture, GDPR masking, and hash-chain (sub-project A) are the core; OTel/SIEM export (B) and op-polish plus the WEB-01 gate (C) follow. This is a credibility feature for regulated and sovereign deployments.

### Recommendations (Dimension 3)

1. **Land the OCSF audit core (sub-project A) and conform to the schema explicitly.** Map our events to OCSF activity/class IDs so a SIEM ingests them without a custom parser. Medium effort, high value for the target market.
2. **Extract audit into a low-in-the-DAG `sqe-audit` crate.** Audit is cross-cutting (auth, coordinator, worker all emit). It must be a leaf others depend on, not buried in the coordinator, or the worker cannot emit events without depending on the coordinator. Design the OCSF event-model layer SQE-agnostic so it can graduate to a published `ocsf-rs` crate later.
3. **Write the honest sovereignty positioning doc.** Per-user OIDC passthrough is the headline. State what SQE does and does not enforce (no storage-layer LF-style grants). This is marketing and credibility, not engineering.
4. **Run a dedicated governance research pass.** Ranger vs OPA vs Cedar tradeoffs, multi-tenancy isolation, and OCSF conformance deserve verified primary sources before we make architectural commitments. The general research pass did not cover them deeply.

---

## Dimension 4: Architecture and developer experience

Research evidence here was also thin. This section combines what we know about the codebase with general Rust build-time guidance from the sources.

### Where SQE stands

The crate structure is mostly healthy. Quack is already split (`sqe-quack-wire`/`-server`/`-client`), Trino is split (`sqe-trino-compat`/`-functions`), and the protocol layer is cleanly decoupled from execution via traits (the `TrinoQueryExecutor`/`TrinoAuthenticator` pattern). The build profile is well-tuned: lld/mold linker, dev profile keeps first-party code at `debug=1` and strips debug from deps, and a `dev-release` profile cranks `codegen-units=16 + incremental`.

The one real problem: **`sqe-coordinator` is 36.7k LOC**, 5x the next concern, and it sits at the bottom of the dependency DAG. Editing one line of `write_handler.rs` (7.2k) recompiles all 36.7k and relinks the binary, and the coordinator cannot start compiling until every upstream crate finishes. It is both the fattest node and the tail of the critical path.

GreptimeDB is a useful peer reference here: a large Rust database that organizes into many focused crates over a DataFusion-based query engine [GreptimeDB docs/deepwiki]. The Cargo-workspace-for-faster-compilation guidance is standard [corrode.dev, InfoWorld].

### Recommendations (Dimension 4)

1. **Feature-gate the existing front-doors first.** Trino, Quack, and future pg/mysql crates behind Cargo features. Code not compiled is the cheapest speedup, and it is hours of work at zero risk. Highest ROI in this dimension.
2. **Break up `sqe-coordinator` into siblings over a thin `sqe-coordinator-core`.** Extract `sqe-write` (write_handler + writer), `sqe-flight` (Flight SQL is just another front door and belongs next to trino-compat), and `sqe-scheduler` (distributed_scan + scheduler + worker_registry + channel_pool). The discipline: cut the dependency edges via narrow traits, do not just move files. If `sqe-write` still depends on the whole coordinator, nothing improves.
3. **Measure before cutting.** Run `cargo build --timings` once and read the Gantt chart for the actual critical path and link-step blockers. We have a ~88.8s baseline cycle to beat.
4. **Repo strategy: graduate, do not spin off early.** Keep one monorepo. Publish the genuinely-generic crates to crates.io from it (you can publish many crates from one repo). `sqe-trino-functions` is the best first publish (reusable by any DataFusion user, real ecosystem gap). `sqe-audit`/`ocsf-rs` graduates once stable. Spin off a separate repo only when a piece earns its own contributor community, which is a problem we do not have yet. Separate repos kill atomic refactors, which is the worst trade for a pre-1.0 engine still churning interfaces.

---

## Prioritized roadmap

Ranked by impact-to-effort, with the cheap high-confidence wins first.

| # | Recommendation | Dim | Impact | Effort | Key references |
|---|---|---|---|---|---|
| 1 | Feature-gate existing front-doors (Trino, Quack), then pg/mysql | D2/D4 | Medium (build time, modularity) | Low | corrode.dev, InfoWorld Cargo workspaces |
| 2 | Validate dynamic filters survive policy plan-rewriting; benchmark on SF10-SF100 | D1 | High | Low-Med | DataFusion dynamic-filters blog |
| 3 | Fix CTAS `PARTITIONED BY (transform(col))` parsing; wire Bloom-on-write into bench tables | D1 | High (pruning) | Low-Med | Our partition-on-write and bloom findings |
| 4 | Ship ADBC + Arrow Flight SQL JDBC drivers; validate columnar end-to-end | D2 | High (BI/dbt drop-in) | Low-Med | StarRocks Flight SQL, Arrow Flight SQL/ADBC |
| 5 | Land OCSF audit core (sub-project A); extract `sqe-audit` leaf crate | D3/D4 | High (regulated/sovereign market) | Med | OCSF schema |
| 6 | Make sort-on-write spill instead of OOM | D1 | Med (stability at scale) | Med | Our sort-merge OOM finding |
| 7 | Add PostgreSQL wire front-end + pg_catalog/information_schema emulation | D2 | High | Med-High | Duckgres, pgwire |
| 8 | Break up `sqe-coordinator` into core + write + flight + scheduler | D4 | Med-High (DX, build) | Med-High | GreptimeDB structure; measure with cargo --timings |
| 9 | Implement multi-join predicate transfer (Bloom SIP across join graph) | D1 | Very High (3.3-4.8x join-heavy) | High | Yang et al. CIDR 2024 |
| 10 | Batch-level runtime adaptivity (no-NULL/no-inactive/ASCII fast paths) + operator fusion, track Comet | D1 | Med-High | High | Photon SIGMOD 2022, Comet/Gluten comparison |
| 11 | Publish `sqe-trino-functions` to crates.io; graduate `ocsf-rs` once stable | D4 | Med (reach, credibility) | Low-Med | repo-strategy reasoning |
| 12 | Probe-side parallelism for CollectLeft self-joins (q95 class) | D1 | Med (specific queries) | High | Our q95 CollectLeft finding |
| 13 | Spike morsel-style work-stealing inside workers / upstream into DataFusion | D1/D4 | High but uncertain | Very High | Leis et al. SIGMOD 2014 |
| 14 | Dedicated governance research pass (Ranger vs OPA vs Cedar, multi-tenancy, OCSF conformance) | D3 | Enabling | Low (research) | follow-up |

### Suggested sequencing

- **This quarter (cheap, high-confidence):** items 1-4. Feature-gating, dynamic-filter validation, the pruning fixes, and the driver story. Mostly verification and low-risk wins that may recover real SF10 ground on their own.
- **Next (credibility and stability):** items 5-8. OCSF audit, spill safety, the Postgres front-end, and the coordinator split.
- **The big bets:** items 9-10 and 13. Predicate transfer is the flagship and the one most likely to move the SF10-SF100 numbers structurally. Morsel scheduling is the moonshot; spike it, do not commit it.
- **Ongoing:** items 11-12 and 14 in parallel as capacity allows.

---

## Caveats and honest limits

- The quantitative speedups are real and primary-sourced, but benchmark-specific. HyPer's 30x is single-node in-memory, not distributed Iceberg-over-S3. The Comet/Gluten 2.4x/2.8x figures are self-maintained, derived-from-TPC-H, single-node, and biased in Comet's favor by the source's own admission. Predicate transfer's 3.3x/4.8x and the dynamic-filter 5-25x are per-query-pattern, not whole-workload. None translate one-to-one to SQE without re-validation on our own rigs. Re-benchmark every claim before believing it for SQE.
- Dimensions 3 and 4 rest on SQE's own context plus vendor docs, not independently verified primary research. Item 14 exists to fix that for governance.
- The morsel-driven recommendation is the least certain. DataFusion's plan-time partition model is not trivially replaceable, so it is partly an upstream conversation.

## Sources

Performance and execution:
- Leis et al., "Morsel-Driven Parallelism," SIGMOD 2014. https://db.in.tum.de/~leis/papers/morsels.pdf
- Yang et al., "Predicate Transfer," CIDR 2024. https://www.cidrdb.org/cidr2024/papers/p22-yang.pdf and https://arxiv.org/pdf/2307.15255
- "Photon: A Fast Query Engine for Lakehouse Systems," SIGMOD 2022. https://people.eecs.berkeley.edu/~matei/papers/2022/sigmod_photon.pdf
- DataFusion dynamic filters blog (2025-09-10). https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/
- Comet vs Gluten comparison. https://datafusion.apache.org/comet/about/gluten_comparison.html
- DataFusion TPC-H 1000 on Embucket. https://embucket.com/blog/running_tpc_h_1000_on_apache_data_fusion

Client and SQL compatibility:
- Introducing Arrow Flight SQL. https://arrow.apache.org/blog/2022/02/16/introducing-arrow-flight-sql/
- ADBC FAQ. https://arrow.apache.org/adbc/current/faq.html
- StarRocks Arrow Flight SQL. https://docs.starrocks.io/docs/unloading/arrow_flight/
- Duckgres (Postgres wire over DuckDB). https://github.com/PostHog/duckgres
- Trino client protocol. https://trino.io/docs/current/develop/client-protocol.html

Security and governance:
- Trino OPA access control. https://trino.io/docs/current/security/opa-access-control.html and https://trino.io/blog/2024/02/06/opa-arrived.html
- Dremio row/column access controls. https://www.dremio.com/blog/new-row-level-and-column-level-access-controls/
- Apache Ranger row filtering and column masking. https://cwiki.apache.org/confluence/display/RANGER/Row-level+filtering+and+column-masking+using+Apache+Ranger+policies+in+Apache+Hive
- OCSF schema. https://github.com/ocsf/ocsf-schema
- Policy language benchmarking (Teleport). https://goteleport.com/blog/benchmarking-policy-languages/

Architecture and DX:
- DataFusion SIGMOD 2024 paper (Lamb et al.). https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf
- GreptimeDB architecture. https://deepwiki.com/GreptimeTeam/greptimedb and https://docs.greptime.com/developer-guide/datanode/query-engine
- Faster Rust compile times. https://corrode.dev/blog/tips-for-faster-rust-compile-times/
- Cargo workspaces for faster compilation. https://www.infoworld.com/article/4050654/organize-rust-projects-for-faster-compilation-with-cargo-workspaces.html
