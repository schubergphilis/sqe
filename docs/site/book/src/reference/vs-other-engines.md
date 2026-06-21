# SQE vs Trino, DuckDB, Spark

SQE is not trying to win every workload. It is built for one shape: query Apache Iceberg tables as the authenticated user, on your own hardware, with a single Rust binary and no JVM. This page is a decision aid. Where another engine is the better fit, it says so.

The detailed function-by-function matrices live on getsqe.com: [compare/trino](https://getsqe.com/compare/trino) and [compare/duckdb](https://getsqe.com/compare/duckdb). The per-page comparison columns in the [SQL Reference](../sql-reference/index.md) carry the same data at function granularity.

## SQE vs Trino

Trino is the industry-standard SQL engine for lakehouses, and it is what SQE replaces in our own platform. The reasons we moved off it are specific:

| Dimension | Trino | SQE |
|---|---|---|
| Identity to catalog and storage | Single service account | Per-user OIDC bearer passthrough, no service account |
| Fine-grained security | External (Apache Ranger), at the connector boundary | In-engine SQL surface, plan-rewritten before optimization (enforcement off by default today) |
| Runtime | JVM: significant heap, GC pauses, 10-30s start | Single Rust binary, no GC, fast start |
| Maturity and breadth | Mature, huge connector ecosystem, many engines | Iceberg-first, narrower surface |

Choose Trino when you need its breadth: a wide connector catalog beyond Iceberg, a mature Ranger-based security deployment you already run, or features SQE does not have. Trino is the stronger general-purpose engine.

Choose SQE when per-user identity to the catalog and storage matters, when JVM start time and GC pauses fight your autoscaling, or when maintaining a patched Trino fork for token passthrough is a cost you want to drop. See [Why SQE](../story/why-sqe.md) for the full account of that migration, and [GRANT and REVOKE](../sql-reference/grant-revoke.md#comparison) for the security comparison.

A note on performance: SQE's stated parity goal is within 2x of Trino on TPC-H SF100 (a roadmap target, [Roadmap, Phase 10](../development/roadmap.md#phase-10---performance--reliability-testing-planned)). The benchmark numbers published in the roadmap are SQE measured against itself across scale and topology, not a head-to-head against Trino on identical data. Treat the parity claim as a goal, and measure on your workload.

## SQE vs DuckDB

DuckDB is an in-process analytical engine. It is excellent on a single machine: a laptop, a notebook, an ETL step inside one process. SQE's embedded mode borrows the same feeling (`sqe-cli --embedded`, quoted-string file reads, `read_csv` / `read_json` / `read_delta`, `hf://` URLs), so the file-level ergonomics overlap.

The split is about deployment and identity. DuckDB is a library you embed; there is no server, no per-user auth, no distributed execution, and no GRANT model (see the [grant comparison](../sql-reference/grant-revoke.md#comparison), where DuckDB is "no" across the security row). SQE runs as a server with OIDC auth, distributes across workers, and is Iceberg-native with a Polaris-style REST catalog.

Choose DuckDB for single-machine analytics, embedding into an application, or fast local exploration where a server is overhead. It is the stronger tool there.

Choose SQE when you need multi-user access with per-user identity, a shared Iceberg catalog, distributed scans on data larger than one box, or a server other clients connect to. For the file-reading ergonomics on one machine, SQE's embedded mode is the bridge.

## SQE vs Spark

Spark SQL is the reference for large-scale batch and the breadth of the Spark ecosystem. It runs on the JVM, uses a service-account model for catalog and storage like Trino, and pushes fine-grained security to Ranger or Lake Formation at the connector boundary (see the [grant comparison](../sql-reference/grant-revoke.md#comparison)).

Choose Spark for very large batch transformation, when you are already invested in the Spark ecosystem (MLlib, structured streaming, the broad connector set), or for scale beyond what SQE has been validated against. Spark is the stronger heavy-batch engine.

Choose SQE for interactive and analytical SQL on Iceberg where per-user identity and a small operational footprint matter, and where a single Rust binary beats standing up a Spark cluster. SQE distributes (coordinator plus stateless workers) but is positioned for query serving, not as a general batch framework.

## The through-line

SQE's distinguishing property is sovereignty: every query runs with the identity and permissions of the user who submitted it, no service-account intermediary, on hardware you control. Trino, DuckDB, and Spark each beat SQE on some axis (breadth, single-machine simplicity, batch scale). None of them pass a per-user bearer token through to both the catalog and storage. That is the trade SQE is built around. See [Why SQE](../story/why-sqe.md).
