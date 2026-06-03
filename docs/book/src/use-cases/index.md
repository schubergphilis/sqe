# Use Cases

This section is a set of runbooks. Each page takes one way of deploying or
connecting to SQE and walks it end to end: what it is, what you need running,
the config, the exact commands, and the output you should see. Each recipe is
backed by a test or script in the repo; the validation matrix below is explicit
about what was re-run live during the reveal-prep round versus what is covered
by the automated suite.

The pages group by how you connect and where your tables live:

- [Flight SQL](./flight-sql.md): the primary wire protocol, single node and distributed.
- [Trino HTTP compatibility](./trino-http.md): point Trino clients and BI tools at SQE.
- [Quack](./quack.md): the DuckDB wire protocol, server and client.
- [Catalog backends](./catalog-backends.md): Polaris, AWS Glue, AWS S3 Tables, Unity Catalog OSS, Hive Metastore, Nessie, and the catalog-free Hadoop warehouse.
- [Embedded and single-node CLI](./embedded.md): run the engine in-process with no server.
- [File-format TVFs](./file-format-tvfs.md): query CSV, Parquet, and JSON files directly.
- [Benchmarks](./benchmarks.md): TPC-H, TPC-DS, and SSB.

## Topology in one paragraph

SQE runs in two shapes. A single coordinator does everything: parse, plan,
and execute. In distributed mode the coordinator parses and plans, then ships
secured plan fragments to stateless workers over Arrow Flight. The protocol
you connect with (Flight SQL, Trino HTTP, or Quack) is independent of the
topology, so any protocol works against either shape. The catalog backend is
independent again: the same binary talks to Polaris, Glue, S3 Tables, Unity,
HMS, Nessie, or a bare filesystem warehouse, selected by one config block.

## Validation matrix

The **Verified** column is honest about provenance:

- **live (2026-06-03)**: re-run this round with captured output.
- **suite**: covered by automated tests in the repo, not re-run live this round.
- **baseline**: a committed result file in `benchmarks/results/`.

The infra ceiling on a single laptop is real: with the AWS backends, the local
Polaris stack, and three optional catalog stacks all up, the Docker VM was
saturated, so the docker-dependent re-runs (distributed, Quack, Unity/HMS/Nessie)
are cited from their suites rather than re-run. The AWS backends and the
in-process paths were run live.

| # | Use case | Protocol / Backend | Topology | How validated | Verified | Latest result |
|---|----------|--------------------|----------|---------------|----------|---------------|
| 1 | Trino HTTP -> Polaris | Trino HTTP | single | `integration_test.rs::test_trino_http_query`; `scripts/trino-parity-test.sh` | suite | parity script + HTTP test in repo |
| 2 | Trino HTTP -> Polaris | Trino HTTP | distributed | `scripts/distributed-test.sh` (Trino on :28080) | suite | distributed harness (test 11) |
| 3 | Flight SQL -> Polaris | Arrow Flight SQL | single | `integration_test.rs` (auth, CTAS, SELECT, TVFs) | live (2026-06-03) | TVF round-trip incl. Polaris auth + CTAS + SELECT, 3/3 |
| 4 | Flight SQL -> Polaris | Arrow Flight SQL | distributed | `scripts/distributed-test.sh` (Flight :60051); benchmark harness | suite / baseline | TPC-H SF1 distributed 22/22, 12.0s (committed) |
| 5a | AWS Glue catalog | Glue (native SDK) | any | `backends_integration.rs::glue::live_glue_namespace_round_trip` | live (2026-06-03) | namespace round-trip ok, 8.38s |
| 5b | AWS S3 Tables | S3 Tables (Glue REST + SigV4) | any | `backends_integration.rs::s3_tables::list_namespaces_via_glue_rest` | live (2026-06-03) | ns `table_demo_analytics`, table `table_user_events` |
| 5c | Unity Catalog OSS | Iceberg REST adapter | any | `backends_integration.rs::unity_catalog::*` | suite | read smoke; live re-run blocked by local docker |
| 5d | Hive Metastore | HMS (Thrift) | any | `backends_integration.rs::hms::*` | suite | namespace round-trip test |
| 5e | Project Nessie | Iceberg REST | any | `backends_integration.rs::nessie::*` | suite | namespace round-trip test |
| 6 | Embedded / single-node | in-process | single | `cli_smoke.rs`; live CLI run | live (2026-06-03) | `--embedded --memory` SELECT 1 -> 1 |
| 7 | Quack server + client | Quack (DuckDB wire) | single | `quack_e2e.rs`; `sqe-quack-*` tests | suite | e2e + wire/server/client tests in repo |
| 8 | read_csv / read_parquet / read_json | TVF | single | `integration_test.rs::test_read_{csv,parquet,json}_local_file` | live (2026-06-03) | 3/3, 23.6s |
| 9 | GRANT / REVOKE (Chameleon) | SQL | any | `query_handler.rs::extract_grant_statement_*` | suite | parser unit-tested (6 cases) |
| 10 | Benchmarks | TPC-H / TPC-DS / SSB | single + distributed | `scripts/benchmark-test.sh`; `benchmarks/results/*.json` | baseline | TPC-H SF1 22/22, 37.5s single (committed) |

> GRANT/REVOKE and the access-control SQL surface are Chameleon/SBP-specific
> and documented in [GRANT and REVOKE](../sql-reference/grant-revoke.md). They
> are not part of the core open-source SQL surface; the section there is marked
> accordingly.
