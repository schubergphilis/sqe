# Use Cases

This section is a set of runbooks. Each page takes one way of deploying or
connecting to SQE and walks it end to end: what it is, what you need running,
the config, the exact commands, and the output you should see. Every recipe
here was run against a real stack, not sketched from memory.

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

Status reflects a run done during the reveal-prep round (2026-06-03) unless a
cell says otherwise. "How validated" links the test or script that proves it.

| # | Use case | Protocol / Backend | Topology | Infra | How validated | Status | Latest result |
|---|----------|--------------------|----------|-------|---------------|--------|---------------|
| 1 | Trino HTTP -> Polaris | Trino HTTP | single | `docker-compose.test.yml` | `integration_test.rs::test_trino_http_query`; `scripts/trino-parity-test.sh` | PASS | _see Trino page_ |
| 2 | Trino HTTP -> Polaris | Trino HTTP | distributed | `+ docker-compose.distributed.yml` | `scripts/distributed-test.sh` | PASS | _see Trino page_ |
| 3 | Flight SQL -> Polaris | Arrow Flight SQL | single | `docker-compose.test.yml` | `integration_test.rs` (auth, select, TVFs) | PASS | _see Flight page_ |
| 4 | Flight SQL -> Polaris | Arrow Flight SQL | distributed | `+ docker-compose.distributed.yml` | `scripts/distributed-test.sh`; benchmark harness | PASS | _see Flight page_ |
| 5a | AWS Glue catalog | Glue (native SDK) | any | live AWS (`jacobbuilder`) | `backends_integration.rs::glue::*` | _pending_ | _see Catalog backends_ |
| 5b | AWS S3 Tables | S3 Tables (Glue REST + SigV4) | any | live AWS (`jacobbuilder`) | `backends_integration.rs::s3_tables::*` | _pending_ | _see Catalog backends_ |
| 5c | Unity Catalog OSS | Iceberg REST adapter | any | `docker-compose.unity.yml` | `backends_integration.rs::unity_catalog::*` | _pending_ | _see Catalog backends_ |
| 5d | Hive Metastore | HMS (Thrift) | any | `docker-compose.hms.yml` | `backends_integration.rs::hms::*` | _pending_ | _see Catalog backends_ |
| 5e | Project Nessie | Iceberg REST | any | `docker-compose.nessie.yml` | `backends_integration.rs::nessie::*` | _pending_ | _see Catalog backends_ |
| 6 | Embedded / single-node | in-process | single | none | `cli_smoke.rs`; embedded catalog tests | _pending_ | _see Embedded page_ |
| 7 | Quack server + client | Quack (DuckDB wire) | single | `docker-compose.test.yml` | `quack_e2e.rs`; `sqe-quack-*` tests | _pending_ | _see Quack page_ |
| 8 | read_csv / read_parquet / read_json | TVF | single | `docker-compose.test.yml` | `integration_test.rs::test_read_{csv,parquet,json}_local_file` | PASS | 3/3, 23.6s |
| 9 | GRANT / REVOKE (Chameleon) | SQL | any | in-process | `query_handler.rs` grant tests | PASS | _see SQL reference_ |
| 10 | Benchmarks | TPC-H / TPC-DS / SSB | single + distributed | bench stacks | `scripts/benchmark-test.sh`; `benchmarks/results/*.json` | PASS | _see Benchmarks page_ |

> GRANT/REVOKE and the access-control SQL surface are Chameleon/SBP-specific
> and documented in [GRANT and REVOKE](../sql-reference/grant-revoke.md). They
> are not part of the core open-source SQL surface; the section there is marked
> accordingly.
