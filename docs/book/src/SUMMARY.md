# Summary

[Introduction](./index.md)

---

# The Story

- [Why SQE](./story/why-sqe.md)
- [From Trino to DataFusion](./story/trino-to-datafusion.md)
- [The Auth Challenge](./story/auth-challenge.md)

---

# Architecture

- [System Overview](./architecture/overview.md)
- [Coordinator](./architecture/coordinator.md)
- [Worker](./architecture/worker.md)
- [Authentication Flow](./architecture/auth-flow.md)
- [Security & Policy](./architecture/security.md)
- [Streaming Execution](./architecture/streaming-execution.md)
- [Research Papers](./architecture/research-papers.md)

---

# Features

- [SQL Support](./features/sql-support.md)
- [Query Plan Inspection](./features/explain.md)
- [Custom SQL Extensions](./features/custom-sql.md)
- [Iceberg Integration](./features/iceberg.md)
- [Write Path](./features/write-path.md)
- [read\_parquet TVF](./features/read-parquet.md)
- [File-format TVFs (read\_csv / read\_json / read\_delta)](./features/file-format-tvfs.md)
- [Observability](./features/observability.md)
- [Trino Compatibility](./features/trino-compatibility.md)
- [Benchmark Suite](./features/benchmarks.md)

---

# SQL Reference

- [Overview](./sql-reference/index.md)
  - [Conditional and null-handling](./sql-reference/conditional.md)
  - [String](./sql-reference/string.md)
  - [Math](./sql-reference/math.md)
  - [Date and time](./sql-reference/datetime.md)
  - [Array, map, struct](./sql-reference/array-map.md)
  - [JSON](./sql-reference/json.md)
  - [Encoding, hashing, URL](./sql-reference/encoding-url.md)
  - [Aggregate functions](./sql-reference/aggregate.md)
  - [Window functions](./sql-reference/window.md)
  - [Table-valued functions](./sql-reference/table-functions.md)
  - [DDL](./sql-reference/ddl.md)
  - [DML](./sql-reference/dml.md)
  - [CALL procedures](./sql-reference/procedures.md)
  - [GRANT and REVOKE](./sql-reference/grant-revoke.md)
  - [SHOW and EXPLAIN](./sql-reference/show-explain.md)
  - [Operators](./sql-reference/operators.md)
  - [Dot-commands](./sql-reference/dot-commands.md)

---

# Getting Started

- [Quickstart](./getting-started/quickstart.md)
- [Catalog backends](./getting-started/catalogs.md)
- [Storage backends (S3 / R2 / GCS / ADLS / HTTPS / hf://)](./getting-started/storage-backends.md)
- [Building from Source](./getting-started/building.md)
- [Using the CLI](./getting-started/cli.md)

---

# Deployment

- [Configuration](./deployment/configuration.md)
- [Docker](./deployment/docker.md)
- [Kubernetes & Helm](./deployment/kubernetes.md)

---

# Operations

- [Lineage (OpenLineage)](./operations/openlineage.md)

---

# Development

- [Rust Crate Structure](./development/crates.md)
- [Testing](./development/testing.md)
- [Roadmap](./development/roadmap.md)
