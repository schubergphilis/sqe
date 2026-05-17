## Why

Iceberg tables are catalog-portable by design: the parquet files plus the `metadata.json` chain are the source of truth, and a catalog is just a pointer to the latest `metadata.json`. Today SQE has no SQL surface for taking advantage of that. The only way a user can put existing Iceberg tables under SQE's catalog is to `INSERT INTO ... SELECT` data out of one catalog and back into a new managed table, which copies every byte and breaks snapshot history.

The case is acute at SF1000 + scale benchmarking. Generating TPC-DS SF1000 into Iceberg form is a one-day operation that produces 1.5 TB of parquet. Throwing that catalog away after a test run and regenerating to test a config change is a non-starter. The right pattern is "golden dataset on S3, ephemeral catalog over it" — and that needs `CALL system.register_table(...)`.

Beyond the bench workflow, the same gap blocks several scenarios:

- **Multi-engine catalogs.** A team uses Spark or Flink to write a fact table on a nightly job; analysts want to query it through SQE. Today they must either point SQE at Spark's catalog (Phase 1 of `pluggable-catalogs/` already covers this) or re-ingest. With registration, they can route the SQE-managed catalog at the Spark-written tables file-by-file or table-by-table.
- **Disaster recovery.** If a Polaris catalog DB is corrupted but S3 is intact, today the recovery story is "restore Postgres from backup, hope it's recent". With a procedural register surface, recovery is a script that lists `metadata/*.metadata.json` and calls `register_table` for each. Catalog rebuilds in minutes from the durable S3 truth.
- **dbt and operational ergonomics.** dbt models, runbooks, and other SQL-shaped automation all express table operations as SQL statements. A registration script in bash + curl is fine for the SF1000 bootstrap but doesn't compose with the rest of an analytical workflow. `CALL system.register_table(...)` is the same shape as Spark's and Trino's procedure syntax, which means dbt macros and existing runbooks port over without translation.
- **Catalog migration.** Moving tables between Polaris and Glue (or between two Polaris instances) is data-free if you can `system.drop_table` (catalog-only, files intact) on the source and `system.register_table` on the destination. Three SQL statements per table instead of a TB-scale copy.

The Iceberg community has converged on a stored-procedure pattern: `CALL <catalog>.system.<procedure>(<args>)`. Spark, Flink, and Trino all implement it for the same set of operations (register_table, rewrite_data_files, expire_snapshots, rollback_to_snapshot, set_current_snapshot, rewrite_manifests). iceberg-rust's `Catalog` trait already has methods for the catalog-side operations across every backend in the vendored fork (REST, Glue, HMS, S3 Tables, JDBC). The work in this change is to plumb the SQL surface through to those trait methods.

## What Changes

Add a procedure-call SQL surface for catalog and table maintenance operations, dispatching through iceberg-rust's `Catalog` trait so every backend gets identical behavior with no per-backend SQE code.

- **Grammar.** Extend `sqe-sql` to recognize `CALL [catalog.]system.procedure(...)` and emit a new `ProcedureStmt` AST node. The form is what sqlparser-rs already parses as a `Statement::Call`; the SQE-side work is recognizing the `system.*` namespace and normalizing named-vs-positional arguments. Outside `system.*`, the parser passes CALL through unchanged for future user-defined procedures.
- **Dispatch.** A new `procedures` module in `sqe-coordinator` owns the procedure registry. Each procedure is a `Procedure` trait impl that knows its name, validates its argument schema, and executes against the session's catalog. The handler is invoked from the same query path as DDL: parse → policy check → execute. No optimizer involvement.
- **Procedures landed in v1.** `system.register_table`, `system.drop_table` (catalog-only, no file delete), `system.set_current_snapshot`, `system.rollback_to_snapshot`. These four cover the SF1000 workflow, multi-engine catalog interop, and the disaster-recovery story. Future procedures (rewrite_data_files, expire_snapshots, rewrite_manifests) fit the same framework without grammar changes.
- **Cross-backend semantics.** All four v1 procedures map directly to existing methods on iceberg-rust's `Catalog` trait. Each backend in `vendor/iceberg-rust/crates/catalog/{rest,glue,hms,sql,s3tables}` implements them or returns a typed "unsupported" error that SQE surfaces as a user-readable message. Hadoop catalog returns unsupported for register (filesystem-discovered tables have no catalog entry to write).
- **Authorization.** Procedures call into `sqe-policy` before executing. The privilege check defers to the same model used by CREATE TABLE / DROP TABLE: a session can `register_table` into namespace N iff it would be allowed to `CREATE TABLE` in N. No new privilege keyword; existing GRANT vocabulary covers it.
- **Documentation.** Add a section to `docs/sql/` describing each procedure, its argument schema, the per-backend support matrix, and recovery / migration recipes. Update the SQL reference with the `CALL` grammar.

Non-goals:

- **No new user-defined procedure DDL.** The `CALL` parser will accept any procedure name, but only the `system.*` namespace dispatches today. User-defined procedures are deferred.
- **No data-rewriting procedures in v1.** `rewrite_data_files`, `expire_snapshots`, `rewrite_manifests` are useful but tangled with the write path. Land the framework first; layer them in as separate changes once the v1 surface is stable.
- **No async / job model.** Every v1 procedure is fast (catalog metadata write). Long-running procedures (compactions) will need a job tracking model; that's deferred and will require its own design discussion.

## Impact

- **Affected specs**: `sql-extensions/` (new requirement set under this change), `catalog-backends/` (per-backend support matrix added).
- **Affected code**: `crates/sqe-sql`, `crates/sqe-coordinator` (new `procedures` module), `crates/sqe-catalog` (procedure-dispatch helpers if needed), tests in `tests/integration/`.
- **Backward compatibility**: additive. The grammar extension recognizes `CALL system.*` that previously parsed as a syntax error. Existing SQL is unaffected.
- **Runtime surface**: zero perf cost when procedures are not used. Procedures bypass the optimizer entirely; they're invoked through the same path as catalog DDL.

## Rollback strategy

The change is contained to the parser, a new coordinator module, and tests. Rolling back removes the grammar extension and the procedures module; existing queries continue to work because nothing in the planner or executor depends on procedures. If a per-backend bug surfaces post-release, the specific procedure can be feature-gated off without removing the framework.
