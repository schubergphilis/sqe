## Why

A copy job wants to stage Parquet files and read them back with `read_parquet` as the signed-in user, with no config change per catalog. File TVFs read object storage with the engine's own storage credentials, not the caller's. The only barrier is the prefix allow-list: `[storage.tvf] allowed_object_store_prefixes` and the per-catalog `tvf_prefixes`. A prefix that covers a table's files lets any caller who passes the gate read those files raw, which bypasses row filters, column masks and column restrictions.

The allow-list can already express a staging area per catalog and per user, with one global line and no restart for new catalogs: `s3://<staging-bucket>/{catalog}/{user}/`. The `{catalog}` placeholder expands only when the session catalog answered the caller's own bearer (`tvf_caller_for`, `session_context.rs`), and discovered warehouses are included. What is missing is a guard. Nothing stops an allowed prefix from overlapping a table location, so one misplaced line turns the staging feature into a policy bypass.

## What Changes

- Every allowed object-store prefix (global and per catalog, placeholders included) is guarded against Iceberg table locations. SQE refuses to create or register an Iceberg table whose location overlaps one (CREATE TABLE ... LOCATION, CTAS, register), before any data file is written.
- When SQE loads an Iceberg table whose location overlaps an allowed prefix, it records that location. File TVFs then refuse every path under it, and SQE logs an error naming the table and the prefix. The table stays queryable under its policies.
- Before reading, file TVFs probe the ancestors of the requested path below the prefix root for Iceberg metadata (`metadata/*.metadata.json`), and refuse on a hit. A table SQE has never loaded is caught that way.
- The refusals apply to admin roles and `allow_all_object_stores` too. Inline-credential reads are exempt, because they use the caller's own keys.
- Startup warns about an allowed prefix with no `{user}` placeholder. Such a prefix is shared by every caller who passes the gate.
- Docs: the production guide stops recommending the warehouse bucket as a TVF prefix (done in this MR). The staging recipe follows with the code: one global prefix line, and a staging root outside every catalog's Polaris `allowedLocations`.
- No new config key and no write-access gate. The approved "per-catalog staging prefix" is the existing prefix list plus this guard.
- Hive external tables are out of scope. Their roots must be allowed prefixes for the tables to work, so the same raw-read gap exists for them and is tracked in #886.

## Capabilities

### New Capabilities
- `tvf-prefix-table-overlap`: the guard between engine-credentialed file TVF prefixes and table locations.

### Modified Capabilities
<!-- None in openspec/specs/. -->

## Impact

- `sqe-core/src/config/mod.rs`: `TvfPolicy` gains overlap matching for template prefixes and a check against recorded table locations.
- Iceberg creation paths in `sqe-coordinator` (DDL, CTAS, register) and the Iceberg load paths in `sqe-catalog` (REST, SQLite) call the guard. The file TVF read path gains the ancestor probe, a few LIST calls per statement under a prefix.
- The process-wide set of overlapping locations is new shared state, bounded by the number of overlapping tables.
- An existing deployment whose prefix already covers an Iceberg table sees TVF reads under that table refused. The refusal is intended: those reads bypass policy today.
- Docs: `operations/production-guide.md` (now), `deployment/configuration.md` and `features/read-parquet.md` (with the code).
