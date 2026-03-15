================================================================================
dbt Core Compatibility: What SQE Needs
================================================================================

dbt Core talks to databases through Python adapter plugins. Each adapter must:

1. CONNECT   — Python DB-API 2.0 or ODBC connection
2. METADATA  — Discover catalogs, schemas, tables, columns
3. DDL       — CREATE TABLE AS, CREATE VIEW AS, DROP, ALTER, RENAME
4. DML       — INSERT INTO, MERGE INTO (for incremental)
5. QUERY     — Standard SELECT (already works)

Here's where SQE stands today vs. what dbt needs:

┌──────────────────────────┬────────────────────┬───────────────────────────────┐
│ dbt Requirement          │ SQE Status         │ Gap / Notes                   │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ Python connection        │ ✅ via ADBC        │ adbc_driver_flightsql has     │
│                          │    Flight SQL      │ DB-API 2.0 interface          │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ getCatalogs/getSchemas/  │ ✅ Implemented     │ SHOW CATALOGS/SCHEMAS/TABLES  │
│ getTables/getColumns     │                    │ + Flight SQL metadata         │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ SELECT queries           │ ✅ Implemented     │ Full DataFusion SQL support   │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ CREATE VIEW AS SELECT    │ ✅ Implemented     │ Via Polaris REST view API     │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ DROP VIEW [IF EXISTS]    │ ✅ Implemented     │ Via Polaris REST view API     │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ CREATE TABLE AS SELECT   │ ✅ Implemented     │ iceberg-rust 0.8 write path   │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ CREATE OR REPLACE TABLE  │ ✅ Implemented     │ DROP IF EXISTS + CTAS         │
│ AS SELECT                │                    │                               │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ DROP TABLE [IF EXISTS]   │ ✅ Implemented     │ Polaris REST catalog          │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ ALTER TABLE RENAME       │ ✅ Implemented     │ Polaris REST catalog          │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ CREATE/DROP SCHEMA       │ ✅ Implemented     │ Polaris namespace operations  │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ INSERT INTO SELECT       │ ✅ Implemented     │ iceberg-rust 0.8 fast append  │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ MERGE INTO               │ 🔜 Blocked        │ Needs iceberg-rust            │
│                          │                    │ OverwriteAction (see          │
│                          │                    │ row-level-writes.md)          │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ DELETE FROM (with pred.) │ 🔜 Blocked        │ Needs iceberg-rust            │
│                          │                    │ OverwriteAction               │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ UPDATE                   │ 🔜 Blocked        │ Needs iceberg-rust            │
│                          │                    │ OverwriteAction               │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ information_schema       │ ✅ Implemented     │ Virtual tables/columns/       │
│                          │                    │ schemata providers            │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ Transactions (BEGIN/     │ ⚠️ N/A            │ Iceberg gives atomic commits  │
│ COMMIT)                  │                    │ per statement; multi-stmt     │
│                          │                    │ txns not needed for dbt       │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ Seeds (batch INSERT)     │ ✅ Via INSERT INTO │ dbt adapter can batch rows    │
│                          │                    │ into INSERT INTO SELECT       │
├──────────────────────────┼────────────────────┼───────────────────────────────┤
│ dbt-sqe Python adapter   │ ❌ Not started     │ ~1500-2500 lines Python       │
│                          │                    │ (see design below)            │
└──────────────────────────┴────────────────────┴───────────────────────────────┘

Summary: All DDL/DML except row-level writes (MERGE/DELETE/UPDATE) is implemented.
The remaining blockers are:
  1. MERGE INTO / DELETE FROM / UPDATE — waiting on iceberg-rust upstream
     (tracked PRs: #2185, #2203, #2219, #1987 — see docs/row-level-writes.md)
  2. dbt-sqe Python adapter — can be built now with append-only incremental;
     merge/delete+insert strategies depend on #1 above


================================================================================
Two Paths to dbt Compatibility
================================================================================

PATH A: Native dbt-sqe adapter (recommended)
─────────────────────────────────────────────
Build a custom dbt adapter plugin that talks to SQE via ADBC Flight SQL.

  dbt Core ←→ dbt-sqe adapter (Python) ←→ ADBC Flight SQL ←→ SQE

Pros:
  + Full control over SQL generation and materialization macros
  + No Trino baggage or protocol translation overhead
  + Arrow-native wire format (ADBC) — no JDBC serialization
  + Can tailor materializations to Iceberg-specific capabilities
  + Clean, minimal dependency chain

Cons:
  - Must implement adapter plugin (~1500-2500 lines Python)
  - Must implement information_schema (or equivalent virtual schema)
  - Must support CTAS, DROP, MERGE in SQE's SQL layer
  - dbt adapter certification process (if you want "trusted" status)


PATH B: Trino compat layer + dbt-trino (shortcut)
──────────────────────────────────────────────────
Use the trino-compat wire protocol adapter and the existing dbt-trino adapter.

  dbt Core ←→ dbt-trino adapter ←→ Trino HTTP protocol ←→ SQE trino-compat

Pros:
  + Zero Python adapter code to write
  + dbt-trino is mature, battle-tested, handles Iceberg materializations
  + dbt-trino already supports: CTAS, CREATE OR REPLACE, MERGE, 
    delete+insert, insert-overwrite, rename, drop
  + Gets you running fastest

Cons:
  - Trino wire protocol is complex (HTTP pagination, session properties,
    transaction semantics, error format)
  - Must faithfully implement Trino's information_schema views
  - Trino SQL dialect has quirks dbt-trino expects (ARRAY[], properties map)
  - Performance: HTTP JSON wire format instead of Arrow-native ADBC
  - You become an ongoing compat target for dbt-trino updates
  - Maintaining two wire protocols (Flight SQL + Trino HTTP)

Recommendation: PATH A. The Trino compat layer is a much larger surface area
to maintain than a dbt adapter. A dbt adapter is ~2000 lines of Python with
well-documented interfaces. The Trino wire protocol is a moving target.


================================================================================
FILE: openspec/specs/dbt-adapter/spec.md    (new spec domain)
================================================================================

# dbt-adapter Specification

## Purpose

Enable dbt Core to use SQE as a data platform via a native Python adapter
plugin (dbt-sqe) that connects over ADBC Arrow Flight SQL.

## Requirements

### Requirement: dbt connection via ADBC Flight SQL

The system SHALL be accessible from dbt Core via a Python adapter plugin
that uses adbc_driver_flightsql for connectivity.

#### Scenario: dbt connection profile

- **GIVEN** a dbt profile configured with:
  ```yaml
  my_project:
    target: dev
    outputs:
      dev:
        type: sqe
        host: localhost
        port: 50051
        user: jacob
        password: "{{ env_var('SQE_PASSWORD') }}"
        catalog: production
        schema: finance
        threads: 4
  ```
- **WHEN** dbt runs `dbt debug`
- **THEN** the connection succeeds and reports the SQE version

### Requirement: Catalog metadata discovery

The system SHALL support metadata queries that dbt uses to discover existing
objects (catalogs, schemas, tables, columns, views).

#### Scenario: dbt resolves existing tables

- **GIVEN** tables exist in `production.finance`
- **WHEN** dbt runs `dbt run` and resolves `{{ ref('stg_transactions') }}`
- **THEN** the adapter queries SQE metadata to determine if the table exists
- **AND** returns schema information (column names, types)

#### Scenario: information_schema queries

- **GIVEN** dbt macros query `information_schema.tables` and
  `information_schema.columns`
- **WHEN** these queries are executed
- **THEN** SQE returns virtual information_schema results derived from
  Iceberg catalog metadata


================================================================================
FILE: openspec/specs/write-path/spec.md    (new spec domain)
================================================================================

# write-path Specification

## Purpose

Support write operations required for data transformation workflows: CREATE
TABLE AS SELECT, INSERT INTO, MERGE INTO, DELETE, DROP, and ALTER TABLE.
These are essential for dbt materializations and general ETL.

## Requirements

### Requirement: CREATE TABLE AS SELECT (CTAS)

The system SHALL support creating new Iceberg tables from query results.

#### Scenario: dbt table materialization

- **GIVEN** an authenticated user with write permissions
- **WHEN** the user submits:
  ```sql
  CREATE TABLE production.finance.monthly_totals AS
  SELECT region, month, SUM(amount) as total
  FROM production.finance.transactions
  GROUP BY region, month
  ```
- **THEN** a new Iceberg table is created via Polaris REST
- **AND** query results are written as Parquet files to S3
- **AND** the table is registered in the catalog

### Requirement: CREATE OR REPLACE TABLE AS SELECT

The system SHALL support atomic table replacement, creating a new snapshot
that fully replaces the table contents.

#### Scenario: dbt full-refresh table materialization

- **GIVEN** an existing table `finance.monthly_totals`
- **WHEN** the user submits `CREATE OR REPLACE TABLE finance.monthly_totals AS SELECT ...`
- **THEN** a new Iceberg snapshot is created with the new data
- **AND** the old snapshot remains accessible via time-travel
- **AND** concurrent readers see either the old or new version (never partial)

### Requirement: INSERT INTO SELECT

The system SHALL support inserting query results into existing Iceberg tables.

#### Scenario: dbt incremental append

- **GIVEN** an existing table `finance.daily_events`
- **WHEN** the user submits `INSERT INTO finance.daily_events SELECT ... WHERE date = '2026-03-13'`
- **THEN** new data files are written and a new snapshot is committed

### Requirement: MERGE INTO

The system SHALL support the MERGE statement for conditional insert/update/delete
based on a join condition.

#### Scenario: dbt incremental merge

- **GIVEN** an existing table `finance.dim_customers`
- **WHEN** the user submits:
  ```sql
  MERGE INTO finance.dim_customers AS target
  USING staging.new_customers AS source
  ON target.customer_id = source.customer_id
  WHEN MATCHED THEN UPDATE SET name = source.name, updated_at = source.updated_at
  WHEN NOT MATCHED THEN INSERT (customer_id, name, updated_at)
    VALUES (source.customer_id, source.name, source.updated_at)
  ```
- **THEN** existing rows are updated and new rows are inserted atomically

### Requirement: DELETE FROM with predicate

The system SHALL support deleting rows matching a predicate, using Iceberg's
position delete or equality delete mechanisms.

#### Scenario: dbt delete+insert incremental strategy

- **GIVEN** an existing table `finance.daily_events`
- **WHEN** the user submits `DELETE FROM finance.daily_events WHERE date = '2026-03-13'`
- **THEN** matching rows are marked as deleted (position/equality delete files)
- **AND** a new snapshot is committed

### Requirement: DROP TABLE

The system SHALL support dropping Iceberg tables via the catalog.

#### Scenario: dbt clean up

- **GIVEN** a table `finance.tmp_staging`
- **WHEN** the user submits `DROP TABLE finance.tmp_staging`
- **THEN** the table is removed from Polaris catalog
- **AND** data files are optionally purged (configurable)

### Requirement: DROP TABLE IF EXISTS

The system SHALL support `DROP TABLE IF EXISTS` without error for non-existent
tables.

#### Scenario: Idempotent drop

- **GIVEN** no table named `finance.nonexistent`
- **WHEN** the user submits `DROP TABLE IF EXISTS finance.nonexistent`
- **THEN** no error is raised

### Requirement: ALTER TABLE RENAME

The system SHALL support renaming tables within a namespace.

#### Scenario: dbt rename materialization

- **GIVEN** a table `finance.monthly_totals__dbt_tmp`
- **WHEN** the user submits `ALTER TABLE finance.monthly_totals__dbt_tmp RENAME TO finance.monthly_totals`
- **THEN** the table is renamed in Polaris catalog


================================================================================
FILE: openspec/changes/phase-2c-dbt/proposal.md
================================================================================

# Proposal: Phase 2c — dbt Core Compatibility

## Summary

Add write-path SQL support (CTAS, INSERT INTO, MERGE INTO, DELETE, DROP, ALTER
RENAME) and a native dbt adapter plugin (dbt-sqe) to enable dbt Core as a
transformation layer on top of SQE.

## Motivation

dbt is the standard transformation tool for analytics engineering. Without dbt
support, SQE is limited to read-only analytical queries. With it, SQE becomes
a full data transformation platform: ingest via Polaris, transform via dbt,
query via BI tools — all through the same engine with the same auth model.

## What Changes

### SQE Engine (Rust)

1. **Write path** (sqe-catalog + sqe-coordinator):
   - CTAS: parse → execute query → write Parquet via iceberg-rust writer →
     commit to Polaris
   - CREATE OR REPLACE: new snapshot replacing all data
   - INSERT INTO SELECT: append to existing table
   - MERGE INTO: read-modify-write cycle with Iceberg atomic commits
   - DELETE FROM: position delete or equality delete files
   - DROP TABLE: Polaris REST delete + optional file purge
   - ALTER TABLE RENAME: Polaris REST rename

2. **information_schema virtual schema** (sqe-coordinator):
   - Virtual tables derived from Flight SQL metadata:
     `information_schema.tables`, `information_schema.columns`,
     `information_schema.schemata`
   - Registered as a special TableProvider that queries Polaris metadata
   - Respects per-user access (user only sees tables they can access)

### dbt Adapter (Python)

3. **dbt-sqe** Python package:
   - Connection via `adbc_driver_flightsql.dbapi`
   - Credential passthrough (username/password → Flight SQL handshake)
   - SQLAdapter subclass with Iceberg-aware materializations
   - Macros for: table, view, incremental (append, delete+insert, merge)
   - Seeds via batch INSERT
   - Snapshots via MERGE

## Dependencies

- Phase 1 (query engine + auth + Flight SQL) must be complete
- Phase 2 (views + write path basics) should be complete
- iceberg-rust 0.8.0 write support (partitioned insert, fanout writers)

## Success Criteria

- `dbt run` with table and view materializations works end-to-end
- `dbt run` with incremental (append) works
- `dbt run` with incremental (merge) works
- `dbt seed` loads CSV data
- `dbt test` runs assertion queries
- `dbt debug` connects and validates
- `dbt docs generate` produces catalog metadata
- All dbt operations run as the authenticated user (OIDC passthrough)

## Trino DCAF Branch Reference

No equivalent. This is new capability.

## Rollback Strategy

dbt-sqe is an independent Python package. SQE write-path additions are
backwards-compatible — existing read-only queries are unaffected.

## Timeline

4-6 weeks (can partially overlap with Phase 3 distributed execution)


================================================================================
FILE: openspec/changes/phase-2c-dbt/design.md
================================================================================

# Design: Phase 2c — dbt Core Compatibility

## dbt-sqe Adapter Architecture

```
dbt Core
  │
  ▼
dbt-sqe (Python package: dbt-sqe)
  │
  ├── SQEConnectionManager
  │     Uses: adbc_driver_flightsql.dbapi.connect()
  │     Auth:  username + password → Flight SQL handshake → Keycloak
  │     Returns: DB-API 2.0 connection + cursor
  │
  ├── SQEAdapter (extends SQLAdapter)
  │     Implements:
  │       - list_relations_without_caching()  → SHOW TABLES / info_schema
  │       - get_columns_in_relation()         → info_schema.columns
  │       - create_schema()                   → CREATE SCHEMA
  │       - drop_schema()                     → DROP SCHEMA
  │       - rename_relation()                 → ALTER TABLE RENAME
  │       - truncate_relation()               → DELETE FROM (no pred)
  │
  ├── SQEColumn (extends Column)
  │     Maps Arrow/Iceberg types → dbt column types
  │
  └── macros/
        ├── adapters.sql         → SQL generation overrides
        ├── materializations/
        │     ├── table.sql      → CREATE [OR REPLACE] TABLE AS
        │     ├── view.sql       → CREATE [OR REPLACE] VIEW AS
        │     └── incremental.sql → INSERT/MERGE/DELETE+INSERT
        └── catalog.sql          → metadata for dbt docs
```

## Connection Manager

```python
from adbc_driver_flightsql.dbapi import connect as flight_connect
from adbc_driver_manager import DatabaseOptions

class SQEConnectionManager(SQLConnectionManager):
    TYPE = "sqe"

    @classmethod
    def open(cls, connection):
        credentials = connection.credentials
        uri = f"grpc://{credentials.host}:{credentials.port}"

        handle = flight_connect(
            uri,
            db_kwargs={
                DatabaseOptions.USERNAME.value: credentials.user,
                DatabaseOptions.PASSWORD.value: credentials.password,
            },
        )
        connection.handle = handle
        connection.state = "open"
        return connection

    def cancel(self, connection):
        connection.handle.close()

    def execute(self, sql, auto_begin=False, fetch=False):
        cursor = self.get_thread_connection().handle.cursor()
        cursor.execute(sql)
        if fetch:
            # ADBC returns Arrow tables — convert to agate for dbt
            table = cursor.fetch_arrow_table()
            return self._arrow_to_agate(table)
        return cursor
```

## information_schema Virtual Schema

SQE must respond to queries like:

```sql
SELECT table_catalog, table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'finance'
```

### Implementation in sqe-coordinator:

Register a virtual `information_schema` schema with TableProviders that pull
metadata from the Polaris REST catalog:

```rust
/// Virtual table provider that resolves metadata from Polaris
struct InfoSchemaTablesProvider {
    catalog_client: Arc<CatalogClient>,
    session: Arc<Session>,  // for user-scoped access
}

impl TableProvider for InfoSchemaTablesProvider {
    fn schema(&self) -> SchemaRef {
        // Standard information_schema.tables columns:
        // table_catalog, table_schema, table_name, table_type
        Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]))
    }

    async fn scan(&self, ...) -> Result<Arc<dyn ExecutionPlan>> {
        // Call Polaris REST: listNamespaces → listTables per namespace
        // Filter by user's access (Polaris does this via bearer token)
        // Return as Arrow RecordBatch
    }
}
```

Similar providers for:
- `information_schema.columns` → Iceberg schema → column details
- `information_schema.schemata` → Polaris namespaces

These benefit from the L1 catalog cache — repeated metadata queries within
a dbt run (which can be hundreds) are served from cache.

## Write Path in SQE

### CTAS Flow

```
CREATE TABLE finance.totals AS SELECT region, SUM(amount) FROM ...

1. Parse → detect CTAS statement
2. Execute the SELECT portion → get Arrow RecordBatches
3. Infer Iceberg schema from Arrow schema
4. Create table in Polaris: POST /v1/namespaces/finance/tables
     { name: "totals", schema: {...}, partition-spec: {...} }
5. Write RecordBatches as Parquet files via iceberg-rust DataFileWriter
     → fanout writer if partitioned (iceberg-rust 0.8.0)
     → upload to S3 with user's token
6. Commit snapshot to Polaris: POST /v1/tables/finance.totals/commits
     { adds: [data_file_1, data_file_2, ...] }
```

### MERGE INTO Flow

```
MERGE INTO target USING source ON condition
  WHEN MATCHED THEN UPDATE SET ...
  WHEN NOT MATCHED THEN INSERT ...

1. Parse → detect MERGE statement
2. Scan target table (full or filtered by MERGE predicate)
3. Scan source (subquery or table)
4. Join on condition in DataFusion
5. Classify rows: matched-update, matched-delete, not-matched-insert
6. For updates/deletes: write position delete files for affected rows
7. For inserts + updates: write new data files with the new/updated rows
8. Commit atomically: add new data files + delete files in one snapshot
```

### Key Design Decision: Copy-on-Write vs Merge-on-Read

For MERGE/DELETE, Iceberg supports two approaches:
- **Copy-on-Write (CoW):** Rewrite entire data files excluding deleted rows
- **Merge-on-Read (MoR):** Write small delete files, merge at read time

Recommendation: Start with **MoR using position deletes** (simpler write path).
iceberg-rust 0.8.0 supports reading position + equality delete files. Later,
add compaction to merge delete files back into data files.

For dbt's typical pattern (incremental with merge on a subset of data), MoR is
more efficient because only changed rows generate delete files.

## dbt Materializations

### table materialization

```sql
-- dbt-sqe generates:
{% materialization table, adapter='sqe' %}
  {%- set existing = adapter.get_relation(this.database, this.schema, this.identifier) -%}

  {% if existing %}
    -- Atomic replacement via Iceberg
    {% call statement('main') %}
      CREATE OR REPLACE TABLE {{ this }} AS (
        {{ sql }}
      )
    {% endcall %}
  {% else %}
    {% call statement('main') %}
      CREATE TABLE {{ this }} AS (
        {{ sql }}
      )
    {% endcall %}
  {% endif %}

  {{ return({'relations': [this]}) }}
{% endmaterialization %}
```

### incremental materialization (merge strategy)

```sql
{% materialization incremental, adapter='sqe' %}
  {%- set strategy = config.get('incremental_strategy', 'append') -%}
  {%- set unique_key = config.get('unique_key') -%}

  {% if strategy == 'append' %}
    INSERT INTO {{ this }} (
      {{ sql }}
    )

  {% elif strategy == 'merge' %}
    MERGE INTO {{ this }} AS DBT_INTERNAL_DEST
    USING ({{ sql }}) AS DBT_INTERNAL_SOURCE
    ON {{ unique_key_condition }}
    WHEN MATCHED THEN UPDATE SET {{ update_columns }}
    WHEN NOT MATCHED THEN INSERT {{ insert_columns }}

  {% elif strategy == 'delete+insert' %}
    DELETE FROM {{ this }}
    WHERE {{ unique_key }} IN (SELECT {{ unique_key }} FROM ({{ sql }}));

    INSERT INTO {{ this }} (
      {{ sql }}
    )
  {% endif %}
{% endmaterialization %}
```

### view materialization

```sql
-- Already supported via Phase 2 Iceberg views
CREATE OR REPLACE VIEW {{ this }} AS (
  {{ sql }}
)
```

## Type Mapping: Arrow ↔ Iceberg ↔ dbt

┌──────────────────┬──────────────────┬──────────────────┐
│ Arrow Type        │ Iceberg Type     │ dbt Type         │
├──────────────────┼──────────────────┼──────────────────┤
│ Utf8             │ string           │ VARCHAR           │
│ Int32            │ int              │ INTEGER           │
│ Int64            │ long             │ BIGINT            │
│ Float32          │ float            │ FLOAT             │
│ Float64          │ double           │ DOUBLE            │
│ Boolean          │ boolean          │ BOOLEAN           │
│ Date32           │ date             │ DATE              │
│ TimestampMicro   │ timestamptz      │ TIMESTAMP         │
│ Decimal128(p,s)  │ decimal(p,s)     │ NUMERIC(p,s)      │
│ Binary           │ binary           │ BINARY            │
│ Struct           │ struct           │ STRUCT (nested)   │
│ List             │ list             │ ARRAY             │
│ Map              │ map              │ MAP               │
└──────────────────┴──────────────────┴──────────────────┘

## Interaction with Caching (Phase 2b)

dbt runs are particularly cache-friendly:
- Same tables referenced many times across models → L1/L2/L3 hot
- Sequential model execution → previous model's output cached for next
- `dbt test` queries same tables as `dbt run` → L5 result cache hits
- Metadata-heavy workflow → information_schema backed by L1 catalog cache

Expected impact: a dbt run with 50 models that would take 10 minutes without
caching could drop to 3-4 minutes with warm L2/L3/L4 caches.

## Interaction with Security (Phase 5)

dbt runs as the authenticated user. Policy enforcement applies:
- If `analyst` role can't see column `ssn`, dbt models selecting `*` from
  that table won't include `ssn` in the output
- CTAS respects the user's visible schema — the new table only contains
  columns the user can see
- MERGE operates on the user's view of the data

This is consistent and correct: dbt transforms what the user can see.


================================================================================
FILE: openspec/changes/phase-2c-dbt/tasks.md
================================================================================

# Tasks: Phase 2c — dbt Core Compatibility

## Phase 2c.1 — Write Path: DDL (sqe-sql + sqe-coordinator + sqe-catalog)

- [x] 2c.1.1 Parse CTAS: CREATE [OR REPLACE] TABLE ... AS SELECT
- [x] 2c.1.2 Parse DROP TABLE [IF EXISTS]
- [x] 2c.1.3 Parse ALTER TABLE ... RENAME TO
- [x] 2c.1.4 Parse CREATE SCHEMA / DROP SCHEMA
- [x] 2c.1.5 Implement CTAS execution: query → infer schema → create table → write → commit
- [x] 2c.1.6 Implement CREATE OR REPLACE TABLE: DROP IF EXISTS + CTAS
- [x] 2c.1.7 Implement DROP TABLE: Polaris REST catalog via iceberg-rust
- [x] 2c.1.8 Implement ALTER TABLE RENAME: Polaris REST catalog via iceberg-rust
- [x] 2c.1.9 Implement CREATE/DROP SCHEMA: Polaris namespace operations
- [ ] 2c.1.10 Integration test: CTAS creates Iceberg table readable by subsequent SELECT
- [ ] 2c.1.11 Integration test: CREATE OR REPLACE atomically swaps table contents

## Phase 2c.2 — Write Path: DML (sqe-sql + sqe-coordinator + sqe-catalog)

- [x] 2c.2.1 Parse INSERT INTO ... SELECT
- [x] 2c.2.2 Parse DELETE FROM ... WHERE
- [x] 2c.2.3 Parse MERGE INTO ... USING ... ON ... WHEN MATCHED/NOT MATCHED
- [x] 2c.2.4 Implement INSERT INTO: execute SELECT → write new data files → commit
- [ ] 2c.2.5 Implement DELETE FROM: 🔜 blocked on iceberg-rust OverwriteAction
- [ ] 2c.2.6 Implement MERGE INTO: 🔜 blocked on iceberg-rust OverwriteAction
- [ ] 2c.2.7 Integration test: INSERT INTO appends data correctly
- [ ] 2c.2.8 Integration test: DELETE FROM removes matching rows
- [ ] 2c.2.9 Integration test: MERGE INTO updates existing + inserts new

## Phase 2c.3 — information_schema (sqe-coordinator)

- [x] 2c.3.1 Implement InfoSchemaTablesProvider (virtual TableProvider)
- [x] 2c.3.2 Implement InfoSchemaColumnsProvider
- [x] 2c.3.3 Implement InfoSchemaSchemataProvider
- [x] 2c.3.4 Register information_schema as virtual schema per session
- [ ] 2c.3.5 Integration test: SELECT * FROM information_schema.tables WHERE table_schema = 'x'
- [ ] 2c.3.6 Integration test: information_schema respects user access (different results per user)

## Phase 2c.4 — dbt-sqe Adapter (Python)

- [ ] 2c.4.1 Scaffold dbt-sqe package (dbt-database-adapter-scaffold)
- [ ] 2c.4.2 Implement SQEConnectionManager (ADBC Flight SQL connect)
- [ ] 2c.4.3 Implement SQECredentials (host, port, user, password, catalog, schema)
- [ ] 2c.4.4 Implement SQEAdapter (list_relations, get_columns, create/drop schema, rename)
- [ ] 2c.4.5 Implement SQEColumn (Arrow ↔ dbt type mapping)
- [ ] 2c.4.6 Implement table materialization macro
- [ ] 2c.4.7 Implement view materialization macro
- [ ] 2c.4.8 Implement incremental materialization: append strategy
- [ ] 2c.4.9 Implement incremental materialization: delete+insert strategy (blocked on DELETE)
- [ ] 2c.4.10 Implement incremental materialization: merge strategy (blocked on MERGE)
- [ ] 2c.4.11 Implement seed macro (batch INSERT from CSV)
- [ ] 2c.4.12 Implement catalog generation macro (for dbt docs)
- [ ] 2c.4.13 Implement snapshot materialization (SCD Type 2 via MERGE — blocked on MERGE)

## Phase 2c.5 — End-to-End dbt Testing

- [ ] 2c.5.1 Create sample dbt project with staging + marts models
- [ ] 2c.5.2 Test: `dbt debug` connects successfully
- [ ] 2c.5.3 Test: `dbt seed` loads test CSV data
- [ ] 2c.5.4 Test: `dbt run` with table materialization
- [ ] 2c.5.5 Test: `dbt run` with view materialization
- [ ] 2c.5.6 Test: `dbt run` with incremental (append)
- [ ] 2c.5.7 Test: `dbt run` with incremental (merge) — blocked on MERGE
- [ ] 2c.5.8 Test: `dbt run` with incremental (delete+insert) — blocked on DELETE
- [ ] 2c.5.9 Test: `dbt run --full-refresh` with CREATE OR REPLACE
- [ ] 2c.5.10 Test: `dbt test` runs assertion queries
- [ ] 2c.5.11 Test: `dbt docs generate` produces catalog JSON
- [ ] 2c.5.12 Test: `dbt snapshot` creates SCD Type 2 table — blocked on MERGE
- [ ] 2c.5.13 Test: dbt run with different users sees policy-filtered results
- [ ] 2c.5.14 Test: concurrent dbt runs from different users don't conflict