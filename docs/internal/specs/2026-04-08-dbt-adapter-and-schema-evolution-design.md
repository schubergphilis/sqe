# Design: dbt-sqe Adapter and ALTER TABLE Schema Evolution

**Date:** 2026-04-08
**Scope:** Two independent features — ALTER TABLE full schema evolution (7.3) and dbt-sqe Python adapter (7.1)

---

## 1. ALTER TABLE Schema Evolution (7.3)

### Operations

| SQL | Behavior |
|---|---|
| `ALTER TABLE t ADD COLUMN c INT` | Append optional NestedField |
| `ALTER TABLE t ADD COLUMN c INT NOT NULL` | Append required NestedField |
| `ALTER TABLE t DROP COLUMN c` | Remove field by name |
| `ALTER TABLE t RENAME COLUMN c TO d` | Update field name, preserve field ID |
| `ALTER TABLE t ALTER COLUMN c SET NOT NULL` | optional → required |
| `ALTER TABLE t ALTER COLUMN c DROP NOT NULL` | required → optional |
| `ALTER TABLE t ALTER COLUMN c SET DATA TYPE BIGINT` | Widen type (safe promotions only) |

Safe type promotions per Iceberg spec: `INT→BIGINT`, `FLOAT→DOUBLE`, `DECIMAL(p,s)→DECIMAL(p',s)` where p'≥p.

### Code Flow

```
sqlparser-rs AlterTableOperation
  → classifier.rs: new StatementKind::AlterSchema
    → query_handler.rs: route to catalog_ops.alter_table_schema()
      → catalog_ops.rs: load table → modify schema → catalog.update_table(TableCommit)
```

### Files to Modify

| File | Change |
|---|---|
| `crates/sqe-sql/src/classifier.rs` | Add `AlterSchema(Box<Statement>)` variant. Match `AddColumn`, `DropColumn`, `RenameColumn`, `AlterColumn` operations → `AlterSchema`. Keep `RenameTable` → `Rename` as-is. |
| `crates/sqe-coordinator/src/query_handler.rs` | Add `StatementKind::AlterSchema(stmt) => self.catalog_ops.alter_table_schema(session, stmt).await` |
| `crates/sqe-coordinator/src/catalog_ops.rs` | New `pub async fn alter_table_schema(&self, session: &Session, stmt: &Statement) -> Result<()>`. Loads table via `catalog.load_table()`, reads current schema via `table.metadata().current_schema()`, applies operations, builds new schema, commits via `catalog.update_table(TableCommit)`. |

### Implementation Details

**ADD COLUMN:** Compute `max_field_id` from current schema fields, create `NestedField::optional(max_id + 1, name, iceberg_type)` (or `::required` if NOT NULL). Reuse `sql_type_to_arrow()` from write_handler for type conversion, then convert Arrow type to Iceberg type.

**DROP COLUMN:** Filter current fields by name. Error if field doesn't exist. Error if field is part of partition spec or sort order (Iceberg constraint).

**RENAME COLUMN:** Find field by name, create new NestedField with same ID and type but new name.

**ALTER COLUMN SET/DROP NOT NULL:** Find field by name, toggle required/optional flag.

**ALTER COLUMN SET DATA TYPE:** Validate promotion is safe per Iceberg type promotion rules. Error on unsafe casts.

**Error handling:** All operations produce `SqeError::SchemaEvolution(msg)` with clear messages like "Cannot drop column 'x': column is part of partition spec".

### Tests

- Unit tests in classifier for all ALTER TABLE variants
- Unit test in catalog_ops for schema modification logic (mock catalog)
- Integration test: `ALTER TABLE ADD COLUMN`, verify via `information_schema.columns`
- Integration test: `ALTER TABLE DROP COLUMN`, verify column gone
- Integration test: `ALTER TABLE RENAME COLUMN`, verify name changed
- Integration test: reject unsafe type promotion
- Integration test: reject dropping partition column

---

## 2. dbt-sqe Adapter (7.1)

### Architecture

```
dbt-core (Python)
  → dbt-sqe adapter (Python, namespace package)
    → ADBC Flight SQL (gRPC)
      → SQE Coordinator (Rust)
```

### Location

`adapters/dbt-sqe/` in the SQE monorepo. Installable via `pip install -e adapters/dbt-sqe`.

### Dependencies

```
dbt-common>=1,<2
dbt-adapters>=1.16,<2
dbt-core>=1.8.0
adbc-driver-flightsql>=0.10.0
pyarrow>=14.0.0
agate>=1.7.0
```

### Connection

ADBC Flight SQL via `adbc_driver_flightsql.dbapi.connect()`. AUTOCOMMIT mode — Iceberg provides atomic per-statement semantics.

Arrow → agate bridge: `cursor.fetch_arrow_table().to_pydict()` → `agate.Table(rows, column_names, column_types)` for dbt metadata introspection.

### profiles.yml

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: sqe
      host: localhost
      port: 50051
      user: admin
      password: "{{ env_var('SQE_PASSWORD') }}"
      catalog: warehouse
      schema: analytics
      threads: 4
```

`catalog` is aliased to dbt's `database` field via `_ALIASES = {"catalog": "database"}`.

### File Structure

```
adapters/dbt-sqe/
├── setup.cfg                        # Package metadata, deps
├── setup.py                         # Minimal (delegates to setup.cfg)
├── dbt/
│   ├── __init__.py                  # Empty (namespace package)
│   ├── adapters/
│   │   ├── __init__.py              # Empty (namespace package)
│   │   └── sqe/
│   │       ├── __init__.py          # Plugin = AdapterPlugin(adapter, credentials, include_path)
│   │       ├── __version__.py       # version = "1.0.0"
│   │       ├── connections.py       # SQEConnectionManager + SQECredentials
│   │       ├── impl.py             # SQEAdapter
│   │       ├── column.py           # SQEColumn
│   │       └── relation.py         # SQERelation
│   └── include/
│       ├── __init__.py              # Empty (namespace package)
│       └── sqe/
│           ├── __init__.py          # PACKAGE_PATH = os.path.dirname(__file__)
│           ├── dbt_project.yml      # name: dbt_sqe, config-version: 2
│           ├── sample_profiles.yml
│           └── macros/
│               ├── adapters.sql     # Metadata + DDL macros
│               ├── catalog.sql      # dbt docs generate
│               └── materializations/
│                   ├── table.sql
│                   ├── view.sql
│                   ├── incremental.sql
│                   └── seed.sql
```

### Python Classes

**SQECredentials** (dataclass, extends `Credentials`):
- Fields: `host`, `port`, `user`, `password`, `database` (catalog), `schema`, `threads`
- `type` property → `"sqe"`
- `_connection_keys()` → `("host", "port", "database", "schema", "user")`
- `_ALIASES = {"catalog": "database"}`

**SQEConnectionManager** (extends `SQLConnectionManager`):
- `TYPE = "sqe"`
- `open(cls, connection)`: Creates ADBC Flight SQL connection with `uri=f"grpc://{host}:{port}"`, passes username/password as connection options
- `cancel(self, connection)`: Calls `connection.handle.cancel()` if active
- `exception_handler(self, sql)`: Catches `adbc_driver_flightsql` errors, translates to `DbtDatabaseError`
- `get_response(cls, cursor)`: Returns `AdapterResponse(_message="OK", rows_affected=cursor.rowcount)`

**SQEAdapter** (extends `SQLAdapter`):
- `ConnectionManager = SQEConnectionManager`
- `Column = SQEColumn`
- `Relation = SQERelation`
- `date_function()` → `"now()"`
- `valid_incremental_strategies()` → `["append", "delete+insert", "merge"]`
- Type conversion methods: map agate types to SQE SQL types (`Text→VARCHAR`, `Number→DOUBLE`, `Boolean→BOOLEAN`, `DateTime→TIMESTAMP`, `Date→DATE`)

**SQEColumn** (extends `Column`):
- Type label mapping: Arrow/Iceberg names → SQL standard (`long→BIGINT`, `string→VARCHAR`, `double→DOUBLE`, `boolean→BOOLEAN`)
- `is_string()`, `is_integer()`, `is_float()`, `is_numeric()` overrides

**SQERelation** (extends `BaseRelation`):
- `quote_policy` defaults: database=True, schema=True, identifier=True (DataFusion uses `"` quoting)
- `renameable_relations` = `[RelationType.Table]`
- `replaceable_relations` = `[RelationType.Table, RelationType.View]`

### Macros

**adapters.sql — Metadata discovery:**
- `sqe__list_relations_without_caching(schema_relation)` → `SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = '{{ schema }}'`
- `sqe__get_columns_in_relation(relation)` → `SELECT column_name, data_type, ordinal_position, is_nullable FROM information_schema.columns WHERE ...`
- `sqe__list_schemas(database)` → `SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '{{ database }}'`
- `sqe__check_schema_exists(information_schema, schema)` → existence check query

**adapters.sql — DDL generation:**
- `sqe__create_table_as(temporary, relation, sql)` → `CREATE OR REPLACE TABLE {{ relation }} AS ({{ sql }})`
- `sqe__create_view_as(relation, sql)` → `CREATE OR REPLACE VIEW {{ relation }} AS ({{ sql }})`
- `sqe__drop_relation(relation)` → `DROP {{ relation.type }} IF EXISTS {{ relation }}`
- `sqe__rename_relation(from, to)` → `ALTER TABLE {{ from }} RENAME TO {{ to }}`
- `sqe__create_schema(relation)` → `CREATE SCHEMA IF NOT EXISTS {{ relation.schema }}`
- `sqe__drop_schema(relation)` → `DROP SCHEMA IF EXISTS {{ relation.schema }}`
- `sqe__current_timestamp()` → `now()`

**materializations/table.sql:**
- Drop existing → CTAS → done. Use `CREATE OR REPLACE TABLE AS` for atomic swap.

**materializations/view.sql:**
- `CREATE OR REPLACE VIEW AS`. Thin wrapper, dbt's global macro works with minor override.

**materializations/incremental.sql:**
- `append`: `INSERT INTO {{ target }} ({{ sql }})`
- `delete+insert`: `DELETE FROM {{ target }} WHERE {{ unique_key }} IN (SELECT {{ unique_key }} FROM __dbt_tmp); INSERT INTO {{ target }} ({{ sql }})`
- `merge`: `MERGE INTO {{ target }} USING ({{ sql }}) AS __dbt_tmp ON {{ merge_condition }} WHEN MATCHED THEN UPDATE SET {{ update_columns }} WHEN NOT MATCHED THEN INSERT ({{ columns }}) VALUES ({{ values }})`

**materializations/seed.sql:**
- Batch `INSERT INTO ... VALUES (...)` with configurable batch size (default 1000).

**catalog.sql:**
- Query `information_schema.tables` + `information_schema.columns` to produce catalog metadata JSON for `dbt docs generate`.

### Tests

- **Unit tests:** Connection manager open/close, credential parsing, type mapping
- **Integration tests (require running SQE stack):**
  - `dbt debug` — connection validation
  - `dbt run` — table materialization (CTAS)
  - `dbt run` — view materialization
  - `dbt run` — incremental append
  - `dbt run` — incremental merge
  - `dbt run` — incremental delete+insert
  - `dbt seed` — CSV loading
  - `dbt test` — assertion queries
  - `dbt docs generate` — catalog metadata
  - Profile with catalog/schema wiring
