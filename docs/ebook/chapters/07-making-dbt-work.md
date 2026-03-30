# Making dbt Work {#sec:dbt}

> dbt doesn't care about your architecture. It cares about list_relations.

With `information_schema`, `SHOW` commands, and namespace resolution in place, the engine could describe itself. Tools could connect, browse schemas, inspect columns. The metadata surface was complete.

Then we ran `dbt run` and nothing happened.

dbt doesn't query your engine the way a human does. It doesn't type SQL and wait for results. It runs a discovery sequence -- a cascade of metadata queries that maps your warehouse before executing a single model. If any step in that sequence returns the wrong shape, the wrong types, or nothing at all, dbt stops. Not with a helpful error. With a Python traceback three screens long.

Making dbt work meant understanding exactly what it asks for, in what order, and what it expects back.


## How dbt Discovers Your Warehouse

dbt's adapter lifecycle starts with `dbt debug`, which validates the connection profile. The adapter opens an ADBC Flight SQL connection with the user's credentials -- the handshake authenticates via OIDC exactly as described in Chapter 4.

Once connected, dbt's machinery starts discovering the warehouse:

1. **Check schema exists**: dbt queries `information_schema.schemata` to verify the target schema exists. If it doesn't, dbt calls `CREATE SCHEMA`.

2. **List existing relations**: dbt queries `information_schema.tables` filtered by schema to find existing tables and views. This determines which models need to be built vs. which already exist and might only need incremental updates.

3. **Get columns for existing tables**: for incremental models, dbt queries `information_schema.columns` to compare the model's output schema against the existing table's schema. Column additions, type changes, and schema drift are detected here.

4. **Execute model SQL**: dbt generates and runs `CREATE TABLE AS SELECT`, `INSERT INTO`, or `CREATE OR REPLACE TABLE AS SELECT` depending on the materialization strategy.

5. **Verify results**: dbt queries `information_schema.tables` again to confirm the model's target table exists after execution.

Every step except step 4 is metadata. dbt spends more time asking "what exists?" than "compute this result." This is why `information_schema` performance matters: a dbt project with 50 models might query metadata hundreds of times in a single run.

The `SessionCatalog` in Polaris caches namespace and table listings, which helps. But the real performance win is that our virtual tables are generated from catalog API responses that are already fast -- Polaris is an in-memory catalog service. The information_schema call graph is: SQL query -> DataFusion resolves virtual table -> provider calls Polaris REST -> Polaris returns metadata -> provider builds Arrow batch -> DataFusion returns results. The Polaris round-trip dominates, and it's typically under 10 milliseconds.

There's a cost we accepted: the `build_columns_table` method loads every table in every namespace to read its schema. For a warehouse with 500 tables, that's 500 `load_table` calls to Polaris. This is fine for dbt's filtered queries (`WHERE table_schema = 'finance'`) because we load all metadata and let DataFusion filter the result, but it's expensive for unfiltered `SELECT * FROM information_schema.columns`. A future optimization would push the filter down into the provider -- detect the `WHERE table_schema = ?` predicate and only load tables from that namespace. We haven't needed it yet. dbt always filters by schema, and DBeaver browses one schema at a time. But it's the kind of optimization that becomes necessary at scale, and the virtual table architecture makes it possible without changing the SQL interface.


## The Connection Profile

dbt connects to SQE through a profile in `~/.dbt/profiles.yml`. The profile specifies the adapter type (`sqe`), host, port, user, password, catalog, and default schema. A typical profile:

```yaml
sqe_warehouse:
  target: dev
  outputs:
    dev:
      type: sqe
      host: localhost
      port: 50051
      user: analyst
      password: "{{ env_var('SQE_PASSWORD') }}"
      catalog: test_warehouse
      schema: finance
```

The `type: sqe` field tells dbt to load the `dbt-sqe` adapter. The password comes from an environment variable -- dbt's Jinja templating handles secrets without putting them in the YAML file. The `catalog` and `schema` fields map directly to the Polaris warehouse and Iceberg namespace. dbt uses these to construct qualified table names: `test_warehouse.finance.my_model`.


## The dbt-sqe Adapter

The Python adapter is the bridge between dbt's framework and SQE's SQL dialect. The design is Path A from our evaluation: a native adapter over ADBC Flight SQL, not a Trino compatibility shim.

```
dbt Core --> dbt-sqe adapter (Python) --> ADBC Flight SQL --> SQE
```

The adapter has four main components:

**SQEConnectionManager** handles the connection lifecycle. It uses `adbc_driver_flightsql.dbapi.connect()` to open an Arrow Flight SQL connection with the user's credentials. The connection speaks the DB-API 2.0 interface that dbt expects.

```python
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
```

**SQEAdapter** implements dbt's discovery methods: `list_relations_without_caching()` queries `information_schema.tables`, `get_columns_in_relation()` queries `information_schema.columns`, `check_schema_exists()` queries `information_schema.schemata`. Each method translates from dbt's abstract relation model to SQL against our virtual tables.

**Materializations** are Jinja SQL templates that generate the right SQL for each model type. The table materialization emits `CREATE OR REPLACE TABLE AS SELECT`. The view materialization emits `CREATE OR REPLACE VIEW AS`. The incremental materialization emits `INSERT INTO` for append strategy, or `MERGE INTO` for merge strategy. Each template maps dbt's abstract model definition to concrete SQL that SQE's parser and planner can handle. The materialization macros are where the adapter's knowledge of SQE's SQL dialect lives -- what statements are supported, what syntax they use, what Iceberg-specific options are available.

The incremental materialization deserves extra attention. dbt's incremental strategy depends on being able to compare the existing table's schema against the model's output schema. For append mode, this means `INSERT INTO target SELECT ... FROM source WHERE ...` with a configurable filter that determines which rows are "new." For merge mode, this means `MERGE INTO target USING source ON key_condition WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`. SQE supports both, but the merge strategy requires Iceberg v2 tables with row-level deletes enabled. The adapter doesn't enforce this -- it generates the SQL and lets the engine return a clear error if the table isn't configured for merge operations.

**SQEColumn** maps between Arrow types (what ADBC returns), Iceberg types (what the catalog stores), and dbt types (what dbt's schema comparison logic expects). This mapping is tedious but critical -- dbt's column type comparison is string-based, so `BIGINT` and `Int64` are different to dbt even though they mean the same thing. The column class normalizes all three representations into a canonical form that dbt's comparisons can work with.

The adapter is roughly 2,000 lines of Python. That sounds like a lot, but most of it is the connection manager, the materialization templates, and the type mapping -- the boring, necessary plumbing that makes dbt's abstract model concrete. The test suite validates each method independently: `list_relations_without_caching` returns the right relations, `get_columns_in_relation` returns the right column types, `check_schema_exists` handles the "schema doesn't exist yet" case.


## Why Not Trino Compat?

We chose Path A over Path B (the Trino compat shim) for a reason that came down to surface area math. A dbt adapter is roughly 2,000 lines of well-documented Python with clear interfaces. The adapter protocol is stable -- dbt's `SQLAdapter` base class hasn't changed its core methods in years. You implement `list_relations_without_caching`, `get_columns_in_relation`, `create_schema`, `drop_schema`, `rename_relation`, and the materialization macros. That's the contract.

A faithful Trino wire protocol implementation is a different beast entirely. HTTP pagination with `nextUri` polling. Session properties embedded in HTTP headers. Transaction semantics with BEGIN/COMMIT that dbt-trino expects to work. Error responses in Trino's specific JSON format. The ARRAY[], MAP(), and ROW() constructor syntax that Trino SQL uses and dbt-trino's macros emit. Type casting with Trino's `CAST()` behavior. And all of this is a moving target -- dbt-trino updates with every Trino release.

The adapter is finished work. The compat layer is ongoing maintenance. We chose the bounded problem.

::: {.fieldreport}
**Field report:** dbt's adapter test suite has 47 tests. We passed 43 on the first run. The 4 failures were all about transaction semantics we intentionally don't support -- BEGIN, COMMIT, ROLLBACK. Iceberg provides atomic commits per statement; multi-statement transactions aren't meaningful for our model. Sometimes incompatibility is a feature.
:::


## The Type Translation Problem

In a later test, a dbt project with 12 models failed because `get_columns_in_relation` returned column types as Iceberg's string representation (`"long"`, `"timestamptz"`) instead of SQL standard names (`"BIGINT"`, `"TIMESTAMP WITH TIME ZONE"`). dbt's schema comparison is string-based -- it compares the type name from the existing table against the type name from the model definition. If they don't match character for character, dbt flags a schema change and tries to handle it.

The fix was adding a type name translation layer in the information_schema columns provider. Another hour of debugging for ten lines of code. These are the bugs that metadata surfaces get. They're never compile errors. They're always runtime, behavioral, tool-specific, and they manifest as "the tool doesn't work" without telling you why.

The Iceberg schema becomes SQL metadata through a translation layer: `field.required` maps to the SQL standard's `is_nullable`. `field.field_type` renders as a string that tools parse into their own type models. Getting these strings right -- matching them character for character to what dbt, DBeaver, and JDBC drivers expect -- is the unglamorous work that makes the difference between "the engine works" and "the engine works with my tools."


## The system.metadata Schema

Alongside the standard `information_schema` and the runtime tables, we added `system.metadata` for Iceberg-specific introspection. This schema exposes what SQL standard views can't represent: table properties, schema properties, and table comments.

`system.metadata.table_properties` returns one row per property per table. Iceberg tables carry properties like `write.format.default`, `write.parquet.compression-codec`, and custom key-value pairs set by the user. A query like `SELECT table_name, property_name, property_value FROM system.metadata.table_properties WHERE schema_name = 'finance' AND property_name LIKE 'write.%'` tells you how every table in the finance namespace is configured for writes. This is information that would otherwise require calling the Polaris REST API directly.

`system.metadata.table_comments` extracts the `"comment"` property from each table's metadata. Some tools -- DBeaver among them -- show table comments in the schema browser. Without this table, comments set in Iceberg are invisible to SQL tools.

`system.metadata.catalogs` is a static single-row table showing the warehouse name and connector type (`"iceberg"`). It exists because Trino's JDBC driver queries it during connection setup. One row, two columns, but without it the driver throws an error before you can run a single query. Nobody builds a query engine to implement static metadata tables. But without them, the tools don't work.


## The JDBC Schema: Keeping DBeaver Happy

The `system.jdbc` schema illustrates a pragmatic truth about building query engines: you spend a surprising amount of time making clients not crash.

Trino's JDBC driver, used by DBeaver and many other tools, queries `system.jdbc.types` to populate its type mappings, `system.jdbc.catalogs` for the catalog tree, `system.jdbc.schemas` for schema browsing, and `system.jdbc.tables` and `system.jdbc.columns` for table metadata. These overlap with `information_schema` but use a different schema layout -- JDBC-specific columns like `JDBC_TYPE`, `PRECISION`, `LITERAL_PREFIX`.

We implemented five tables in `JdbcSchemaProvider`. The types table is static -- a hardcoded list of SQL types with their JDBC type codes, precision, and scale. Each row describes a type the engine supports: `BOOLEAN` with JDBC type code 16, `INTEGER` with code 4, `VARCHAR` with code 12. These codes come from the JDBC specification and must be exact -- tools parse them programmatically to determine type compatibility, auto-completion suggestions, and data import wizards.

The other four JDBC tables are dynamic, pulling from the same Polaris catalog data as `information_schema` but formatted to match the JDBC metadata result set contracts. The column names differ. The type representations differ. The nullability encoding differs. Same underlying data, different suit.

The payoff: DBeaver's schema browser populated. Tables appeared in the tree. Columns showed their types. Double-clicking a table showed its data. The tool worked because we spoke its language.

::: {.ailog}
*[To be completed by AI Logbook agent]*
:::


## The First Real Run

The adapter passed its test suite. The integration tests passed. `dbt debug` connected, authenticated, and reported green. We ran `dbt run` against a project with five seed files and a silver layer of transformation models.

Products seeded. Customers failed.

```
Database Error in seed file seeds/customers.csv
  Cannot add files that are already referenced by table
```

The products seed had 500 rows. The customers seed had 2,000. That threshold was the clue. DataFusion's write path batches rows internally — at some threshold, a single seed operation becomes multiple batch writes. Each batch wrote a Parquet file. Each file was named with a static prefix: `insert-00000.parquet`. The counter reset per batch. The second batch tried to write `insert-00000.parquet` into the same Iceberg snapshot that already contained `insert-00000.parquet` from the first batch.

The Iceberg commit protocol rejected it. You can't add a file that's already referenced. The table format was doing exactly what it was designed to do.

The fix was one line: replace the static prefix with a UUID per write operation. Files became `019abc7f-...-00000.parquet`, `019abc7f-...-00001.parquet`. Unique by construction. The collision was structurally impossible afterward.

> The unit tests had only tested small payloads. The first real seed that crossed the batch threshold was customers.csv, row 501.

This is a category of bug that unit tests reliably miss. You test one batch. You don't test two batches writing to the same table in the same transaction. dbt seeds large datasets; that's their purpose. The bug was invisible until something real ran against the engine.

---

With seeds working, the silver layer models ran next. `stg_orders` failed immediately:

```
Error: Invalid function 'year'
```

The model used `year(order_date)`, `month(order_date)`, `day_of_week(order_date)` — standard Trino date extraction functions. DataFusion has `date_part()` and `extract()`, but not standalone `year()`, `month()`, or `day_of_week()`. They're different names for the same operation, and dbt projects written for Trino use the Trino names.

We added 17 Trino-compatible UDFs: date extraction (`year`, `month`, `day`, `hour`, `minute`, `second`, `day_of_week`, `day_of_year`, `week_of_year`, `quarter`), date arithmetic (`date_add`, `date_diff`, `date_trunc`), conditionals (`if`, `nullif`, `coalesce` — DataFusion has these, but the Trino aliases needed wiring), and introspection (`typeof`, `version`). Each one delegates to the DataFusion equivalent under the hood. The registration is mechanical; the value is compatibility without forking.

While fixing the date functions, a second type error surfaced in a different model. The error message was:

```
TypeSignatureClass::Native(LogicalType::Boolean) is not compatible
with TypeSignatureClass::Scalar(DataType::Utf8)
```

The model was calling `lower(is_active)` where `is_active` was a boolean column. `lower()` expects a string. The SQL was wrong — but the error was gibberish. It referenced internal DataFusion type names that have no meaning to someone writing SQL. The error was technically correct and practically useless.

That observation planted a seed.

---

We fixed the UDFs. The models ran. And then a different kind of failure appeared: a model failed because it referenced a staging table that hadn't been materialized yet (a model ordering issue in the project). The error was:

```
TrinoQueryError(type=INTERNAL_ERROR, name=INTERNAL_ERROR,
  message="Query execution failed", error_code=1)
```

We'd seen this before and dismissed it. Now it was the only error visible when a model failed. Every failure — missing table, wrong type, auth problem, S3 timeout, syntax error — returned the same response: `INTERNAL_ERROR(1)`, `"Query execution failed"`. The Trino compat layer was swallowing every DataFusion error and replacing it with the most generic possible response.

dbt's error output showed us what we'd built: a black box. Something failed. We had no idea what. We had to add logging at the engine layer, trace back through coordinator logs, correlate timestamps, and find the real error buried several layers down. That's acceptable for debugging one failure. It's unworkable when dbt runs 50 models and a third of them fail.

The fix was structural. We defined 27 error codes — `SqeErrorCode` — covering the full taxonomy of query engine failures: `TABLE_NOT_FOUND(11)`, `COLUMN_NOT_FOUND(12)`, `TYPE_MISMATCH(21)`, `PERMISSION_DENIED(31)`, `CATALOG_ERROR(41)`, `STORAGE_ERROR(51)`, and so on. Each code maps to both a Trino error type (for the compat layer) and a gRPC status code (for the Flight SQL layer). An auto-classifier inspects DataFusion error messages and pattern-matches them to the right code: messages containing "table not found" map to `TABLE_NOT_FOUND`, messages containing "permission denied" map to `PERMISSION_DENIED`.

After the fix:

```
TrinoQueryError(type=INVALID_INPUT, name=TABLE_NOT_FOUND,
  message="table 'test_ns.stg_customers' not found", error_code=11)
```

The error code is meaningful. The error type is correct. The message names the specific table. dbt can display this to the analyst who wrote the model, and they can fix it without involving the engine team.

The three bugs weren't independent. The file collision only became visible when the seed data was large enough to matter. The missing functions only became visible when seeds worked and models ran. The useless error messages only became visible when models started failing for real reasons that needed diagnosis.

Each fix revealed the next problem. That's how real integration testing works. You can't write a unit test for "what breaks when a dbt analyst writes their first real project against your engine." You can only run the project and read what happens.

::: {.fieldreport}
**Field report:** The three fixes together — UUID file naming, 17 Trino function aliases, 27 structured error codes — were roughly 600 lines of Rust. None of them were architecturally interesting. They were all "this thing doesn't work the way the ecosystem expects it to." That's most of what makes an engine usable.
:::


## What dbt Teaches

dbt doesn't care about your engine's architecture. It doesn't care about bearer passthrough, plan rewriting, or Arrow Flight. It cares about `list_relations_without_caching`. It cares about column types matching as strings. It cares about `CREATE OR REPLACE TABLE AS SELECT` working exactly the way it expects.

This is humbling work. You can build the most elegant query optimizer in the world, and dbt will ignore it entirely until `information_schema.columns` returns the right column type strings. You can implement zero-copy Arrow Flight transfers, and DBeaver won't show a single table until `system.jdbc.types` has the right JDBC type codes.

The lesson applies beyond dbt. Every tool in the SQL ecosystem -- BI dashboards, schema browsers, data quality tools, lineage trackers -- starts by reading the catalog. If the catalog is incomplete, every tool is crippled. If the catalog is slow, every tool feels sluggish. If the catalog lies (stale data, missing tables, wrong types), every tool built on top of it produces wrong results.

We spent roughly a third of our total development time on metadata surfaces -- information_schema, system tables, JDBC types, SHOW commands, the dbt adapter. None of it makes the engine faster. All of it makes the engine usable. The unglamorous work is where adoption lives.

Build a catalog worth reading. Then build an adapter that translates it into the exact language each tool speaks. The translation is tedious. It's also the difference between an engine that works and an engine people use.
