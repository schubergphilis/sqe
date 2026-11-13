## Why

`CREATE OR REPLACE TABLE ... AS SELECT` has two paths. When the SELECT fits the live table's columns and partitioning, the replace is one `rewrite_files` commit on the existing table (`ctas_replace_in_place`). When the schema or partitioning changes, SQE writes the replacement under `<name>__sqe_replace_<uuid>`, then calls `drop_table` and `rename_table` (#892). Between those two catalog calls the table does not exist:

- a concurrent `SELECT` fails with "table not found";
- a namespace listing taken in the gap omits the table, so `information_schema.columns` answers zero rows for it. The #890 integration test saw this on 2-4 of 150 reads, and dbt then renders an empty column list;
- the snapshot history of the replaced table is gone, so time travel and `rollback_to_snapshot` cannot reach back across the replace;
- catalog grants on the table entity are dropped with it;
- a failure between DROP and RENAME leaves the data reachable only under the staging name.

Spark's atomic `REPLACE TABLE` and Iceberg Java's replace transaction avoid all of this with one catalog update on the existing table.

## What Changes

- A schema- or partition-changing `CREATE OR REPLACE TABLE ... AS SELECT` becomes one commit on the existing table:
  - add the new schema and make it current (or select an equal existing one);
  - add the new partition spec and make it the default (or select an equal existing one);
  - set the merged properties;
  - add a snapshot on `main` that removes every live data and delete file and adds the new ones.

  The table keeps its UUID, its location, its snapshot history, its other branches and tags, and its catalog grants.
- Field ids are reused by column name from the current schema. New names get fresh ids above `last-column-id`. Partition field ids are reused by (source id, transform).
- The commit asserts the table UUID, the current schema id, the last assigned field id, the default spec id and the last assigned partition id. A concurrent schema change makes the replace fail and remove the files it wrote. A concurrent data commit is retried, the way the in-place path retries.
- Refused before any data is written:
  - a replace that would downgrade the format version;
  - a schema-changing replace while `WRITE_BRANCH` is set.
- The staged `__sqe_replace_` path and its DROP + RENAME swap are removed for Iceberg catalogs.
- `information_schema_listing_race_e2e` also reads the table being replaced, and every read must return the full column list.

## Capabilities

### New Capabilities
- `atomic-create-or-replace`: a `CREATE OR REPLACE TABLE ... AS SELECT` with any schema or partitioning is one atomic catalog commit on the existing table, never a drop and re-create.

### Modified Capabilities
<!-- None: no spec in openspec/specs/ covers CTAS. -->

## Impact

- `sqe-write`:
  - `handle_ctas_streaming_on` loses the staged branch;
  - a new replace planner builds the evolved metadata;
  - the data write targets an evolved `Table` view of the live table;
  - one combined commit through `SessionCatalog::commit_schema_update`, or an equivalent raw-commit entry point that also carries the snapshot.
- `sqe-catalog`: possibly a raw commit helper that takes the snapshot updates as well as the schema updates. For non-REST catalogs (SQLite in the embedded CLI) it uses `TableCommit::from_parts`, as `commit_schema_update` already does.
- Privileges: the replace needs the right to commit schema, spec, property and snapshot updates to the existing table, instead of create + drop + rename in the namespace. Probe 1.4 records which Polaris privileges that is.
- Tests:
  - integration tests for every scenario in the spec;
  - `information_schema_listing_race_e2e` extended;
  - unit tests for field id and spec id assignment.
- Docs: the CTAS page loses its warning that a schema-changing replace drops history.
- No new dependency and no wire-format change.
