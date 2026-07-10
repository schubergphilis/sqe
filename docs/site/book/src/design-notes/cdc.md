# Change Data Capture (CDC) scans

SQE supports snapshot-range incremental reads over Iceberg tables. The feature
covers the Phase 1 range scan described by iceberg-rust #2152: query the rows
appended or removed between two snapshot ids. The full changelog view from
iceberg-rust #1636 is deferred.

## Syntax

```sql
SELECT *
FROM ns.t
FOR INCREMENTAL BETWEEN SNAPSHOT 100 AND SNAPSHOT 105;
```

The range is open-closed: `100` is excluded, `105` is included. Both must be
integer snapshot ids. Branch names and tag names are not accepted here (use
`FOR VERSION AS OF` for point-in-time pins).

Meta columns become visible when selected explicitly:

```sql
SELECT id, amount, _change_type, _change_ordinal, _commit_snapshot_id
FROM orders
FOR INCREMENTAL BETWEEN SNAPSHOT 100 AND SNAPSHOT 105;
```

## Semantics

- **Range**: `(start, end]`. Rows in data files added in any snapshot up to and
  including `end`, going back until (but not including) `start`.
- **Sentinel**: `start = 0` reads from the beginning of history.
- **Parent chain walk**: only snapshots reachable from `end` count. Branch
  commits not on that chain are skipped.
- **Deletes**: delete files added in the range are reconciled only against
  data files also in the range. A position-delete file added at snapshot 102
  that targets a data file added at snapshot 90 is dropped. Equality deletes
  are retained.
- **Meta columns**: `_change_type` is the literal string `insert` or
  `delete`. `_change_ordinal` is a per-snapshot sequence. `_commit_snapshot_id`
  is the snapshot that produced the change.

## Error cases

- Descending range (`start > end`): rejected at parse time.
- Missing start or end snapshot id: rejected during resolve with the id named
  in the error.
- Non-ancestor start: when `start != 0` and no parent chain from `end` lands
  on `start`, the resolver rejects the query.
- Meta columns referenced outside an incremental scan: the query fails with
  an error naming the requirement. The three meta columns are not regular
  table columns.

## Intended dbt use case

The dbt-sqe adapter plans to expose an `append_changes` incremental strategy
that stores the last-seen snapshot id in dbt state. On each run the adapter
emits:

```sql
SELECT * FROM source FOR INCREMENTAL BETWEEN SNAPSHOT <last> AND SNAPSHOT <current>
```

and merges the result into the target table. The adapter work lives in a
separate repository (dbt-sqe) and is tracked in Phase G tasks 8.14-8.15; the
SQL engine side is complete as of this release.

## Current limitations

- Full coordinator wiring of the incremental parser + planner into a physical
  scan executor ships in a follow-up commit. The planner (`crates/sqe-catalog/
  src/incremental_scan.rs`) is reachable and unit-tested today; integration
  tests that stand up a real catalog plus S3 stack live in the next phase of
  work.
- Equality deletes are not yet written by SQE. Range reads tolerate them when
  Spark or Trino writes them, but SQE's own DELETE path still emits position
  deletes.
- Changelog view (per-row deltas with `_before`/`_after` payloads) is not in
  scope for Phase G.
