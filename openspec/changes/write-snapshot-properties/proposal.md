## Why

A client that copies tables into Iceberg through SQE needs to know, after a crash, which tables already hold a complete copy. Today it can only write a marker as a table property in a second commit after the data commit. A crash between the two leaves data without a marker. The client then has to guess from row counts or snapshot ids whether the copy finished. Spark and other Iceberg writers solve this with snapshot properties: user keys in the snapshot summary, written in the same commit as the data. SQE has no way to set them.

## What Changes

- New session statement `SET SNAPSHOT_PROPERTIES ('key' = 'value', ...)` and `RESET SNAPSHOT_PROPERTIES`. They work over Flight SQL and the embedded CLI, and over any other protocol whose session the coordinator holds.
- Inside a transaction the properties belong to that transaction and are dropped at COMMIT or ROLLBACK. Outside one they stay on the session until `RESET`, the same way `WRITE_BRANCH` does.
- Every user write commits its snapshot with those keys in the summary: INSERT, CTAS, INSERT OVERWRITE, DELETE, UPDATE, MERGE and Flight SQL bulk ingest, in a transaction or not.
- A write that would commit nothing (zero rows, no matching rows) still commits an empty snapshot that carries the properties, so a marker for an empty table is never lost.
- Flight SQL bulk ingest also reads `snapshot-property.<key>` entries from `CommandStatementIngest.options`, Spark's write-option prefix. They apply to that request only, on top of the session or transaction properties.
- Keys are validated. Iceberg's computed summary keys, `operation`, the `partitions.` prefix, Iceberg's WAP keys and the `sqe.`, `commit.` and `write.` prefixes are refused. Key length, value length, pair count and total size are capped.
- `t$snapshots` builds its `summary` JSON with a JSON serializer. Today it escapes only `"`, so a value with a backslash or control character produces invalid JSON. Fixing that comes first, because these values are user-controlled.

## Capabilities

### New Capabilities
- `write-snapshot-properties`: user keys in the snapshot summary of every write, set per session, per transaction or per ingest request, validated, and readable through `t$snapshots`.

### Modified Capabilities
<!-- None: no existing spec in openspec/specs/ covers writes or Flight ingest. -->

## Impact

- `sqe-sql`: classifier variants for `SET` / `RESET SNAPSHOT_PROPERTIES`.
- `sqe-core`: a `snapshot_properties` field on `Session`.
- `sqe-catalog`: the transaction scope and the task-local `StatementScope` carry the properties. `t$snapshots` serializes its JSON with serde_json.
- `sqe-write`: every write's commit applies the properties. Empty writes commit an empty snapshot when properties are set.
- `sqe-coordinator`: dispatch, session manager setter, DoPut option parsing, transaction cleanup.
- `sqe-cli`: embedded dispatch.
- Docs: the SQL reference for the new statements and the Flight SQL connecting page for the ingest option.
- No new dependencies and no wire-format change. Older clients are unaffected.
