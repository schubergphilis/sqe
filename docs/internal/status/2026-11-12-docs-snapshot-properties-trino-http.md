**STATUS 2026-11-12: `write-snapshot-properties` task 1.2 done (`docs/snapshot-properties-trino-http`).**

Trino HTTP builds a new `Session` per request (`sqe-trino-compat/src/server.rs`), so the session manager never holds it. The transaction id rides `X-Trino-Transaction-Id` and resolves in the transaction registry. `SET SNAPSHOT_PROPERTIES` therefore works over Trino HTTP inside a transaction and is refused outside one. The design and spec record this, with a new scenario.

No code changed.

**NEXT:** tasks 1.1 (`t$snapshots` JSON via serde_json) and 1.3 (`SnapshotProperties` validation type).
