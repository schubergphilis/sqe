## Context

Iceberg snapshots carry a `summary` map. Iceberg fills it with computed keys (`added-records`, `total-data-files`, and so on) plus `operation`, and writers may add their own. Spark exposes them through `snapshot-property.<key>` write options. Other engines read keys such as `wap.id`.

SQE writes through `commit_with_retry` (`sqe-write/src/commit.rs`) and `WriteHandler::commit_rewrite_with_retry` (`write_handler.rs`). Neither builds the action itself: each takes a closure, and about eleven call sites in `write_handler.rs` and `write_handler/ingest.rs` build a `fast_append`, `overwrite_files` or `row_delta` action. All three support `set_snapshot_properties` in vendored iceberg-rust. Maintenance already uses them for the `sqe.maintenance.*` keys (`maintenance_scheduler.rs`, applied in `maintenance.rs`).

Session state that shapes writes already exists. `SET WRITE_BRANCH` is classified in `sqe-sql/src/classifier.rs`, stored by `SessionManager::set_write_branch`, and reaches the commit through the task-local `StatementScope` in `sqe-catalog/src/txn.rs`, set by `write_scope`. A transaction stages its writes on a branch and publishes it to `main` as a fast-forward, so each staged snapshot's summary survives publication.

Bearer-token clients share one session per token (`SessionManager::session_for_bearer` maps a token fingerprint to one session id). Parallel loads under one token therefore share session state. Two kinds of transaction exist. A Flight SQL `BeginTransaction` returns an id that the client sends with each request (`in_request_transaction` in `flight_sql.rs`), so each holder has its own transaction. A SQL `BEGIN` is stored on the session (`set_session_transaction` in `query_handler.rs`), so every connection on that session joins it.

## Goals / Non-Goals

**Goals:**
- A marker written in the same commit as the data, for every kind of user write.
- No race between parallel loads that share a session, when they use Flight SQL `BeginTransaction` transactions or per-request ingest options.
- A property that SQE cannot apply is refused with an error, never dropped.
- Keys that would change engine behaviour, or that Iceberg computes, cannot be set.

**Non-Goals:**
- Carrying properties forward through compaction or `rewrite_data_files`. Readers walk snapshot ancestry to find the marker.
- Keeping a marker after snapshot expiry removes its snapshot.
- Setting properties per statement with SQL syntax (a clause on INSERT). Transactions already scope them per statement group.
- Table properties. They keep their own `ALTER TABLE ... SET TBLPROPERTIES` path.

## Decisions

**1. Session statement modelled on `WRITE_BRANCH`, with a transaction scope.**
`SET SNAPSHOT_PROPERTIES ('k' = 'v', ...)` replaces the whole set. `SET SNAPSHOT_PROPERTIES ()` and `RESET SNAPSHOT_PROPERTIES` clear it. The statement is allowed inside a transaction. Run inside one, it sets the properties on that transaction's `TxnScope`, and they are dropped when the transaction ends. Run outside one, it stores them on the session through a new `SessionManager::set_snapshot_properties`. A write inside a transaction uses only the transaction's properties. Session properties do not leak into it.
*Why:* the per-transaction form is race-free under a shared bearer session when each loader holds its own Flight SQL `BeginTransaction` id, which the ADBC driver uses for a connection with autocommit off. A SQL `BEGIN` lives on the session, so loaders sharing a token and using SQL `BEGIN` share one transaction and its properties. The session form covers single autocommit writes from one client per session; parallel loaders under one token race on it, and the docs say so.
*Alternatives:* a `SET SESSION` property is refused on Flight and is a flat string. A table-function or comment hint is invisible to the classifier and to audit.

**2. Refuse where the session is not held.**
The existing `SetWriteBranch` arm logs and ignores the value when no session manager is wired. The new arm returns an error in that case, naming Flight SQL as the supported route. Before implementation, read the Trino HTTP session path and record in the spec whether it reaches a held session.

**3. Carried in `StatementScope`, read once, passed by value.**
`write_scope` gains a `snapshot_properties: Option<Arc<SnapshotProperties>>` argument. The caller (`query_handler.rs` execute, `flight_sql.rs` ingest, `embedded.rs`) resolves the transaction or session value. `scope()` and `with_params()` inherit the field the way `params` is inherited. Each commit closure reads `sqe_catalog::txn::snapshot_properties()` once, before `commit_with_retry`. Task-locals do not cross `tokio::spawn`, so the value is cloned into the closure and never read from a spawned task.
*Why:* the action is built inside each caller's closure, so there is no single hook. A small helper `apply_snapshot_properties` per action type keeps each site to one line.
*Alternative:* patch the vendored `Transaction` to read a task-local. Rejected: a vendor patch for an SQE concern, and it would also stamp maintenance commits.

**4. Empty writes commit an empty snapshot when properties are set.**
Today a write with nothing to add or delete skips the commit. With properties set, it commits a `fast_append` with no files. iceberg-rust allows that when snapshot properties are present (`snapshot.rs`, "No file or snapshot property changes to commit"). Without properties nothing changes.
*Why:* a client that copies an empty table must find its marker, or it redoes the table on every resume.

**5. Ingest options use Spark's prefix.**
`do_put_statement_ingest` reads `CommandStatementIngest.options` entries named `snapshot-property.<key>`, validates them with the same rules, and merges them over the transaction or session set for that request only. Other option names stay ignored, as today.

**6. Validation.**
- Key: 1 to 128 bytes from `[A-Za-z0-9._-]`. Value: at most 1024 bytes, no control characters. At most 32 pairs and 8 KiB in total, because the summary lives in `metadata.json` and every snapshot repeats it.
- Refused keys:
  - Iceberg's computed summary keys (the `COMPUTED_SUMMARY_PROPERTIES` list, which Iceberg would overwrite) and `operation`.
  - Any key under `partitions.`.
  - `wap.id`, `published-wap-id`, `source-snapshot-id`: other engines act on them.
  - Anything under `sqe.`: `last_compaction_snapshot_ms` treats a `sqe.maintenance.*` key as a compaction.
  - Anything under `commit.` or `write.`: the snapshot producer reads snapshot properties as manifest-merge configuration.
- Matching is case-insensitive.

**7. Fix `t$snapshots` JSON first.**
`iceberg_metadata_tvf.rs` builds the `summary` column by hand and escapes only `"`. It switches to `serde_json` before any user value can reach it.

## Risks / Trade-offs

- [Session properties stay set after the write] -> Document `RESET`. Recommend the transaction form, which clears itself. The audit record of each write lists the property keys.
- [A transaction with several statements makes several snapshots] -> Each carries the properties set when it ran. The published head carries the last statement's. Readers look for the key on the head or walk ancestry.
- [A new write path forgets the helper] -> A table-driven integration test covers every write kind, in and out of a transaction, and asserts the key in the committed summary.
- [Compaction replaces the head snapshot] -> Out of scope, and documented: a reader walks ancestry, because compaction produces a `replace` snapshot without user keys.
- [Empty snapshots add metadata] -> Only when properties are set, at one snapshot per write.

## Migration Plan

Additive. Nothing changes until a client sets properties. Rollback is a binary downgrade: snapshots that were already written keep their keys, and older SQE versions ignore them.

## Open Questions

- Does a Trino HTTP session reach the session manager? Settle it in task 1 by reading the code. If not, the spec scenario for HTTP is the refusal.
