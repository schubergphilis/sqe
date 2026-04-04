# Row-Level Write Operations: MERGE INTO, DELETE, UPDATE

## Summary

Row-level write operations (MERGE INTO, DELETE FROM, UPDATE) are **implemented** for Iceberg tables via Copy-on-Write using the [risingwavelabs/iceberg-rust](https://github.com/risingwavelabs/iceberg-rust) fork (rev `1978911ec4`), which provides the `rewrite_files()` transaction API.

## Motivation

SQE needed row-level mutations to be a viable Trino replacement:

- **MERGE INTO** — the most common pattern for incremental data pipelines (upserts). Required by dbt incremental models.
- **DELETE FROM** — GDPR right-to-erasure, data corrections, partition cleanup.
- **UPDATE** — in-place corrections without full table rewrites.

## Current State (as of 2026-04-01)

### What SQE Has

| Component | Status |
|-----------|--------|
| SQL parsing (MERGE, DELETE, UPDATE) | ✅ sqlparser handles all three |
| Statement classification & routing | ✅ `StatementKind::Merge`, `Delete`, `Update` routed in classifier |
| Query execution (SELECT part of MERGE) | ✅ DataFusion handles the join/match logic |
| Append writes via `FastAppendAction` | ✅ Used by INSERT INTO and CTAS |
| **DELETE FROM via CoW** | ✅ `delete_handler.rs` — rewrite_files() |
| **UPDATE via CoW** | ✅ `update_handler.rs` — rewrite_files() |
| **MERGE INTO via CoW** | ✅ `merge_handler.rs` — rewrite_files() |
| Schema inference | ✅ `QueryHandler::get_schema()` |
| Per-session catalog with bearer token | ✅ `SessionCatalog` with Polaris passthrough |
| Integration tests (all three operations) | ✅ Against Polaris + MinIO |
| TPC-C write benchmarks | ✅ 17/17 pass |

### Iceberg Dependency

Uses `risingwavelabs/iceberg-rust` fork (rev `1978911ec4`) for `rewrite_files()` transaction support. When upstream iceberg-rust ships `OverwriteAction` (tracked in Epic #2186), the dependency can be migrated back to the official crate.

### Future: Merge-on-Read

MoR with position deletes is not yet implemented. Upstream PRs to watch:

| PR | Title | Status | Impact |
|----|-------|--------|--------|
| **#2203** | `RowDeltaAction` for row-level modifications | Active | Enables MoR path |
| **#2219** | Delta writer (position + equality delete writer) | Active | Combined writer for MoR |
| **#1987** | Delete file support in `SnapshotProducer` | Active | Enables committing delete files |

MoR would reduce write amplification for write-heavy workloads but requires compaction to maintain read performance.

## Design

### Iceberg Delete Strategies

Iceberg v2 supports two strategies for row-level deletes:

**Copy-on-Write (CoW):**
- Read affected data files entirely
- Rewrite without the deleted/modified rows
- Produce new data files + remove old ones via `OverwriteAction`
- Pro: simple reads (no delete file reconciliation)
- Con: write amplification (rewrites entire files even for single-row changes)

**Merge-on-Read (MoR):**
- Write small position/equality delete files marking deleted rows
- Readers reconcile deletes at scan time
- Pro: fast writes (small delete files)
- Con: read overhead (must merge deletes during scans)

**SQE strategy: CoW implemented, MoR planned.** CoW is simpler and is fully operational via the RisingWave fork's `rewrite_files()`. MoR will be added when upstream ships the required primitives.

### Architecture

```
SQL: MERGE INTO target USING source ON condition
     WHEN MATCHED THEN UPDATE SET ...
     WHEN NOT MATCHED THEN INSERT ...

                    ┌──────────────────┐
                    │   SQL Parser     │
                    │   (sqlparser)    │
                    └───────┬──────────┘
                            │ StatementKind::Merge
                            ▼
                    ┌──────────────────┐
                    │  QueryHandler    │
                    │  execute()       │
                    └───────┬──────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │  MergeHandler               │
              │                             │
              │  1. Plan source + target     │
              │     via DataFusion           │
              │                             │
              │  2. Execute the join to      │
              │     produce matched/         │
              │     unmatched rows           │
              │                             │
              │  3. Classify each row:       │
              │     - matched → UPDATE/DEL   │
              │     - not matched → INSERT   │
              │                             │
              │  4. For CoW: rewrite         │
              │     affected data files      │
              │     without deleted rows,    │
              │     add new rows             │
              │                             │
              │  5. Commit via               │
              │     OverwriteAction          │
              └─────────────────────────────┘
```

### DELETE FROM Implementation

```
DELETE FROM target WHERE condition

1. Scan target table metadata to find data files
2. For each data file that may contain matching rows:
   a. Read the file
   b. Apply the WHERE filter
   c. If all rows match → mark file for removal
   d. If partial match → rewrite file without matching rows (CoW)
3. Commit via OverwriteAction:
   - Remove old data files
   - Add rewritten data files (partial matches)
```

### UPDATE Implementation

```
UPDATE target SET col = expr WHERE condition

1. Same as DELETE, but:
   a. Read affected files
   b. Apply WHERE filter
   c. For matching rows: apply SET expressions
   d. Rewrite file with modified rows (CoW)
2. Commit via OverwriteAction
```

### MERGE INTO Implementation

```
MERGE INTO target USING source ON condition
  WHEN MATCHED AND <cond> THEN UPDATE SET ...
  WHEN MATCHED AND <cond> THEN DELETE
  WHEN NOT MATCHED THEN INSERT (cols) VALUES (vals)

1. Execute the join: DataFusion LEFT OUTER JOIN of source and target
2. For each result row, classify:
   - Matched + update condition → apply SET, write to new file
   - Matched + delete condition → omit from rewrite (delete)
   - Not matched → write as new INSERT row
3. For CoW: identify affected target data files, rewrite them
4. Commit via OverwriteAction:
   - Remove old data files (affected ones)
   - Add new data files (rewritten + inserted)
```

## SQE Implementation

| File | Change |
|------|--------|
| `Cargo.toml` | Switched to risingwavelabs/iceberg-rust fork (rev `1978911ec4`) |
| `crates/sqe-coordinator/src/merge_handler.rs` | MERGE INTO execution via CoW |
| `crates/sqe-coordinator/src/delete_handler.rs` | DELETE FROM execution via CoW |
| `crates/sqe-coordinator/src/update_handler.rs` | UPDATE execution via CoW |
| `crates/sqe-coordinator/src/query_handler.rs` | Routes Merge/Delete/Update to handlers |
| `crates/sqe-coordinator/src/write_handler.rs` | Shared CoW rewrite logic |
| `crates/sqe-coordinator/src/lib.rs` | Modules registered |

## Testing Strategy

### Unit Tests (no external deps)

- Parse and classify MERGE/DELETE/UPDATE statements
- Row classification logic (matched/unmatched/insert/update/delete)
- CoW rewrite logic with in-memory RecordBatches

### Integration Tests (require Polaris + MinIO)

- DELETE FROM with WHERE clause → verify rows removed
- DELETE FROM without WHERE → verify table emptied
- UPDATE SET → verify values changed
- MERGE with WHEN MATCHED THEN UPDATE → verify upsert
- MERGE with WHEN MATCHED THEN DELETE → verify conditional delete
- MERGE with WHEN NOT MATCHED THEN INSERT → verify new rows added
- Concurrent MERGE operations → verify conflict detection
- MERGE with schema mismatch → verify error handling

### dbt Compatibility Tests

- `dbt run` with `incremental` materialization strategy → MERGE INTO
- `dbt run` with `delete+insert` strategy → DELETE + INSERT
- Verify `dbt test` passes after incremental runs

## Acceptance Criteria

- [x] `DELETE FROM table WHERE condition` removes matching rows
- [x] `DELETE FROM table` removes all rows (empty table, metadata preserved)
- [x] `UPDATE table SET col = expr WHERE condition` modifies matching rows
- [x] `MERGE INTO target USING source ON cond WHEN MATCHED THEN UPDATE ...` works
- [x] `MERGE INTO target USING source ON cond WHEN NOT MATCHED THEN INSERT ...` works
- [x] `MERGE INTO` with multiple WHEN clauses works
- [x] All operations are atomic (commit or rollback, no partial state)
- [x] Bearer token passthrough works for all operations (no privilege escalation)
- [x] Audit log captures DELETE/UPDATE/MERGE operations
- [x] Metrics track row-level write operations
- [x] Works in single-node mode (distributed deferred)

## Rollback Strategy

Each operation is a single Iceberg snapshot commit. Rollback = revert to previous snapshot via Polaris REST API. No partial state is possible — Iceberg's snapshot isolation guarantees atomicity.

If the feature is unstable, the `StatementKind::Merge/Delete` arms in `query_handler.rs` can be reverted to `NotImplemented` in a single commit.

## Timeline

All three operations (DELETE, UPDATE, MERGE INTO) were implemented using the RisingWave iceberg-rust fork rather than waiting for upstream. Implementation completed 2026-03-28.

## Remaining Action Items

- [ ] **Watch** upstream iceberg-rust Epic #2186 for `OverwriteAction` — migrate from RisingWave fork to official crate when available
- [x] ~~Implement DELETE FROM~~ (done)
- [x] ~~Implement UPDATE~~ (done)
- [x] ~~Implement MERGE INTO~~ (done)
- [x] ~~Integration tests~~ (done)
- [x] ~~TPC-C write benchmarks~~ (17/17 pass)
