# Row-Level Write Operations: MERGE INTO, DELETE, UPDATE

## Summary

Implement row-level write operations (MERGE INTO, DELETE FROM, UPDATE) for Iceberg tables via the Polaris REST Catalog. These operations require Iceberg v2 position deletes and overwrite transactions, which are not yet available in iceberg-rust but are actively being developed upstream.

## Motivation

SQE currently supports append-only writes (INSERT INTO, CTAS). To be a viable Trino replacement, we need row-level mutations:

- **MERGE INTO** — the most common pattern for incremental data pipelines (upserts). Required by dbt incremental models.
- **DELETE FROM** — GDPR right-to-erasure, data corrections, partition cleanup.
- **UPDATE** — in-place corrections without full table rewrites.

Without these, users must drop and recreate tables for any data correction — unacceptable for production workloads.

## Current State (as of 2026-03-15)

### What SQE Has Today

| Component | Status |
|-----------|--------|
| SQL parsing (MERGE, DELETE, UPDATE) | ✅ sqlparser handles all three |
| Statement classification & routing | ✅ `StatementKind::Merge`, `Delete` routed in classifier |
| Query execution (SELECT part of MERGE) | ✅ DataFusion handles the join/match logic |
| Append writes via `FastAppendAction` | ✅ Used by INSERT INTO and CTAS |
| Schema inference | ✅ `QueryHandler::get_schema()` |
| Per-session catalog with bearer token | ✅ `SessionCatalog` with Polaris passthrough |

### What's Blocked by iceberg-rust

| Capability | iceberg-rust 0.8 | Needed For |
|------------|:-:|---|
| `OverwriteAction` transaction | ❌ | DELETE, UPDATE, MERGE |
| `RowDeltaAction` transaction | ❌ | MERGE (CoW strategy) |
| `PositionDeleteFileWriter` | ❌ | All row-level deletes |
| `EqualityDeleteFileWriter` | ✅ exists | Alternative delete strategy |
| Delete file commit in `SnapshotProducer` | ❌ stubbed (TODO) | All delete operations |
| `RewriteFilesAction` (compaction) | ❌ | Post-delete cleanup |

## Upstream Progress: iceberg-rust PRs

Actively tracked PRs in [apache/iceberg-rust](https://github.com/apache/iceberg-rust):

### Critical Path

| PR | Title | Author | Status | Impact |
|----|-------|--------|--------|--------|
| **#2185** | `OverwriteAction` with CoW delete support | @glitchy | 🟢 Active review by PMC member | **Foundation** — core primitive for all row-level ops |
| **#2203** | `RowDeltaAction` for row-level modifications (CoW) | @wirybeaver | 🟢 Active | Builds on #2185, enables MERGE/UPDATE/DELETE |
| **#2219** | Delta writer (position + equality delete writer) | @DAlperin | 🟢 Active | Combined writer for row-level changes |
| **#1987** | Delete file support in `SnapshotProducer` | @ethan-tyler | 🟢 Active | Enables committing delete files to snapshots |

### Tracking Issues

| Issue | Title | Notes |
|-------|-------|-------|
| **#2186** | Copy-on-Write and Merge-on-Read support | Main epic, coordinated by @glitchy |
| **#2201** | MERGE INTO support for DataFusion | Includes DataFusion upstream work |
| **#2205** | SQL UPDATE support for DataFusion | Separate from MERGE |
| **#340** | Position delete writer | Open since 2024 |
| **#1607** | RewriteFiles support (compaction) | Post-delete cleanup |

### Expected Landing Sequence

```
1. PR #2185 — OverwriteAction (core primitive)          → weeks away
2. PR #1987 — SnapshotProducer delete file support       → depends on #2185
3. PR #2219 — Position + equality delete writer          → parallel to #1987
4. PR #2203 — RowDeltaAction (CoW)                       → depends on #2185 + #1987
5. New release (likely iceberg-rust 0.9 or 0.10)         → months
```

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

**SQE strategy: start with CoW, add MoR later.** CoW is simpler and iceberg-rust's `OverwriteAction` PR (#2185) targets it first.

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

## SQE Changes Required

### When iceberg-rust ships OverwriteAction

| File | Change |
|------|--------|
| `Cargo.toml` | Bump iceberg to 0.9+ (or whatever version ships it) |
| `crates/sqe-coordinator/src/merge_handler.rs` | **New** — MERGE INTO execution logic |
| `crates/sqe-coordinator/src/delete_handler.rs` | **New** — DELETE FROM execution logic |
| `crates/sqe-coordinator/src/update_handler.rs` | **New** — UPDATE execution logic |
| `crates/sqe-coordinator/src/query_handler.rs` | Route `Merge`/`Delete`/`Update` to new handlers instead of `NotImplemented` |
| `crates/sqe-coordinator/src/write_handler.rs` | Extract shared CoW rewrite logic |
| `crates/sqe-coordinator/src/lib.rs` | Register new modules |

### Shared CoW Logic (write_handler.rs)

```rust
/// Identify which data files in a table are affected by a predicate.
async fn find_affected_files(
    table: &Table,
    predicate: &Expr,  // DataFusion expression
) -> Result<Vec<DataFile>>

/// Rewrite a data file, applying a transform to each row.
/// Returns the new data file (or None if all rows were removed).
async fn rewrite_data_file(
    table: &Table,
    file: &DataFile,
    transform: impl Fn(RecordBatch) -> Result<RecordBatch>,
) -> Result<Option<DataFile>>

/// Commit an overwrite transaction: remove old files, add new files.
async fn commit_overwrite(
    table: &Table,
    removed: Vec<DataFile>,
    added: Vec<DataFile>,
) -> Result<()>
```

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

- [ ] `DELETE FROM table WHERE condition` removes matching rows
- [ ] `DELETE FROM table` removes all rows (empty table, metadata preserved)
- [ ] `UPDATE table SET col = expr WHERE condition` modifies matching rows
- [ ] `MERGE INTO target USING source ON cond WHEN MATCHED THEN UPDATE ...` works
- [ ] `MERGE INTO target USING source ON cond WHEN NOT MATCHED THEN INSERT ...` works
- [ ] `MERGE INTO` with multiple WHEN clauses works
- [ ] All operations are atomic (commit or rollback, no partial state)
- [ ] Bearer token passthrough works for all operations (no privilege escalation)
- [ ] Audit log captures DELETE/UPDATE/MERGE operations
- [ ] Metrics track row-level write operations
- [ ] Works in single-node mode (distributed deferred)

## Rollback Strategy

Each operation is a single Iceberg snapshot commit. Rollback = revert to previous snapshot via Polaris REST API. No partial state is possible — Iceberg's snapshot isolation guarantees atomicity.

If the feature is unstable, the `StatementKind::Merge/Delete` arms in `query_handler.rs` can be reverted to `NotImplemented` in a single commit.

## Timeline & Dependencies

```
                         iceberg-rust upstream
                         ─────────────────────
                         PR #2185 OverwriteAction merges
                                │
                         PR #2203 RowDeltaAction merges
                                │
                         iceberg-rust 0.9/0.10 release
                                │
                         ─────────────────────
                         SQE implementation
                         ─────────────────────
                                │
                                ├── Bump iceberg dep
                                ├── Implement DELETE FROM (simplest)
                                ├── Implement UPDATE (builds on DELETE)
                                ├── Implement MERGE INTO (most complex)
                                ├── Integration tests
                                └── dbt compatibility tests
```

**Estimated effort once iceberg-rust ships:** 2-3 weeks for a single developer.

## Action Items

- [ ] **Watch** PRs #2185, #2203, #2219, #1987 for merge status
- [ ] **Consider contributing** review/testing to PR #2185 to accelerate landing
- [ ] **Prototype** the CoW rewrite logic against current iceberg-rust (the DataFusion join + row classification can be built now, only the commit path is blocked)
- [ ] **Track** iceberg-rust releases for 0.9+ with OverwriteAction
