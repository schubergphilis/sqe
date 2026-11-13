## Context

`WriteHandler::handle_ctas_streaming_on` (`crates/sqe-write/src/write_handler.rs`) loads the live table under `OR REPLACE`, runs `refuse_replace_under_policy`, and then takes one of two paths.

**In place.** When `ctas_replace_is_in_place` holds, `ctas_replace_in_place` writes the new files into the live table. It then commits one `rewrite_files` that removes every live data file and the deletes they supersede. The statement's properties go in the same commit (#894). The commit retries through `commit_rewrite_and_properties_with_retry`, and each attempt re-reads the live files from a fresh load. A concurrent append is therefore replaced too: the replace wins. `ctas_replace_is_in_place` compares column names, types and nullability, the JSON-column marks and the partition spec.

**Staged.** Otherwise the replacement is created as `<name>__sqe_replace_<uuid>` with the merged properties (#896, `carry_live_properties`). The "Swap a staged replacement into place (500)" block then calls `drop_table` and `rename_table`. That gap is #892.

Two things the change can build on:

- **Raw commits.** `SessionCatalog::commit_schema_update(ident, updates, requirements)` (`crates/sqe-catalog/src/rest_catalog.rs`) posts raw `TableUpdate`s and `TableRequirement`s. For REST catalogs it posts the JSON body. For other catalogs it uses `TableCommit::from_parts` and `update_table`, an SQE vendor patch. It resends a refused commit only when the commit guards itself: it asserts the current schema id and changes it (`schema_commit_guards_itself`).
- **The equal-schema case.** The ALTER path (`crates/sqe-write/src/catalog_ops.rs`, around 1137 and 2246) already handles it. Polaris reuses the id of a structurally equal existing schema, then rejects `SetCurrentSchema { -1 }` with "no schema has been added". SQE therefore selects the existing id explicitly, or skips the update when that schema is already current.

Iceberg Java's replace transaction (`TableMetadata.buildReplacement` plus `BaseTransaction` in replace mode), which Spark's `REPLACE TABLE` uses:

- **Field ids:** reused by full column name against the current schema; the type is not compared. Other columns get fresh ids from `last-column-id + 1`.
- **Schemas:** a schema equal to an existing one reuses its id.
- **Partition specs:** on v2+, partition field ids are reused by (source id, transform). An equal spec reuses its id. Without `PARTITIONED BY` the result is unpartitioned.
- **Sort order:** unsorted unless the statement gives one.
- **Properties:** merged. The old keys stay and the new ones override.
- **Kept:** the UUID, the snapshots and every ref except `main`. `main` is removed (`RemoveSnapshotRef`), so the first new snapshot has no parent.
- **Format version:** it can be upgraded; a downgrade throws.
- **Requirements:** `AssertTableUUID`, plus `AssertLastAssignedFieldId` when a schema is added, plus `AssertLastAssignedPartitionId` when a spec is added.

The vendored iceberg-rust has no replace-table action. `replace_files.rs` swaps files under one schema. `TableMetadataBuilder` has every primitive needed: `add_schema`, `set_current_schema`, `add_partition_spec`, `set_default_partition_spec`, `remove_ref`, `upgrade_format_version` and `set_properties`. For an equal existing schema, though, it emits `AddSchema` + `SetCurrentSchema(-1)`, the pair Polaris rejects. Its `changes` list cannot be posted verbatim.

## Goals / Non-Goals

**Goals:**
- No moment at which the table is absent, listed short or unreadable while a replace of any shape runs.
- One catalog commit. Either all of the new schema, spec, properties and data are visible, or none of them are.
- The table keeps its UUID, location, history, other refs and catalog grants.
- A failure at any step leaves the original table exactly as it was. The files the attempt wrote are removed.
- Every rule the two paths already enforce still holds: the policy refusal, the #894 and #896 property handling, the JSON-column marks, lineage, snapshot properties and the cache invalidation in `after_commit`.

**Non-Goals:**
- Atomic replace inside a transaction. CTAS stays refused there.
- Atomic replace on a write branch. A schema change is table-wide, so it is refused (Decision 6).
- Hive external tables and file tables. They keep their own CTAS paths.
- Rewriting written data after a concurrent schema change. The data files embed field ids, so the replace fails instead (Decision 5).
- Pruning old schemas or specs from metadata. Alternating shapes grow `schemas` and `partition-specs` by at most one entry per new shape; an equal shape reuses its entry.

## Decisions

**1. Evolve the live table; never create a second one.**
The planner starts from the live `TableMetadata` and builds the evolved metadata locally with `TableMetadataBuilder`:
- the new schema with reused field ids;
- the new spec with reused partition field ids;
- the merged properties;
- the format version, which only goes up.

It wraps that metadata in a `Table` with the live identifier and `FileIO`. The existing streaming writer then writes the data files to the live table's location with the new field ids and partition values. Nothing in the catalog changes until the commit.
*Why:* the data files must carry the field ids and spec id the commit will install. Writing against the evolved view gets this right with the writer SQE already has.
*Alternatives:* staging a second table and re-pointing it in the catalog is the current gap. Writing first and evolving the schema after is two commits with an invalid state in between.

**2. Field and spec ids follow Iceberg Java.**
- **Column ids:** reused by exact column name from the live current schema; other columns get `last-column-id + 1, ...`. When a column keeps its name but changes type, the id is reused only when the change is an allowed Iceberg promotion (int to long, float to double, decimal precision widening). Otherwise it gets a fresh id.
- **Nested fields:** ids are reused by full path.
- **Partition fields:** ids are reused by (source id, transform).
- **Equal shapes:** a schema or spec equal to an existing one reuses that one's id.

*Why:* Java reuses the id whatever the type. But SQE projects scans by field id (!784). An old file whose column has the same id and an incompatible type would then be read through the new type when a reader time-travels or a later schema maps back. A fresh id for an incompatible type makes the old column a different field, which is what it is.
*Note:* this is the one place SQE departs from Java. Spark-written history stays readable either way.

**3. The new snapshot is an overwrite with `main`'s head as its parent; `main` is not removed.**
The replace commits, in this order:
- `UpgradeFormatVersion`, when the version goes up;
- `AddSchema` + `SetCurrentSchema(-1)`, or `SetCurrentSchema(<id>)` for an equal existing schema, or nothing when it is already current;
- `AddPartitionSpec` + `SetDefaultSpec(-1)`, or `SetDefaultSpec(<id>)`, or nothing;
- `SetProperties` / `RemoveProperties`;
- `AddSnapshot` + `SetSnapshotRef(main)`. The snapshot is an `overwrite` that deletes every live data file and delete file and adds the new files, built by the same code `ctas_replace_in_place` uses, run against the evolved table.

*Why:* `ctas_replace_in_place` already promises that time travel and `rollback_to_snapshot` cross a replace. The two paths should not differ in that promise just because the schema changed. A parented overwrite snapshot also gives correct summary counts (`deleted-records`, `total-records`). Iceberg allows a parent snapshot with another schema id; that is ordinary schema evolution.
*Alternatives:* Java's `RemoveSnapshotRef(main)` + a parentless append. It is what Spark writes, but rollback across the replace then has no ancestry to follow. User-visible decision U3.

**4. Requirements pin the base the ids were predicted from.**
The commit asserts:
- `AssertTableUuid`;
- `AssertCurrentSchemaId(live)`;
- `AssertLastAssignedFieldId(live)`;
- `AssertDefaultSpecId(live)`;
- `AssertLastAssignedPartitionId(live)`;
- `AssertRefSnapshotId(main, live head)`.

*Why:* the schema and spec ids the planner predicts (`max + 1`, or reuse) end up inside the snapshot's manifests. They must equal the ids the catalog assigns, or the snapshot points at the wrong schema without any error. Java asserts only the last assigned ids. But a concurrent `ALTER TABLE ... DROP COLUMN` adds a schema without raising `last-column-id`, so SQE also pins the current schema and the default spec. The schema-id assertion also makes `schema_commit_guards_itself` true, so a 5xx can be resent safely.

**5. Conflicts: data races retry; schema races fail.**
A requirement failure on `AssertRefSnapshotId(main)` alone means another writer committed data. The planner reloads the table. When the UUID, current schema id, last column id, default spec id and last partition id are all unchanged, it rebuilds only the snapshot (new manifests over the same data files, every live file removed again) and recommits. The retry budget is the in-place path's. Any other requirement failure ends the statement with an error that names the concurrent change, and the cleanup guard deletes the data files, manifests and manifest list this attempt wrote.
*Why:* the replace wins over concurrent appends, as the in-place path's retries already make it do. A concurrent schema change would need the data rewritten under new ids, so it fails.

**6. Refusals before any data is written.**
- `format-version` lower than the live table's: "CREATE OR REPLACE cannot downgrade format version N to M".
- A schema- or partition-changing replace while `WRITE_BRANCH` is set: "a schema change applies to the whole table; RESET WRITE_BRANCH first". The in-place path keeps its current branch behaviour.
- In a transaction: refused, as today.

**7. Properties as #896, in the same commit.**
- The statement's keys override.
- The live keys stay, except the reserved, location-bound and `sqe.json-columns` keys.
- `sqe.json-columns` is recomputed from the SELECT.
- `sqe.column-tags` is filtered to columns that survive by name.

`carry_live_properties` already computes this for the staged path; it moves to the new path unchanged. A column that is dropped and re-added under the same name keeps its tags (name match), as on the staged path. A column the SELECT renames loses them. Masks and row filters are keyed by name in the policy store, so they already follow the name.

**8. Sort order.**
The default sort order becomes unsorted (order id 0) unless the live order references only columns that survive with reused ids. In that case it stays. SQE has no sort-order DDL, so a live sort order was set by another engine (Spark `WRITE ORDERED BY`).
*Why:* Java resets to unsorted. Keeping a still-valid order respects the other engine's setting, which SQE has no statement to restore. User-visible decision U4.

**9. Commit entry point.**
Extend `SessionCatalog` with a raw commit that takes the full update and requirement lists, including `AddSnapshot`, or reuse `commit_schema_update` if its body is general enough. Non-REST catalogs go through `TableCommit::from_parts` + `update_table`. The vendored SQL catalog applies the updates with `TableMetadataBuilder`, which accepts the same sequence. The embedded CLI is covered by that.

**10. The staged path goes.**
The `__sqe_replace_` branch, its warning and its swap block are deleted for Iceberg catalogs. The in-place path stays as the cheap case: no schema updates, `rewrite_files`.

## Risks / Trade-offs

These assumptions are open until the probes in task group 1 confirm them:

- **A1:** Polaris accepts one commit holding `AddSchema`, `SetCurrentSchema`, `AddPartitionSpec`, `SetDefaultSpec`, `SetProperties`, `AddSnapshot` and `SetSnapshotRef`, with the requirements of Decision 4.
- **A2:** Polaris assigns exactly the schema and spec ids the planner predicts, for a new shape and for a previously seen shape. The #890 race test alternates two shapes, so the reuse case is hit constantly.
- **A3:** `SetDefaultSpec(<explicit id>)` for an equal historical spec works where `SetDefaultSpec(-1)` after a reused `AddPartitionSpec` may not, as with schemas.
- **A4:** the privileges Polaris checks for that commit, compared with create + drop + rename today. A user who could do the staged replace but lacks the update privileges would now be refused. That is a behaviour change, listed as U5.

Trade-offs:

- **Metadata growth:** each new shape adds a schema and maybe a spec. Equal shapes reuse.
- **Storage:** the replaced files stay on storage until snapshot expiry, because history is kept. Today's DROP without purge also leaves them, but unreferenced.

## User-visible decisions

| # | Decision | Default in this design |
|---|---|---|
| U1 | History and UUID across a schema-changing replace | kept (time travel and rollback cross the replace) |
| U2 | Concurrent data commit during a replace | the replace wins (retried), as the in-place path does |
| U3 | Parent of the replace snapshot | `main`'s head (Java: parentless after removing `main`) |
| U4 | Sort order after a replace | kept when still valid, otherwise unsorted (Java: unsorted) |
| U5 | Privileges | update rights on the table replace create + drop + rename |
| U6 | Same name, incompatible type | fresh field id (Java: reuses the id) |
| U7 | `WRITE_BRANCH` set | schema-changing replace refused |
| U8 | Format version lower than the live table's | refused |
| U9 | Concurrent schema change | the replace fails and cleans up; no rewrite |
| U10 | Staged DROP + RENAME fallback | removed |

## Migration Plan

There is no data migration. Tables replaced by the old staged path have a new UUID and start their history at that replace; nothing changes for them. Rollback is the previous release.

## Open Questions

- Whether `commit_schema_update` can carry `AddSnapshot` unchanged, or needs a sibling that also drives the snapshot cleanup guard (task 2.1).
