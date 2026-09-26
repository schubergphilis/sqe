# Backup and Restore

SQE holds no table data. The tables live in your catalog and your object store, and those are what you back up. This page says what to back up, the one consistency rule that makes a restore work, how to restore, what "restored" means, and how often to prove it.

## What to back up

| Component | Back up | Why |
|---|---|---|
| Catalog database (for example the Polaris Postgres) | Yes, point-in-time | It holds the pointer from each table to its current metadata file. |
| Object store (data files, manifests, metadata files) | Yes, bucket versioning or replication | It holds every file a table snapshot references. |
| Audit volume, native log | Yes | The audit trail. Verify a restored copy with `sqe-server --verify-audit`. |
| Audit volume, export spool | No | It is a shipping buffer, rebuilt from new events. |
| Session file (`persistence = "file"`) | No | It holds live session tokens and nothing worth restoring. |
| SQE itself | No | Stateless: redeploy the image and the chart values. |

## The consistency rule

An Iceberg table spans both stores. The catalog points at a metadata file, the metadata file lists manifests, and the manifests list data files in the object store. A catalog restored to time T points at the metadata of time T, so every file that metadata references must still exist in the object store.

Two SQE procedures delete files:

- `CALL system.expire_snapshots` removes snapshots older than the table's `history.expire.max-snapshot-age-ms` (default 5 days) unless `older_than` says otherwise, and deletes files that only those snapshots referenced.
- `CALL system.remove_orphan_files` deletes files no snapshot references, older than 3 days by default.

A catalog backup older than those windows can point at files that are already gone. So:

> **Keep deleted object versions at least as long as your oldest catalog backup.** With bucket versioning, set the noncurrent-version expiry to at least the catalog backup retention. A delete then leaves a restorable version behind for as long as any catalog backup could need it.

Without versioning, a catalog restore is only safe to a point inside the snapshot-expiry and orphan-sweep windows.

## Restoring to a point in time

1. Stop writes: scale the coordinator to zero and pause anything that runs maintenance procedures.
2. Restore the catalog database to time T.
3. In the object store, restore the versions current at T for any object deleted after T (remove the delete markers created after T).
4. Start SQE and run the checks below.

For a single damaged table, you can re-point the catalog at a known-good metadata file with `CALL system.register_table` instead of restoring the whole catalog.

## What "restored" means

A restore counts as done when all of these hold:

- The catalog answers and every critical table loads its current snapshot.
- A full scan of each critical table finishes without a missing-file error.
- Row counts of the critical tables match the record you took before the test.
- A restored audit log passes `sqe-server --verify-audit`, and its last hash matches the value recorded at backup time.

## How often to prove it

Run a restore test at least once a quarter, after any change to the backup setup, and before go-live (see [Go-live and Retirement Checklists](./go-live-checklist.md)). Record for each test:

- Date, environment and the restore point T.
- Tables checked and the result of each check above.
- Time from start to "restored" (your measured recovery time).
- Newest data present after the restore (your measured data loss window).
- Who ran it.

Set retention per class to match your recovery point objective and your log retention policy. The numbers are yours; the rule above keeps them consistent with each other.
