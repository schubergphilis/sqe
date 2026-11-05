**STATUS 2026-11-04: maintenance file steps reload the committed table for its vended credential (`fix/orphan-files-vended-credentials`, portal request §2).**

Live on the portal's quickstart stack, a table writer's deleting `remove_orphan_files` failed at the first metadata read with `failed to load signing credential`, while its dry run worked. Cause: since `bab5a9af5` the deleting call commits a table property first and then runs `RemoveOrphanFilesAction` on the table `tx.commit` returned. The REST catalog's `update_table` builds that table's `FileIO` from the catalog properties only (`load_file_io(.., None)`); the vended credential arrives only with `loadTable`. `expire_snapshots` had the same shape: `committed.cleanup_expired_files(..)` deleted through the commit's table, so on a vending catalog without a static key the cleanup failed (unnoticed in the portal run, which expired nothing with files).

`reload_committed` (maintenance.rs) reloads the table after the commit and cache invalidation and checks that the reload is this commit's metadata; if another writer committed in between, the file step is refused with `CommitConflict` and nothing is deleted. Both calls use it.

Gates: `cargo test -p sqe-coordinator --lib -- maintenance` 110 passed (new: `reload_committed_returns_this_commit_or_refuses`); the integration results are in the MR. The test stacks set static S3 keys, so neither the unit nor the integration suite reproduces the missing credential; it needs a vending catalog without a static key (the portal quickstart).

Same pattern elsewhere, not fixed here: `puffin_carry::carry_forward_statistics` reads and writes Puffin files through `committed.file_io()` after every rewrite, compaction and DML commit. It is best effort, so on a vending catalog it would drop statistics silently.

**NEXT:** `puffin_carry` on vending catalogs: reload (or reuse the loaded table's `FileIO`) before carrying statistics, and check whether that explains the missing NDV sidecars.
