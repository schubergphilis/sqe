**STATUS 2026-11-04: Puffin statistics carry uses a vended `FileIO` after rewrites (`fix/puffin-carry-vended-credentials`, follow-up to `fix/orphan-files-vended-credentials`).**

`puffin_carry::carry_forward_statistics` read the previous sidecar, wrote the new one and deleted it on a lost commit through `committed.file_io()`. The REST catalog builds a commit's table with `load_file_io(.., None)`, so that `FileIO` has no vended credential. On a vending catalog without a static key, every compaction (`rewrite_data_files`, local and distributed) and `rewrite_manifests` dropped the table's NDV statistics silently: the carry is best effort, falls back to an upper-bound estimate when the read fails, then fails the write and only logs a warning and counts `sqe_puffin_sidecar_total{outcome="write_failed"}`.

The DML write path was not affected: `write_handler::carry_puffin_statistics` already reloads the table before the carry and passes that table, so its `file_io()` is a loadTable's.

Fix: `carry_forward_statistics` takes the `FileIO` to use as a parameter. The write handler passes its reloaded table's; maintenance goes through the new `carry_rewrite_statistics`, which reloads the table and falls back to the pre-rewrite table's `FileIO` if the reload fails.

Gates: `cargo test -p sqe-write --lib -- puffin` 16 passed; `cargo test -p sqe-coordinator --lib --features test-sqlite -- statistics rewrite` 9 passed (incl. `rewrite_data_files_carries_statistics_onto_the_new_snapshot`); clippy `-D warnings` clean on sqe-write and sqe-coordinator. The sqlite rig and the integration stacks use local files or static S3 keys, so no test reproduces the missing credential; it needs a vending catalog without a static key (the portal quickstart).

**NEXT:** verify on the portal quickstart that `CALL system.rewrite_data_files` leaves a statistics file on the new snapshot (`sqe_puffin_sidecar_total{op="rewrite",outcome="registered"}`).
