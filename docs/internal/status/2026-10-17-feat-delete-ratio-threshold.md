**STATUS 2026-10-17: compaction triggers on the deleted-row share, so delete-heavy V3 tables get compacted (`feat/delete-ratio-threshold`).**

`sqe_compaction::delete_ratio_heavy_files` divides a data file's deleted rows by its `record_count`. The deleted rows come from position deletes whose `referenced_data_file` is that file. A deletion vector counts on its own, because it already holds the older deletes; without one, V2 single-file position delete files are summed. Equality deletes, position delete files spanning several data files, and files with an unknown or zero row count are skipped. `maintenance::delete_triggered_files` takes the union with the delete-file count, and both rewrite paths and `analyze_table_health` call it, so the report and the rewrite pick the same files.

Where the knob is set:
- the `CALL system.rewrite_data_files` argument `delete_ratio_threshold` (0 < r <= 1, parse-time error otherwise);
- `[maintenance.compaction] delete_ratio_threshold` (default 0.0 = off, validated 0..=1);
- the table property `sqe.maintenance.compaction.delete-ratio-threshold` (an out-of-range value falls back to the global setting).

`ProcedureCall` and `CompactionParams` lose `Eq` because the knob is an `f64`.

The compaction read plan uses unsplit `plan_files` tasks, so `record_count` is present. Only `sqe-catalog`'s scan splitter clears it.

IT `rewrite_delete_ratio_threshold_triggers_on_v3_deletion_vector`: a V3 file of 10 rows with 5 deleted. A 0.9 threshold skips it, 0.3 rewrites it and the count stays 5, and a second pass at 0.3 skips because the rewrite dropped the deletion vector.

**NEXT:**
- `CALL system.table_health` reads `[maintenance.compaction]` directly. It does not resolve the per-table `sqe.maintenance.compaction.*` overrides the scheduler applies, so its `delete_heavy_files` can differ from what the scheduler acts on for a table with overrides.
- Consider turning the ratio on by default (Spark uses 0.3) once it has run on a real V3 table.
