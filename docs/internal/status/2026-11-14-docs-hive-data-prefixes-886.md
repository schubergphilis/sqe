**STATUS 2026-11-14: OpenSpec for a separate Hive data-prefix list (`docs/hive-data-prefixes-886`, #886, openspec hive-data-prefixes).**

Design only, no code. Hive table and partition locations are gated by the file TVF allow-list (`TvfPolicy::check_path` in `sqe-catalog/src/hive/store.rs`), so every Hive location must also be a `read_parquet` prefix, and a raw TVF read of the table's files skips its row filters, masks and column restrictions. The change proposes `[hive] allowed_data_prefixes` plus per-root `data_prefixes` for the Hive gate, a refusal of file TVF reads under Hive prefixes and loaded Hive locations (shared registry and refusal point with `tvf-prefix-table-overlap`), a startup error per overlapping prefix pair, and a one-release fallback to the TVF list. `openspec validate hive-data-prefixes --strict` passes.

Open for the maintainer: D5 (who may `ATTACH ... TYPE hive_external`; whether admin roles and `allow_all_object_stores` bypass the Hive gate) and the fallback length (D4). Recommended: ATTACH unchanged, keep the bypass for the Hive gate only.

**NEXT:** decide D5/D4, then implement after or together with `tvf-prefix-table-overlap` 4.1/4.3; #884 next in the batch.
