**STATUS 2026-10-12: an INSERT stores what the user wrote or fails (`fix/insert-cast-834`, #834).**

`INSERT INTO t VALUES ('abc')` into a BIGINT column succeeded and read back as 0. The same happened through `INSERT ... SELECT` and for `VALUES (NULL)` into a NOT NULL column. The cause had two parts in `sqe_compaction::writer`:

- `apply_stamped_schema` cast each column with arrow's safe cast. A safe cast turns a value it cannot convert into NULL. It now uses a checked cast (`CastOptions { safe: false }`), so the write fails with `Failed to cast column 'n' from Utf8 to Int64`.
- The stamped schema is all-nullable. A NULL in a required column therefore reached the parquet writer, which stored the slot's zeroed value. `check_required_columns` now runs after stamping on the streaming path (`stamp`) and in `stamp_batches`, and refuses the write with `NULL value not allowed for NOT NULL column: n`. It reads the array's cached `null_count`.

Tests:
- Unit tests in `sqe-compaction`: an unconvertible value fails the cast; valid values and NULLs still cast; a NULL is refused in a required column and passes in an optional one.
- Integration test `insert_value_integrity_e2e`: five INSERT shapes fail and leave the table unchanged; `VALUES ('7')` still lands.
- Verified live on the distributed rig.

**NEXT:** #832 / #833 (CREATE OR REPLACE VIEW with an unchanged schema; a view that selects from a view), then #816 and #803.
