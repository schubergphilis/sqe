**STATUS 2026-10-23: dbt-sqe result types follow the Arrow schema; list `unique_key` works (`fix/dbt-sqe-agate-integer-types`).**

Portal #42: verify MERGE for dbt incremental end to end, and stop agate reading 0/1 integers as booleans.

- `connections.py`: `get_result_from_cursor` builds agate column types from the Arrow schema (bool, integer via dbt's `Integer`, float/decimal via dbt's `Number`, date, timestamp, else Text). Before: 0/1 bigint -> Boolean, `'001'` -> Number 1. Rows are built by position, so a repeated column name keeps both columns. Unit tests in `tests/test_connections.py` (28 passed; the 2 new ones fail on the old code).
- `incremental.sql`: `unique_key` may be a list for `merge` and `delete+insert`. Multi-key delete uses `MERGE ... WHEN MATCHED THEN DELETE` (the engine has no correlated `EXISTS` in DELETE).

Live probe (coordinator from main 255005bd on `tests/sqe-test.toml`, dbt-core 1.12.5, three `dbt run`s, contents asserted after each):
- `merge`, `unique_key='id'`, with an `is_incremental()` filter: 1->1, 2->2, 3->3, 4->3. Correct.
- `merge`, `unique_key=['id','k']`: failed to parse before the fix; correct after.
- `delete+insert`, `unique_key=['id','k']`: correct after the fix.
- `run_query` 0/1/'001' rendered into a table: `'False','True','1'` before, `'0','1','001'` after.

Engine findings from the probe (not dbt-sqe):
- **DELETE / UPDATE with `IN (subquery)` change the wrong rows** (single-key `delete+insert` lost id 1). Fixed on `fix/dml-in-subquery-row-alignment`.
- MERGE `WHEN MATCHED THEN DELETE` reports `rows_affected` 2 for 1 deleted row.
- `DELETE ... WHERE EXISTS (correlated subquery)` fails: "Physical plan does not support logical expression Exists".

Local env note: `requirements-lock.txt` does not install with hash checking on macOS arm64/py3.12 (pyarrow 26.0.0 hash set misses that wheel); the probe venv used the same pins without hashes.

**NEXT:** unchanged
