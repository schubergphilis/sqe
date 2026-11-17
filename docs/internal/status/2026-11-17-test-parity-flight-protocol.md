**STATUS 2026-11-17: Trino parity harness is strict, covers Flight SQL and Trino's function list (`test/parity-flight-protocol`, openspec trino-sql-parity 1.1-1.6).**

`tests/parity/` now compares SQE with Trino 484 on column types, values canonicalised by Trino's type, and order under `ORDER BY`, over the Trino HTTP protocol and Flight SQL (ADBC). The old lenient verdict (two decimal places, types ignored) is still printed for comparison; it scored 153 of 153 on both protocols.

`function_coverage.py` calls every scalar, aggregate and window signature in Trino's `SHOW FUNCTIONS` whose types Iceberg can carry, with sample arguments (typed NULLs when Trino rejects them). A signature the generator cannot call on Trino counts against its function.

`scripts/parity-test.sh` takes `--protocol http|flight|both` and `--all` (queries, Iceberg scenarios, functions; reports in `target/parity`).

`testing/drivers/` holds the JDBC (Trino and Flight SQL drivers) and ODBC (Arrow Flight SQL ODBC, built from source in Docker) probes used for !1826 and !1829, with their last results.

The `alter_set_properties` Iceberg scenario lowered a v3 table to v2. SQE creates v3 tables by default and Trino 484 v2, and Trino refuses the downgrade too. The scenario now creates a v2 table on both engines and upgrades it to v3.

Baseline (SQE at origin/main f3c5ac427), strict:

| Suite | Total | HTTP | Flight |
|---|---|---|---|
| Queries | 153 | 137 | 129 |
| Iceberg scenarios | 20 | 20 | 20 |
| Trino functions | 296 | 121 | 115 |

Per-query and per-function tables: `docs/evidence/trino-parity/2026-11-17-*.md`. Root causes and the work plan: `openspec/changes/trino-sql-parity/`.

Not run: the tempto suite and `scripts/trino-compat-test.sh`. `parity-test.sh` is not wired into CI or a make target, so its non-zero exit on the baseline breaks nothing.

**NEXT:** trino-sql-parity task 2 (unsigned results as `bigint` on Flight, HTTP NaN and type labels), then 3 (function return types).
