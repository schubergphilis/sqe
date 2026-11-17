### Added

- Trino parity harness compares strictly and over Flight SQL, and measures Trino function coverage

    `tests/parity/` compares column types, values and order with Trino 484 over the Trino HTTP protocol and Flight SQL (`--protocol both`). `function_coverage.py` calls every function in Trino's `SHOW FUNCTIONS` whose types Iceberg can carry. `scripts/parity-test.sh --all` runs the query, Iceberg and function suites. Baseline: 137/129 of 153 queries (HTTP/Flight), 20 of 20 Iceberg scenarios, 121/115 of 296 functions. `testing/drivers/` adds the JDBC and ODBC driver probes.
