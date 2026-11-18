# docs/trino-parity-testing

**STATUS:** MR open, docs only.

- `development/testing.md`: new "Trino parity" section (commands, suites,
  strict verdicts, function denominator from Trino `SHOW FUNCTIONS`, the
  2026-11-17 baseline) and "Driver probes" (Trino JDBC 65/74, Flight SQL JDBC
  71/72, Flight SQL ODBC 105/114); CI table rows for both.
- `features/trino-compatibility.md`: "How close to Trino" with the baseline
  and the main gap causes.
- `compare/trino-compatibility.md` and `README.md`: the "~96%" figure is now
  described as matrix coverage (names), next to the measured numbers.
- Blog: `2026-11-17-153-of-153-was-a-rounding-error.md`.

The numbers are the baseline on main before `fix/trino-result-boundary`; the
blog describes that branch as in review.

**NEXT:** refresh the baseline tables (testing page, book, compare page,
README) after each parity fix branch merges.
