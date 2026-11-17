# SQE-vs-Trino parity harness

Runs the same SQL against SQE and a real Trino baseline, both pointed at the
**same Polaris REST catalog** (shared Iceberg tables on rustfs S3). Because the
data is identical, any result difference is an engine behavior difference.

```
   tpch.tiny --CTAS via Trino--> iceberg.tpch_demo.* (Polaris + rustfs)
                                   |                     |
                          SQE :28080            Trino :38080
                                   \                     /
                              parity_compare.py diffs the results
```

## Run

```bash
scripts/parity-test.sh                   # up stack, load demo data, compare
scripts/parity-test.sh --no-build        # skip SQE image rebuild
scripts/parity-test.sh --reload          # drop + reload the demo schema
scripts/parity-test.sh --protocol both   # SQE over Trino HTTP and Flight SQL
scripts/parity-test.sh --all --protocol both   # + Iceberg scenarios + function coverage
```

The script brings up `rustfs + polaris + sqe + trino` (from
`docker-compose.test.yml` + `docker-compose.compare.yml`), bootstraps the
warehouse, loads TPC-H tiny once via Trino's built-in `tpch` connector
(`CREATE TABLE ... AS SELECT * FROM tpch.tiny.*`), then runs the comparator.
Exit code is non-zero if any query diverges. Reports go to `$PARITY_OUT`
(default `target/parity`).

Flight SQL needs the ADBC driver: `pip install -r tests/parity/requirements.txt`
in a venv and point `PARITY_PYTHON` at its python.

## Verdicts

The Trino baseline is always read over its HTTP protocol; SQE is read over
each protocol asked for (`--protocol http|flight|both`). Every SQE result is
compared with the baseline (`parity_client.compare`):

| Verdict | Meaning |
|---|---|
| `MATCH` | same column types, same values, same order under a top-level `ORDER BY` |
| `TYPE` | same values, different column type (`length()` integer vs bigint) |
| `ORDER` | same rows, different order where the query fixes one |
| `DIFF` | different values or row count |
| `SQE-ERR` / `TRINO-ERR` | one side failed |
| `BOTH-ERR` | both rejected the statement; counts as parity |
| `MISSING` | (function coverage) the function name does not resolve on SQE |

Values are compared after canonicalising both sides by Trino's column type:
decimals as decimals, floating point to 9 significant digits, timestamps
without trailing fraction zeros. Flight results are Arrow; `parity_client`
maps the Arrow type to the Trino type name and encodes the values the way
Trino's JSON does, so an unsigned Arrow type shows up as `arrow:uint64`.
`parity_compare.py` also prints the older lenient count (rounded to 2 places,
types ignored) so a run stays comparable with earlier reports.

## Files

- `parity_client.py` -- the shared clients (Trino HTTP, Flight SQL over ADBC),
  the Arrow-to-Trino type map and `compare`.
- `parity_compare.py` -- runs each query in `queries.json` against both engines.
- `function_coverage.py` -- every scalar, aggregate and window signature that
  Trino's `SHOW FUNCTIONS` lists, called with sample arguments of its declared
  types (typed NULLs when Trino rejects the samples). Signatures whose types
  Iceberg cannot carry (HyperLogLog, Geometry, IPADDRESS, ...) or that take a
  lambda the generator cannot build are listed but not counted. A function is
  covered when every counted signature is `MATCH`; one that matched only on
  typed NULLs is reported apart, and a signature for which no generated call
  works on Trino counts as `UNTESTED`, against the function.
- `queries.json` -- the query set. Uses the TPC-H column names from Trino's
  `tpch` connector, which are **bare** (`extendedprice`, not `l_extendedprice`).
  Add queries here to grow coverage; covers counts, aggregates, multi-way joins
  (q3/q5/q10), window functions, DISTINCT, HAVING, subquery-IN, CASE.

## Standalone comparator

If the stack and demo data already exist:

```bash
python3 tests/parity/parity_compare.py \
  --sqe http://localhost:28080/v1/statement \
  --trino http://localhost:38080/v1/statement \
  --schema tpch_demo
```

## Iceberg SQL parity (DDL/DML/metadata/interop)

`parity_compare.py` is read-only. `iceberg_parity.py` covers the Iceberg-specific
surface by running a *sequence* of DDL/DML on both engines and diffing a verify
query. Two scenario kinds (see `iceberg_scenarios.json`):

- **parity** — SQE and Trino each run the identical setup on their OWN table
  (`{t}_sqe` / `{t}_trino`), then a verify SELECT is diffed. Proves the write
  path produces the same logical result.
- **interop** — one engine writes a SHARED table; both engines read it and the
  reads are diffed. Proves SQE-written Iceberg (including merge-on-read
  position-delete files) is Trino-readable and vice versa.

```bash
python3 tests/parity/iceberg_parity.py tests/parity/iceberg_scenarios.json --protocol both
```

## Baseline

The current numbers and the work to close the gaps are in the OpenSpec change
`openspec/changes/trino-sql-parity/`; each run's per-function table is in
`docs/evidence/trino-parity/`.

Table names are fully qualified (`tpch_demo.<t>`) on purpose: SQE's write path
resolves an UNqualified name to the `default` namespace while reads use the
session schema, so qualifying isolates these tests to the Iceberg operations.
That namespace split is a known gap (see below), not exercised here.

### Known gaps this surfaced (excluded from the green set)

- **Unqualified-write namespace**: `CREATE/INSERT` land in `default`, reads use
  the session schema, so `CREATE TABLE t; INSERT; SELECT * FROM t` fails
  "table not found" under a non-`default` session schema. (Same root as #343.)
- **Schema-evolution read**: `SELECT <col>` where `<col>` was added by
  `ALTER TABLE ADD COLUMN` returns a broken response for rows written before the
  add (should NULL-backfill).
- **ALTER RENAME COLUMN**: accepted but not persisted.
- **MERGE with inline `(VALUES ...) AS src(cols)` source**: source-column
  resolution fails; table/subquery sources work.
- **Snapshot/history count**: SQE does not record the CREATE as a snapshot/
  history entry (off-by-one vs Trino).

## Notes

- Polaris persistence is in-memory; the demo schema is lost if the `polaris`
  container restarts (the data files survive in rustfs, but the catalog
  registration does not). Re-run the script to reload.
- The `tpch` connector is wired into the Trino baseline via
  `tests/trino/catalog/tpch.properties` (mounted in `docker-compose.compare.yml`).
