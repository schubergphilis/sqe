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
scripts/parity-test.sh                 # up stack, load demo data, compare
scripts/parity-test.sh --no-build      # skip SQE image rebuild
scripts/parity-test.sh --reload        # drop + reload the demo schema
```

The script brings up `rustfs + polaris + sqe + trino` (from
`docker-compose.test.yml` + `docker-compose.compare.yml`), bootstraps the
warehouse, loads TPC-H tiny once via Trino's built-in `tpch` connector
(`CREATE TABLE ... AS SELECT * FROM tpch.tiny.*`), then runs the comparator.
Exit code is non-zero if any query diverges, so it works as a CI gate.

## Files

- `parity_compare.py` -- runs each query in `queries.json` against both engines
  over the Trino HTTP wire protocol, follows pagination, normalizes numerics
  (rounding) and row order, and reports `MATCH` / `DIFF` / one-sided `ERROR` /
  `BOTH-ERR`. `BOTH-ERR` counts as parity (identical rejection).
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
python3 tests/parity/iceberg_parity.py tests/parity/iceberg_scenarios.json
```

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
