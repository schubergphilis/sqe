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

## Notes

- Polaris persistence is in-memory; the demo schema is lost if the `polaris`
  container restarts (the data files survive in rustfs, but the catalog
  registration does not). Re-run the script to reload.
- The `tpch` connector is wired into the Trino baseline via
  `tests/trino/catalog/tpch.properties` (mounted in `docker-compose.compare.yml`).
