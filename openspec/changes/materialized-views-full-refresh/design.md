# Design: materialized views, full refresh

## Decisions

### D1. An MV is a plain Iceberg table with properties

No view object in the catalog: Spark, Trino and other engines then read the MV as a normal table, which is acceptance test 1. Properties (names per apache/iceberg#11041; if the proposal's names change before it lands, SQE follows them and reads the old names too):

- `materialized-view.definition` (the SQL, as written, after identifier qualification),
- `materialized-view.definer` (principal name),
- `materialized-view.dialect` = `sqe`,
- per refresh, in the snapshot summary and mirrored in properties: `materialized-view.source.<n>.uuid`, `.snapshot-id`, `.name`.

SQE recognises an MV by `materialized-view.definition`. `DROP TABLE` on an MV is refused with a hint to `DROP MATERIALIZED VIEW`, and the reverse.

### D2. Refresh = one replace commit

`REFRESH` plans the stored definition, records each source's current snapshot id *before* reading (sources are read at those snapshots, via time travel, so the recorded ids are exactly the data read), and writes through the #892 one-commit CREATE OR REPLACE path with the source fields in the summary. Any failure before the commit leaves the old snapshot current.

### D3. Who refreshes, and with what identity

The issue asks for refresh "as the view's owner". SQE runs every query as the authenticated user and has no stored credentials (no service account), so it cannot act as the owner when someone else asks. Proposal:

- `REFRESH` runs with the caller's token, so the caller needs read access to every source and write access to the MV table.
- Only the definer or a principal with admin rights on the catalog may run `REFRESH` or `CREATE OR REPLACE` on an existing MV.
- A scheduler that refreshes on someone's behalf uses that person's (or a dedicated person-like principal's) token.

**Open question for the owner:** confirm this, or decide on stored-definer impersonation (token exchange). The latter is a new trust model and would need its own change.

### D4. No MV over policed data

At CREATE and at every REFRESH, for each source table the policy engine is asked whether a mask or row filter is in force for the acting user. If so, the statement is refused: "materialized view source `<cat.ns.t>` has a column mask or row filter in force; materialized views over policed data are not supported". This prevents the MV from materialising one user's view of the data for everyone who reads the table. Acceptance test 4.

### D5. Freshness

`system.metadata.materialized_views` gets one row per MV with Trino's columns; `freshness` is `FRESH` when every recorded source snapshot equals the source's current snapshot, `STALE` otherwise, `UNKNOWN` when a source cannot be loaded by the caller. A per-source detail is exposed as an array column `sources` (name, used snapshot, current snapshot).

### D6. Lineage and dbt

One OpenLineage `COMPLETE` (or `FAIL`) run event per refresh with the sources as inputs and the MV as output. dbt-sqe's `materialized_view` materialization: CREATE OR REPLACE on first run or definition change (compare the stored definition), REFRESH otherwise.
