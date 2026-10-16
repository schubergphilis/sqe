**STATUS 2026-10-16: client result path measured; dead multi-endpoint builder removed (`fix/result-path-810`, #810).**

Item 4 of #810 (keepalive and frame size on the coordinator-to-worker endpoint) was already in `apply_grpc_transport_endpoint` (65df8538). This MR covers items 1 and 3.

**Measurement.** Loopback on one Mac: the `dev-release` `sqe-server` from 2026-10-15 as the coordinator, `policy.engine = "passthrough"`, `query.max_result_rows` raised to 1e9. The client was Python ADBC Flight SQL 1.11 with pyarrow 23. The query was `SELECT value, value*2, value*3, CAST(value AS VARCHAR) FROM generate_series(..)`: 80M rows, 4 columns, about 3.2 GB of Arrow buffers. The four-stream case splits the range into four queries on four connections.

| streams | wall | throughput | coordinator CPU | client CPU |
|---|---|---|---|---|
| 1 | 3.8-4.3 s | 0.74-0.84 GB/s | ~145% | ~130% |
| 4 | 1.9-2.2 s | 1.4-1.7 GB/s | ~300-370% | ~170-220% |

Producing the result costs almost nothing: a server-side `count`/`sum(length)` over 40M rows answers in 0.13 s. The export is therefore transfer- and encode-bound. One stream reaches about 6.5 Gbit/s, roughly 65% of a 10 GbE link.

The numbers do not show that the coordinator's single-stream encode is the ceiling. The four-stream run parallelised client decode as much as server encode, the coordinator did not peg one core, and four streams gave 2x rather than 4x. Attributing the limit needs a client on a separate host, or a discard client that does not decode.

**Change.** Removed `SqeFlightSqlService::build_flight_info_distributed`, `QueryResultLocation` and their seven unit tests from `flight_sql.rs`. They had no production caller. Their design (one endpoint per worker, each with a worker `Location`) cannot apply to the distributed-scan topology: workers stream scan output to the coordinator, and every join, aggregate and sort runs there. `build_flight_info_single`'s doc now says so.

**Found on the way:**
- The default `query.max_result_rows = 1_000_000` is the first wall a large-export client hits, long before stream throughput matters.
- ADBC refuses two shapes with `endpoint 0 returned inconsistent schema`. An unaliased expression column is the first: GetFlightInfo advertises `count(*)`, but DoGet delivers `_col0`, because `get_schema_inner` plans the raw text without `normalize_for_planning`. `EXPLAIN` is the second: GetFlightInfo advertises an empty schema, since `EXPLAIN` is not classified as a query, while DoGet delivers `plan_type`/`plan`. Both are fixed on their own branch.
- Under `policy.engine = "ranger"`, `generate_series(..)` is refused with `tag state unknown for table generate_series()`, the same gap #846 closed for Iceberg metadata TVFs (filed as #847).

**NEXT:** multi-endpoint results (coordinator per-partition tickets, flag-gated, unordered projection/filter plans only), but only if a separate-host measurement shows the coordinator stream, not the client, is the limit.
