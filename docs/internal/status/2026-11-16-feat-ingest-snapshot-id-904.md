**STATUS 2026-11-16: Flight SQL ingest returns the committed snapshot id as a gRPC trailer (`feat/ingest-snapshot-id-904`, #904).**

arrow-flight's `FlightSqlService::do_put` turns the `i64` from `do_put_statement_ingest` into `DoPutUpdateResult { record_count }`, which has no room for a snapshot id. The id now travels as trailing metadata `x-sqe-snapshot-id`, as decided for #904.

- `sqe-write`: `handle_ingest_streaming` returns `IngestOutcome { rows, snapshot_id }`. An append reads the id from the committed table (the branch ref when the ingest wrote to a branch, else the current snapshot); an empty ingest returns the id of its empty snapshot. A replace returns `None`: the rewrite helper does not hand back the committed table, and reloading it could race a concurrent writer.
- `sqe-coordinator`: new `flight_trailers.rs`. `SnapshotTrailerLayer` puts a `SnapshotIdSlot` into the extensions of every `DoPut`; tonic and arrow-flight carry the http extensions into the `tonic::Request`, the ingest handler fills the slot after the commit, and the layer adds the id to the trailers frame next to `grpc-status`. Inside a transaction the slot stays empty, since the snapshot is on the transaction branch until COMMIT.
- `sqe-server`: the layer sits after `FlightTraceLayer`.
- New dependency edge: `http-body = "1"` in `sqe-coordinator` (already in the graph through tonic).
- Docs: `getting-started/connecting.md`, "Bulk ingest", says where the id is, when it is absent, and that ADBC drivers do not expose trailers.

`.config/budgets.json`: sqe-coordinator lines 67500 -> 67800 (67720 with this change: the 220-line trailer layer and its unit tests). !1818 and !1819 raise the same line; whichever merges second takes the larger value.

Gate:

- `cargo clippy -p sqe-write -p sqe-coordinator -p sqe-server --all-targets -- -D warnings`: exit 0.
- `cargo test -p sqe-coordinator --lib -- flight_trailers`: 2 passed.
- `cargo test -p sqe-write --lib -- ingest`: 13 passed.
- `make test-integration FILTER=flight_ingest_e2e`: 26 passed, 0 failed, including the new `ingest_returns_the_committed_snapshot_id_as_a_trailer` (create and append give the newest `table_snapshots` id; replace and an append inside a transaction give no trailer).

Left open: the replace-mode id (return the committed table from the rewrite helper). #904 asks 1, 2 and 4 merged earlier (!1813 and its follow-ups); this MR is ask 3.

**NEXT:** #907 `cosine_similarity` in `sqe-trino-functions`.
