# Quack (DuckDB wire protocol)

Quack is DuckDB's RPC protocol. A DuckDB client can `ATTACH 'quack:host:port'`
and run queries against a remote engine as if it were a local database. SQE
implements the server side, so a plain DuckDB CLI, or SQE's own
`sqe-quack-client`, can talk to it.

The protocol is pre-release upstream (DuckDB plans to stabilize it for v2.0).
SQE's implementation tracks the DuckDB source, not the older `rpc_*` docs. The
full wire reference lives in [the Quack protocol notes](./quack-protocol.md).

## Server

The Quack endpoint runs inside the coordinator. It is off by default; set a
port to enable it (DuckDB's documented default is `9494`):

```toml
[coordinator]
quack_port = 9494
```

The endpoint is `POST /quack` over HTTP/1.1 with keep-alive, content type
`application/vnd.duckdb`. A `GET /` returns a plain-text identification string,
useful for checking whether a host speaks Quack.

## Client

Any DuckDB client can attach:

```sql
-- from the DuckDB CLI
ATTACH 'quack:localhost:9494' AS sqe;
SELECT 1 AS one;
```

SQE also ships `sqe-quack-client`, a native Rust client used by the test suite
and embeddable in Rust applications.

## Verified round-trip

The end-to-end path is exercised by `quack_e2e::quack_select_one_round_trip`:
authenticate against Polaris, spawn the Quack server, open a connection, prepare
and run `SELECT 1`, and read the typed result back. The unit suites
(`sqe-quack-server`, `sqe-quack-client`, `sqe-quack-wire`) cover the connection
lifecycle, auth rejection, type round-trips, and the wire codec against captured
DuckDB messages. The e2e re-run needs the Polaris stack; this round it was not
repeated under the local Docker constraint noted in the validation matrix.

## How it is tested

- `crates/sqe-coordinator/tests/quack_e2e.rs::quack_select_one_round_trip`:
  full connection -> prepare -> result-chunk flow against a live catalog.
- `crates/sqe-quack-server/tests/`: connection lifecycle, auth rejection,
  disconnect, prepare, and the security property that a policy error does not
  leak its reason to the client.
- `crates/sqe-quack-client/tests/loopback.rs`: type round-trips
  (int, varchar, decimal via Arrow).
- `crates/sqe-quack-wire/tests/upstream_fixtures.rs`: the wire codec decoded
  against captured real DuckDB messages.

## Notes

- Auth is bearer-token, same as Flight and Trino. An empty token is rejected
  before the auth provider runs.
- TLS is optional and expected to terminate at a reverse proxy in production.
- The data-type mapping between DuckDB and Iceberg is in
  [the Quack data-type matrix](./quack-datatype-matrix.md).
