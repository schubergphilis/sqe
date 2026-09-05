# 705: sqe-cli and sqe-bench each hand-roll a Trino HTTP client and a Flight client, with divergent bugs

- Scope: sqe-cli (http.rs, flight.rs), sqe-bench (client/trino.rs, client/flight.rs), sqe-trino-compat (protocol types)
- Severity: medium
- Category: maintainability
- Effort: M (days)
- Status: open

## Problem

There are two independent Trino HTTP protocol clients and two independent Flight SQL clients in the workspace, none shared:

| Client | File | Lines |
|---|---|---|
| CLI Trino HTTP | `crates/sqe-cli/src/http.rs` | 131 |
| Bench Trino HTTP | `crates/sqe-bench/src/client/trino.rs` | 329 |
| CLI Flight | `crates/sqe-cli/src/flight.rs` | 114 |
| Bench Flight | `crates/sqe-bench/src/client/flight.rs` | 202 |

Each has bugs the other does not:

- The CLI client caps pagination and then returns whatever it collected as a success, `crates/sqe-cli/src/http.rs:63` and `:76-107`:
  ```rust
  const MAX_PAGINATION_ROUNDS: usize = 1000;
  for _ in 0..MAX_PAGINATION_ROUNDS { ... }
  Ok(QueryResult { columns, rows })
  ```
  With the server's 1000-row pages (`server.rs:28`) any result over one million rows is silently truncated.
- The bench client extends rows before checking `resp.error` (`client/trino.rs:100-108`), breaks on `state == "FINISHED"` even if `nextUri` is present (`:131-133`), does not validate `nextUri` origin (the CLI does, `http.rs:96-104`), and sends `X-Trino-User` (`:69-71`) which the server ignores. It maps Trino `decimal` to `Float64` and `tinyint`/`smallint`/`integer` to `Int32` (`:149-168`), so the compare-with-Trino path canonicalizes through a lossy conversion.
- Both binaries depend on the whole `sqe-trino-compat` server crate (axum, moka, tracing-opentelemetry, sqe-auth, sqe-metrics) only to deserialize `TrinoResponse` and `TrinoColumn` (`crates/sqe-cli/Cargo.toml`, `crates/sqe-bench/Cargo.toml`, `client/trino.rs:7`).
- `crates/sqe-bench/src/client/mod.rs:1-3` carries `#![allow(dead_code)]` with the comment "used by the test and load commands which are not yet wired", but both are wired (`crates/sqe-bench/src/main.rs:197`, `:311`). The allow now hides real dead code.

## Why it matters

A wire-protocol fix (page retry semantics from issue 701, session headers, error shapes) has to be re-implemented in two clients that are tested by nobody in CI. The compare-with-Trino harness is the engine's correctness oracle; a lossy decimal conversion in the oracle client turns real mismatches into rounding noise (the memory of "q75 rounding" artifacts is consistent with this). Silent truncation in the CLI means a user running `sqe-cli -e "SELECT ..." > out.csv` gets a shorter file with exit code 0.

## Proposed change

1. Split the wire types out of `sqe-trino-compat` into a small `sqe-trino-protocol` crate (`TrinoResponse`, `TrinoColumn`, `TrinoError`, `TrinoStats`, type-signature helpers). `sqe-trino-compat`, `sqe-cli`, and `sqe-bench` depend on it; `sqe-cli` and `sqe-bench` drop the server crate.
2. Add `sqe-trino-protocol::client::TrinoHttpClient` (reqwest, Basic or Bearer, `X-Trino-Catalog`/`Schema`, same-origin `nextUri` check, error-before-data, unbounded pagination with a per-page callback, DELETE on drop for cancel) and use it from both binaries. Do the same for a `sqe-flight-client` wrapper over `FlightSqlServiceClient` (handshake, token, `do_get` collect or stream).
3. In the bench Trino client, decode Trino types faithfully: `decimal(p,s)` to `Decimal128`, integers to their widths, timestamps to `Timestamp`. Comparison then operates on equal Arrow types.
4. Replace `MAX_PAGINATION_ROUNDS` with a streaming display loop; if a cap is kept for safety, exceeding it must be an error.
5. Remove the stale `#![allow(dead_code)]` and delete what is actually dead.

## Acceptance criteria

- `cargo tree -p sqe-cli -e normal | grep axum` and the same for `sqe-bench` return nothing.
- A unit test against a mocked server returning 1500 pages verifies the CLI receives all rows.
- A unit test verifies the client rejects a `nextUri` with a different origin and surfaces `error` before consuming `data`.
- Bench compare round-trips a `decimal(18,2)` column through the Trino client as `Decimal128(18,2)`.
- `crates/sqe-bench/src/client/mod.rs` has no crate-level `allow(dead_code)`.

## Related

- 701 (page retry semantics the client must implement).
- 706 (bench bin/lib split; the client module is currently unreachable from `tests/`).
- Memory: Trino compare silent skip; compare known artifacts (q75 rounding).
