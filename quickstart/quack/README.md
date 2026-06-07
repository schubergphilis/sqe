---
slug: quack
title: "Quack: the DuckDB wire protocol"
description: "Enable SQE's DuckDB Quack RPC endpoint so a DuckDB client can query SQE as if it were a local database. Quack is a pre-release DuckDB protocol; this quickstart validates the server side and documents how a client attaches."
---

# Quack: the DuckDB wire protocol

Quack is DuckDB's RPC protocol. A DuckDB client can `ATTACH 'quack:host:port'`
and query a remote engine as though it were a local database. SQE implements the
server side, so a DuckDB client can run queries against SQE's catalogs over Quack.

This quickstart turns the Quack endpoint on and proves the server speaks it.

## Status: experimental (server enablement)

Read this first. Quack is **pre-release upstream** (DuckDB plans to stabilize it
around v2.0), and that shapes what this quickstart can honestly claim:

- **What `run.sh` validates:** SQE starts with the Quack endpoint enabled and
  answers the `GET /` identification probe. That is the server side.
- **What it does not run:** a full client round-trip (a DuckDB client executing
  queries). The client side needs a *quack-capable DuckDB build* (the protocol is
  not in stock DuckDB releases), and SQE's own `sqe-quack-client` is a Rust
  library, not a standalone CLI. So there is no clean, image-only client to drive
  here. The end-to-end path is covered by the engine's test suite instead (see
  "How it is tested").

So this is **one** quickstart, not the separate `quack-server` + `quack-client`
pair on the roadmap: the client has no runnable of its own to ship as a quickstart.
A small `sqe-quack-client` CLI would make a true round-trip demo possible; that is
a potential follow-up, tracked separately from this docs work.

## What you get

| Service | Role |
|---|---|
| `rustfs` + `bucket-init` | S3-compatible warehouse storage. |
| `nessie` | The Iceberg REST catalog (auth-less). |
| `sqe` | The coordinator, with `quack_port = 9494` -> the Quack endpoint. |

## Run it

```bash
cd quickstart/quack
cp .env.example .env
./run.sh             # up -> GET / identification probe -> capture OUTPUT.md
./run.sh --down      # tear down
```

## Enabling Quack

The endpoint runs inside the coordinator. It is off by default; set a port to
enable it (DuckDB's documented default is `9494`):

```toml
[coordinator]
quack_port = 9494
```

The surface is `POST /quack` (HTTP/1.1, keep-alive, content type
`application/vnd.duckdb`) for the RPC, plus `GET /` which returns a plain-text
identification string. `GET /` is the cheap way to check whether a host speaks
Quack, and it is what `run.sh` probes:

```
$ curl http://localhost:19494/
This is a DuckDB Quack RPC endpoint, served by SQE.
```

## Connecting a client

Any quack-capable DuckDB client attaches by URI:

```sql
-- from a DuckDB CLI that supports the Quack protocol
ATTACH 'quack:localhost:9494' AS sqe;
SELECT * FROM sqe.my_namespace.my_table LIMIT 10;

-- or the table-function form
SELECT * FROM quack_query('quack:localhost:9494', 'SELECT 1 AS one');
```

SQE also ships `sqe-quack-client`, a synchronous Rust client (`QuackClient`)
used by the test suite and embeddable in Rust applications:

```rust
let mut client = QuackClient::connect("quack:localhost:9494", Some("token"))?;
let result = client.execute("SELECT 1 AS one")?;
```

The DuckDB-to-Iceberg type mapping is in `docs/quack-datatype-matrix.md`; the
wire reference is in `docs/quack-protocol.md`.

## Security: read before exposing the port

SQE logs two warnings when Quack starts, captured in [`OUTPUT.md`](./OUTPUT.md):

- **No rate limiting on the auth path.** The endpoint is an un-throttled
  brute-force / IdP-amplification oracle. Restrict network access to the port.
- **Plaintext, binds 0.0.0.0.** Without TLS, user OIDC bearer tokens travel in
  cleartext and can be captured and replayed. Set `[coordinator.tls]`
  `cert_file`/`key_file`, or do not expose the Quack port on untrusted networks.

Auth is bearer-token, the same as Flight and Trino; an empty token is rejected
before the auth provider runs. This quickstart uses the `anonymous` provider for
a dev-only stack (no IdP), exactly like the `nessie` quickstart.

## How it is tested

`run.sh` brings the stack up and asserts the `GET /` identification probe from a
clean state (validated 2026-06-07). The full connection -> prepare -> result-chunk
round-trip is covered by the engine's tests, which need the catalog stack:

- `crates/sqe-coordinator/tests/quack_e2e.rs::quack_select_one_round_trip`
- `crates/sqe-quack-server/tests/` (connection lifecycle, auth rejection)
- `crates/sqe-quack-client/tests/loopback.rs` (type round-trips)
- `crates/sqe-quack-wire/tests/upstream_fixtures.rs` (wire codec vs real DuckDB)
