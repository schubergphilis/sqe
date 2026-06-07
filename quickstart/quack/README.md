---
slug: quack
title: "Quack: the DuckDB wire protocol"
description: "SQE speaks DuckDB's Quack RPC protocol both ways: as a server (a DuckDB CLI queries SQE) and as a client (SQE's quack_query() pulls from a remote Quack endpoint). run.sh proves the forward round-trip with a local DuckDB 1.5.3; the reverse is documented and verified."
---

# Quack: the DuckDB wire protocol

Quack is DuckDB's RPC protocol. A DuckDB client can `ATTACH 'quack:host:port'`
and query a remote engine as though it were a local database.

SQE speaks Quack **both ways**:

- **As a server** -- a DuckDB client queries SQE's catalogs over Quack
  (`coordinator.quack_port`).
- **As a client** -- SQE's `quack_query()` table function pulls rows from a
  remote Quack endpoint (another SQE, or a DuckDB running `quack_serve`).

`run.sh` turns the server on, proves it with the `GET /` probe, and -- if a
quack-capable DuckDB CLI is on your PATH -- runs the forward round-trip (DuckDB
querying an SQE Iceberg table). The reverse direction (SQE as the client) is a
manual demo documented below.

## Status: working, but pre-release

Quack is **pre-release upstream** (DuckDB plans to stabilize it around v2.0), and
the DuckDB client extension ships from the `core_nightly` repository. The
round-trip works today (validated 2026-06-07 with duckdb 1.5.3) but is not a
stable surface. Specifics:

- **What `run.sh` always validates:** SQE starts with the Quack endpoint enabled
  and answers the `GET /` identification probe (the server side, docker-only).
- **The client round-trip:** if `duckdb` is on your PATH, `run.sh` seeds an
  Iceberg table in SQE and has DuckDB query it over Quack (`quack_query()`),
  capturing the result. This needs **duckdb 1.5.3+** with the quack extension
  (`INSTALL quack FROM core_nightly`, fetched over the network). Without a local
  DuckDB, `run.sh` skips this step and prints how to enable it; the server probe
  still runs.
- SQE also ships `sqe-quack-client`, a Rust **library** (`QuackClient`),
  embeddable in Rust apps. It is not a standalone CLI -- a small CLI wrapper would
  be a tidy follow-up, but the stock DuckDB CLI already gives a working client.

This is **one** quickstart, not the separate `quack-server` + `quack-client` pair
on the roadmap: the server and client are two ends of the same round-trip, and the
client is a stock DuckDB CLI rather than a thing we ship.

## What you get

| Service | Role |
|---|---|
| `rustfs` + `bucket-init` | S3-compatible warehouse storage. |
| `nessie` | The Iceberg REST catalog (auth-less). |
| `sqe` | The coordinator, with `quack_port = 9494` -> the Quack endpoint. |

## Prerequisites

- Docker (with Compose v2). The SQE image builds from this repo on first run.
- Optional, for the client round-trip: a local `duckdb` 1.5.3+ on your PATH, with
  network access to fetch the quack extension (`INSTALL quack FROM core_nightly`).
  Without it, `run.sh` runs the server probe and skips the round-trip.

## Run it

```bash
cd quickstart/quack
cp .env.example .env
./run.sh             # up -> GET / probe -> DuckDB round-trip (if duckdb on PATH) -> capture
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

## Connecting a DuckDB client

This is what `run.sh` runs (host port `19494` maps to the container's `9494`).
The quack extension is pre-release, so it comes from `core_nightly`; the
`anonymous` provider accepts any non-empty token, supplied via `CREATE SECRET`:

```sql
INSTALL quack FROM core_nightly; LOAD quack;
CREATE SECRET (TYPE quack, TOKEN 'anonymous');

-- table-function form: run SQL on SQE, stream the rows back into DuckDB
SELECT * FROM quack_query('quack:localhost:19494', 'SELECT kind, amount FROM nessie.demo.events');

-- or attach SQE as a database
ATTACH 'quack:localhost:19494' AS sqe;
```

Against a real IdP-backed stack you would pass a real bearer token instead of
`anonymous` (e.g. a Polaris/Keycloak access token). SQE also ships
`sqe-quack-client`, a synchronous Rust client (`QuackClient`) for embedding in
Rust applications:

```rust
let mut client = QuackClient::connect("quack:localhost:19494", Some("token"))?;
let result = client.execute("SELECT 1 AS one")?;
```

The DuckDB-to-Iceberg type mapping is in `docs/quack-datatype-matrix.md`; the
wire reference is in `docs/quack-protocol.md`.

## SQE as a Quack client (the reverse direction)

The same `quack_query()` table function is registered on every SQE session, so
SQE can be the *client*: it pulls rows from a remote Quack endpoint inline. That
remote can be a DuckDB instance running `quack_serve`. Point a DuckDB at SQE and
SQE at DuckDB and you have federation in both directions over one protocol.

Start a DuckDB Quack server on the host (it returns immediately after spawning
its background listener, so keep the process alive):

```bash
# 1-3 colors, served on 0.0.0.0:9495 with a static token, plaintext.
{ printf "INSTALL quack FROM core_nightly; LOAD quack;
  CREATE TABLE colors AS SELECT * FROM (VALUES (1,'red'),(2,'green'),(3,'blue')) t(id,name);
  CALL quack_serve('quack:0.0.0.0:9495', disable_ssl := true, allow_other_hostname := true, token := 'sqe-token');
"; sleep 600; } | duckdb :memory:
```

Then, from SQE (over Flight), run `quack_query()` against it. SQE in the
container reaches the host server at `host.docker.internal` (Docker Desktop; on
Linux add `extra_hosts: ["host.docker.internal:host-gateway"]` to the `sqe`
service, or run DuckDB as a sidecar and use its service name):

```bash
docker compose exec -T -e SQE_PASSWORD=anonymous sqe sqe-cli --port 50051 --user anonymous \
  -e "SELECT * FROM quack_query('quack:host.docker.internal:9495', 'sqe-token', 'SELECT id, name FROM colors ORDER BY id')"
```

Verified 2026-06-07 (SQE pulling from a duckdb 1.5.3 server):

```
+----+-------+
| id | name  |
+----+-------+
| 1  | red   |
| 2  | green |
| 3  | blue  |
+----+-------+
```

Note the `quack_serve` argument shape: the first positional is a `quack:` URI;
`disable_ssl`, `allow_other_hostname`, and `token` are **named** parameters
(`name := value`). SQE's client uses plain HTTP for any `quack:` URI (only
`quacks:` is TLS), which is why `disable_ssl := true` is required here.

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

`run.sh` brings the stack up, asserts the `GET /` probe, then (with a local
duckdb 1.5.3) seeds `nessie.demo.events` and has DuckDB query it over Quack,
capturing the aggregated result to [`OUTPUT.md`](./OUTPUT.md). Validated
2026-06-07 from a clean state. The connection -> prepare -> result-chunk path is
also covered by the engine's tests:

- `crates/sqe-coordinator/tests/quack_e2e.rs::quack_select_one_round_trip`
- `crates/sqe-quack-server/tests/` (connection lifecycle, auth rejection)
- `crates/sqe-quack-client/tests/loopback.rs` (type round-trips)
- `crates/sqe-quack-wire/tests/upstream_fixtures.rs` (wire codec vs real DuckDB)

## Gotchas

- **Pre-release.** The protocol targets DuckDB v2.0 and the client extension is
  from `core_nightly`. The round-trip works with duckdb 1.5.3 but pin the version;
  a different DuckDB build may not match the wire format.
- **`quack_query` is invisible until the extension loads.** It will not show in
  `duckdb_functions()` until `INSTALL quack FROM core_nightly; LOAD quack`.
- **Token must be non-empty.** The anonymous provider takes any non-empty token;
  an empty one is rejected before the auth provider runs.
- **Reverse direction needs host networking.** SQE-in-a-container reaches a host
  DuckDB at `host.docker.internal` on Docker Desktop; on Linux add
  `extra_hosts: ["host.docker.internal:host-gateway"]` to the `sqe` service.
