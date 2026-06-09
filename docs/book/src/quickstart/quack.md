---
slug: quack
title: "Quack: the DuckDB wire protocol"
description: "SQE speaks DuckDB's Quack RPC protocol both ways: as a server (a DuckDB CLI queries SQE) and as a client (SQE's quack_query() pulls from a remote Quack endpoint). run.sh proves the forward round-trip with a local DuckDB 1.5.3; the reverse is documented and verified."
---

# Quack: the DuckDB wire protocol

Quack is DuckDB's RPC protocol. A DuckDB client can `ATTACH 'quack:host:port'`
and query a remote engine as though it were a local database.

SQE speaks Quack **both ways**:

- **As a server** — a DuckDB client queries SQE's catalogs over the Quack
  endpoint (`quack_port` in the coordinator config).
- **As a client** — SQE's `quack_query()` table function pulls rows from a
  remote Quack endpoint (another SQE instance, or a DuckDB running
  `quack_serve`).

## How it works

- The stack is a queryable SQE with a Nessie catalog and RustFS warehouse
  storage — the same base as the [Nessie quickstart](./nessie.md) — plus the
  Quack endpoint enabled on the coordinator.
- Setting `quack_port` in the coordinator config is all that is needed to enable
  the endpoint. It serves a `GET /` identification probe and a `POST /quack`
  RPC surface.
- `run.sh` always validates the server-side probe (`GET /`). If a local DuckDB
  1.5.3+ is on your PATH, it also seeds an Iceberg table in SQE and has DuckDB
  query it over Quack via the `quack_query()` table function.
- The reverse direction (SQE as a Quack client, pulling from a DuckDB
  `quack_serve` instance) is documented in the repo README and verified
  separately — SQE's `quack_query()` function is available on every session.

Note that Quack is a **pre-release protocol**: DuckDB plans to stabilize it
around v2.0, and the client extension ships from `core_nightly`. The round-trip
works today (validated with duckdb 1.5.3) but is not a stable surface yet.

## What it demonstrates

- Enabling SQE's Quack server endpoint with a single config key.
- The `GET /` identification probe confirming the endpoint is live.
- A DuckDB client querying an SQE Iceberg table over the Quack protocol
  (forward round-trip).
- SQE as a Quack client: `quack_query()` pulling rows from a remote DuckDB
  `quack_serve` instance (reverse direction, documented and verified).

**Status:** experimental (2026-06-07).

## Run it

Full config, `docker compose`, queries, and captured output are in the repo:

**→ [quickstart/quack/](https://github.com/schubergphilis/sqe/tree/main/quickstart/quack/)**

```bash
cd quickstart/quack
cp .env.example .env
./run.sh
```
