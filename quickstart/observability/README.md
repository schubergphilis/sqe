---
slug: observability
title: "Observability: metrics + Grafana"
description: "Scrape SQE's Prometheus metrics with VictoriaMetrics and visualise them in Grafana. A minimal queryable SQE (Nessie catalog) generates real metrics: queries, cache, sessions, scan pruning, coordinator memory."
---

# Observability: metrics + Grafana

SQE exposes Prometheus metrics on its `metrics.prometheus_port` (`/metrics`, port
9090 in this quickstart). This stack scrapes them with **VictoriaMetrics** and
renders them in **Grafana**, so you can watch query rate, cache hit/miss,
active sessions, scan pruning, and coordinator memory while you run queries.

A minimal queryable SQE (Nessie catalog + RustFS, anonymous auth) sits underneath
just to generate real metrics; the focus here is the monitoring pipeline.

## What you get

| Service | Role |
|---|---|
| `sqe` + `nessie` + `rustfs` + `bucket-init` | A queryable SQE (Nessie catalog) -- the metrics source. |
| `victoriametrics` | Scrapes `sqe:9090/metrics` every 5s (`observability/scrape.yml`). |
| `grafana` | VictoriaMetrics datasource + a provisioned "SQE Overview" dashboard. |

## Prerequisites

- Docker (with Compose v2). Pulls VictoriaMetrics + Grafana on first run.
- The SQE image (`sqe-quickstart:latest`); the compose builds it from source if absent.

## Run it

```bash
cd quickstart/observability
cp .env.example .env
./run.sh             # up -> queries -> assert metrics scraped -> capture
./run.sh --down      # tear everything down
./run.sh --check     # up -> queries -> assert SQE metrics are scraped
```

`run.sh` brings the stack up, runs `queries.sql` a few times to generate metrics,
waits for VictoriaMetrics to scrape, then asserts SQE's metrics are present and
captures them to [`OUTPUT.md`](./OUTPUT.md). Open Grafana at
`http://localhost:13000` (admin / admin) for the **SQE Overview** dashboard. Tear
down with `./run.sh --down`.

## How it works

```
sqe (:9090/metrics)  -- scrape 5s -->  victoriametrics (:8428)  <-- query --  grafana (:3000)
```

- **SQE** serves Prometheus text metrics at `/metrics` on `prometheus_port`
  (set in `sqe.toml`). The coordinator binds it on `0.0.0.0:9090`.
- **VictoriaMetrics** scrapes the in-network target `sqe:9090`
  (`observability/scrape.yml`, `job_name: sqe-coordinator`).
- **Grafana** is provisioned with VictoriaMetrics as the default datasource and
  the `SQE Overview` dashboard (`observability/sqe-dashboard.json`).

A few of the metrics SQE exposes: `sqe_rows_returned_total`, `sqe_active_sessions`,
`sqe_cache_hits_total` / `sqe_cache_misses_total`, `sqe_files_pruned_minmax_total`,
`sqe_coordinator_memory_used_bytes`, `sqe_s3_requests_total` / `sqe_s3_bytes_read_total`.

## Configuration explained

### `sqe.toml` (the metrics source)

```toml
[metrics]
prometheus_port = 9090
```

`prometheus_port` is the only line this scenario cares about: it is where SQE
serves the Prometheus text exposition (`/metrics`). It is separate from the
`/healthz` port (9091). The rest of `sqe.toml` is a minimal queryable SQE
(Nessie catalog over RustFS, `anonymous` auth) whose only job here is to emit
real counters; it mirrors the [`nessie`](../nessie/) quickstart's config.

### `observability/scrape.yml` (VictoriaMetrics)

The scrape config defines the `sqe-coordinator` job that polls `sqe:9090` every
5 seconds. `up{job="sqe-coordinator"}` is `1` when the scrape succeeds, which is
the end-to-end proof the pipeline works.

### Grafana provisioning (`observability/`)

- `grafana-datasource.yml` registers VictoriaMetrics as the default datasource
  (so the dashboard queries resolve without manual setup).
- `grafana-dashboards.yml` tells Grafana to load dashboards from disk.
- `sqe-dashboard.json` is the provisioned **SQE Overview** dashboard (query rate,
  cache hit/miss, sessions, scan pruning, coordinator memory).

### `.env.example`

Offset host ports for the four web/UI surfaces: `VM_PORT` (VictoriaMetrics
`18428`), `GRAFANA_PORT` (`13000`), plus the SQE Flight port. Defaults work as-is.

## Output

Captured from a clean run (`./run.sh`), in [`OUTPUT.md`](./OUTPUT.md) -- the
scrape target is up and SQE counters are populated, e.g.:

```
scrape target up{job="sqe-coordinator"} = 1
sqe_rows_returned_total                 = <n>
sqe_active_sessions                     = <n>
```

## How it is tested

`./run.sh --check` runs queries to generate metrics, then asserts the invariants
in `run.sh`:

- SQE's own `/metrics` endpoint exposes the `sqe_*` family (non-empty), and
  specifically `sqe_rows_returned_total` and `sqe_active_sessions` (the
  load-bearing check, reliable the moment the coordinator is up),
- VictoriaMetrics has scraped the coordinator: `up{job="sqe-coordinator"}` is
  `1` (the end-to-end proof the scrape pipeline works).

The check polls VictoriaMetrics until a real SQE sample lands before asserting,
so the scrape has actually happened. Validated 2026-06-07.

## Gotchas

- **Metrics port**: SQE serves `/metrics` on `prometheus_port` (9090 here),
  separate from the `/healthz` port (9091). VictoriaMetrics scrapes `sqe:9090`.
- **Traces / logs**: this quickstart wires metrics. SQE also emits OTLP traces
  (`metrics.otlp_endpoint`) and an audit log (`metrics.audit_log_path`); point
  those at an OTLP collector (e.g. OpenObserve) to complete logs + traces.
- **Grafana anonymous viewer** is enabled for convenience; admin/admin for edits.
