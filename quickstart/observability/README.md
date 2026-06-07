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
./run.sh
```

`run.sh` brings the stack up, runs `queries.sql` a few times to generate metrics,
waits for VictoriaMetrics to scrape, then asserts SQE's metrics are present and
captures them to [`OUTPUT.md`](./OUTPUT.md). Open Grafana at
`http://localhost:13000` (admin / admin) for the **SQE Overview** dashboard. Tear
down with `./run.sh --down`.

## How the pipeline fits together

```
sqe (:9090/metrics)  --scrape 5s-->  victoriametrics (:8428)  <--query--  grafana (:3000)
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

## Output

Captured from a clean run (`./run.sh`), in [`OUTPUT.md`](./OUTPUT.md) -- the
scrape target is up and SQE counters are populated, e.g.:

```
scrape target up{job="sqe-coordinator"} = 1
sqe_rows_returned_total                 = <n>
sqe_active_sessions                     = <n>
```

## How it is tested

`run.sh` runs queries, then queries the VictoriaMetrics API for
`up{job="sqe-coordinator"}` and `sqe_*` counters, asserting SQE is scraped, and
captures a sample of the raw `/metrics`. Validated 2026-06-07.

## Gotchas

- **Metrics port**: SQE serves `/metrics` on `prometheus_port` (9090 here),
  separate from the `/healthz` port (9091). VictoriaMetrics scrapes `sqe:9090`.
- **Traces / logs**: this quickstart wires metrics. SQE also emits OTLP traces
  (`metrics.otlp_endpoint`) and an audit log (`metrics.audit_log_path`); point
  those at an OTLP collector (e.g. OpenObserve) to complete logs + traces.
- **Grafana anonymous viewer** is enabled for convenience; admin/admin for edits.
