---
slug: observability
title: "Observability: metrics + Grafana"
description: "Scrape SQE's Prometheus metrics with VictoriaMetrics and visualise them in Grafana. A minimal queryable SQE (Nessie catalog) generates real metrics: queries, cache, sessions, scan pruning, coordinator memory."
---

# Observability: metrics + Grafana

SQE exposes Prometheus metrics on a configurable `prometheus_port`. This
quickstart scrapes them with **VictoriaMetrics** and renders them in **Grafana**,
so you can watch query rate, cache hit/miss, active sessions, scan pruning, and
coordinator memory while running queries.

A minimal queryable SQE (Nessie catalog + RustFS, anonymous auth) generates real
metrics; the focus here is the monitoring pipeline.

## How it works

- **SQE** serves Prometheus text metrics at `/metrics` on its `prometheus_port`.
  The coordinator binds the metrics port on all interfaces so VictoriaMetrics
  can reach it in-network.
- **VictoriaMetrics** scrapes the SQE metrics endpoint every 5 seconds using a
  scrape config that names the target `sqe-coordinator`.
- **Grafana** is provisioned with VictoriaMetrics as the default datasource and
  an **SQE Overview** dashboard. Open it at `http://localhost:13000` (admin /
  admin) after the stack starts.
- `run.sh` starts the stack, runs `queries.sql` a few times to generate metrics,
  waits for a scrape, then queries the VictoriaMetrics API to assert SQE's
  counters are present.

A selection of the metrics SQE exposes: `sqe_rows_returned_total`,
`sqe_active_sessions`, `sqe_cache_hits_total` / `sqe_cache_misses_total`,
`sqe_files_pruned_minmax_total`, `sqe_coordinator_memory_used_bytes`,
`sqe_s3_requests_total` / `sqe_s3_bytes_read_total`.

## What it demonstrates

- The full Prometheus to VictoriaMetrics to Grafana scrape pipeline against a live
  SQE instance.
- SQE's `prometheus_port` config and the metrics it exposes out of the box.
- The provisioned **SQE Overview** Grafana dashboard.
- Asserted via the VictoriaMetrics query API: the `up{job="sqe-coordinator"}`
  target is 1 and SQE counters are populated.

**Status:** validated (2026-06-07).

## Run it

Full config, `docker compose`, dashboard, and captured output are in the repo:

**See: [quickstart/observability/](https://github.com/schubergphilis/sqe/tree/main/quickstart/observability/)**

```bash
cd quickstart/observability
cp .env.example .env
./run.sh
```
