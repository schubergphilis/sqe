# OpenLineage

SQE emits OpenLineage 2-0-2 events for queries that mutate data: INSERT, CTAS, MERGE, UPDATE, DELETE, plus DDL on tables. SELECT events are off by default and can be enabled per environment.

Use these events to drive a lineage UI (Marquez, DataHub) or to feed a metadata catalog. Off by default. Zero hot-path overhead when disabled.

## What gets emitted

| Statement | Emits? | Inputs | Outputs |
|---|---|---|---|
| SELECT | only if `emit_selects = true` | yes | none |
| INSERT, CTAS, MERGE | always | yes | yes |
| UPDATE, DELETE | always | yes | yes |
| CREATE TABLE, ALTER, DROP | always | none | yes (with schema facet) |
| OPTIMIZE, VACUUM, REWRITE_MANIFESTS | never | n/a | n/a |

Each query produces two events: a START at submit time and a COMPLETE on success or FAIL on error.

## Configuration

```toml
[metrics.openlineage]
enabled        = true
job_namespace  = "sqe-prod"     # per-env

# at least one sink required
file_path      = "/var/log/sqe/lineage.jsonl"
http_endpoint  = "https://marquez.example.com/api/v1/lineage"

# HTTP transport
auth_mode           = "bearer"           # "none" | "bearer" | "user_token"
api_key             = "..."              # required when auth_mode = "bearer"
http_timeout_ms     = 5000
http_retry_attempts = 1

# disk spool fallback (recommended when HTTP is configured)
spool_path           = "/var/spool/sqe-ol"
spool_max_bytes      = 104857600
replay_interval_secs = 30

# back-pressure (rarely needs tuning)
channel_capacity = 10000
emit_selects     = false
```

Every TOML key has an `SQE_METRICS__OPENLINEAGE__*` env override.

## Sink choice

Pick one or both:

- **File sink**: append-only JSONL at `file_path`. Use for SIEM ingestion, debug, or local development.
- **HTTP sink**: POST to an OL collector. Use for Marquez/DataHub. Add a `spool_path` so a collector outage does not lose events.

When both are set, every event goes to both. Failures on one sink do not block the other.

## Marquez quickstart

```bash
docker run -p 5000:5000 -p 5001:5001 marquezproject/marquez
```

In `sqe.toml`:

```toml
[metrics.openlineage]
enabled = true
http_endpoint = "http://localhost:5000/api/v1/lineage"
auth_mode = "none"
```

Restart SQE. Run a query. Browse Marquez at http://localhost:3000.

## DataHub quickstart

DataHub's OL receiver expects a bearer token:

```toml
[metrics.openlineage]
enabled = true
http_endpoint = "https://datahub.example.com/openapi/openlineage/api/v1/lineage"
auth_mode = "bearer"
api_key = "${DATAHUB_TOKEN}"
spool_path = "/var/spool/sqe-ol"
```

## Troubleshooting

**No events appear in Marquez or DataHub.** Check `enabled = true` and that at least one sink is configured. Validation is enforced at startup; the server refuses to start with a misconfigured block.

**Spool directory grows.** The collector is unreachable. SQE buffers up to `spool_max_bytes` then drops newest events. Inspect `/var/spool/sqe-ol/spool.jsonl*`. Restore the collector and the replay loop drains within `replay_interval_secs`.

**`sqe_lineage_dropped_events_total` counter increments.** The bounded mpsc channel was full. Increase `channel_capacity` or investigate why the emitter is slow (collector too slow? HTTP timeouts?).

**`sqe_lineage_sink_errors_total{sink="http"}` increments.** Same: collector unreachable. With `spool_path` configured, events still reach disk.

## v1 limitations

- No mTLS to the collector. Bearer auth only.
- `auth_mode = "user_token"` (forwarding the user's OIDC bearer per event) is wired but currently falls back to the static `api_key` in v1; full per-event token forwarding is a follow-up.
- Maintenance procedures (OPTIMIZE, VACUUM, REWRITE_MANIFESTS) are never emitted.
- MERGE column-level lineage is emitted at the dataset level; per-branch annotations (MERGE_INSERT vs MERGE_UPDATE) are deferred until DataFusion exposes a Merge LogicalPlan node.
- DDL paths (CREATE TABLE, DROP, etc.) emit events with `plan = None`. The dataset target is captured but column lineage is empty for DDL.
- Embedded CLI (`sqe-cli` ad-hoc mode) does not emit lineage. Production server (`sqe-server`) is the only emit path.
