# Audit Logging

SQE writes a tamper-evident audit log for authentication events, session lifecycle
changes, permission grants and revokes, catalog DDL, and a subset of query executions
(see [Coverage in this release](#coverage-in-this-release) below).
The log is append-only JSONL (one JSON object per line). Each record carries an integrity
block that lets offline tooling detect modification or truncation.

## Enabling the log

Set `audit_log_path` under `[metrics]`:

```toml
[metrics]
audit_log_path = "/var/log/sqe/audit/audit.jsonl"
```

An empty string disables logging. The path must exist or be writable by the SQE process.
Under Helm, set `audit.enabled = true` (default) and `audit.persistence.enabled = true`
to back the log with a persistent volume claim.

## `[metrics.audit]` config block

```toml
[metrics.audit]
format                = "native"   # "native" | "ocsf" | "both"
gdpr_tags             = []         # tag names that mark a column as GDPR-sensitive
gdpr_identifier_mode  = "tokenize" # "tokenize" | "drop" | "keep"
superdebug_log_results = false      # NEVER true in production
```

All keys are optional. The defaults above are the production-safe values.

### `format`

Controls which wire schema is written to disk.

| Value | Behavior |
|-------|----------|
| `native` | Canonical `AuditEvent` JSON written to `audit_log_path`. Default. |
| `ocsf` | OCSF JSON written to `<stem>.ocsf.jsonl`; native file carries legacy flat entries only. |
| `both` | Native JSON written to `audit_log_path`; OCSF JSON also written to `<stem>.ocsf.jsonl`. |

For example, if `audit_log_path = "/var/log/sqe/audit/audit.jsonl"` and `format = "both"`,
the files are `audit.jsonl` (native) and `audit.ocsf.jsonl` (OCSF).

### `gdpr_tags`

A list of tag names. Any column in a queried Iceberg table whose tag set (read from the
Iceberg table property `sqe.column-tags`) contains one of these names is treated as
GDPR-sensitive. Before the event is chained and written, the matching column identifiers
and their adjacent literal values are removed from the logged SQL text.

Empty list (the default) disables GDPR column masking. PII pattern redaction (emails,
SSNs, phone numbers, card numbers, and secret-keyword literals) runs unconditionally
regardless of this setting.

Tag resolution uses the existing policy metadata cache via `sqe-policy`'s `TagSource`.
No extra network calls are made on the audit write path.

Fail-closed rule: when the tag state for a table is unknown at write time (cache miss,
parse error), all SQL literals are stripped from the query text rather than risking a
leak. A known-empty tag map is not a cache miss; it means the table has no tags and
no masking is applied.

### `gdpr_identifier_mode`

Controls how tagged column identifiers appear after masking. Has no effect when
`gdpr_tags` is empty.

| Value | Result |
|-------|--------|
| `tokenize` | Identifier replaced with a stable per-column token (`col_<8 hex chars>`). The same column produces the same token within a deployment, so log lines remain correlatable. Default. |
| `drop` | Identifier replaced with the literal string `[GDPR]`. |
| `keep` | Identifier left in place. Literal values adjacent to the column are still stripped. |

The token is derived as `sha256(salt + lowercase(column_name))`. The salt is set once
at startup and does not need to be secret; it separates token namespaces across
deployments.

### `superdebug_log_results`

Default `false`. When `true`, SQE emits a loud `WARN` log line and writes a
self-audit event of kind `admin_ddl` to the audit trail recording that the flag
is active. Result rows are never written to any audit sink regardless of this flag;
the flag name is intentionally alarming. Enable only in isolated development
environments and disable before going to production.

Enabling `superdebug_log_results` in production violates SOC 2, ISO 27001, and
GDPR data-minimisation requirements.

## Auth provider claim paths

The `oidc_password` and `bearer_token` auth providers accept three optional claim-path
fields that enrich the audit actor identity:

| Field | Provider | Default | Purpose |
|-------|----------|---------|---------|
| `subject_claim` | `oidc_password`, `bearer_token` | `"sub"` | JWT claim used as `actor.subject` (stable opaque identifier, distinct from `user_claim`). |
| `email_claim` | `oidc_password`, `bearer_token` | `""` | Dot-separated JSON path to the email address in the JWT payload. Empty string disables extraction. |
| `groups_claim` | `oidc_password`, `bearer_token` | `""` | Dot-separated JSON path to the groups array. Separate from `roles_claim`. Empty string disables extraction. |

Example `bearer_token` provider config:

```toml
[[auth.providers]]
type = "bearer_token"
jwks_url = "https://auth.corp.example/.well-known/jwks.json"
audience = "sqe"
subject_claim = "sub"
email_claim = "email"
groups_claim = "groups"
```

When extraction is enabled, the enriched fields appear in the `actor` block of every
event: `actor.subject`, `actor.email`, `actor.groups`. Fields that are absent from
the token are omitted from the event rather than serialized as null.

## OCSF class mapping

When `format = "ocsf"` or `format = "both"`, each canonical `AuditEvent` is mapped
to an OCSF class before writing. The mapping is fixed at the kind level.

| SQE event kind | OCSF class | Class UID | Category | Category UID |
|----------------|-----------|-----------|----------|--------------|
| `query` | Datastore Activity | 6005 | Application Activity | 6 |
| `policy_decision` | Datastore Activity | 6005 | Application Activity | 6 |
| `auth` | Authentication | 3002 | Identity & Access Management | 3 |
| `session` | Authorize Session | 3003 | Identity & Access Management | 3 |
| `grant` | Account Change | 3001 | Identity & Access Management | 3 |
| `admin_ddl` | Entity Management | 3004 | Identity & Access Management | 3 |

A policy denial (`policy_decision` kind with `Failure` outcome) maps to class 6005 with
`status_id = 2`. This lets SIEM tools correlate policy denials alongside normal query
activity in a single class.

Standard OCSF fields used:

- `class_uid` / `category_uid`: from the table above.
- `status_id`: `1` for success, `2` for failure.
- `time`: millisecond epoch timestamp.
- `severity_id`: fixed at `1` (Informational).
- `actor.user.name`: username.
- `actor.user.uid`: subject claim, when present.
- `actor.user.email_addr`: email claim, when present.
- `actor.user.groups`: array of group objects `{"name": "..."}`.
- `actor.user.roles`: roles array.
- `resources`: array of `{"name": "catalog.namespace.table", "type": "Table"|"View"}`.
- `src_endpoint.ip`: client IP, when present.
- `metadata.product.name`: `"SQE"`.
- `metadata.uid`: integrity hash of the record.

SQE-specific fields that have no OCSF home (query hash, statement type, scan stats,
policy decisions) travel under `unmapped` as a flat object.

## Tamper-evident hash chain

Every record carries an `integrity` block:

```json
"integrity": {
  "seq": 42,
  "prev_hash": "a3b4c5...",
  "hash": "d6e7f8..."
}
```

The hash formula is:

```
hash = sha256(prev_hash || canonical_json_with_hash_field_blanked)
```

The first record uses a genesis sentinel (`0000...0000`, 64 hex zeros) as `prev_hash`.
Sequence numbers are zero-based and strictly increasing.

The `verify_chain` function in `sqe-metrics` walks a loaded slice of events and returns
an error if any record has an unexpected sequence number, a `prev_hash` that does not
match the previous record's `hash`, or a recomputed hash that does not match the stored
value. This detects tampering, record deletion, and truncation anywhere in the file.

Redaction and GDPR masking run before chain stamping so the chain covers the
post-redaction bytes. Modifying a record to restore redacted content will break the
chain.

## PII redaction (always-on)

Before any record is written, `redact_pii` runs unconditionally on the SQL query text.
It replaces:

- Email addresses with `[EMAIL]`
- SSNs (`XXX-XX-XXXX`) with `[SSN]`
- Phone numbers with `[PHONE]`
- Credit-card-like sequences with `[CARD]`
- Secret-keyword literals (`TOKEN '...'`, `PASSWORD '...'`, `ACCESS_KEY_ID '...'`,
  `SECRET_ACCESS_KEY '...'`, `SESSION_TOKEN '...'`, `API_KEY '...'`,
  `CLIENT_SECRET '...'`, `BEARER '...'`) with `[REDACTED]`

The secret-literal pass guards against `CREATE SECRET ... TOKEN '<jwt>'` landing
verbatim in the log. This is belt-and-suspenders: the token is redacted regardless of
whether the statement is the direct SQL text or arrives via a prepared statement.

`redact_pii` is pattern-matching, not a SQL parser. It catches known PII shapes but
does not catch free-form sensitive literals such as `WHERE patient_id = 'P-998877'`.
For that, GDPR column masking (see above) strips all literals adjacent to tagged
columns, and the fail-closed path strips all literals when tag state is unknown.

## Coverage in this release

The table below describes what produces a canonical `AuditEvent` written to the
OCSF spool and OCSF file.

| Path | Sink | Format |
|------|------|--------|
| Buffered `execute` SELECTs (Trino-compat, quack-server, Flight prepared statements, Flight ticket statements) | OCSF file + native sink | Canonical `AuditEvent` with structured Actor and resources |
| Flight SQL streaming SELECTs | OCSF file + native sink | Canonical `AuditEvent` with structured Actor and resources |
| DML / DDL (INSERT INTO, CTAS, DELETE, UPDATE, MERGE, CREATE TABLE, ALTER TABLE, DROP TABLE, etc.) | OCSF file + native sink | Canonical `AuditEvent` |
| GRANT / REVOKE | OCSF file + native sink | Canonical `AuditEvent` |
| Authentication events | OCSF file + native sink | Canonical `AuditEvent` |
| Session lifecycle events | OCSF file + native sink | Canonical `AuditEvent` |
| Secret-bearing statements (CREATE SECRET, DROP SECRET, SHOW SECRETS, ATTACH, DETACH) | Native sink only | Legacy flat `AuditEntry` (redacted path; credentials never reach canonical form) |

Secret-bearing statements stay on the redacted legacy path. Routing them through the
canonical path would risk writing credential literals to the OCSF file before
redaction applies. The legacy path runs redaction inline, so bearer tokens and
catalog credentials embedded in SQL text are stripped before any byte is written.

The legacy `AuditEntry` format is flat JSON. It carries username, statement type,
duration, status, and `tables_touched` (unqualified table names), but not structured
resources, actor email, groups, or policy decision fields.

## Exporting to a SIEM (OTLP)

SQE ships a background exporter that tails the OCSF spool and forwards records to any
OTLP-compatible collector (OpenTelemetry Collector, Grafana Alloy, Datadog Agent, etc.).
The exporter is off by default.

### Config block

```toml
[metrics.audit_export]
enabled          = false          # set to true to activate
target           = "otlp"         # only "otlp" is supported; "kafka" is reserved but not built
otlp_endpoint    = ""             # e.g. "http://otel-collector:4317"; empty -> falls back to metrics.otlp_endpoint
spool_path       = ""             # empty -> <audit_log_path>.ocsf.spool.jsonl
batch_max        = 512            # maximum records per OTLP export batch
flush_interval_ms = 2000          # shipper poll interval in milliseconds
max_spool_bytes  = 1073741824     # 1 GiB; spool size above this emits a WARN
start_at         = "now"          # "now" (default) | "beginning"
```

All keys are optional. The defaults shown are the production-safe values.

#### `enabled`

`false` by default. Setting to `true` activates the OTLP exporter and the spool
writer. Disabling (`false`) leaves the rest of the audit stack unchanged: the OCSF
file is still written when `format = "ocsf"` or `format = "both"`, and the hash
chain is unaffected. The export spool is a separate file.

#### `target`

Only `"otlp"` is implemented. `"kafka"` is reserved for a future release. Configuring
any other value logs a warning and the exporter does not start.

#### `otlp_endpoint`

The OTLP/gRPC endpoint URL for the collector. If empty, the exporter falls back to
`metrics.otlp_endpoint`. If both are empty the server logs a warning at startup and
the exporter does not start.

#### `spool_path`

Path to the OCSF JSONL spool file that buffers events before export. If empty, the
path is derived from `audit_log_path` by replacing the extension with
`.ocsf.spool.jsonl` (e.g. `audit.jsonl` -> `audit.ocsf.spool.jsonl`). The file is
created on startup if it does not exist.

The export spool is independent of the `format` setting. When `audit_export.enabled =
true`, every canonical event written via `log_event` goes to the spool regardless of
whether `format` is `native`, `ocsf`, or `both`.

#### `batch_max`

Maximum number of OCSF records sent in a single OTLP export call. Default: 512.

#### `flush_interval_ms`

How often the background shipper polls the spool for new records. Default: 2000 ms.
Lower values reduce latency to the SIEM at the cost of more frequent OTLP calls.

#### `max_spool_bytes`

Spool size threshold in bytes. When the spool file exceeds this value the exporter
emits a `WARN` log. The exporter continues and queries are never blocked. Default:
1 073 741 824 (1 GiB). Spool rotation and automatic pruning are not implemented in
this release; size management is left to the operator.

#### `start_at`

Controls where the shipper starts on the first run, when no cursor file exists.

| Value | Behavior |
|-------|----------|
| `"now"` | Scan the spool to its current tail and advance the cursor there without shipping. Historical records are not replayed. New records written after startup are shipped. Default. |
| `"beginning"` | Ship from the oldest record in the spool. Use after moving the spool or recovering from a cursor loss. |

On subsequent restarts the persisted cursor (`<spool_path>.cursor`) always wins.
`start_at` is not re-applied when a cursor file exists. This guarantees at-least-once
delivery: a restart resumes exactly where the last successful export acked.

### Behavior and durability

The exporter provides at-least-once delivery. Records are never removed from the spool.
The background shipper tails the spool from the byte offset recorded in the cursor file,
batches up to `batch_max` records, sends them to the collector, and advances the cursor
only after the collector returns a successful ack.

A collector outage grows the spool. Queries continue without delay. When the collector
recovers, the shipper replays from the last cursor position.

The exporter uses a dedicated OTLP log pipeline. It is not connected to the
`trace_sample_rate` tracing bridge, so audit records are never sampled or dropped by
the trace sampler.

### OTLP record mapping

Each spool line (one OCSF JSON object) becomes one `LogRecord` in the OTLP batch:

| OTLP field | Value |
|------------|-------|
| `body` | Full OCSF JSON text of the record |
| `severity_number` | `INFO` for `status_id = 1` (success); `WARN` for `status_id = 2` (failure) |
| `timestamp` | OCSF `time` field (millisecond epoch converted to nanoseconds) |
| `observed_timestamp` | Wall clock at ship time |

Indexed attributes set on every record:

| Attribute | Type | Source |
|-----------|------|--------|
| `ocsf.class_uid` | int | OCSF `class_uid` (e.g. 6005 for Datastore Activity) |
| `ocsf.category_uid` | int | OCSF `category_uid` |
| `audit.kind` | string | Human-readable class label (e.g. `"datastore_activity"`, `"authentication"`) |
| `audit.status_id` | int | OCSF `status_id` (1 = success, 2 = failure) |
| `user.name` | string | `actor.user.name` from the OCSF record |
| `audit.seq` | int | `metadata.sequence` from the OCSF record (the hash-chain sequence number) |

SIEM queries that filter on any of these attributes avoid parsing the full body.

### Export metrics

| Metric | Type | Description |
|--------|------|-------------|
| `sqe_audit_export_records_total{status}` | Counter | Records shipped, labeled `status="ok"` or `status="error"` |
| `sqe_audit_export_batch_failures_total` | Counter | OTLP export calls that returned an error |
| `sqe_audit_export_spool_lag_bytes` | Gauge | Bytes between the cursor offset and the current end of spool |
| `sqe_audit_export_cursor_seq` | Gauge | Last sequence number successfully acked |
| `sqe_audit_export_last_success_timestamp` | Gauge | Unix timestamp of the last successful export |

`sqe_audit_export_spool_lag_bytes = 0` means the shipper is caught up. A rising value
while `sqe_audit_export_batch_failures_total` is also rising points to a collector
connectivity problem.

### Deferred

- **Spool rotation and retention.** Only a bounded-growth `WARN` at `max_spool_bytes`
  is implemented. Rotation, age-based pruning, and size-capped compaction are not
  built yet.
- **Kafka target.** The `target = "kafka"` config key is reserved. It is not
  implemented in this release.

## Never-log-result-rows policy

Result rows are never written to any audit sink. `AuditEvent` has no field for result
data and the serialization path has no code path that writes row values. The
`superdebug_log_results` flag does not change this; it is a marker for a future
diagnostic mode and its only current effect is the warning and self-audit event
described above.
