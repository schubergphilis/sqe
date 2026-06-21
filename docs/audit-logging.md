# Audit Logging

SQE writes a tamper-evident audit log that records every query execution, authentication
event, session lifecycle change, permission grant or revoke, and catalog DDL operation.
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

## Never-log-result-rows policy

Result rows are never written to any audit sink. `AuditEvent` has no field for result
data and the serialization path has no code path that writes row values. The
`superdebug_log_results` flag does not change this; it is a marker for a future
diagnostic mode and its only current effect is the warning and self-audit event
described above.
