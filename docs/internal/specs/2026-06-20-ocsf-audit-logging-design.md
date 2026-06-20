# OCSF Audit Logging and Identity Enrichment (Sub-project A)

- Date: 2026-06-20
- Status: Approved design, pre-implementation
- Author: Jacob Verhoeks
- Scope: Sub-project A of a 3-part effort. A is the canonical audit event model, identity enrichment, OCSF mapping, GDPR-tag masking, and tamper-evidence. B (multi-sink export to OTel/SIEM, Kafka slot) and C (operational-logging polish, gate the unauthenticated query endpoint) are separate specs that depend on A's event shape.

## Context

SQE already has a working audit pipeline that this design extends rather than replaces:

- `sqe-metrics::audit::AuditEntry` plus `AuditLogger`: a thread-safe logger using an `mpsc` channel and a dedicated writer thread, writing JSONL, with PII redaction (`redact_pii`), SQL-literal stripping (`strip_sql_literals`), and SHA-256 query hashing. Emitted from `query_handler.rs`, `streaming.rs`, `maintenance.rs`.
- `sqe-coordinator::query_tracker::QueryRecord`: in-memory query history (moka cache) exposed over `/api/v1/queries`. Carries richer identity and timing than `AuditEntry` (roles, client_ip, queued/planning/execution ms, bytes scanned).
- `sqe-policy::PolicySummary`: row filters applied, columns masked, columns restricted, denied. Already surfaced into `AuditEntry`.
- `sqe-policy::tag_source::TagSource`: trait returning `column -> tags` for a fully-qualified table, backed by the Iceberg property `sqe.column-tags`. The Polaris-backed `CacheTagSource` reuses `sqe-catalog::TableMetadataCache`. Fail-closed semantics: `None` means unknown tag state and the caller must treat it conservatively.
- `sqe-metrics::otel`: tracing plus OpenTelemetry wiring, with `otlp_endpoint` already configured. Used by B, not A.

Identity is the main gap. `SessionUser` carries only `username` and `roles` (one flat list). The JWT contains `sub`, `email`, `preferred_username`, and potentially a `groups` claim distinct from roles, but only `sub` (as username), `exp`, and one configurable roles path are extracted. OPA receives only `{username, roles}`.

Several security-relevant events are logged through `tracing` only, never reaching the audit trail: authentication success and failure, session lifecycle, GRANT/REVOKE, some catalog and secret DDL, and policy deny / circuit-breaker transitions.

## Goals

1. A single canonical internal audit event that all event kinds flow through.
2. An OCSF serialization of that event, selectable by config, mapped to verified OCSF classes.
3. Identity enrichment so audit records carry subject, email, and groups distinct from roles.
4. Fully-qualified resource capture: `catalog.namespace.object` with table vs view distinction, resolved from the logical plan.
5. GDPR-tag-driven masking of the logged SQL text and resource fields, reusing the existing `TagSource`.
6. A never-log-result-rows policy by default, with one guarded opt-in.
7. Tamper-evidence on the local audit log via a hash chain (SOC2 / ISO 27001 evidence).
8. A compliance mapping showing how the above satisfies SOC2, ISO 27001, and GDPR controls.

## Non-goals (deferred)

- OTel-logs sink and Kafka sink transport. That is Sub-project B. A produces OCSF bytes and writes them to a sink; A's sinks are file-based.
- Operational-logging field consistency, populating `client_ip` on every code path, and gating the unauthenticated `/api/v1/queries` endpoint. That is Sub-project C. A records the endpoint risk because A4 enriches that endpoint with identity.
- Changing the OPA input document. Threading new identity fields into policy decisions is optional follow-up, not required for A.

## Decisions

These were settled during brainstorming and are fixed for A:

- Layer OCSF on top of a canonical event. Do not rewrite `AuditEntry` into an OCSF struct.
- On-disk default stays native JSONL. OCSF is opt-in: `[audit] format = "native" | "ocsf" | "both"`, default `native`.
- Hash-chained local log for integrity. The SIEM path (B) is additional, not a replacement.
- GDPR tags come from Polaris/Ranger via the existing `TagSource` (`sqe.column-tags`). A configurable tag-name set selects which tags trigger log masking.
- Never log result rows. One guarded `superdebug_log_results` flag is the only escape hatch.
- Query content (SQL text) is logged, with GDPR masking applied to it.
- GDPR masking: always strip the literal values compared against GDPR columns. The column identifier is tokenized as a stable hash by default, configurable to `drop` or `keep`.

## Architecture and data flow

The existing `AuditLogger` keeps its channel plus dedicated writer thread. Three changes:

1. A canonical `AuditEvent` becomes the type sent on the channel, replacing the narrower `AuditEntry` at the emit boundary. `AuditEntry` either becomes `AuditEvent` or a `Query`-kind variant of it.
2. The single JSONL writer becomes a list of sinks. Each sink receives the canonical event and serializes independently. For A: a native-JSONL sink (today's format, unchanged) and an OCSF-JSON sink. Both are file-backed in A.
3. Redaction and hash-chaining run on the writer thread, before sinks, so the chain is well-ordered (single-threaded) and every sink sees the same redacted bytes.

```
emit site
  -> AuditEvent (canonical, in-memory, raw)
  -> [redaction: PII + GDPR-tag masking]
  -> [hash-chain: seq, prev_hash, hash]
  -> sinks: [ native-jsonl-file | ocsf-jsonl-file ]   (selected by config)
```

Redaction runs on the writer thread rather than the emit site so that the hot query path stays non-blocking, consistent with today's design. The `TagSource` lookups needed for GDPR masking are cache-backed, so this is cheap; if a lookup returns `None` (unknown), the writer applies the conservative fallback described in A5.

## A1. Canonical event model

One `AuditEvent` carrying a discriminated `kind` plus shared sections. Sketch (final field names settled during implementation):

```rust
pub struct AuditEvent {
    pub time: DateTime<Utc>,
    pub kind: AuditKind,            // Query | Auth | Session | Grant | AdminDdl | PolicyDecision
    pub actor: Actor,               // identity
    pub outcome: Outcome,           // Success | Failure { error_type, error_code, message }
    pub resources: Vec<Resource>,   // fully-qualified objects touched
    pub policy: Option<PolicySummary>,
    pub timing: Option<Timing>,     // queued/planning/execution ms, duration_ms
    pub stats: Option<QueryStats>,  // rows_returned, bytes_scanned, rows_scanned, spill_bytes, peak_memory
    pub query: Option<QueryInfo>,   // redacted SQL text, query_hash, statement_type
    pub session_id: Option<String>,
    pub client_ip: Option<String>,
    pub integrity: Integrity,       // seq, prev_hash, hash (set on writer thread)
}

pub struct Actor {
    pub username: String,
    pub subject: Option<String>,    // JWT sub
    pub email: Option<String>,
    pub roles: Vec<String>,
    pub groups: Vec<String>,        // distinct from roles
}

pub struct Resource {
    pub catalog: Option<String>,
    pub namespace: Vec<String>,
    pub name: String,
    pub object_type: ObjectType,    // Table | View
}
```

`QueryStats` holds the SQE-specific numbers that do not map to OCSF core fields. They travel in OCSF `unmapped` or `enrichments`, never forced into core fields.

## A2. Event kinds

All kinds flow through the one logger. Kinds and their current state:

- `Query`: exists today. Extended with structured `resources` and full `Actor`.
- `Auth`: success and failure. Today only `tracing` in `flight_sql.rs`. Promote to audit. Failure carries reason without leaking credentials.
- `Session`: create and expire. New.
- `Grant`: GRANT and REVOKE. Wired in the grant backend today but not audited. Promote.
- `AdminDdl`: CREATE/DROP CATALOG, secret create/show/drop, ATTACH/DETACH. Partly covered by the existing secret-redaction tests; normalize under one kind.
- `PolicyDecision`: deny, and circuit-breaker open/close. Today `tracing` only. Promote.

## A3. OCSF mapping layer

A pure function `to_ocsf(&AuditEvent) -> serde_json::Value`. Class assignment uses UIDs verified against schema.ocsf.io on 2026-06-20:

| SQE event kind | OCSF class | class_uid | category_uid |
|---|---|---|---|
| Query | Datastore Activity | 6005 | 6 |
| Auth (success/failure) | Authentication | 3002 | 3 |
| Session create | Authorize Session | 3003 | 3 |
| Grant / Revoke | Account Change | 3001 | 3 |
| Catalog / secret DDL | Entity Management | 3004 | 3 |
| Policy deny | Datastore Activity, status Failure, deny detail in `enrichments` | 6005 | 6 |

OCSF envelope fields populated from the canonical event: `class_uid`, `category_uid`, `activity_id`, `severity_id`, `status_id`, `time`, `metadata.product` (name `SQE`, vendor and version), `actor.user` (name, uid from subject, email_addr, groups), and the class-specific resource or entity fields.

OCSF `actor.user.groups` carries `groups`; `roles` map to `actor.user` role detail or `enrichments` since OCSF user has no first-class flat role list. SQE-specific stats (`query_hash`, `bytes_scanned`, `spill_bytes`, `peak_memory`, fragment counts) go into `unmapped` or `enrichments`.

For Datastore Activity, resources map to the OCSF `databucket` / `database` / table identity fields using the fully-qualified `catalog.namespace.name`, with `object_type` recorded so views are distinguishable from tables.

## A4. Identity enrichment

Extend `Identity` (sqe-auth) and `SessionUser` (sqe-core) with `subject`, `email`, and `groups`. Add configurable claim paths with safe defaults so existing deployments do not change behavior:

- `subject_claim` default `"sub"`
- `email_claim` default empty (off)
- `groups_claim` default empty (off)
- existing `roles_claim` unchanged (default `"realm_access.roles"`)

Extraction reuses the existing dot-path navigation in `bearer_token.rs` and `oidc_provider.rs`. `groups` stays distinct from `roles`: when both `roles_claim` and `groups_claim` are set, they populate separate fields. When `groups_claim` is empty, `groups` is empty and the audit record simply omits it.

The new fields flow into `Actor`. Threading them into the OPA input document is deferred (non-goal).

## A5. GDPR-tag masking in logs

Config: `[audit] gdpr_tags = ["gdpr", "pii"]` (empty disables), and `[audit] gdpr_identifier_mode = "tokenize" | "drop" | "keep"` (default `tokenize`).

On the writer thread, for each `Resource` in the event, call `TagSource.column_tags(catalog, namespace, name)`:

- `Some(map)`: for each column whose tag set intersects `gdpr_tags`, that column is a masking target.
- `None` (unknown tag state): conservative fallback. Apply the strictest literal stripping to the SQL text for that resource and mark the record `gdpr_masking = "fallback"`, because a GDPR-tagged column may exist that we cannot see. This mirrors the fail-closed contract of `TagSource`.

Masking actions:

- Literal values compared against a masking-target column are always stripped from `query.text`, extending the existing `redact_pii` and `strip_sql_literals` passes.
- The column identifier in `query.text` and in `resources` column lists is handled per `gdpr_identifier_mode`: `tokenize` replaces it with a stable salted hash so `WHERE email = ?` stays correlatable across queries without printing the field name; `drop` removes it; `keep` leaves the identifier but still strips the values.

This is log-side masking and is independent of query-result column masking done by the policy plan rewriter. A column can be readable in results but masked in logs, or the reverse.

## A6. Result-row logging policy

Result rows are never written to any audit sink. The canonical event has no field for result data. One config flag, `[audit] superdebug_log_results = false`, is the only path that can emit row samples, and it:

- defaults off,
- emits a loud startup warning when on,
- writes an audit `AdminDdl`-style record noting that result logging is enabled and by whom,
- is documented as non-compliant for production.

## A7. Hash-chained integrity

Each persisted record carries an `integrity` section computed on the writer thread:

- `seq`: monotonic counter from the start of the file or stream.
- `prev_hash`: hash of the previous record (genesis value for `seq = 0`).
- `hash = SHA-256(prev_hash || canonical_redacted_bytes)`.

The single-threaded writer guarantees a well-ordered chain. A periodic checkpoint record (every N records or T seconds) records the current `seq` and `hash` so a verifier can detect truncation of the tail. A `verify` routine walks a file and confirms the chain, surfaced as a test and a small CLI path.

The chain covers the redacted bytes, so integrity verification never requires unredacted data.

## A8. Compliance mapping

- GDPR: data minimization through A5 masking and the never-log-results policy (A6); identity of who accessed what through A4; reduced erasure burden because raw PII is not persisted in logs.
- SOC2 and ISO 27001: tamper-evidence through A7; a complete trail of access, authentication, and privilege change through A2; the SIEM export (Sub-project B) provides the monitoring and retention control on top.
- Carried into Sub-project C: the unauthenticated `/api/v1/queries` endpoint must be gated, because A4 enriches it with identity (subject, email, groups). A does not change that endpoint but records the obligation.

## Configuration additions

Under `[audit]` (new keys, all with backward-compatible defaults):

```toml
[audit]
format = "native"                 # native | ocsf | both
gdpr_tags = ["gdpr", "pii"]        # empty disables tag masking
gdpr_identifier_mode = "tokenize"  # tokenize | drop | keep
superdebug_log_results = false
# integrity hash chain is on whenever an audit path is enabled
```

Under auth provider config (new claim paths, defaults preserve current behavior):

```toml
subject_claim = "sub"
email_claim = ""                   # off by default
groups_claim = ""                  # off by default
```

## Testing strategy

Test-driven throughout.

- Golden-file tests: each event kind serializes to schema-valid OCSF. Assert required envelope fields and the exact `class_uid` / `category_uid` per the A3 table.
- Redaction tests: GDPR-tagged columns and the values compared against them never appear in either the native or OCSF serialization. Cover `tokenize`, `drop`, `keep`, and the `None`-tag fallback.
- Never-log-results test: with `superdebug_log_results` off, no result data reaches any sink; with it on, the enable event is itself audited.
- Hash-chain tests: a valid chain verifies; a tampered record or a truncated tail is detected.
- Identity-extraction tests: `subject`, `email`, and `groups` extract from configured claim paths, and `groups` stays distinct from `roles`. Default config leaves behavior unchanged.
- Resource-resolution tests: `catalog.namespace.object` is fully qualified and views are distinguished from tables.
- Regression: existing `audit_e2e_test.rs` continues to pass with `format = "native"`.

## Risks and open items

- OCSF field-level mapping detail (which exact `databucket`/`database` fields a query resource uses, and how roles fit `actor.user`) is settled against the schema browser during implementation, kind by kind, with golden files as the contract.
- Tokenized identifiers use a salted hash. The salt must be stable within a deployment for correlation and is configured or derived once at startup. It is not a secret-grade control, only a correlation aid.
- Writer-thread `TagSource` lookups on a cache miss return `None` and trigger the conservative fallback rather than blocking the writer on a Polaris round-trip. Acceptable: it masks more, never less.
```

