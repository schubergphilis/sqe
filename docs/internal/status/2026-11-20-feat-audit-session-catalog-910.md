**STATUS 2026-11-20: audit records carry the session's catalog and namespace; `USE CATALOG` works over Flight (`feat/audit-session-catalog-910`, #910).**

The native audit event gains an optional `session` object (`catalog`, `namespace`) and the OCSF line gains `unmapped.session_catalog` / `unmapped.session_namespace`, only when set, so existing records hash identically. Every statement-level emitter fills it (buffered, failed, oversized, ingest, streaming finalizer, Flight ingest guard); authentication, maintenance, scheduler and system emitters leave it unset. The value comes from `lineage_scope`: the session catalog, else the configured default catalog; schema else `default`.

Second bug: `USE CATALOG x` never parsed under the Generic dialect, and the Flight `SetCatalog` arm was a no-op. The classifier now recognises `USE CATALOG <name>` and the Flight handler sets the session catalog (a dotted name is refused).

Limits: the legacy secret-bearing `AuditEntry` path (CREATE SECRET and similar) still carries no session scope.

**NEXT:** unchanged
