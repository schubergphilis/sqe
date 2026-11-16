**STATUS 2026-11-16: OpenSpec changes for #902, #901, #908 and #905 (`docs/openspec-portal-requests`, Part of #902 #901 #908 #905).**

Four proposals, no code. Each takes the issue's acceptance tests as spec scenarios and the issue's "checked against main" notes as design decisions and tasks. All four pass `openspec validate --strict`.

- `admission-queue-and-drain` (#902): fair bounded queue in front of the global permit (round robin over per-user lanes, permit handoff on drop), `RefusalReason` on `SqeError` mapped to Flight SQL metadata, Trino `errorName` and HTTP 429, drain that waits on `QueryTracker::active_count()`, `sqe_idle_seconds`.
- `system-catalog-split` (#901): one reserved-schema helper replaces every `catalog == "system"` test; a composite provider serves `runtime`, `metadata`, `jdbc`, `information_schema` and delegates the rest to a Polaris catalog `system`.
- `signed-catalog-scope` (#908): ES256 scope per bearer `jti` for one configured client; hidden at registration, enforced by a plan-time walker plus a write check on non-plan paths. Header renamed to the neutral `x-sqe-catalog-scope`.
- `materialized-views-full-refresh` (#905): MV = plain Iceberg table with apache/iceberg#11041 properties, refresh through the one-commit replace path, refusal over masked or row-filtered sources.

Open decisions for the owner: #905 refresh identity (proposal: as the caller, definer or admin only, because there is no service account); #908 header name.

**NEXT:** review the proposals; then implement #902 first (P2, self-contained), then #901, #908.
