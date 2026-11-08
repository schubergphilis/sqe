**STATUS 2026-11-08: `system.runtime.query_history` reads finished queries back from the audit log (`feat/query-history-audit-read-812`, #812 part b).**

- `sqe_metrics::audit::read_query_events(active, from, to, limit)` reads `AuditKind::Query` events from the native log and its rotated generations. It skips any generation whose successor starts before `from` (minus 60 s of clock slack), stops at the first generation that starts after `to`, drops torn lines, and keeps the newest `limit` events.
- `system.runtime.query_history` (`system_runtime.rs`): the tracker's records plus the audited queries, deduplicated by `query_id`. Every filter is pushed down as Inexact. Bounds on `end` (and lower bounds on `created`) choose the read window, `[query_history] lookback_secs` applies otherwise (default 86400, new config key), and the scan is capped at 100000 rows. The owner filter matches `actor.username`. A `QUERY_CANCELLED` failure maps to CANCELED. With `format = "ocsf"` the table shows the tracker only, with a one-time warning.
- Window arithmetic uses checked chrono ops, so filters at the edge of chrono's range or a huge `lookback_secs` cannot panic the scan.
- Docs: `operations/audit-logging.md` (Query history from the log) and `deployment/configuration.md` (`lookback_secs`).

Tests:
- Unit: `audit::history` (3 tests) and `system_runtime` (22 tests). They cover dedupe against a live record, FAILED/CANCELED, the owner filter, a widening `end` filter, stats, and edge bounds.
- IT: `query_history_restart_e2e::query_history_restart_keeps_finished_queries`. Handler A runs a query and is dropped; handler B uses the same audit file with a fresh tracker; `query_history` returns FINISHED for the query, `queries` returns nothing. It needs no Polaris (mock catalog), so it runs as a plain test: `cargo test -p sqe-coordinator --test it -- audit_ query_history` gave 28 passed, 0 failed.

Left open on #812:
- (c) the web UI history view with paging.
- The acceptance target of < 1 s over 7 days of rotated logs is not measured. The reader is a linear scan of the files in the window, with no index.

**NEXT:** #812 (c) web UI history view, then the #637/#636/#589 scripted refactors and #576.
