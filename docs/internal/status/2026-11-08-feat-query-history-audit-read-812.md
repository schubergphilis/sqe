**STATUS 2026-11-08: finished queries read back from the audit log, in SQL (`system.runtime.query_history`) and in a web UI History tab (`feat/query-history-audit-read-812`, closes #812).**

- `sqe_metrics::audit::read_query_events(active, from, to, limit)` reads `AuditKind::Query` events from the native log and its rotated generations, newest first. It walks the generations newest to oldest, binary-searches each one for the window end (line starts, 64 KiB floor), reads 256 KiB blocks backwards, and stops once a line, plus 60 s of clock slack, is older than `from` or older than the oldest of the `limit` events it keeps. A `"kind":"query"` substring check runs before the JSON parse. Torn lines, including a half-written tail, are skipped. The result is oldest first, as before.
- `system.runtime.query_history` (`system_runtime.rs`): the tracker's records plus the audited queries, deduplicated by `query_id`. Every filter is pushed down as Inexact. Bounds on `end` (and lower bounds on `created`) choose the read window, `[query_history] lookback_secs` applies otherwise (default 86400, new config key), and the scan is capped at 100000 rows. The owner filter matches `actor.username`. A `QUERY_CANCELLED` failure maps to CANCELED. With `format = "ocsf"` the table shows the tracker only, with a one-time warning. The log read and the merge are `AuditHistorySource::read` and `merge_logged`, shared with the web UI.
- Web UI (#812 c): `GET /api/v1/queries/history?since&state&limit&before` (admin-gated with the other dashboard routes, runs on `spawn_blocking`) returns `{items, nextBefore}`. `web_ui::query_history_page` pages by a `(end, query_id)` keyset cursor. It counts only log rows toward a full page, so an older tracker row cannot take the place of an unread, newer log row, and it widens the log read (x4, up to 100000) when a state filter leaves a page short. The dashboard's History tab has a window selector (hour to whole log), a state filter, Refresh and Load more; a query only the log holds opens from its row (no fragments).
- Window arithmetic uses checked chrono ops, so filters at the edge of chrono's range or a huge `lookback_secs` cannot panic the scan.
- Docs: `operations/audit-logging.md` (Query history from the log, read cost), `operations/web-ui.md` (History tab, endpoint; also corrected the claim that the query endpoints omit client IP and roles: they carry both, the session id is what is never exposed), `deployment/configuration.md` (`lookback_secs`).

Read time, `history_seven_day_read_time` (`#[ignore]`, release; 1,008,000 events of about 1.3 KB, one per 600 ms, six 256 MiB generations), before -> after the newest-first reader:

| case | before | after |
|---|---|---|
| last hour | 110 ms | 25.5 ms |
| last day (100k cap) | 370 ms | 267 ms |
| 7 days, cap 100k | 2.823 s | 268 ms |
| one hour, 3 days back | 73 ms | 15 ms |
| page of 50, newest | 1.865 s | 0.4 ms |
| page of 50, 5 days back | 587 ms | 2.6 ms |

The < 1 s acceptance target for a time-bounded query over 7 days of rotated logs is met at the reader. Run: `cargo test -p sqe-metrics --release --lib history_seven_day_read_time -- --ignored --nocapture`.

Tests:
- Unit: `audit::history` (4 tests, 1 ignored perf). `matches_a_forward_scan_on_random_windows` compares the reader with a brute-force forward scan on 200 random windows and limits over three rotated files with out-of-order times, non-query events, a legacy line and a torn tail.
- Unit: `system_runtime::history_pages_walk_tracker_and_log_once` walks pages of 7 over 120 logged queries plus tracker rows (one also logged, one tracker-only between two log rows, one running), a sparse `failed` filter at limit 1 (forces the read to widen), a `since` window, and the cursor round trip. `sqe-server` `api_query_history_pages_finished_queries`: finished-only, 400 on a bad `since` or `before`.
- IT: `query_history_restart_e2e::query_history_restart_keeps_finished_queries` (mock catalog, plain test): `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --test it -- audit_ query_history`.

Left open on #812: nothing.

**NEXT:** the #637/#636/#589 scripted refactors, then #576.
