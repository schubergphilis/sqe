# 607: Audit hash chain restarts at genesis on every boot, the writer queue is unbounded, and write failures drop events with no counter

- Scope: sqe-metrics (audit/logger.rs, audit/chain.rs)
- Severity: medium
- Category: security
- Effort: S (hours) to M (days)
- Status: fixed (branch `fix/errors-audit-observability`, 2026-09-05)
- Issue: #614

## Problem

**Chain integrity does not span restarts.** The writer thread creates a fresh
`HashChain::new()` at startup (`crates/sqe-metrics/src/audit/logger.rs:325`),
whose `prev_hash` is the all-zero `GENESIS` constant
(`crates/sqe-metrics/src/audit/chain.rs:7,15-18`). The sink file is opened in
append mode (`logger.rs:243-249`). So a long-running audit file contains one
chain segment per process start, each beginning at `seq = 0` with
`prev_hash = GENESIS`. `verify_chain` (`chain.rs:77-78`) starts from GENESIS
once and therefore fails on any file that has seen a restart. Conversely,
deleting or truncating the tail of the file back to a restart boundary is
undetectable: the next segment legitimately starts from genesis. The chain
proves intra-run ordering only, which is not what "tamper-evident audit log"
suggests.

**Queue is unbounded, failures are silent.** Events are sent through
`std::sync::mpsc::channel::<AuditMsg>()` (`logger.rs:319`), which has no
capacity bound. If the disk stalls, the queue grows until OOM. When the
writer fails, the entry is logged at `error!` and dropped
(`logger.rs:556,675,680,715,720`); when the writer thread is gone, the same
(`logger.rs:860,872`). There is no `sqe_audit_events_dropped_total` counter,
no fail-closed option (refuse queries when audit cannot be written), and no
health signal. Compare `sqe-lineage`, which uses a bounded channel and
increments a `dropped` counter (`crates/sqe-lineage/src/observer.rs:166-170`).

**Worker actions are not audited at all.** Scan tickets, credential refreshes
and compaction groups executed on workers produce `info!` logs only
(`crates/sqe-worker/src/flight_service.rs:1000-1004,1142-1146,1175-1181`).
Which worker touched which files for which fragment is not reconstructible
from the audit trail when the coordinator log is incomplete.

## Why it matters

Audit logging is the control that regulated deployments will read first.
Today an operator cannot (a) verify a file that spans a redeploy, (b) know
how many events were lost during a disk incident, or (c) choose to stop
serving rather than run unaudited.

## Proposed change

1. On startup, if the native sink file is non-empty, read the last line, parse
   `integrity.seq` and `integrity.hash`, and seed `HashChain` from them
   (`HashChain::resume(seq, hash)`). If the tail is unparseable, write a
   `kind: "audit_resume"` event that records the break and start a new segment
   with an explicit `prev_hash: "BROKEN:<reason>"` so verifiers can distinguish
   a crash from tampering.
2. Replace the unbounded channel with `std::sync::mpsc::sync_channel(N)` where
   `N` comes from `audit.queue_capacity` (default 10_000). On full queue,
   increment `sqe_audit_events_dropped_total{reason="queue_full"}`; on write
   error, `{reason="io"}`.
3. Add `audit.fail_mode = "open" | "closed"`. In closed mode, `log()` returns
   `Err` when the queue is full or the writer has died, and
   `QueryHandler` refuses new statements with `SqeErrorCode::InternalError`
   until the writer recovers.
4. Give the worker a minimal audit path: reuse `AuditLogger` with
   `AuditKind::Maintenance`-style events for `compact_file_group` and a new
   `AuditKind::Scan` for tickets (fragment id, file count, principal from the
   ticket is absent, so record the coordinator trace id instead).

## Acceptance criteria

- Test: write 3 events, drop the logger, reopen on the same path, write 2 more,
  `verify_chain` over all 5 lines passes.
- Test: truncate the file after event 4 of 5, reopen, write 1; verification
  reports a break at seq 5.
- Metric `sqe_audit_events_dropped_total` exists and increments in a test with
  a 1-slot queue and a blocked writer.
- Config docs describe `fail_mode`.

## Related

- June 2026 codebase analysis item 20 (audit module documentation) and the
  July review's "durable audit storage" Helm item.
- Memory: "OCSF audit logging" project note (sub-project A) is the natural
  place to land the resume logic.

## Resolution (2026-09-05)

`HashChain::resume` continues seq/hash from the last parseable sink line; an unparseable tail starts a `BROKEN:` segment and emits `AuditKind::AuditResume`. The writer uses `sync_channel(audit.queue_capacity)` (default 10_000). Drops increment `sqe_audit_events_dropped_total{reason}`. `audit.fail_mode=closed` makes `AuditLogger::is_writable()` false so QueryHandler refuses new statements. Tests: `resume_across_restart_verifies_full_chain`, `truncate_tail_then_resume_reports_break`, `full_queue_increments_dropped_metric`. Config: `docs/site/book/src/deployment/configuration.md`.
