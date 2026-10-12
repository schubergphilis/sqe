**STATUS 2026-10-12: lineage and audit sinks under a collector outage (`fix/sink-outage-816`, #816 part 2).**

Part 1 (Ranger bundle single-flight and refresh-ahead, metadata soft-TTL jitter) is !1404. This change covers the sink half of #816.

OpenLineage spool:
- At its cap the spool now drops the oldest segments, not the newest event, so a long outage keeps the recent lineage a catching-up consumer needs. Dropped events are counted in `dropped_count` and the new `sqe_lineage_spool_dropped_events_total`. The cap warning is logged once per crossing.
- `sqe_lineage_dropped_events_total` (full in-memory channel) is now registered; it was created but never exported.
- Spooled lines are flushed before the file is dropped. A tokio `File` finishes writes in the background, so a line could be missing from disk or glued to the next one.
- Replay stops at the first segment that still fails, so events are never replayed out of order. A lock keeps the cap from deleting a segment the replay task is renaming.

Audit OTLP shipper: `max_spool_bytes` is now a hard cap, not a warning. The spool is only the SIEM delivery buffer; the native audit log keeps every record. Past the cap the shipper cuts shipped records first, then the oldest unshipped ones down to half the cap, counted in the new `sqe_audit_export_spool_dropped_records_total` and warned once per crossing. It runs on failed passes too, so an outage cannot fill the disk. `SpoolCompactor::trim_head` copies the kept tail outside the writer lock, then under it moves only the bytes appended meanwhile, renames the copy over the spool and reopens the sink, so the audit writer is never stalled long enough to fill its queue.

Tests: `sink_outage_chaos_test` (drop-oldest with counter, in-order replay after recovery, a hung collector never blocks queries), `spool_drops_an_event_larger_than_the_cap`, shipper `outage_past_max_spool_bytes_drops_the_oldest_and_replays_the_rest` (real writer: spool held under the cap, drops counted, writer keeps writing into the trimmed file, newest records replay in order, native log keeps all) and `spool_over_cap_drops_shipped_records_first`.

**NEXT:** the native audit log (`audit_log_path`) is still append-only with no cap, so rotation is up to the platform; decide whether SQE should rotate it (the hash chain resumes from its last line). With audit export enabled but no OTLP endpoint resolved, the spool is written and no shipper runs, so it has no cap. #835; #803 when it recurs.
