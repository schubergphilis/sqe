**STATUS 2026-10-12: lineage and audit sinks under a collector outage (`fix/sink-outage-816`, #816 part 2).**

Part 1 (Ranger bundle single-flight and refresh-ahead, metadata soft-TTL jitter) is !1404. This change covers the sink half of #816.

OpenLineage spool:
- At its cap the spool now drops the oldest segments, not the newest event, so a long outage keeps the recent lineage a catching-up consumer needs. Dropped events are counted in `dropped_count` and the new `sqe_lineage_spool_dropped_events_total`. The cap warning is logged once per crossing.
- `sqe_lineage_dropped_events_total` (full in-memory channel) is now registered; it was created but never exported.
- Spooled lines are flushed before the file is dropped. A tokio `File` finishes writes in the background, so a line could be missing from disk or glued to the next one.
- Replay stops at the first segment that still fails, so events are never replayed out of order. A lock keeps the cap from deleting a segment the replay task is renaming.

Audit OTLP shipper: unchanged by design. Its spool is the local audit log, so `max_spool_bytes` stays a warning and nothing is dropped; the new test proves an outage past the threshold keeps every record and replays them in order.

Tests: `sink_outage_chaos_test` (drop-oldest with counter, in-order replay after recovery, a hung collector never blocks queries), `spool_drops_an_event_larger_than_the_cap`, shipper `outage_past_max_spool_bytes_keeps_every_record_and_replays`.

**NEXT:** `{catalog}` placeholder for `[storage.tvf] allowed_object_store_prefixes`; #835; #803 when it recurs.
