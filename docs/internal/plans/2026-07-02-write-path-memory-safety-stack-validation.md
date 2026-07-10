# Write-path memory safety: stack-validation runbook

Status: ready to run. All write-path memory-safety code is merged to `main`
(MR !508 Layer A + Layer B ingest/MERGE-B1, MR !509 fanout wiring + cow-keep
tracking, MR !510 MERGE B2 target streaming + fanout auto-tune).

The streaming and bounded-writer paths ship **opt-in, default-off** because
they cannot be exercised locally. There is no Polaris+S3 stack in the dev
environment, so file-level `fs_io`+TempDir unit tests are the only automated
coverage. The Iceberg-commit parity of streamed and cutover output is
integration-only. This runbook is the acceptance gate before any of these
flags is defaulted on.

Design spec: `docs/internal/specs/2026-07-02-write-path-memory-safety-design.md`.

## What ships off by default, and why

| Config key (`[query]`) | Default | Effect when set |
|---|---|---|
| `write_buffer_tracking` | `true` | Layer A pool reservations active. A denied grow returns a typed `ResourceExhausted` instead of OOM. Already on; validated only at unit level. |
| `merge_target_streaming` | `false` | Copy-on-write MERGE streams its target from the pinned `old_data_files` instead of buffering the whole target into a MemTable. Requires `write_buffer_tracking = true`. |
| `fanout_max_open_writers` | `0` | `0` = unbounded `TaskWriter` (current behavior). A positive value caps open per-partition writers and switches to `BoundedFanoutWriter` (LRW cutover). |
| `fanout_buffer_budget` | `"0"` | `"0"` = no byte budget. A memory string (e.g. `"256MB"`) caps total buffered fanout bytes and forces a cutover when exceeded. |

Both fanout knobs at their defaults leave partitioned writes on the unbounded
`TaskWriter`. Setting either one opts into the bounded writer; the other, if
left at `0`, auto-derives from the coordinator pool via `auto_fanout_caps`
(`max_open` clamped to [8, 64], byte budget `pool/8` floored at one row-group
estimate).

## Preconditions

- A running quickstart stack: Polaris (or another REST catalog) + S3-compatible
  storage. See `scripts/integration-test.sh` and the quickstart recipe.
- A coordinator built from `main` at or after commit `67fb3a1` (MR !510).
- A way to set `[coordinator] memory_limit` small for the tiny-pool cases
  (a per-run config override; see the parity-rig recipe).

## Part 1: MERGE target streaming (`merge_target_streaming`)

### 1.1 Row/snapshot parity vs the buffered path

Run the same MERGE twice against equivalent tables, once with the flag off
(buffered MemTable target) and once on (streamed target). The results must be
identical.

```toml
# config-a.toml (baseline, current behavior)
[query]
merge_target_streaming = false

# config-b.toml (streamed target)
[query]
write_buffer_tracking = true   # default, stated for clarity
merge_target_streaming = true
```

Workload: a MERGE that exercises all three arms on a target big enough that
the buffered path would materialise a non-trivial MemTable.

```sql
MERGE INTO sales.orders t
USING staging.order_updates s ON t.id = s.id
WHEN MATCHED AND s.op = 'delete' THEN DELETE
WHEN MATCHED THEN UPDATE SET amount = s.amount, status = s.status
WHEN NOT MATCHED THEN INSERT (id, amount, status) VALUES (s.id, s.amount, s.status);
```

Acceptance:
- Post-MERGE `SELECT count(*)` and a full-table checksum (e.g. ordered
  `SELECT ... ORDER BY id` hashed, or `SELECT count(*), sum(amount)`) match
  between config-a and config-b.
- One new snapshot per run in `table_snapshots('sales', 'orders')`; the
  `added-data-files` / `deleted-data-files` summary counts are equivalent
  between the two configs (file identities differ, counts and semantics do not).
- No error, no coordinator restart.

### 1.2 Tiny-pool forcing (graceful degradation)

With `merge_target_streaming = true`, the target no longer inflates a MemTable,
so the streamed path should tolerate a much smaller pool than the buffered
path. Confirm both the failure mode and the improvement.

```toml
[coordinator]
memory_limit = "256MB"   # deliberately small
[query]
merge_target_streaming = true
```

Acceptance:
- If the MERGE still exceeds the pool, it fails with a typed
  `ResourceExhausted` error naming a write buffer (e.g. `merge-*-buffer`),
  and the coordinator stays up to serve the next query. It does **not** OOM-kill.
- The buffered path (`merge_target_streaming = false`) at the same tiny pool
  should fail earlier or harder; note the delta so the streaming benefit is
  recorded.

## Part 2: Bounded fanout writer (`fanout_max_open_writers` / `fanout_buffer_budget`)

### 2.1 Correctness with cutover (open-writer cap)

Partition on a column with more distinct values than the cap, so the writer
must cut over (close and later reopen partitions).

```toml
[query]
fanout_max_open_writers = 4   # far below the partition cardinality
```

```sql
CREATE TABLE analytics.events_p PARTITIONED BY (identity(region)) AS
SELECT * FROM raw.events;   -- raw.events spans, say, 20 regions
```

Acceptance:
- `SELECT count(*)` on the result equals the source row count. No rows dropped
  by a cutover-and-reopen.
- Per-partition counts match a `GROUP BY region` over the source.
- One cutover log line per eviction is emitted (the `cutovers()` counter is
  non-zero). Confirm it fired.

### 2.2 Byte-budget cutover

Force cutovers by budget rather than by count.

```toml
[query]
fanout_max_open_writers = 64      # high, so the byte budget is the binding limit
fanout_buffer_budget = "64MB"     # small enough to trip mid-write
```

Acceptance:
- Same row-count and per-partition-count parity as 2.1.
- Cutovers driven by the byte budget are observed (log/counter).

### 2.3 Cutover -> `rewrite_data_files` round-trip

A bounded write that cuts over produces more, smaller files per partition than
an unbounded write. Confirm compaction reconciles them without data loss.

```sql
-- after the bounded partitioned write from 2.1 / 2.2
CALL system.rewrite_data_files('analytics', 'events_p');
```

Acceptance:
- `SELECT count(*)` unchanged before and after the rewrite.
- `table_files('analytics', 'events_p')` shows fewer, larger files after.
- A fresh snapshot is recorded; time-travel to the pre-rewrite snapshot still
  returns the same rows.

### 2.4 Tiny-pool forcing (graceful degradation)

```toml
[coordinator]
memory_limit = "256MB"
[query]
fanout_max_open_writers = 8
fanout_buffer_budget = "32MB"
```

Acceptance:
- A partitioned write whose working set exceeds even the bounded budget fails
  with a typed `ResourceExhausted` naming the fanout buffer, not an OOM kill.
- The coordinator survives and serves the next query.

### 2.5 Auto-derivation sanity

Set exactly one knob and leave the other at `0`; confirm the unset one
auto-derives from the pool (`auto_fanout_caps`) rather than staying unbounded.

```toml
[coordinator]
memory_limit = "8GB"
[query]
fanout_max_open_writers = 16   # fanout_buffer_budget left at "0" -> should auto-derive ~1GB
```

Acceptance:
- Bounded mode is active (cutovers possible), and the derived byte budget is
  in the expected band (`pool/8`, here ~1GB), visible in a startup/first-write
  log line if one is emitted, or inferable from cutover behavior.

## Part 3: Decision gate

Only after Parts 1 and 2 pass on the stack:

- Record the parity results and the tiny-pool degradation deltas in this file
  (or a linked results note) so the evidence is durable.
- Then, and only then, consider spec open decision #4: defaulting the flags on
  (e.g. both-fanout-knobs-`0` flipping to bounded, or `merge_target_streaming`
  defaulting true). That flip is a separate, signed-off follow-up MR; it must
  not ride along with validation.

Until this gate is cleared, the defaults stay as shipped: tracking on,
streaming and bounded-fanout off.
