# Distributed compaction

Phase 4c fans `CALL system.rewrite_data_files` out to the worker fleet.
The data flow below covers how a rewrite job is planned, dispatched,
executed, and committed, and why commit authority never leaves the
coordinator.

See [Configuration](../deployment/configuration.md) for the
`[maintenance.distribution]` config block (`mode`, `min_workers`,
timeouts, `partial_progress`) and [CALL procedures](../sql-reference/procedures.md)
for the per-call `distributed => 'auto'|'local'|'require'` override.

## Summary

A distributed rewrite job is still one atomic Iceberg commit. The
coordinator, not the workers, owns that commit. Workers do the expensive
part (reading data files, applying deletes, sorting, writing new
Parquet) directly against S3, using their own S3 credentials and no
catalog token at all. They report back a small, Avro-encoded description
of what they wrote. The coordinator decodes those descriptions, re-checks
the row-count invariant across the whole job, and commits a single
`RewriteFilesAction` that swaps every old file for every new file at
once.

The design goal: move compute and data-plane I/O to the fleet, keep
every correctness-bearing decision, including the decision that the job
succeeded at all, on the coordinator.

## Data flow

```
CALL system.rewrite_data_files(..., distributed => 'auto')
        |
        v
COORDINATOR (sqe-coordinator::maintenance)
  1. load_table, pin snapshot_id + sequence_number
  2. plan_delete_aware_read + collect_live_delete_files
  3. bin-pack eligible files into groups
        |
        v  one signed CompactGroupRequest per group (Arrow Flight do_action)
        |
WORKER (sqe-worker::compaction::compact_file_group)
  4. verify_compaction_signature (HMAC over the exact wire bytes)
  5. StaticTable::from_metadata_file(metadata_location) -- S3 creds only,
     no catalog token
  6. assert current_snapshot_id == request.snapshot_id (snapshot pin)
  7. re-plan the delete-aware read locally, resolve this group's files
     (resurrection guard: any requested path missing from the re-plan
     fails loud instead of reading it blind)
  8. read + apply deletes + optional sort + write new Parquet -> S3
  9. Avro-encode the new DataFiles into CompactGroupResponse
        |
        v  Progress* then one Done frame (CompactGroupFrame)
        |
COORDINATOR (sqe-coordinator::compaction_dispatch + sqe-compaction::dispatch)
 10. decode_group_response against THIS table load's schema/partition
     type/spec id/format version
 11. aggregate_group_outcomes: re-check added_rows <= removed_rows across
     the WHOLE job (each worker already checked its own group)
 12. Transaction::rewrite_files(): add every new DataFile, delete every
     old data file + covered position delete, commit ONE snapshot
```

Every group in a job is dispatched independently and can land on a
different worker. The job either commits every group's output in one
transaction or commits nothing: a group that exhausts its retries fails
the whole job before any commit is attempted, and any group still
in flight when that happens is simply abandoned (its output becomes an
orphan, see below).

## Why workers re-plan instead of receiving a serialized scan plan

The coordinator's read plan (`plan_delete_aware_read`) produces
`FileScanTask`s with fields that are not meant to survive serialization
(iceberg-rust marks some `#[serde(skip)]`). Sending a coordinator-built
plan over the wire and expecting a worker to resume it would silently
drop those fields.

Instead, the worker receives only what it needs to redo the planning
step itself: `metadata_location`, `snapshot_id`, and the list of data
file paths in its group. It loads a catalog-free `StaticTable` from that
metadata location, asserts the table's current snapshot still matches
`snapshot_id` (the coordinator's plan is only valid against the snapshot
it read), then calls the same `plan_delete_aware_read` the coordinator
used to resolve which delete files apply to which data file. This
duplicates a small amount of manifest I/O per worker but means the
worker's delete accounting is independently derived, not trusted
verbatim from the coordinator, matching the same resurrection guard
`rewrite_group` already enforces on the local path.

## Trust boundary: workers get S3 credentials, never a catalog token

A worker executing `compact_file_group` builds its `FileIO` from the
`S3Conn` embedded in the request (endpoint, region, access key, secret
key, session token, path-style, allow-http): the coordinator's own
static storage credentials, the same ones it would use for a local
rewrite, not a per-caller vended credential. Nothing in the request
carries a Polaris/catalog token, and nothing in the worker's compaction
path calls the catalog. The worker cannot list namespaces, cannot look up
other tables, and cannot commit anything: `StaticTable` never talks to a
catalog, and the worker's `compact_file_group` handler returns its
`CompactGroupResponse` over Flight instead of writing to
`sqe_system`/Polaris directly.

The blast radius of a compromised or buggy worker is therefore bounded
to "can read/write objects the coordinator's S3 credentials can reach,"
not "can commit arbitrary changes to the catalog." Commit authority is a
single-writer property of the coordinator, by construction, not by
convention.

**The request body carries live S3 credentials.** `CompactGroupRequest`
is signed (HMAC-SHA256 over the exact wire bytes, verified by the
worker) so a request cannot be forged or tampered with, but the
signature does not encrypt the body. The Arrow Flight channel the
coordinator opens to a worker for `do_action("compact_file_group")` is
the same channel used for scan-fragment dispatch and credential pushes;
in production that channel must run over TLS, exactly like the
coordinator's own client-facing Flight SQL listener. Deploy worker
Flight endpoints behind TLS termination (or a TLS-terminated mesh) before
enabling distributed compaction outside a trusted, single-tenant network.

## Coordinator moves kilobytes, workers move the data

Bytes read from data files, deletes applied, and bytes written for the
rewritten Parquet output all flow directly between a worker and S3. The
coordinator never streams a row of table data for a distributed job. What
crosses the coordinator-worker Flight channel is metadata: one
`CompactGroupRequest` per group (paths, a snapshot id, S3 credentials,
a sort spec) out, and one `CompactGroupResponse` per group (Avro-encoded
`DataFile` entries, row/byte counts, uploaded paths) back. A job
compacting gigabytes of Parquet moves on the order of kilobytes of
metadata through the coordinator, the same shape as the existing
distributed scan path (coordinator hands out signed tickets, workers read
Parquet from S3 directly), applied to the write side.

## Placement and retries

The coordinator places groups largest-first across healthy workers,
capped at `max_inflight_groups_per_worker` per worker
(`sqe_compaction::dispatch::place_groups_largest_first`). A group that
fails on one worker is retried on a different healthy worker
(`group_attempts`, default 2); the failing worker is only marked
unhealthy for a transport-class failure (connection refused, timeout, a
connection that goes silent mid-stream), never for an application-level
failure the worker deliberately returned (resurrection guard, a
delete-accounting mismatch, a bad signature). An application-level
failure is evidence about the group or the request, and would fail
identically on any other worker, so it must not take a healthy worker out
of the fleet for every other job. A group that has failed on every
currently-healthy worker, or has exhausted every attempt, fails the whole
job.

## Continuous dispatch pipelining

Dispatch keeps every healthy worker filled up to
`max_inflight_groups_per_worker` at all times. As soon as any in-flight
group resolves, whether it succeeds or needs a retry attempt on a
different worker, the freed slot is refilled immediately from the pending
queue instead of waiting for every other group in the same wave to finish
first. Earlier dispatch ran in waves: compute a batch of assignments,
spawn all of them, wait for the whole batch to drain, then compute the
next one, so a worker that finished its one group early sat idle until
every sibling group in that wave also finished. The pure refill decision
(`next_group_assignment` in `sqe_compaction::dispatch`) is unit-tested for
largest-group-first priority, cap enforcement, exclusion-set fallthrough,
no double-assignment, and no assignment once every worker is saturated.

Pipelining changes only scheduling. Per-group retry on a different worker
(`group_attempts`), the transport-vs-application failure classification,
the stall guard, and the aggregate-then-commit step described above are
unchanged: pipelining decides which worker gets the next group and when,
not what happens once a group's output comes back. A job compacting a
given set of groups produces the same committed files whether dispatch
ran the old wave-based scheduler or the new continuous one; only the
wall-clock time to get there changes, because a fast worker no longer
waits on a slow sibling in the same wave before picking up its next group.

## Live progress and the heartbeat timeout

Before this change, a worker's `compact_file_group` action computed an
entire group, the delete-applying read, optional sort, and rolling
write, before it emitted anything at all: `Progress` and `Done` arrived
back-to-back once the whole rewrite had already finished. Under that
scheme `group_heartbeat_timeout_secs` only ever bounded the wait for a
frame once the worker had already produced its first one; a worker
wedged mid-compute produced no frames at all and was only caught by the
much coarser `group_timeout_secs`, the end-to-end bound on the whole
dispatch attempt (default 3600s).

Workers now emit a `Progress` frame every `PROGRESS_INTERVAL_BATCHES`
record batches (a fixed internal constant, currently 8) processed during
the write loop, across every write path: plain rewrite, sort, and
z-order. The coordinator's per-frame wait, `group_heartbeat_timeout_secs`,
resets on every frame it receives, so a fresh frame arrives well inside
that window as long as the worker keeps making forward progress.
`group_heartbeat_timeout_secs` is therefore a meaningful mid-compute
liveness bound now, not just a frame-delivery one: a worker that stalls
partway through, a wedged read or a hung write, stops producing frames
and is caught here. Its group is retried on a different healthy worker,
exactly like any other retryable dispatch failure, up to `group_attempts`.

`PROGRESS_INTERVAL_BATCHES` is not an operator-facing knob; the only
tuning surface is `group_heartbeat_timeout_secs` itself, sized to
however long an operator is willing to wait between progress signals
before treating a worker as stalled. Output is unchanged: only frames
were added to the wire protocol, and the data files a worker writes are
identical to before.

## Commit-conflict retries and orphaned worker output

A concurrent writer that commits between the coordinator's read and its
`RewriteFilesAction` commit produces a retryable conflict, exactly like
the local (non-distributed) rewrite path. When that conflict actually
surfaces as an `Err`, the coordinator re-plans the job from scratch
(fresh snapshot, fresh groups) and re-dispatches every group again, rather
than trying to patch the stale attempt against the new snapshot.

That last sentence has a load-bearing qualifier: **when it surfaces as an
`Err`.** The vendored `iceberg::transaction::Transaction::commit`
(`vendor/iceberg-rust/crates/iceberg/src/transaction/mod.rs`, `do_commit`)
silently reloads the table and REPLAYS the same `RewriteFilesAction`
unchanged whenever it detects its base is stale, and only returns an `Err`
to the coordinator once its own internal commit-retry budget
(`commit.retry.*` table properties) is exhausted. A single, one-shot
concurrent writer is absorbed entirely inside that call -- the coordinator
never sees an `Err` for it, so the "re-plan from scratch" retry described
above never runs. `RewriteFilesAction` has no equivalent of upstream
Iceberg's `validateNoNewDeletesForDataFiles` check, so if that one
concurrent writer landed a position delete on one of the files this job
is compacting, the silently-replayed commit can still resurrect the
deleted rows -- see the "Partial-progress commits" section below for how
`partial_progress` batches guard against this specifically, and note that
the DEFAULT (non-batched) path is exposed to the identical window, closing
it only requires the missing vendor-side validation.

The prior attempt's workers may already have written Parquet files to
S3 before a conflict was detected (whether that conflict surfaced to the
coordinator or was absorbed internally by `Transaction::commit`). Any
files an actual re-plan produced but did not end up committing are never
referenced by any commit, so they become orphans. That is an accepted
trade-off, not a bug: reclaiming them is `CALL system.remove_orphan_files`'s
job, on its normal age-thresholded sweep, the same mechanism that already
reclaims orphaned output from other failure paths. Correctness comes from
never committing a stale plan; cleanup of writes that turned out to be
unneeded is a separate, deliberately decoupled concern.

## Partial-progress commits (opt-in)

`[maintenance.distribution] partial_progress` (default `false`) lets a
distributed rewrite commit its successful groups in batches instead of
holding everything until the whole job finishes. Off, the job behaves
exactly as before: `commit_eligible_groups` treats every eligible group as
a single batch, so a terminal failure anywhere still commits nothing.

On, eligible groups are chunked into batches of `partial_progress_batch`
(default 10), and each batch commits as its own
`Transaction::rewrite_files()`, in the identical sequence the
single-commit path already uses: the sequence number pinned once at plan
time (`seq_at_start`), never advanced between batches, the same
`set_check_file_existence(true)` gate, the same snapshot-property stamp,
and the same added-rows-not-exceeding-removed-rows invariant, checked
over just that batch's own files.

Correctness does not depend on batching being disjoint by luck.
`eligible_groups` partitions every input data file into exactly one group
up front, and batching only chunks that partition further, so no data
file, and no position-delete file (each has exactly one
`referenced_data_file`), ever appears in two batches. After batch K
commits, batch K+1's input files are still exactly where they were:
`check_file_existence` on K+1's commit finds them, unless a concurrent
external writer removed one, exactly the same conflict it already catches
on the non-batched path. Pinning every batch to the same `seq_at_start`,
rather than to whichever snapshot each batch actually lands on, keeps a
concurrent equality delete from dodging a later batch's rewritten rows.

That handles equality deletes. Position deletes need a second guard,
because pinning `seq_at_start` does nothing for them: a position delete
matches by `(file_path, position)`, not by sequence number, and once a
batch's input file is replaced by its compacted output the position
delete's path matches nothing at all. If a concurrent writer lands a
position delete on one of THIS batch's own input files after the batch's
worker output was produced but before it commits, that output does not
reflect the delete -- and, per the previous section, `Transaction::commit`
can silently replay that stale output rather than surfacing a catchable
`Err`. So for every batch after the first (`committed_batches > 0`), the
coordinator does not simply wait for a retryable `Err`: **before every
commit attempt** it reloads the table and checks whether any position
delete now references one of this batch's own input files that it did not
already know about. If so, it re-dispatches the same groups against the
reloaded snapshot -- so the worker output actually reflects the delete --
before ever attempting to commit, instead of committing what it already
has. This closes the realistic single-race window for batches after the
first. A sub-millisecond gap remains between the coordinator's own reload
and `Transaction::commit`'s internal one (a delete landing in exactly that
gap); closing it needs the missing vendor-side
`validateNoNewDeletesForDataFiles`-equivalent check, which is out of scope
here. The first batch shares that same residual, unclosed window -- see
the caveat in the previous section.

A retryable commit conflict that the pre-commit check above did not
already preempt -- an unrelated concurrent writer, or `Transaction::
commit`'s own internal retry budget exhausted by sustained contention --
is still retried for any batch after the first: the coordinator reloads
the table and RE-DISPATCHES the same groups against the reload before
recommitting, up to the same retry budget the outer job-level retry uses.
It does not recommit the prior attempt's worker output unchanged (an
earlier version of this logic did, which is what let the position-delete
race above resurrect rows in the first place). The first batch's failure
is not retried at this inner layer at all; it bubbles up for the outer
`rewrite_data_files_distributed` loop to handle by re-planning and
re-dispatching the whole job, exactly as it did before `partial_progress`
existed -- with the same "may be silently absorbed by `Transaction::commit`
first" caveat from the previous section. Only a batch failure that is not
retryable, or one whose retries are exhausted, after at least one batch
has already committed, is terminal: the already-committed batches are
never rolled back, and the job reports `status = "partial"` in
`sqe_system.maintenance_log` with the partial byte/file/row counts and the
error that ended it, instead of failing the job outright.

`partial_progress` trades a larger commit-conflict surface, N commits
instead of one, each independently racing concurrent writers, for
incremental durability on very large tables, where losing an entire
multi-hour job to one late group failure is expensive. It is opt-in for
that reason: the default keeps the simpler, fully atomic guarantee, the
whole job commits or none of it does, for tables where a single
conflict-driven re-plan is cheap enough to just re-run.

## Multi-coordinator HA: the lease is an efficiency layer, not a correctness mechanism

Phase 4d adds a multi-coordinator HA lease
(`crates/sqe-coordinator/src/maintenance_lease.rs`) so that when more than
one coordinator runs the maintenance scheduler
(`maintenance.scheduler.lease = "catalog"`, see
[Configuration](../deployment/configuration.md)), only one of them
dispatches a rewrite for a given table in a given tick. The lease is a row
appended to `sqe_system.maintenance_log`, claimed with an Iceberg
optimistic-concurrency commit
(`Transaction::rewrite_files().delete_files([current_claim]).add_data_files([new_claim]).set_check_file_existence(true)`)
immediately before the scheduler dispatches the one expensive step of a
tick, the rewrite itself, and released the same way after. A coordinator
that finds the lease already held by a live holder skips its tick for that
table rather than attempting the rewrite at all.

State the invariant plainly: this lease is an efficiency layer, not a
correctness mechanism. Correctness against two coordinators double-
compacting the same table is already established above, in "Commit-
conflict retries": the coordinator that owns a rewrite job commits it with
`Transaction::rewrite_files()`, pinned to the snapshot it read and gated
by `set_check_file_existence(true)`. If two coordinators ever plan and
dispatch a rewrite for the same table at the same time, whether because
the lease was never configured (`lease = "none"`), a lease operation
failed and the scheduler proceeded unleased rather than blocking an
otherwise-eligible compaction, or a lease was legitimately stolen mid-job,
exactly one commit still wins. The loser's commit hits a file-existence
check against files the winner already deleted, fails as a non-retryable
conflict, and that coordinator's job ends having modified nothing. Nothing
about the lease's presence, absence, or failure changes this outcome. The
lease exists only to stop a second coordinator from paying for the loser's
redundant scan, delete-apply, and re-encode work when Iceberg would have
discarded that work anyway.

A holder crash mid-job is the same guarantee working the other way.
Because the rewrite is one atomic Iceberg commit, a coordinator that dies
mid-scan or mid-write has committed nothing: the table is untouched. The
lease it held simply outlives it until `lease_ttl_secs` (default 300s)
elapses, at which point the next coordinator to check the lease finds the
claim expired and steals it. No cleanup or reconciliation step is required
against the table itself; the only "recovery" is the next coordinator
picking the table back up on its own next due tick. (The lease's very
first claim for a brand-new table has no existing lease row to
compare-and-swap against and uses an unprotected append instead, so two
coordinators racing that one first-ever claim can both succeed; every
claim after that is fully exclusive. That first-claim race is a documented,
accepted gap in the lease's own exclusivity, not a gap in table
correctness: the Iceberg commit above still decides the table outcome
regardless.)

## What stays the same as the local path

The worker-side rewrite calls the exact same primitive the coordinator's
local path uses: `sqe_compaction::rewrite::rewrite_group`. Delete
application, the sort/z-order path, Parquet compression, and the
post-delete row cross-check are not reimplemented for the distributed
case, they are the same audited code, given a worker's inputs (S3
credentials instead of a catalog session, one pre-selected group instead
of the coordinator's full bin-packed set) instead of the coordinator's.
The commit itself, `Transaction::rewrite_files()` with
`set_enable_delete_filter_manager(true)`, `set_check_file_existence(true)`,
and the sequence-number pin, is line-for-line the same sequence the local
path already runs.
