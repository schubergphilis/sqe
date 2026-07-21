# Distributed compaction

Phase 4c fans `CALL system.rewrite_data_files` out to the worker fleet.
The data flow below covers how a rewrite job is planned, dispatched,
executed, and committed, and why commit authority never leaves the
coordinator.

See [Configuration](../deployment/configuration.md) for the
`[maintenance.distribution]` config block (`mode`, `min_workers`,
timeouts) and [CALL procedures](../sql-reference/procedures.md) for the
per-call `distributed => 'auto'|'local'|'require'` override.

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

## Commit-conflict retries and orphaned worker output

A concurrent writer that commits between the coordinator's read and its
`RewriteFilesAction` commit produces a retryable conflict, exactly like
the local (non-distributed) rewrite path. On that retry the coordinator
re-plans the job from scratch (fresh snapshot, fresh groups) and
re-dispatches every group again, rather than trying to patch the stale
attempt against the new snapshot.

The prior attempt's workers may already have written Parquet files to
S3 before the conflict was detected. Those files are never referenced by
any commit, the retry produced an entirely new set of output files, so
they become orphans. That is an accepted trade-off, not a bug: reclaiming
them is `CALL system.remove_orphan_files`'s job, on its normal
age-thresholded sweep, the same mechanism that already reclaims orphaned
output from other failure paths. Correctness comes from never committing
a stale plan; cleanup of writes that turned out to be unneeded is a
separate, deliberately decoupled concern.

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
