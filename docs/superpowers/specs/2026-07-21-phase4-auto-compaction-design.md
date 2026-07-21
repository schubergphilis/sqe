# Phase 4: Autonomous Distributed Compaction (Auto-Compaction) Design

**Status:** Proposed (design approved in shape; delivery-mode refined 2026-07-21)
**Predecessors:** Phase 1 (delete guard + partition-aware grouping, MR !660), Phase 2 (delete-applying streaming rewrite + seq-pin + conflict retry, MR !661), Phase 3 (sort/z-order, MR !661), plus prunable-sort-layout + `delete_file_threshold` + `rewrite_all` (MRs !662, !663).

## Non-negotiable invariants

1. **The interactive query path stays 100% user-identity.** The maintenance service principal is structurally unreachable from any listener: its provider object is never constructed on an auth-chain path.
2. **Zero changes to `vendor/iceberg-rust`.** Every primitive the design needs already exists in the fork (verified): `StaticTable::from_metadata_file`, avro `DataFile` read/write helpers, `RewriteFilesAction::set_snapshot_properties`, `set_new_data_file_sequence_number`.
3. **Commit authority never leaves the coordinator.** Workers can produce data files in object storage; only the coordinator can change table state.
4. **Runs in both single-node and distributed mode.** The same job definition executes coordinator-local when no worker fleet is present and fans out to workers when one is. Execution is adaptive, not gated.

## 1. Motivation

Small-file and delete-file accumulation is the dominant read-performance decay mode for Merge-on-Read Iceberg tables. Today SQE has correct, delete-aware compaction, but it requires a human with write privilege to type `CALL system.rewrite_data_files`, and it runs entirely on the coordinator (capped at one node's NIC and CPU while that node is also serving interactive queries).

Phase 4 adds three capabilities around the shipped compaction core:

1. A **maintenance service principal**: an opt-in, least-privilege, non-human identity used only by the background path.
2. A **scheduler** (orchestrator): SQE has none today. An in-coordinator cron loop discovers opted-in tables and runs jobs through the existing procedure dispatcher.
3. **Adaptive distributed execution**: file-group rewrites run local when alone and fan out across the worker fleet when present.

## 2. Resolved decisions

### Fork A. Scheduling and HA

An in-coordinator `spawn_supervised("maintenance-scheduler", ...)` loop (the existing idiom), registered into the `_task_guards` list in `sqe_server.rs`. It ticks on `tick_secs`, evaluates cron expressions with per-table deterministic jitter (to avoid a fleet-wide 02:00 thundering herd on Polaris/S3), and runs due jobs by calling the same internal functions `CALL system.rewrite_data_files` calls. One dispatcher, one audit path, two triggers (scheduled and manual `CALL`).

External-trigger deployments leave `scheduler.enabled=false` and drive timing from a Kubernetes `CronJob` (`concurrencyPolicy: Forbid`) issuing `CALL system.run_maintenance(...)`. Both modes ship.

**Double-fire safety under multi-coordinator HA, layered so correctness never depends on the lease:**

- **Layer 1 (correctness, exists today):** Iceberg optimistic concurrency. The commit already sets `check_file_existence(true)`; if two coordinators compact the same table, the loser's `RewriteFilesAction` fails because its inputs were already removed, the retry re-plans from the new snapshot, finds nothing eligible, and exits "skipped". No corruption, no data loss. The loser's orphaned outputs are reclaimed by its `WriteCleanupGuard` / the orphan sweep.
- **Layer 2 (efficiency, later phase):** a lease in `sqe_system.maintenance_log` so the two coordinators do not both rewrite terabytes and throw one copy away. **Open item:** the exact catalog CAS primitive must be proven with a two-racer integration test before it ships. Iceberg fast-appends are commutative and do not conflict, so a claim-as-append will not give mutual exclusion; the claim commit must use an action that carries a base-snapshot assertion that fails on a concurrent writer. This does not block any earlier phase.
- **Layer 3 (deployment):** external K8s `CronJob` with `Forbid` gives HA-safe scheduling with near-zero new machinery.

Rejected: a job-queue/DB (SQE has no shared DB and should not grow one; all durable state fits one Iceberg table); K8s Lease as the only mechanism (couples core correctness-efficiency to one orchestrator, against SQE's sovereignty story; kept as an optional backend); commit-conflict as the only guard (correct but wasteful); user-table properties as the lease store (pollutes the history auditors read).

### Fork B. Distributed execution

**Adaptive worker-side full group rewrite.** `distribution.mode`:

- `"auto"` (default): distribute when `>= min_workers` healthy workers are registered, else run coordinator-local. This is the "works in single mode or distributed mode" requirement.
- `"local"`: always coordinator-local (single-node, dev).
- `"require"`: refuse to run without the fleet (for operators who want to guarantee background work never loads the coordinator).

A worker receives one file group, performs the delete-applying read, optional sort/z-order, and rolling Parquet write directly to the table's data location, then returns `DataFile` metadata. The coordinator plans groups, dispatches, validates invariants, and issues exactly one atomic `RewriteFilesAction` commit.

**Why not stream-back (workers decode, coordinator writes):** compaction is byte-symmetric; streaming decoded batches back moves every byte across the network twice and still funnels all encode+upload through one NIC, parallelizing only decode/sort. It fails the high-performance goal at the scale that motivates it. Coordinator-local remains the `"local"`/single-node path, not a separate distributed mode.

**Feasibility findings (verified in-tree, no vendored changes):**

1. Workers must not receive serialized `FileScanTask`s: the vendored type marks `partition`, `partition_spec`, `name_mapping` as skip-serialize, so shipping tasks silently corrupts identity-partitioned reads. Workers re-plan locally instead.
2. `StaticTable::from_metadata_file` lets a worker build a fully pinned table from a `metadata.json` S3 path plus a FileIO from vended S3 creds, with zero catalog access.
3. `write_data_files_to_avro` / `read_data_files_from_avro` carry `DataFile` metadata worker-to-coordinator in Iceberg's own encoding.
4. `RewriteFilesAction::set_snapshot_properties` stamps the commit with job identity.

**Wire protocol:** a new Flight `do_action("compact_file_group")` on the worker, signed exactly like `do_get` tickets (worker-secret header + HMAC-SHA256 over request bytes, reusing the existing constant-time verify). Request carries `job_id`, `group_id`, `metadata_location`, `snapshot_id` (worker refuses if the metadata's current snapshot differs), `group_file_paths`, `target_file_size_bytes`, `compression`, optional sort spec, and the S3 connection block (same field set as `ScanTask`). The action returns a stream: `Progress` heartbeat frames (liveness; no frame for `group_heartbeat_timeout_secs` => cancel and reschedule) then a `Done` frame with avro-encoded new `DataFile`s + counts + uploaded paths (for orphan accounting).

**Worker execution** (all from a new shared `sqe-compaction` crate, extracted from today's `rewrite_group`): verify HMAC; build FileIO; `StaticTable::from_metadata_file`; assert snapshot id; `plan_delete_aware_read` restricted to the group's paths (fail loud if any path missing, same resurrection guard as today); delete-applying arrow stream; optional spillable sort on the worker's DataFusion runtime; rolling write via the shared `write_data_files_streaming` with `WriteCleanupGuard` armed; per-group `expected_rows_after_deletes` cross-check; stream `Done`, disarm guard only after ack. Workers never see a catalog token; their capability is exactly "read the pinned snapshot's files, write new files under the table location".

**Coordinator assembly and the single atomic commit:** load table via the maintenance principal's catalog bridge; capture `seq_at_start` + `metadata_location`; plan groups exactly as today; dispatch to healthy workers via `WorkerRegistry` + `WorkerLoadTracker::reserve` RAII guards, largest-first, bounded by `max_inflight_groups_per_worker` (default 1, so compaction never saturates a worker serving interactive scans); collect responses, `read_data_files_from_avro`; re-run the global `added_rows <= removed_rows` invariant; one `RewriteFilesAction` with `enable_delete_filter_manager(true)`, `check_file_existence(true)`, `set_new_data_file_sequence_number(seq_at_start)`, covered position deletes dropped in the same swap, snapshot stamped with job id + principal + trigger; existing 4-attempt conflict-retry loop re-plans and re-dispatches.

The sequence-number pin survives distribution because pin and commit both stay coordinator-side and workers provably read the pinned snapshot (`StaticTable` from the exact `metadata_location` + snapshot-id assertion). Concurrent equality deletes still land above the pin and still apply.

**Accepted trade-off:** once a worker returns `Done`, its cleanup guard disarms; on a coordinator commit-conflict retry, that attempt's written files become orphans reclaimed by the age-thresholded sweep. At terabyte scale this is real transient storage per retry. It is inherent to distributed-write + optimistic commit and is accepted, not fixed.

### Fork C. The service principal

Wire the already-written-but-unwired `OidcM2mProvider` through a dedicated `[maintenance.principal]` config block owned solely by the maintenance subsystem. **Do not** add an `AuthProviderConfig::M2m` variant and **do not** touch `build_auth_chain`. That inversion is the compliance argument: the interactive path cannot accidentally use the service token because the provider is never constructed on any listener path.

Four reinforcing layers: (1) ownership: a `MaintenancePrincipal(OidcM2mProvider)` newtype lives inside the scheduler; (2) session isolation: per-job ephemeral `Session` minted from the M2m `Identity`, never inserted into `SessionManager`, dropped when the job ends; (3) capability narrowing: the scheduler invokes only the maintenance dispatcher, never the SQL query path; (4) config validation: warn if the principal client_id collides with any auth-provider client_id, error if `mode != "off"` without a principal block.

**Three switches gate any autonomous write:** global `mode` (default `"off"`, then `"advisory"`, then `"active"`); per-table `sqe.maintenance.enabled=true` property set by the owner; a least-privilege Polaris principal role with exactly TABLE_READ_DATA + TABLE_WRITE_DATA on the opted-in namespaces (no CREATE/DROP/admin; Polaris enforces server-side as defense-in-depth). A table with the property but no grant fails loud (audit + metric), never a silent skip; a table with a grant but no property is never selected.

**Token flow:** `OidcM2mProvider` (client_credentials, single-flight cache, pre-emptive refresh) -> per-job `Identity { catalog_token, user_id, roles }` -> ephemeral Session -> `create_catalog_bridge` -> Polaris. Token refreshed immediately before commit for long jobs. Workers receive only S3 creds in the signed request (never the catalog token); when Polaris credential vending is un-stubbed the request carries per-table vended creds scoped to the table location, no wire change.

## 3. State and the "no table hacking" guarantee

The user table is touched in exactly three standard ways, none a hack, no locks:

1. The one-time opt-in property, set by the owner via `ALTER TABLE`, visible and attributable in table history.
2. The compaction commit itself, a normal atomic snapshot stamped with job/principal properties.
3. Read-only, snapshot-pinned direct reads of `metadata.json` by workers via `StaticTable` (S3 creds only, no catalog, no commit rights).

All scheduling state, last-run tracking, job history, and the HA lease live in a separate `sqe_system.maintenance_log` Iceberg table, not in user-table properties. That table is the SOC2 evidence ledger (`SELECT`, not log-grep) and doubles as durable last-run state so a coordinator restart does not re-fire a just-run job.

## 4. Configuration (`[maintenance]`, flat sub-struct idiom, serde defaults, zero-interval validation)

```toml
[maintenance]
mode = "off"                         # "off" | "advisory" | "active"; default off

[maintenance.principal]
token_endpoint = "https://idp.example.com/realms/sqe/protocol/openid-connect/token"
client_id      = "sqe-maintenance"
client_secret  = "${SQE_MAINTENANCE_CLIENT_SECRET}"
scope          = "PRINCIPAL_ROLE:sqe_maintenance"
user_id        = "svc-sqe-maintenance"     # audit display identity
roles          = ["maintenance"]
refresh_skew_secs = 60

[maintenance.scheduler]
enabled          = false             # external-trigger deployments leave this off
tick_secs        = 60
schedule         = "0 2 * * *"       # global default; per-table property overrides
jitter_secs      = 900
max_concurrent_jobs = 1
lease            = "catalog"         # "none" | "catalog" | "kubernetes"
lease_ttl_secs   = 300
state_table      = "sqe_system.maintenance_log"
single_scheduler_acknowledged = false  # required true for enabled=true + lease="none"

[maintenance.compaction]
target_file_size      = "512MB"
min_input_files       = 5
delete_file_threshold = 2
strategy              = "binpack"    # "binpack" | "sort" | "zorder"

[maintenance.distribution]
mode                           = "auto"    # "auto" | "local" | "require"
min_workers                    = 2
max_inflight_groups_per_worker = 1
group_attempts                 = 2
group_timeout_secs             = 3600
group_heartbeat_timeout_secs   = 120
```

Per-table property overrides: `sqe.maintenance.enabled`, `sqe.maintenance.compaction.schedule`, `.target-file-size-bytes`, `.strategy`, `.sort-order`, `.delete-file-threshold`.

## 5. HA safety argument

- **Atomicity:** all mutation is one `RewriteFilesAction` per attempt through Polaris's single-writer pointer; a job that dies before commit changed nothing visible.
- **No lost rows:** per-group `expected_rows_after_deletes` (on the worker, re-validated from reported counts), global `added_rows <= removed_rows` before commit, `check_file_existence` turning plan/snapshot skew into a hard error. All three exist today and are preserved.
- **No resurrected rows:** the seq-pin captured at plan time, applied at commit, both coordinator-side; workers read the exact pinned metadata and assert the snapshot id.
- **Double-fire:** lease prevents it in the common case; if the lease fails, Iceberg optimistic concurrency guarantees exactly one commit wins and the loser re-plans to a no-op. Waste, never corruption.
- **Coordinator crash mid-job:** nothing committed; lease TTL-expires; next leader re-evaluates from `maintenance_log`; orphaned worker output bounded by the orphan sweep age threshold (set above `group_timeout_secs`).
- **Undo:** compaction never deletes data files (that is snapshot expiry's job); any committed compaction is reversible within the retention window via `rollback_to_snapshot`. Operational rule: `expire_snapshots` must lag compaction by the retention SLA.

## 6. Observability

- `CALL system.table_health('t')` (new, read-only, no write privilege): live file count, small-file count under target, avg/p50 size, delete-file and delete-heavy counts, eligible group count, estimated rewrite bytes, last compaction (from snapshot stamps), opt-in status. Advisory mode publishes the same per table.
- Prometheus: `sqe_maintenance_job_total{table,trigger,status}`, `_job_duration_seconds`, `_groups_total{status}`, `_bytes_rewritten_total`, `_rows_removed_total`, `_files_before/after`, `_skipped_total{reason}`, `_lease_holder`, `_worker_inflight_groups{worker}`, and table-health gauges from the advisory pass.
- Audit: new `AuditKind::Maintenance` events at tick decisions, job start, per-group dispatch, retry, commit, failure, lease acquire/steal/release, and every authorization denial; actor is the principal's SessionUser, session_id carries the job id for correlation; snapshot stamping gives an independent second record.
- `sqe_system.maintenance_log`: `(job_id, table, trigger, principal, started_at, finished_at, status, files_in, files_out, bytes_in, bytes_out, rows_removed, snapshot_id, error)` + lease rows.

## 7. Phased delivery (revised for dual-mode)

Because compaction must work single-node and distributed, execution mode is adaptive from the first active phase (`distribution.mode="auto"`): the same job runs local when alone and distributes when a fleet is present. The distributed worker path lands as the scale enhancement, not a separate product mode.

- **Phase 4a: Advisory + principal (mutates nothing).** Wire `[maintenance.principal]` + `OidcM2mProvider`; `[maintenance]` config; `CALL system.table_health`; advisory scheduler loop (discovery + health + metrics + audit) behind `mode="advisory"`; bootstrap `sqe_system.maintenance_log`. Read-only grants. Ships fleet-wide small-file / delete-debt visibility.
- **Phase 4b: Active, single-coordinator, adaptive execution.** `mode="active"`; scheduler runs `rewrite_data_files` under the ephemeral maintenance session; per-table property opt-in; snapshot stamping; write grants; `distribution.mode="auto"` where "auto" with no fleet == the shipped coordinator-local path (so single-node works immediately). `lease="none"` + acknowledgment flag; document the K8s CronJob external-trigger recipe as HA-safe.
- **Phase 4c: Distributed rewrite.** Extract `sqe-compaction` crate (mechanical move; coordinator-local path re-exports so 4b behavior is provably unchanged); worker `compact_file_group` action + HMAC; job runner with dispatch/retry/heartbeats; `"auto"` now actually fans out to the fleet. Benchmark gate: distributed rewrite of an N-group table on 4 workers beats coordinator-local by > 2.5x wall-clock at SF10-scale data.
- **Phase 4d: HA + scale polish.** Catalog lease (claim/steal/release; gated on the two-racer CAS prototype), optional `kubernetes` lease backend, multi-coordinator scheduler enablement, partial-progress commits for very large tables, vended per-table S3 creds once vending is un-stubbed.

Each phase is independently shippable and revertible by config.

## 8. Risks and open questions

1. **`session_has_write_privilege` role-name heuristic** (`maintenance.rs`): name-string security. The ephemeral maintenance session should carry an explicit internal marker checked by `authorize_or_deny` rather than relying on role-name matching; Polaris stays the real enforcer. Decide during 4b.
2. **State-table bootstrap authority:** creating `sqe_system.maintenance_log` needs CREATE on that namespace, contradicting "no CREATE grants". The operator creates it at install (Helm hook / quickstart SQL); the principal gets read+write only; SQE refuses `lease="catalog"` if the table is absent, with a precise error.
3. **Catalog-lease CAS semantics:** must be proven with a two-racer integration test before 4d (fast-appends are commutative and will not conflict; the claim needs a base-snapshot-asserting action). Correctness-independent, so it gates only 4d.
4. **Sort groups are whole partitions:** a 100GB partition lands on one worker and serializes the job tail. 4d mitigations: range-split sort groups with a coordinator-side merge plan, or cap sort compaction per run. Bin-pack has no such issue.
5. **Worker fetch throughput:** land the fetch/decode staging work (branch `fix/scan-fetch-decode-pipeline`) before benchmarking 4c or the distributed win is understated.
6. **~~MoR row-resurrection bug~~ CLEARED:** this was the Phase 1 root cause and is fixed in Phase 2's delete-applying rewrite (MR !661), proven by `rewrite_applies_equality_deletes`. Not a Phase 4 gate.
7. **Equality-delete aging:** compaction drops covered position deletes but leaves equality deletes to age out; a fuller autonomous system eventually schedules delete-file rewrite / expiry too. The scheduler is deliberately generic (`job_key` per procedure) so adding job types later is config, not architecture.
8. **Multi-catalog scope:** 4a ships single-catalog (default warehouse); multi-catalog opt-in is explicit config (`maintenance.catalogs = [...]`).

## 9. Key file anchors

- `crates/sqe-coordinator/src/maintenance.rs` (rewrite core to extract; catalog bridge; auth gate)
- `crates/sqe-auth/src/oidc_m2m.rs` (principal provider, ready to wire)
- `crates/sqe-worker/src/flight_service.rs` (HMAC verify to reuse; new `do_action` path)
- `crates/sqe-coordinator/src/worker_registry.rs` (`healthy_workers`, `WorkerLoadTracker::reserve`)
- `crates/sqe-coordinator/src/bin/sqe_server.rs` (`_task_guards` registration seam)
- `crates/sqe-core/src/config.rs` (SessionConfig idiom to copy; auth variants stay untouched)
- Vendored (unmodified): `table.rs` `StaticTable::from_metadata_file`; `spec/manifest/data_file.rs` avro helpers; `transaction/rewrite_files.rs` `set_snapshot_properties` / `set_new_data_file_sequence_number`.
