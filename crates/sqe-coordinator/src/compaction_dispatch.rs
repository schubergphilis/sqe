//! Coordinator-side Arrow Flight glue for the distributed `compact_file_group`
//! job runner (Phase 4c Task 4).
//!
//! Mirrors the dispatch pattern already used for scan tickets
//! (`distributed_scan::dispatch_to_worker`) and credential pushes
//! (`credential_refresh::push_credentials_to_worker_inner`): build/reuse a
//! Flight channel, sign the exact wire bytes, attach the worker-secret and
//! signature headers, call `do_action`, and drain the response stream. What
//! is new here: the action is `"compact_file_group"`, the response is a
//! `CompactGroupFrame` stream (a `Progress` heartbeat then one `Done`), and
//! failed groups are retried on a *different* healthy worker up to
//! `group_attempts` times before the whole job fails (no partial commit).
//!
//! The pure placement/decode/aggregation logic this module drives lives in
//! `sqe_compaction::dispatch`, which has no Flight/tonic dependency and is
//! unit-tested there. This module is the network-facing half: the
//! `WorkerRegistry`/`WorkerLoadTracker` bookkeeping, the retry loop, and the
//! `do_action` RPC itself, none of which are practical to unit test without
//! a live worker (see the `#[ignore]`d integration test at the bottom).
//!
//! [`dispatch_and_collect_groups`] is called by
//! `maintenance::rewrite_data_files_distributed_once`, itself reachable from
//! `MaintenanceHandler::handle()` and the active-mode scheduler once
//! `distribution.mode` resolves to `Distributed` (Phase 4c Task 5).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use iceberg::spec::DataFile;
use sqe_compaction::wire::{CompactGroupRequest, CompactGroupResponse, S3Conn, SortSpecWire};
use sqe_core::{Result as SqeResult, SqeError};
use tracing::warn;

use crate::channel_pool::ChannelPool;
use crate::worker_registry::{WorkerLoadTracker, WorkerRegistry};

/// Metadata header carrying the shared coordinator/worker secret. Same name
/// as `distributed_scan`'s and `credential_refresh`'s constants (not
/// reused directly: each dispatch site keeps its own copy, matching the
/// existing repo convention rather than introducing a new shared-constants
/// module for a single string).
const WORKER_SECRET_HEADER: &str = "x-sqe-worker-secret";

/// Metadata header carrying the HMAC-SHA256 tag (hex) over the exact
/// `CompactGroupRequest` wire bytes. Must match
/// `sqe_worker::flight_service::COMPACT_SIGNATURE_HEADER` exactly, or every
/// signed request fails the worker's `verify_compaction_signature` check.
pub(crate) const COMPACT_SIGNATURE_HEADER: &str = "x-sqe-compact-signature";

/// Compute the HMAC signature for a `CompactGroupRequest`'s wire bytes, or
/// `None` when `secret` is empty (dev mode: the deployment opted into
/// `worker.allow_unauthenticated`, so there is nothing to sign and the
/// header is omitted entirely). Delegates the HMAC computation itself to
/// `sqe_compaction::wire::sign` so the coordinator and worker share exactly
/// one signing implementation; only the empty-secret bypass convention is
/// mirrored from `distributed_scan::sign_ticket`.
pub(crate) fn sign_compact_request(secret: &str, bytes: &[u8]) -> Option<String> {
    if secret.is_empty() {
        return None;
    }
    Some(sqe_compaction::wire::sign(bytes, secret))
}

/// One file group awaiting (or retrying) dispatch. Carries just what the
/// dispatch loop needs -- paths and total size -- not full `DataFile`s,
/// since those are already held by the caller (used to build `old_files`
/// for the commit) and would be redundant to clone into every in-flight
/// attempt.
#[derive(Debug, Clone)]
struct PendingGroup {
    group_id: u32,
    file_paths: Vec<String>,
    total_bytes: u64,
    attempts_left: usize,
    excluded: HashSet<String>,
}

/// True when `excluded` already contains every currently-healthy worker
/// URL, meaning a group carrying this exclusion set can never be placed
/// again without the healthy set changing -- which nothing in this dispatch
/// loop can make happen for an application-level failure (see
/// `DispatchError::is_transport`), since `excluded` only ever grows.
/// `excluded.is_empty()` (a group that has never failed) is never "stuck".
///
/// This is the stall guard for [`dispatch_and_collect_groups`]'s dispatch
/// loop: without it, a group that has exhausted every healthy worker but
/// still has `attempts_left > 0` would spin the loop's 200ms backoff
/// forever instead of failing the job.
fn is_permanently_stuck(excluded: &HashSet<String>, healthy: &[String]) -> bool {
    !excluded.is_empty() && healthy.iter().all(|url| excluded.contains(url))
}

/// Build the `CompactGroupRequest` for one group. `group_id` doubles as the
/// group's index into the caller's original group list, so the caller can
/// zip responses back to their input `DataFile`s by id without needing the
/// dispatch loop to round-trip them.
#[allow(clippy::too_many_arguments)]
fn build_compact_request(
    job_id: &str,
    group_id: u32,
    table_ident: &str,
    metadata_location: &str,
    snapshot_id: i64,
    group_file_paths: Vec<String>,
    target_file_size_bytes: u64,
    compression: &str,
    sort: Option<&SortSpecWire>,
    s3: &S3Conn,
) -> CompactGroupRequest {
    CompactGroupRequest {
        job_id: job_id.to_string(),
        group_id,
        table_ident: table_ident.to_string(),
        metadata_location: metadata_location.to_string(),
        snapshot_id,
        group_file_paths,
        target_file_size_bytes,
        compression: compression.to_string(),
        sort: sort.cloned(),
        s3: s3.clone(),
    }
}

/// Dispatch one signed `CompactGroupRequest` to `worker_url` via
/// `do_action("compact_file_group")` and collect the terminal `Done` frame.
///
/// `heartbeat_timeout` bounds the wait for each individual frame (so a
/// worker that goes silent mid-stream is detected before the full
/// `group_timeout`); `group_timeout` bounds the whole call end to end.
/// Mirrors `distributed_scan::dispatch_to_worker`'s pool-invalidation
/// behavior: a channel that errors with `Unavailable`/`DeadlineExceeded` is
/// evicted from the pool so the next attempt reconnects fresh.
///
/// Classifies a failed dispatch so the retry loop can tell a
/// transport-level fault (connection refused, deadline exceeded, a worker
/// that goes silent mid-stream) from an application-level failure the
/// worker's `compact_file_group` handler returned deliberately -- e.g. the
/// resurrection guard, a delete-accounting mismatch, or a bad signature.
/// Only the former is evidence the WORKER is unhealthy; the latter is
/// evidence about the GROUP (or the request), and fails identically on any
/// other worker too, so [`dispatch_and_collect_groups`] must not mark the
/// worker unhealthy for it -- doing so would let one poison group take a
/// perfectly healthy worker out of the fleet for every other job.
#[derive(Debug)]
struct DispatchError {
    message: String,
    /// `true` when the failure indicates the WORKER (connection, timeout)
    /// rather than the GROUP/request is at fault.
    is_transport: bool,
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl DispatchError {
    fn transport(message: String) -> Self {
        Self {
            message,
            is_transport: true,
        }
    }

    fn application(message: String) -> Self {
        Self {
            message,
            is_transport: false,
        }
    }
}

async fn dispatch_group_to_worker(
    request: &CompactGroupRequest,
    worker_url: &str,
    pool: &ChannelPool,
    worker_secret: &str,
    group_timeout: Duration,
    heartbeat_timeout: Duration,
) -> Result<CompactGroupResponse, DispatchError> {
    let body = request.to_bytes().map_err(|e| {
        DispatchError::application(format!("failed to encode CompactGroupRequest: {e}"))
    })?;

    let channel = pool.get(worker_url).await.map_err(|e| {
        pool.invalidate(worker_url);
        DispatchError::transport(format!("failed to connect to worker {worker_url}: {e}"))
    })?;
    let mut client = FlightServiceClient::new(channel);

    // Sign the exact wire bytes before they are moved into the Action body,
    // mirroring the scan-ticket signing convention (#206) applied to this
    // RPC in Task 2/3.
    let signature = sign_compact_request(worker_secret, &body);

    let action = Action {
        r#type: "compact_file_group".to_string(),
        body: bytes::Bytes::from(body),
    };
    let mut grpc_request = tonic::Request::new(action);
    if !worker_secret.is_empty() {
        let secret_value = worker_secret.parse().map_err(|e| {
            DispatchError::application(format!(
                "worker_secret cannot be encoded as a metadata header value: {e}"
            ))
        })?;
        grpc_request
            .metadata_mut()
            .insert(WORKER_SECRET_HEADER, secret_value);
        if let Some(sig) = signature {
            let sig_value = sig.parse().map_err(|e| {
                DispatchError::application(format!(
                    "compaction signature cannot be encoded as a metadata header value: {e}"
                ))
            })?;
            grpc_request
                .metadata_mut()
                .insert(COMPACT_SIGNATURE_HEADER, sig_value);
        }
    }

    // `do_action` itself returns as soon as the worker accepts the request
    // and spawns the rewrite (see `sqe_worker::flight_service`'s
    // `compact_file_group` arm): the rewrite runs concurrently with the
    // response stream below, which is what lets `Progress` frames arrive
    // mid-compute rather than only once the whole group is done.
    // `group_timeout` still wraps this call as the end-to-end bound in case
    // the worker never even accepts the request (auth/decode failure before
    // any stream is returned).
    let response = tokio::time::timeout(group_timeout, client.do_action(grpc_request))
        .await
        .map_err(|_| {
            pool.invalidate(worker_url);
            DispatchError::transport(format!(
                "worker {worker_url} compact_file_group exceeded the {}s group timeout",
                group_timeout.as_secs()
            ))
        })?
        .map_err(|e| {
            let transport = matches!(
                e.code(),
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
            );
            if transport {
                pool.invalidate(worker_url);
            }
            let message = format!("worker {worker_url} compact_file_group failed: {e}");
            if transport {
                DispatchError::transport(message)
            } else {
                // e.g. Status::internal (resurrection guard, delete-accounting
                // mismatch, snapshot-pin mismatch) or Status::unauthenticated
                // (bad secret/signature): a request/group problem, not a
                // worker-health problem.
                DispatchError::application(message)
            }
        })?;

    let mut stream = response.into_inner();
    let mut done: Option<CompactGroupResponse> = None;
    loop {
        // Bounds the wait for each individual frame. The worker emits a
        // `Progress` frame every `PROGRESS_INTERVAL_BATCHES` record batches
        // it processes during the delete-applying read + rolling write
        // (`sqe_worker::compaction`), so as long as it is making forward
        // progress a fresh frame arrives well inside `heartbeat_timeout` and
        // this `timeout(..).await` is re-armed on every loop iteration --
        // i.e. every real frame resets the window. A worker that stalls
        // mid-compute (wedged read, hung write, deadlock) stops producing
        // frames entirely and is caught here, not just at the coarser
        // `group_timeout` end-to-end bound.
        let next = tokio::time::timeout(heartbeat_timeout, stream.message())
            .await
            .map_err(|_| {
                DispatchError::transport(format!(
                    "worker {worker_url} compact_file_group produced no frame within the \
                     {}s heartbeat timeout",
                    heartbeat_timeout.as_secs()
                ))
            })?
            .map_err(|e| {
                // A mid-stream gRPC error can now legitimately be either
                // class: an application-level failure (resurrection guard,
                // delete-accounting mismatch, snapshot-pin mismatch) surfaces
                // here too once the worker streams concurrently with the
                // rewrite, not only as the initial `do_action` call's `Err`
                // (see the classification above). Apply the identical
                // code-based split so a poison group does not incorrectly
                // mark a healthy worker unhealthy.
                let transport = matches!(
                    e.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                );
                if transport {
                    pool.invalidate(worker_url);
                }
                let message = format!("worker {worker_url} compact_file_group stream error: {e}");
                if transport {
                    DispatchError::transport(message)
                } else {
                    DispatchError::application(message)
                }
            })?;
        let Some(result) = next else { break };
        let frame = sqe_compaction::wire::CompactGroupFrame::from_bytes(&result.body)
            .map_err(|e| {
                DispatchError::application(format!(
                    "worker {worker_url}: failed to decode CompactGroupFrame: {e}"
                ))
            })?;
        match frame {
            sqe_compaction::wire::CompactGroupFrame::Progress { .. } => {}
            sqe_compaction::wire::CompactGroupFrame::Done(resp) => done = Some(resp),
        }
    }

    done.ok_or_else(|| {
        // The stream ended (transport-level EOF) without ever delivering a
        // Done frame: the connection dropped mid-transfer, which is a
        // worker/connection signal, not a group signal.
        DispatchError::transport(format!(
            "worker {worker_url} closed the compact_file_group stream without a Done frame"
        ))
    })
}

/// Dispatch every group of a distributed rewrite job to the worker fleet,
/// retrying a failed group on a different healthy worker up to
/// `group_attempts` times, and return every group's [`CompactGroupResponse`]
/// once all have succeeded.
///
/// Uses a CONTINUOUS scheduler, not a wave: every healthy worker is kept
/// filled up to `max_inflight_per_worker` at all times, and as soon as any
/// in-flight group resolves, the freed slot is refilled immediately from the
/// pending queue (largest group first) rather than waiting for every other
/// group dispatched alongside it to also finish. This is what
/// `sqe_compaction::dispatch::next_group_assignment` decides one assignment
/// at a time; see its doc comment for the exact priority rules (largest
/// pending group first, ties broken by input order, a group whose only
/// eligible workers are excluded or at cap is skipped in favor of the next
/// one that fits). The retry/exclusion/stall-guard/classification semantics
/// below are identical to the previous wave-based scheduler; only the
/// wall-clock scheduling changed.
///
/// On a group exhausting its retries, returns `Err` immediately: per the
/// design, a distributed rewrite either commits everything or nothing, so
/// there is no point continuing to dispatch remaining groups once one has
/// permanently failed (any group already in flight -- including, in this
/// continuous scheduler, groups dispatched well before the failing one --
/// is simply dropped; whatever it already wrote to object storage becomes
/// an orphan reclaimed by the age-thresholded orphan sweep, not cleaned up
/// here). The same applies when the stall guard trips.
///
/// `groups` are the coordinator's own bin-packed file groups (as produced by
/// `pack_file_groups_partition_aware`/`group_files_by_partition`); this
/// function only needs their paths and total size, not the full `DataFile`s
/// (the caller already holds those for the `old_files` side of the commit).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn dispatch_and_collect_groups(
    job_id: &str,
    table_ident: &str,
    metadata_location: &str,
    snapshot_id: i64,
    groups: &[Vec<DataFile>],
    target_file_size_bytes: u64,
    compression: &str,
    sort: Option<&SortSpecWire>,
    s3: &S3Conn,
    registry: &Arc<WorkerRegistry>,
    load_tracker: &WorkerLoadTracker,
    worker_secret: &str,
    max_inflight_per_worker: usize,
    group_attempts: usize,
    group_timeout: Duration,
    heartbeat_timeout: Duration,
) -> SqeResult<Vec<CompactGroupResponse>> {
    let mut pending: Vec<PendingGroup> = groups
        .iter()
        .enumerate()
        .map(|(i, files)| PendingGroup {
            group_id: i as u32,
            file_paths: files.iter().map(|f| f.file_path().to_string()).collect(),
            total_bytes: files.iter().map(|f| f.file_size_in_bytes()).sum(),
            attempts_left: group_attempts.max(1),
            excluded: HashSet::new(),
        })
        .collect();

    let mut responses: Vec<CompactGroupResponse> = Vec::with_capacity(pending.len());
    let pool = registry.channel_pool();
    let mut futs = FuturesUnordered::new();

    while !pending.is_empty() || !futs.is_empty() {
        // Refill pass: keep assigning pending groups to free worker slots
        // until either nothing is left to assign, or nothing more fits
        // right now (every healthy worker at cap, or every remaining group
        // excluded from every healthy worker). This runs every time we get
        // here -- including immediately after a single in-flight group
        // resolves below -- which is the crux of the continuous scheduler:
        // a worker that just freed up is refilled on the spot instead of
        // waiting for a whole wave of sibling groups to also finish.
        loop {
            if pending.is_empty() {
                break;
            }
            let healthy = registry.healthy_workers().await;
            if healthy.is_empty() {
                break;
            }

            // Stall guard: a retried group's `excluded` set only grows, and
            // application-level failures (see `DispatchError::is_transport`)
            // deliberately do NOT shrink `healthy`. If a group has already
            // failed on every worker currently healthy, no future pass can
            // ever place it -- waiting would spin the 200ms backoff below
            // forever. Fail the job now instead. (Any other groups still
            // in-flight in `futs` at this point are dropped, same as the
            // retry-exhaustion path below; see the module doc.)
            if let Some(stuck) = pending
                .iter()
                .find(|g| is_permanently_stuck(&g.excluded, &healthy))
            {
                return Err(SqeError::Execution(format!(
                    "distributed compaction job {job_id}: group {} has failed on every currently \
                     healthy worker ({:?}); no worker left to retry on",
                    stuck.group_id, stuck.excluded
                )));
            }

            let loads: Vec<sqe_compaction::dispatch::WorkerLoad> = healthy
                .iter()
                .map(|url| sqe_compaction::dispatch::WorkerLoad {
                    url: url.clone(),
                    in_flight: load_tracker.in_flight(url) as usize,
                })
                .collect();
            let slots: Vec<sqe_compaction::dispatch::PendingSlot> = pending
                .iter()
                .map(|g| sqe_compaction::dispatch::PendingSlot {
                    total_bytes: g.total_bytes,
                    excluded: g.excluded.clone(),
                })
                .collect();

            let Some((idx, worker_url)) = sqe_compaction::dispatch::next_group_assignment(
                &slots,
                &loads,
                max_inflight_per_worker,
            ) else {
                break;
            };

            let group = pending.remove(idx);
            let guard = load_tracker.reserve(&worker_url);
            let request = build_compact_request(
                job_id,
                group.group_id,
                table_ident,
                metadata_location,
                snapshot_id,
                group.file_paths.clone(),
                target_file_size_bytes,
                compression,
                sort,
                s3,
            );
            let pool = pool.clone();
            let secret = worker_secret.to_string();
            futs.push(async move {
                let _guard = guard;
                let result = dispatch_group_to_worker(
                    &request,
                    &worker_url,
                    &pool,
                    &secret,
                    group_timeout,
                    heartbeat_timeout,
                )
                .await;
                (group, worker_url, result)
            });
        }

        if futs.is_empty() {
            // Nothing in flight, and the refill pass above placed nothing:
            // either there are no healthy workers at all, or pending is
            // empty (loop condition would already be false) and we should
            // not be here. Re-check healthy explicitly for the error
            // message; a transient empty healthy set that recovers is
            // handled by the backoff-and-retry below.
            let healthy = registry.healthy_workers().await;
            if healthy.is_empty() {
                return Err(SqeError::Execution(format!(
                    "distributed compaction job {job_id}: no healthy workers available; \
                     {} group(s) undispatched",
                    pending.len()
                )));
            }
            // Every healthy worker is at capacity; back off briefly rather
            // than busy-loop.
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }

        // Wait for exactly ONE in-flight group to resolve, then loop back
        // to the refill pass above to fill the slot it just freed --
        // instead of draining the rest of `futs` first (the old wave
        // behavior).
        let (mut group, worker_url, result) =
            futs.next().await.expect("futs is non-empty, checked above");

        match result {
            Ok(resp) => responses.push(resp),
            Err(e) => {
                warn!(
                    job_id,
                    group_id = group.group_id,
                    worker = %worker_url,
                    attempts_left = group.attempts_left,
                    transport = e.is_transport,
                    error = %e,
                    "distributed compaction: group dispatch failed"
                );
                // Only a transport-class failure (connection, timeout,
                // a worker that goes silent) is evidence the WORKER is
                // unhealthy; drop it immediately, matching
                // distributed_scan's fragment-failover behavior. An
                // application-level failure (resurrection guard,
                // delete-accounting mismatch, bad signature) is
                // evidence about the GROUP/request, fails identically
                // on any other worker, and must not take a healthy
                // worker out of the fleet for every other job.
                if e.is_transport {
                    registry.mark_unhealthy(&worker_url).await;
                }
                group.excluded.insert(worker_url.clone());
                group.attempts_left = group.attempts_left.saturating_sub(1);
                if group.attempts_left == 0 {
                    return Err(SqeError::Execution(format!(
                        "distributed compaction job {job_id}: group {} exhausted retries \
                         (last failure on {worker_url}): {e}",
                        group.group_id
                    )));
                }
                pending.push(group);
            }
        }
    }

    Ok(responses)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_s3() -> S3Conn {
        S3Conn {
            endpoint: "http://localhost:9000".to_string(),
            region: "us-east-1".to_string(),
            access_key: "ak".to_string(),
            secret_key: "sk".to_string(),
            session_token: String::new(),
            path_style: true,
            allow_http: true,
        }
    }

    // ---- is_permanently_stuck (stall guard) --------------------------------

    #[test]
    fn a_group_that_never_failed_is_never_stuck() {
        let healthy = vec!["w1".to_string(), "w2".to_string()];
        assert!(!is_permanently_stuck(&HashSet::new(), &healthy));
    }

    #[test]
    fn a_group_excluded_from_every_healthy_worker_is_stuck() {
        let healthy = vec!["w1".to_string(), "w2".to_string()];
        let mut excluded = HashSet::new();
        excluded.insert("w1".to_string());
        excluded.insert("w2".to_string());
        assert!(is_permanently_stuck(&excluded, &healthy));
    }

    #[test]
    fn a_group_excluded_from_only_some_healthy_workers_is_not_stuck() {
        let healthy = vec!["w1".to_string(), "w2".to_string(), "w3".to_string()];
        let mut excluded = HashSet::new();
        excluded.insert("w1".to_string());
        assert!(!is_permanently_stuck(&excluded, &healthy));
    }

    #[test]
    fn a_group_excluded_from_a_now_shrunk_healthy_set_is_stuck() {
        // Simulates: w1 and w2 both failed this group (transport errors,
        // both marked unhealthy and excluded); only w3 remains healthy and
        // has not been excluded yet -> not stuck.
        let healthy = vec!["w3".to_string()];
        let mut excluded = HashSet::new();
        excluded.insert("w1".to_string());
        excluded.insert("w2".to_string());
        assert!(!is_permanently_stuck(&excluded, &healthy));

        // Once w3 also fails and gets excluded, with no other healthy
        // worker left, the group is stuck.
        excluded.insert("w3".to_string());
        assert!(is_permanently_stuck(&excluded, &healthy));
    }

    // ---- DispatchError classification --------------------------------------

    #[test]
    fn dispatch_error_constructors_tag_the_right_category() {
        let transport = DispatchError::transport("connection refused".to_string());
        assert!(transport.is_transport);
        assert_eq!(transport.to_string(), "connection refused");

        let application = DispatchError::application("resurrection guard tripped".to_string());
        assert!(!application.is_transport);
        assert_eq!(application.to_string(), "resurrection guard tripped");
    }

    // ---- build_compact_request -------------------------------------------

    #[test]
    fn build_compact_request_carries_every_field_through() {
        let sort = SortSpecWire::Columns(vec![("a".to_string(), true)]);
        let req = build_compact_request(
            "job-1",
            3,
            "catalog.ns.tbl",
            "s3://bucket/warehouse/tbl/metadata/v3.metadata.json",
            42,
            vec!["s3://bucket/data/f1.parquet".to_string()],
            128 * 1024 * 1024,
            "zstd",
            Some(&sort),
            &sample_s3(),
        );
        assert_eq!(req.job_id, "job-1");
        assert_eq!(req.group_id, 3);
        assert_eq!(req.table_ident, "catalog.ns.tbl");
        assert_eq!(req.snapshot_id, 42);
        assert_eq!(req.group_file_paths, vec!["s3://bucket/data/f1.parquet".to_string()]);
        assert_eq!(req.compression, "zstd");
        assert_eq!(req.sort, Some(sort));
    }

    // ---- signing + the worker's expected header name ----------------------

    /// Pins the exact header name the coordinator must send: it must match
    /// `sqe_worker::flight_service::COMPACT_SIGNATURE_HEADER` byte for byte,
    /// or every signed request is rejected by `verify_compaction_signature`.
    #[test]
    fn compact_signature_header_matches_the_workers_expected_name() {
        assert_eq!(COMPACT_SIGNATURE_HEADER, "x-sqe-compact-signature");
    }

    #[test]
    fn signed_request_verifies_against_the_shared_secret_and_rejects_a_wrong_one() {
        let req = build_compact_request(
            "job-1",
            0,
            "catalog.ns.tbl",
            "s3://bucket/meta.json",
            1,
            vec!["s3://bucket/f1.parquet".to_string()],
            1024,
            "zstd",
            None,
            &sample_s3(),
        );
        let bytes = req.to_bytes().unwrap();
        let secret = "shared-worker-secret";
        let sig = sign_compact_request(secret, &bytes).expect("non-empty secret must sign");

        assert!(sqe_compaction::wire::verify(&bytes, &sig, secret));
        assert!(!sqe_compaction::wire::verify(&bytes, &sig, "wrong-secret"));
    }

    #[test]
    fn sign_compact_request_omits_signature_in_dev_mode_with_empty_secret() {
        let bytes = b"whatever-body".to_vec();
        assert_eq!(sign_compact_request("", &bytes), None);
    }

    #[test]
    fn a_tampered_body_fails_verification() {
        let req = build_compact_request(
            "job-1",
            0,
            "catalog.ns.tbl",
            "s3://bucket/meta.json",
            1,
            vec!["s3://bucket/f1.parquet".to_string()],
            1024,
            "zstd",
            None,
            &sample_s3(),
        );
        let mut bytes = req.to_bytes().unwrap();
        let secret = "shared-worker-secret";
        let sig = sign_compact_request(secret, &bytes).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        assert!(!sqe_compaction::wire::verify(&bytes, &sig, secret));
    }

    // ---- full coordinator+worker Flight integration -----------------------
    //
    // Not runnable in this environment: needs a live worker Flight service
    // (and, transitively, S3-compatible storage + a real Iceberg table for
    // the worker's `compact_file_group` action to operate against). Manual
    // run against the docker quickstart stack:
    //
    // 1. Start a worker: `cargo run -p sqe-worker -- --port 50061 ...`
    // 2. Register it in a `WorkerRegistry` and mark it healthy.
    // 3. Call `dispatch_and_collect_groups` with real groups from a live
    //    table's `collect_live_data_files`, a real `metadata_location`,
    //    S3 credentials for the same bucket, and the shared worker secret.
    // 4. Assert every group's `CompactGroupResponse` decodes via
    //    `sqe_compaction::dispatch::decode_group_response` against the same
    //    table's schema/partition type/spec id/format version.
    #[tokio::test]
    #[ignore = "requires a live worker Flight service; run manually against the quickstart stack"]
    async fn dispatch_and_collect_groups_against_a_live_worker() {
        // Intentionally left as documentation: see the comment above for the
        // manual procedure. A synthetic in-process FlightService could
        // replace this once one exists for worker-side testing; today
        // `sqe-worker`'s own tests spin up a real tonic server per test
        // (see `flight_service.rs`'s `compact_file_group_*` tests), which
        // this module cannot cheaply reuse without a shared test harness.
    }
}
