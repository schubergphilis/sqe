# 601: One symmetric worker secret authenticates every direction, so a compromised worker can harvest all users' S3 credentials

- Scope: sqe-worker (flight_service.rs, heartbeat.rs), sqe-coordinator (flight_sql.rs heartbeat handler, worker_registry.rs, distributed_scan.rs)
- Severity: high
- Category: security
- Effort: M (days)
- Status: open
- Issue: #608

## Problem

The same string, `worker.worker_secret`, is used for four different trust
decisions:

1. Worker authenticates the coordinator on `do_get`, `refresh_credentials`,
   `compact_file_group`, `do_exchange` via a plaintext header:
   `crates/sqe-worker/src/flight_service.rs:490-507` (`verify_worker_secret`).
2. Worker verifies the scan-ticket HMAC with the same key:
   `flight_service.rs:539-541`
   (`Hmac<Sha256>::new_from_slice(self.worker_secret.as_bytes())`).
3. Coordinator authenticates workers on `heartbeat` with the same header:
   `crates/sqe-coordinator/src/flight_sql.rs:2333-2347`.
4. The heartbeat body is a free-form URL and the registry stores whatever it
   receives as long as it is routable:
   `crates/sqe-worker/src/heartbeat.rs:79-82` and
   `crates/sqe-coordinator/src/worker_registry.rs:285-321`
   (`inner.workers.entry(url.to_string()).or_insert_with(...)`).

Consequences that follow directly:

- Any process that knows the secret (every worker pod, every operator with
  read access to any worker's `sqe.toml` or Secret) can heartbeat an
  attacker-controlled URL. The coordinator then dispatches scan tickets there.
  Each ticket carries `s3_access_key`, `s3_secret_key`, `s3_session_token`
  for the querying user (`crates/sqe-planner/src/scan_task.rs`, fields
  listed at 84+). One compromised worker becomes a credential harvester for
  every user of the cluster.
- The HMAC over the ticket (issue #206) adds no authentication beyond the
  header, because the signing key is the same secret that travels in the
  header of every request. Whoever can present the header can also sign. The
  comment at `flight_service.rs:190-194` claims the tag proves "the coordinator
  authored" the task; it proves only that a secret holder did.
- Nothing binds a ticket to a specific worker (no audience/worker URL inside
  the signed bytes) and nothing binds a heartbeat to the peer that sent it, so
  tickets and registrations are freely replayable across nodes.

## Why it matters

The stated auth model is "no service account, every query runs as the
authenticated user". That property holds only if worker compromise is
contained. Today worker compromise equals coordinator compromise for
credential purposes, and the blast radius is every user's short-lived S3
credentials, not one pod's.

## Proposed change

Split identities and make the registry trust the transport, not a string:

1. `coordinator.ticket_signing_key` (coordinator-only, never on workers'
   config for signing) and `worker.shared_secret` (heartbeat auth). Workers
   hold the signing key's verification counterpart. With HMAC that still means
   a shared key, so prefer Ed25519: coordinator holds the private key, workers
   the public key. `sqe-compaction::wire::sign/verify` already centralises the
   primitive; extend it rather than adding a third call site.
2. Put the target worker URL and an expiry into the signed ticket bytes
   (`ScanTask` gets `audience: String`, `expires_at: i64`); the worker refuses
   tickets whose audience is not its own advertise URL or that are older than
   `worker.ticket_ttl_secs`.
3. Bind heartbeats to the peer: when mTLS is on, take the worker identity from
   the client certificate SAN and require the advertised URL host to match; when
   mTLS is off, reject a heartbeat whose body host differs from the TCP peer
   address unless `coordinator.allow_unverified_advertise = true`.
4. Log an audit event (`AuditKind::Maintenance` or a new `Cluster` kind) on
   "Discovered new worker via heartbeat" so registry changes are visible.

## Acceptance criteria

- A test in `sqe-coordinator` sends a heartbeat with a valid secret from
  127.0.0.1 advertising `http://10.9.9.9:50052`; with default config the
  registry rejects it and emits an audit event.
- A test in `sqe-worker` presents a ticket signed for audience `w1` to a
  worker configured as `w2` and gets `UNAUTHENTICATED`.
- A worker binary built with only the public verification key cannot produce
  a ticket the coordinator's test harness accepts.
- `docs/site/book` security chapter documents the two-key model.

## Related

- Issue 603 (TLS is server-side only, so the header secret is also sniffable).
- Issue 609 (heartbeat protocol content).
- July 2026 production-readiness review, "signed worker tickets" was listed as a
  strength; the signature exists but is keyed identically to the bearer header.
