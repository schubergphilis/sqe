# 401: Every distributed scan ticket carries the coordinator's static S3 key pair; workers never see a user identity

- Scope: sqe-coordinator, read side (query_handler.rs try_distribute, distributed_scan.rs, codec.rs) with sqe-planner ScanTask and sqe-worker executor.rs as consumers
- Severity: high
- Category: security
- Effort: L (week+)
- Status: open

## Problem

CLAUDE.md states the auth model as "No service account -- every query runs as the
authenticated user via OIDC password grant -> bearer token passthrough to Polaris/S3" and the
worker as receiving "secured plan fragments + user bearer tokens". The distributed read path
does neither.

`try_distribute` builds one `ScanTask` per morsel and fills the storage fields from the
process-wide `[storage]` config, never from the session or from Polaris-vended credentials:

- `crates/sqe-coordinator/src/query_handler.rs:4007-4012` and `:4035-4040`
  ```rust
  s3_access_key: storage.s3_access_key.clone(),
  s3_secret_key: storage.s3_secret_key.expose().to_string(),
  s3_session_token: String::new(),
  ```

The plain-text key pair is serialized into the Flight ticket
(`crates/sqe-planner` `ScanTask` fields `s3_access_key`, `s3_secret_key`, `s3_session_token`
are plain `String`s with `Serialize`) and shipped on every `do_get`
(`crates/sqe-coordinator/src/distributed_scan.rs:982-1030`). The HMAC signature protects
integrity, not confidentiality; without `[coordinator.tls]` the secret crosses the network in
clear, and `main.rs:133-135` only warns when TLS is off.

The worker uses exactly those fields to build its object store
(`crates/sqe-worker/src/executor.rs:1314-1323`). No bearer token, subject or username reaches
the worker at all. `s3_session_token` is hard-coded empty, so an STS/SSO deployment cannot
distribute (see memory note "S3 session token gap").

`SqePhysicalCodec::try_encode` (`crates/sqe-coordinator/src/codec.rs:41-70`) additionally
JSON-encodes the whole `ScanTask` vector, secrets included, into the DataFusion proto buffer.
The codec has no production caller (`grep -rn SqePhysicalCodec crates/` only hits its own
tests) but it is a ready-made leak if anyone wires it.

## Why it matters

- The per-user security model stops at the coordinator. Object storage sees one engine
  identity for every distributed read, so bucket policies, S3 access logs and CloudTrail cannot
  attribute reads to users. Polaris credential vending (`table-data-read`) is not the
  data-path gate for any distributed query, and a `require_vended_credentials` mode (proposed
  in the 2026-07-25 review, P0-5) cannot be honoured on the worker path as currently shaped.
- A long-lived static key pair is copied into every ticket, every worker's memory, and any
  debug dump of a `ScanTask`. One compromised worker or one captured ticket yields warehouse-wide
  read access indefinitely, with no expiry or scope.
- STS/SSO credentials (session token) cannot be used at all, which blocks the memory-noted
  Glue/SSO deployments from ever going distributed.

## Proposed change

1. Replace the three credential strings on `ScanTask` with a `credentials: ScanCredentials`
   enum: `Vended { access_key, secret_key, session_token, expiry }` (from Polaris
   `load_table` config, already available on `SessionCatalog`), `WorkerDefault` (worker
   resolves from its own env/IRSA chain), or `UserBearer { token }` for remote-signing
   catalogs. Drop the static fallback from `try_distribute`; make `WorkerDefault` the
   explicit single-tenant opt-in.
2. Thread the vended credential expiry into `DistributedScanExec::with_credential_expiry`
   so `CredentialRefreshTracker` (`credential_refresh.rs`) refreshes real vended tokens
   instead of a never-set value.
3. Wrap the credential fields in `sqe_core::SecretString` so `Debug`/`Serialize` redact by
   default, and delete `codec.rs` or gate it behind a feature until it has a caller.
4. Add `sqe.credential_source` to the audit `QueryStats` so operators can see which mode a
   query ran under.

## Acceptance criteria

- `make test-distributed` passes with `[storage] s3_access_key` and `s3_secret_key` unset and
  Polaris credential vending enabled.
- A unit test asserts that `serde_json::to_string(&scan_task)` and `format!("{scan_task:?}")`
  do not contain the secret.
- An integration test with an STS session token (RustFS supports static session tokens) runs a
  distributed scan.
- `grep -rn 'storage.s3_secret_key' crates/sqe-coordinator/src/query_handler.rs` returns
  nothing.

## Related

- 2026-07-25 review P0-5 (shared storage credentials) still holds; this issue is the
  distributed-path half that P0-5 did not examine.
- Memory notes "S3 session token gap" and "Glue sidecar bridge".
- Issue 408 (scan-offload architecture) decides where the credential decision should live.
