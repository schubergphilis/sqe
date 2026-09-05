# 603: Worker TLS exists only on the listening side; no client in the cluster is configured to speak it

- Scope: sqe-worker (tls.rs, main.rs, heartbeat.rs, advertise.rs), sqe-coordinator (channel_pool.rs, transport.rs)
- Severity: high
- Category: security
- Effort: S (hours) to M (days)
- Status: open

## Problem

Turning on `[coordinator.tls]` makes the worker listen with TLS and advertise
an `https://` URL, but neither peer that must connect to it is given a client
TLS configuration.

- Worker server side: `crates/sqe-worker/src/main.rs:64-73` builds
  `ServerTlsConfig` from `config.coordinator.tls` and
  `crates/sqe-worker/src/advertise.rs:52` switches the advertised scheme to
  `https` when that block is enabled.
- Coordinator to worker: the pool builds channels with
  `Endpoint::new(url.to_string())?` and never calls `tls_config`
  (`crates/sqe-coordinator/src/channel_pool.rs:106`; grep for
  `ClientTlsConfig` across `crates/` hits only `sqe-cli/src/flight.rs:5,46`).
- Worker to coordinator heartbeat: `crates/sqe-worker/src/heartbeat.rs:68-72`
  builds `Endpoint::new(coordinator_url)` with no TLS settings either.

The workspace enables tonic with `features = ["transport", "tls-ring"]`
(`Cargo.toml:115`) and no root-store feature. tonic refuses an `https` URI on
a channel without a `ClientTlsConfig` (its connector returns
"Connecting to HTTPS without TLS enabled"). So with TLS on, the coordinator
cannot dispatch to any worker and the worker cannot heartbeat an `https`
coordinator; with TLS off, `bootstrap.rs:141-148` prints a warning and user
S3 credentials plus the worker secret travel in cleartext on every ticket.
There is no configuration that yields an encrypted, working distributed
cluster. This should be confirmed on the two-worker rig, but the absence of
any client TLS code path is not in doubt.

Secondary issues in the same area:

- `tls.rs:3-8` documents that the builder is a copy of
  `sqe_coordinator::tls::build_server_tls_config` because of a dependency
  cycle. Two copies of certificate loading will drift (for example when
  adding reload or a key-passphrase).
- Workers reuse the coordinator's certificate identity. A single cert with the
  coordinator's SAN cannot validate for `https://10.1.2.3:50052` derived
  in `advertise.rs:70-74` unless the cert carries every worker IP.

## Why it matters

Every scan ticket contains a user's live S3 access key, secret and session
token. The design's core security claim (per-user credentials, no shared
service account) is only as strong as the coordinator-to-worker hop. Today that
hop is plaintext in every deployable configuration.

## Proposed change

1. Add a small `sqe-transport` crate (or a module in `sqe-core` behind a
   `tonic` feature) exposing `build_server_tls(&TlsConfig)` and
   `build_client_tls(&TlsConfig, domain: Option<&str>)`. Delete
   `crates/sqe-worker/src/tls.rs` and the coordinator copy.
2. `ChannelPool::connect` and `heartbeat::send_heartbeat` call
   `Endpoint::tls_config(build_client_tls(...))` whenever the URL scheme is
   `https`. Use `tls.ca_file` as the trust root; when empty, fall back to
   `tls-native-roots`.
3. Add `[worker.tls]` (cert/key for the worker identity, optional) with a
   fallback to `[coordinator.tls]` for single-cert dev setups, and require
   `worker.advertise_url` to be a hostname covered by the cert when TLS is on
   (fail at boot otherwise, mirroring the routable-address check in
   `advertise.rs`).
4. Make `SqeConfig::validate` refuse `tls.enabled && worker_urls contain
   https` unless the client side is configured, so the failure is at boot, not
   at first query.

## Acceptance criteria

- `make test-distributed` gains a TLS variant (self-signed CA, coordinator +
  2 workers) that runs TPC-H q1 end to end.
- A worker started with TLS and a coordinator without a CA fails at
  coordinator boot with a config error naming the missing CA, not at query
  time with a transport error.
- One TLS builder implementation in the workspace
  (`grep -rn "fn build_server_tls_config" crates` returns one hit).

## Related

- Issue 601 (secret in plaintext header depends on this hop being encrypted).
- Helm chart values for `tls` (see 800-series deployment review) must add
  worker cert mounts.
