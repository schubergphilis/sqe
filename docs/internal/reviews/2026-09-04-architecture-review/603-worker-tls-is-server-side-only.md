# 603: Worker TLS exists only on the listening side; no client in the cluster is configured to speak it

- Scope: sqe-worker (tls.rs, main.rs, heartbeat.rs, advertise.rs), sqe-coordinator (channel_pool.rs, transport.rs)
- Severity: high
- Category: security
- Effort: S (hours) to M (days)
- Status: fixed (branch `fix/identity-transport-trust-p1`, 2026-09-05)
- Issue: #610

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

## Resolution (2026-09-05)

There is now one TLS builder in the workspace, `crates/sqe-core/src/tls.rs`, and
both client hops use it.

**One builder.** `sqe_core::tls::ServerTlsMaterial::load` reads cert, key and
optional CA once and derives all three shapes: `tonic_server()` for the Flight
listeners, `rustls_server_config()` for the axum listeners (MCP, Quack), and
`sqe_core::tls::tonic_client(&TlsConfig, domain)` for the client side.
`crates/sqe-worker/src/tls.rs` (the hand-copied mirror) is deleted;
`crates/sqe-coordinator/src/tls.rs` is now a re-export plus the axum certificate
reloader. `grep -rn "Identity::from_pem" crates/` returns one production hit
(`sqe_core::tls::tonic_identity`, shared by the server and client paths) and
`grep -rn "fn build_server_tls_config" crates/` returns none.

**Coordinator to worker.** `ChannelPool` carries a `ClientTlsConfig` built once
from `[coordinator.tls]` (`ChannelPool::shared_with_timeouts_and_tls`, wired in
both `src/main.rs` and `src/bin/sqe_server.rs`). `ChannelPool::get` attaches it
whenever the target URL scheme is `https`, with the URL host as the SNI domain.
A bad `ca_file` now fails at coordinator boot instead of at first dispatch.

**Worker to coordinator.** `heartbeat::start_heartbeat_task` takes an
`Option<ClientTlsConfig>`, built in `bootstrap.rs` from the worker's effective
TLS block, and `send_heartbeat` applies it for an `https://` coordinator URL.

**Trust roots.** The workspace `tonic` dependency gains `tls-native-roots`, and
`sqe-core` exposes it as a default-on feature so `tonic_client` falls back to
the platform store when `ca_file` is empty. A private cluster CA must still be
named in `ca_file`; that is documented in the config reference.

**Worker identity.** `[worker.tls]` is a new optional `TlsConfig` on
`WorkerConfig`. `WorkerConfig::effective_tls` returns it when it carries a
cert/key and falls back to `[coordinator.tls]` otherwise, so the single-cert dev
setup is unchanged. The worker Flight listener, the heartbeat client and the
advertise-scheme derivation all read the effective block. Under TLS,
`build_advertise_url` refuses a bare-IP advertise host (derived or explicit,
IPv4 or IPv6) unless `worker.tls.allow_ip_advertise = true`, mirroring the
existing routable-address check and failing at worker boot.

**Boot validation.** `SqeConfig::validate` calls
`sqe_core::tls::https_peers_need_trust_root`, which refuses `coordinator.tls`
enabled plus `https://` entries in `coordinator.worker_urls` when no trust root
is available, with an error naming `ca_file` and listing the offending URLs. It
also validates the shape of `[worker.tls]` the same way as `[coordinator.tls]`.

Tests (all green): `sqe-core` `tls::tests::*` (16) including
`tls_server_config_floor_is_tls12`, `tls_ca_file_requires_a_client_certificate`,
`tls_client_config_built_for_https_with_ca_file`,
`tls_client_config_attaches_our_identity_for_mtls`,
`tls_validate_rejects_https_worker_urls_without_ca`; `sqe-core`
`config::tests::test_worker_tls_*` (4); `sqe-coordinator`
`channel_pool::tests::channel_pool_https_target_is_not_refused_for_missing_tls`
plus three more; `sqe-worker` `advertise::tests::tls_*` (4) and
`heartbeat::tests::heartbeat_to_https_coordinator_is_not_refused_for_missing_tls`,
`heartbeat::tests::heartbeat_uses_the_supplied_client_tls`.

Remaining items, deliberately not done here:

- **No TLS variant of `make test-distributed`.** The acceptance criterion asked
  for one; it is out of scope for this branch. What it needs is written up in
  the branch report: a self-signed CA plus per-node leaf certs generated into
  the compose context, hostname-addressable service names so the advertise-URL
  hostname rule is satisfiable, `ca_file` on every node, and the TPC-H q1 assert
  reusing the existing distributed harness.
- **The boot check does not fire in the default build.** Because
  `tls-native-roots` is on by default, "a trust root is available" is always
  true, so `https://` workers with a private CA and no `ca_file` still fail at
  handshake time rather than at boot. Making that case a boot error means
  distinguishing "public CA" from "private CA", which the config cannot express
  today. The check does fire for a `--no-default-features` sqe-core build.
- **Workers still reuse one certificate unless `[worker.tls]` is set.** The
  block exists and is honoured; issuing per-worker certificates is a deployment
  change (Helm cert mounts, tracked with the 800-series review).
