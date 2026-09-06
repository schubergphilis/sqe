//! Coordinator-side TLS glue.
//!
//! The builder itself lives in [`sqe_core::tls`] and is shared with the worker
//! (finding 210: there used to be three of them, and they disagreed). What is
//! left here is the part only the coordinator has: the axum listeners (MCP,
//! Quack) can hot-swap their certificate, so they get a reloader.
//!
//! The tonic (Flight) listener cannot. tonic 0.14 builds its acceptor once
//! inside `Server::tls_config` and exposes no handle to swap it, so the Flight
//! port serves the certificate it started with until the process restarts.
//! Cover that in Kubernetes with a reloader sidecar; see the rotation note on
//! `sqe_core::config::TlsConfig::cert_file` and
//! `docs/site/book/src/deployment/configuration.md#tls`.

use std::time::{Duration, SystemTime};

use axum_server::tls_rustls::RustlsConfig;
use sqe_core::config::TlsConfig;
use sqe_core::{ServerTlsMaterial, SqeError};

pub use sqe_core::tls::{
    ensure_crypto_provider, host_is_ip_literal, https_peers_need_trust_root, is_https_url,
    sni_domain, tonic_client, ENABLED_PROTOCOL_VERSIONS, MIN_PROTOCOL_VERSION,
    NATIVE_ROOTS_AVAILABLE,
};

/// How often the reloader stats the certificate and key.
///
/// cert-manager renews Kubernetes TLS secrets on a 60-to-90 day cycle and the
/// kubelet projects the new secret into the pod within a minute or two, so a
/// 60 s poll turns a rotation into a sub-two-minute swap with no dropped
/// connections. Same shape as the API-key watcher in `sqe-auth`.
pub const DEFAULT_CERT_RELOAD_INTERVAL: Duration = Duration::from_secs(60);

/// mtimes of the files that make up the served identity. Comparing this is
/// cheaper than re-reading and re-parsing the PEM every minute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CertStamp {
    cert: Option<SystemTime>,
    key: Option<SystemTime>,
    ca: Option<SystemTime>,
}

impl CertStamp {
    /// Stat the configured files. A missing file stamps as `None`, a state
    /// distinct from any real mtime, so a file that disappears and comes back
    /// counts as changed.
    pub fn read(tls: &TlsConfig) -> Self {
        Self {
            cert: file_mtime(&tls.cert_file),
            key: file_mtime(&tls.key_file),
            ca: file_mtime(&tls.ca_file),
        }
    }
}

fn file_mtime(path: &str) -> Option<SystemTime> {
    if path.is_empty() {
        return None;
    }
    std::fs::metadata(path).ok()?.modified().ok()
}

/// Reload `rustls_config` in place when the certificate material on disk has
/// changed since `last`.
///
/// Returns the new leaf-certificate fingerprint when a reload happened, and
/// `None` when nothing changed. `last` only advances on success, so a
/// half-written pair (cert-manager writes the cert and the key as two
/// operations) is retried on the next tick rather than skipped.
///
/// # Errors
/// Returns [`SqeError::Config`] when the new material does not load or the
/// axum swap fails. The listener keeps serving the previous certificate.
pub async fn reload_if_changed(
    tls: &TlsConfig,
    rustls_config: &RustlsConfig,
    last: &mut CertStamp,
) -> sqe_core::Result<Option<String>> {
    let current = CertStamp::read(tls);
    if current == *last {
        return Ok(None);
    }

    // Load through the shared builder first: it validates the pair, gives us
    // the fingerprint, and is the only thing that knows about the protocol
    // floor and the client-CA requirement.
    let material = ServerTlsMaterial::load(tls)?;

    if tls.ca_file.is_empty() {
        // `reload_from_pem_file` goes through rustls' default builder, which
        // panics without a process-level provider. The listener boot path
        // installs one, but the reloader must not depend on having been
        // spawned from there.
        ensure_crypto_provider();
        rustls_config
            .reload_from_pem_file(&tls.cert_file, &tls.key_file)
            .await
            .map_err(|e| SqeError::Config(format!("TLS certificate reload failed: {e}")))?;
    } else {
        // `reload_from_pem_file` rebuilds a default server config and would
        // silently drop the client-certificate verifier, turning an mTLS
        // listener into a server-only one at the first rotation. Swap the
        // whole config instead.
        rustls_config.reload_from_config(material.rustls_server_config());
    }

    *last = current;
    Ok(Some(material.cert_fingerprint().to_string()))
}

/// Poll the certificate files every `interval` and hot-swap `rustls_config`
/// when they change.
///
/// Detached background task; callers that never need to cancel it drop the
/// handle. Covers the axum listeners only.
pub fn spawn_cert_reload_watch(
    tls: TlsConfig,
    rustls_config: RustlsConfig,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last = CertStamp::read(&tls);
        loop {
            tokio::time::sleep(interval).await;
            match reload_if_changed(&tls, &rustls_config, &mut last).await {
                Ok(Some(fingerprint)) => tracing::info!(
                    cert_file = %tls.cert_file,
                    cert_fingerprint = %fingerprint,
                    "TLS certificate reloaded without restart"
                ),
                Ok(None) => {}
                Err(error) => tracing::warn!(
                    cert_file = %tls.cert_file,
                    %error,
                    "TLS certificate reload failed -- keeping the previous certificate"
                ),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> String {
        format!(
            "{}/../sqe-core/tests/fixtures/tls/{name}",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    /// Copy the fixture pair into a scratch dir so a test can overwrite it.
    fn scratch_tls(
        tag: &str,
        cert: &str,
        key: &str,
        ca: Option<&str>,
    ) -> (tempfile::TempDir, TlsConfig) {
        let dir = tempfile::Builder::new()
            .prefix(&format!("sqe-tls-{tag}-"))
            .tempdir()
            .unwrap();
        let cert_path = dir.path().join("tls.crt");
        let key_path = dir.path().join("tls.key");
        std::fs::copy(fixture(cert), &cert_path).unwrap();
        std::fs::copy(fixture(key), &key_path).unwrap();
        let tls = TlsConfig {
            cert_file: cert_path.display().to_string(),
            key_file: key_path.display().to_string(),
            ca_file: ca.map(fixture).unwrap_or_default(),
            ..TlsConfig::default()
        };
        (dir, tls)
    }

    /// Force a distinct mtime. `std::fs::copy` preserves nothing useful and
    /// second-granularity filesystems would otherwise hide the change.
    fn bump_mtime(path: &str) {
        let content = std::fs::read(path).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        std::fs::write(path, content).unwrap();
    }

    #[test]
    fn tls_builder_is_shared_with_sqe_core() {
        // Re-export, not a copy: a min-version or mTLS change lands in one
        // place for the Flight port, the MCP port and the worker.
        assert_eq!(MIN_PROTOCOL_VERSION, rustls::ProtocolVersion::TLSv1_2);
        assert_eq!(ENABLED_PROTOCOL_VERSIONS.len(), 2);
    }

    #[tokio::test]
    async fn tls_mcp_listener_requires_client_cert_when_ca_set() {
        // The MCP listener used to ignore ca_file entirely, so an mTLS
        // deployment had an mTLS Flight port and a server-only MCP port on the
        // same certificate (finding 210). Both now come off one material, so
        // asserting the material the listener is built from carries a client
        // verifier is asserting what the listener does at handshake time.
        let (_dir, tls) = scratch_tls("mtls", "server.crt", "server.key", Some("ca.crt"));
        let material = ServerTlsMaterial::load(&tls).unwrap();
        assert!(
            material.requires_client_cert(),
            "ca_file must make the MCP listener demand a client certificate"
        );
        // The config really is constructible for axum; this is the exact call
        // `start_mcp_server` makes.
        let _rustls_config = RustlsConfig::from_config(material.rustls_server_config());

        // And without ca_file the same path must not start demanding one.
        let (_dir2, plain) = scratch_tls("plain", "server.crt", "server.key", None);
        assert!(!ServerTlsMaterial::load(&plain)
            .unwrap()
            .requires_client_cert());
    }

    #[tokio::test]
    async fn tls_reload_picks_up_an_overwritten_cert() {
        let (_dir, tls) = scratch_tls("reload", "server.crt", "server.key", None);
        let material = ServerTlsMaterial::load(&tls).unwrap();
        let original_fingerprint = material.cert_fingerprint().to_string();
        let rustls_config = RustlsConfig::from_config(material.rustls_server_config());
        let before = rustls_config.get_inner();

        let mut stamp = CertStamp::read(&tls);
        assert!(
            reload_if_changed(&tls, &rustls_config, &mut stamp)
                .await
                .unwrap()
                .is_none(),
            "an unchanged file must not trigger a reload"
        );

        // cert-manager rotation: same paths, new material.
        std::fs::copy(fixture("server-rotated.crt"), &tls.cert_file).unwrap();
        std::fs::copy(fixture("server-rotated.key"), &tls.key_file).unwrap();
        bump_mtime(&tls.cert_file);
        bump_mtime(&tls.key_file);

        let new_fingerprint = reload_if_changed(&tls, &rustls_config, &mut stamp)
            .await
            .unwrap()
            .expect("an overwritten certificate must trigger a reload");
        assert_ne!(new_fingerprint, original_fingerprint);
        assert_eq!(
            new_fingerprint,
            ServerTlsMaterial::load(&TlsConfig {
                cert_file: fixture("server-rotated.crt"),
                key_file: fixture("server-rotated.key"),
                ..TlsConfig::default()
            })
            .unwrap()
            .cert_fingerprint(),
            "the reloaded fingerprint must be the rotated certificate's"
        );
        assert!(
            !std::sync::Arc::ptr_eq(&before, &rustls_config.get_inner()),
            "the served rustls config must have been swapped"
        );

        // Idempotent: no further change, no further reload.
        assert!(reload_if_changed(&tls, &rustls_config, &mut stamp)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn tls_reload_preserves_the_client_ca_requirement() {
        let (_dir, tls) = scratch_tls("reload-mtls", "server.crt", "server.key", Some("ca.crt"));
        let material = ServerTlsMaterial::load(&tls).unwrap();
        assert!(material.requires_client_cert());
        let rustls_config = RustlsConfig::from_config(material.rustls_server_config());
        let before = rustls_config.get_inner();

        let mut stamp = CertStamp::read(&tls);
        std::fs::copy(fixture("server-rotated.crt"), &tls.cert_file).unwrap();
        std::fs::copy(fixture("server-rotated.key"), &tls.key_file).unwrap();
        bump_mtime(&tls.cert_file);
        bump_mtime(&tls.key_file);

        reload_if_changed(&tls, &rustls_config, &mut stamp)
            .await
            .unwrap()
            .expect("rotation must reload");
        assert!(
            !std::sync::Arc::ptr_eq(&before, &rustls_config.get_inner()),
            "the mTLS listener must have been swapped too"
        );
        // The freshly loaded material still demands a client certificate, so
        // the reload path did not fall back to a default server config.
        assert!(ServerTlsMaterial::load(&tls)
            .unwrap()
            .requires_client_cert());
    }

    #[tokio::test]
    async fn tls_reload_keeps_the_old_cert_when_the_new_pair_is_broken() {
        let (_dir, tls) = scratch_tls("reload-bad", "server.crt", "server.key", None);
        let material = ServerTlsMaterial::load(&tls).unwrap();
        let rustls_config = RustlsConfig::from_config(material.rustls_server_config());
        let before = rustls_config.get_inner();
        let mut stamp = CertStamp::read(&tls);

        std::fs::write(&tls.cert_file, b"half-written garbage").unwrap();
        bump_mtime(&tls.cert_file);

        assert!(reload_if_changed(&tls, &rustls_config, &mut stamp)
            .await
            .is_err());
        assert!(
            std::sync::Arc::ptr_eq(&before, &rustls_config.get_inner()),
            "a failed reload must leave the served certificate untouched"
        );

        // Once the matching pair lands, the next tick recovers: the stamp was
        // not advanced past the failure.
        std::fs::copy(fixture("server-rotated.crt"), &tls.cert_file).unwrap();
        std::fs::copy(fixture("server-rotated.key"), &tls.key_file).unwrap();
        bump_mtime(&tls.cert_file);
        bump_mtime(&tls.key_file);
        assert!(reload_if_changed(&tls, &rustls_config, &mut stamp)
            .await
            .unwrap()
            .is_some());
    }
}
