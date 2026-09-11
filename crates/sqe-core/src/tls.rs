//! One TLS builder for every SQE listener and every SQE client.
//!
//! Before this module there were three independent implementations: the
//! coordinator's tonic builder, a hand-copied mirror of it in `sqe-worker`,
//! and a fourth, unrelated `RustlsConfig::from_pem_file` call for the
//! axum/MCP listener. They disagreed about mTLS (the MCP listener ignored
//! `ca_file` entirely) and none of them pinned a protocol floor. Findings 210
//! and 603 in `docs/internal/reviews/2026-09-04-architecture-review/`.
//!
//! Everything here derives from one [`ServerTlsMaterial::load`] that reads the
//! cert, the key, and the optional CA exactly once:
//!
//! - [`ServerTlsMaterial::tonic_server`] for the Flight (gRPC) listeners,
//! - [`ServerTlsMaterial::rustls_server_config`] for the axum listeners (MCP,
//!   Quack), which take an `Arc<rustls::ServerConfig>` directly,
//! - [`tonic_client`] for the two client hops that were plaintext-only:
//!   coordinator to worker (`ChannelPool`) and worker to coordinator
//!   (heartbeat).
//!
//! Both server paths enable TLS 1.2 and TLS 1.3 and nothing older, and both
//! require a client certificate when `ca_file` is set.

use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

use crate::config::TlsConfig;
use crate::error::{Result, SqeError};

/// Protocol floor for every SQE TLS listener. TLS 1.1 and below are never
/// offered, on any port, regardless of what the rustls default happens to be
/// in the version we are pinned to.
pub const MIN_PROTOCOL_VERSION: rustls::ProtocolVersion = rustls::ProtocolVersion::TLSv1_2;

/// The exact version set handed to the rustls builder: TLS 1.2 and TLS 1.3.
pub const ENABLED_PROTOCOL_VERSIONS: &[&rustls::SupportedProtocolVersion] =
    &[&rustls::version::TLS12, &rustls::version::TLS13];

/// `true` when [`tonic_client`] can fall back to the operating system trust
/// store for an `https://` peer that has no explicit `ca_file`.
///
/// Compiled from the `tls-native-roots` feature (on by default). Config
/// validation reads this to decide whether an `https://` worker URL without a
/// `ca_file` is reachable at all; see [`https_peers_need_trust_root`].
pub const NATIVE_ROOTS_AVAILABLE: bool = cfg!(feature = "tls-native-roots");

/// Cert, key, and optional client CA, read once and parsed once.
///
/// Cloning is cheap for the rustls side (an `Arc`) and copies the PEM buffers
/// for the tonic side; the type is built at boot and per reload, never per
/// connection.
#[derive(Clone)]
pub struct ServerTlsMaterial {
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
    ca_pem: Option<Vec<u8>>,
    rustls: Arc<ServerConfig>,
    requires_client_cert: bool,
    cert_fingerprint: String,
}

// The private key must never reach a log line, and `#[derive(Debug)]` on a
// struct that holds one is exactly how that happens.
impl std::fmt::Debug for ServerTlsMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTlsMaterial")
            .field("cert_fingerprint", &self.cert_fingerprint)
            .field("requires_client_cert", &self.requires_client_cert)
            .field("key_pem", &"<redacted>")
            .finish()
    }
}

impl ServerTlsMaterial {
    /// Read and parse the configured cert / key / CA.
    ///
    /// # Errors
    /// Returns [`SqeError::Config`] when TLS is not enabled, when a configured
    /// file cannot be read, when the PEM does not parse, or when the key does
    /// not match the certificate.
    pub fn load(config: &TlsConfig) -> Result<Self> {
        if !config.is_enabled() {
            return Err(SqeError::Config(
                "TLS material requested but tls.cert_file / tls.key_file are not both set"
                    .to_string(),
            ));
        }
        let cert_pem = read_pem(&config.cert_file, "cert")?;
        let key_pem = read_pem(&config.key_file, "key")?;
        let ca_pem = if config.ca_file.is_empty() {
            None
        } else {
            Some(read_pem(&config.ca_file, "CA")?)
        };
        Self::from_pem(cert_pem, key_pem, ca_pem)
    }

    /// [`Self::load`], but `Ok(None)` instead of an error when TLS is off.
    /// This is the shape every listener's boot path wants: plaintext is a
    /// supported (dev) mode, not a failure.
    ///
    /// # Errors
    /// Propagates [`Self::load`] failures when TLS *is* enabled.
    pub fn load_optional(config: &TlsConfig) -> Result<Option<Self>> {
        if !config.is_enabled() {
            return Ok(None);
        }
        Self::load(config).map(Some)
    }

    /// Build from PEM buffers already in memory (used by [`Self::load`] and by
    /// tests that do not want to touch the filesystem).
    ///
    /// # Errors
    /// Returns [`SqeError::Config`] when any buffer fails to parse or the
    /// cert/key pair is inconsistent.
    pub fn from_pem(cert_pem: Vec<u8>, key_pem: Vec<u8>, ca_pem: Option<Vec<u8>>) -> Result<Self> {
        // An explicit provider rather than the process-global default: two
        // components installing different defaults is a silent-misconfig
        // class we do not want on the TLS path.
        let provider = Arc::new(rustls::crypto::ring::default_provider());

        let certs = CertificateDer::pem_slice_iter(&cert_pem)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| SqeError::Config(format!("TLS certificate is not valid PEM: {e}")))?;
        if certs.is_empty() {
            return Err(SqeError::Config(
                "TLS certificate file contains no CERTIFICATE block".to_string(),
            ));
        }
        let key = PrivateKeyDer::from_pem_slice(&key_pem)
            .map_err(|e| SqeError::Config(format!("TLS private key is not valid PEM: {e}")))?;

        let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(ENABLED_PROTOCOL_VERSIONS)
            .map_err(|e| SqeError::Config(format!("TLS protocol version setup failed: {e}")))?;

        let (builder, requires_client_cert) = match ca_pem.as_ref() {
            Some(ca) => {
                let mut roots = RootCertStore::empty();
                for entry in CertificateDer::pem_slice_iter(ca) {
                    let cert = entry.map_err(|e| {
                        SqeError::Config(format!("TLS client CA is not valid PEM: {e}"))
                    })?;
                    roots.add(cert).map_err(|e| {
                        SqeError::Config(format!("TLS client CA is not a usable trust anchor: {e}"))
                    })?;
                }
                let verifier =
                    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                        .build()
                        .map_err(|e| {
                            SqeError::Config(format!("TLS client CA verifier setup failed: {e}"))
                        })?;
                // The whole point of ca_file: the listener must *ask* for a
                // client certificate and reject the handshake without one.
                let offers = verifier.offer_client_auth();
                (builder.with_client_cert_verifier(verifier), offers)
            }
            None => (builder.with_no_client_auth(), false),
        };

        let cert_fingerprint = sha256_hex(certs[0].as_ref());
        let rustls = builder.with_single_cert(certs, key).map_err(|e| {
            SqeError::Config(format!("TLS certificate/key rejected by rustls: {e}"))
        })?;

        Ok(Self {
            cert_pem,
            key_pem,
            ca_pem,
            rustls: Arc::new(rustls),
            requires_client_cert,
            cert_fingerprint,
        })
    }

    /// TLS settings for a tonic (Arrow Flight) listener. Carries the client CA
    /// as `client_ca_root` when `ca_file` was set, so the gRPC port enforces
    /// the same mTLS requirement as the axum ports.
    pub fn tonic_server(&self) -> ServerTlsConfig {
        let mut config =
            ServerTlsConfig::new().identity(tonic_identity(&self.cert_pem, &self.key_pem));
        if let Some(ca) = self.ca_pem.as_ref() {
            config = config.client_ca_root(Certificate::from_pem(ca));
        }
        config
    }

    /// TLS settings for an axum listener (`axum_server::tls_rustls`).
    ///
    /// Same protocol floor and same client-certificate requirement as
    /// [`Self::tonic_server`]; this is what closes the "mTLS Flight port and a
    /// server-only MCP port on the same cert" split in finding 210.
    pub fn rustls_server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.rustls)
    }

    /// `true` when the listener demands a client certificate (`ca_file` set).
    pub fn requires_client_cert(&self) -> bool {
        self.requires_client_cert
    }

    /// Lowercase hex SHA-256 of the leaf certificate DER. Stable identity for
    /// "did the served certificate actually change" checks after a reload.
    pub fn cert_fingerprint(&self) -> &str {
        &self.cert_fingerprint
    }

    /// Versions enabled on the listener. Always [`ENABLED_PROTOCOL_VERSIONS`];
    /// exposed so a test can assert the floor without reaching into rustls
    /// internals (`ServerConfig::versions` is crate-private).
    pub fn protocol_versions(&self) -> &'static [&'static rustls::SupportedProtocolVersion] {
        ENABLED_PROTOCOL_VERSIONS
    }

    /// Lowest protocol version the listener will negotiate.
    pub fn min_protocol_version(&self) -> rustls::ProtocolVersion {
        MIN_PROTOCOL_VERSION
    }
}

/// Client-side TLS for an `https://` peer inside the cluster.
///
/// Trust root: `ca_file` when set, otherwise the platform trust store (the
/// `tls-native-roots` feature). A private cluster CA must be named in
/// `ca_file`; the OS store will not contain it.
///
/// When the block also carries a cert and key, that identity is attached as
/// the *client* certificate. rustls only sends it if the peer asks for one, so
/// this is inert for server-only TLS and is what makes an mTLS cluster
/// (`ca_file` set on both ends) actually complete a handshake.
///
/// `domain` is the SNI / certificate-verification name. Pass the target URL's
/// host; without it tonic derives it from the URI, which is right for a
/// hostname and wrong for anything behind a rewrite.
///
/// # Errors
/// Returns [`SqeError::Config`] when a configured file cannot be read.
pub fn tonic_client(config: &TlsConfig, domain: Option<&str>) -> Result<ClientTlsConfig> {
    let mut tls = ClientTlsConfig::new();

    if config.ca_file.is_empty() {
        // No-op unless a `tls-*-roots` feature is on; see NATIVE_ROOTS_AVAILABLE.
        tls = tls.with_enabled_roots();
    } else {
        tls = tls.ca_certificate(Certificate::from_pem(read_pem(&config.ca_file, "CA")?));
    }

    if config.is_enabled() {
        let cert = read_pem(&config.cert_file, "cert")?;
        let key = read_pem(&config.key_file, "key")?;
        tls = tls.identity(tonic_identity(&cert, &key));
    }

    if let Some(domain) = domain {
        if !domain.is_empty() {
            tls = tls.domain_name(domain);
        }
    }

    Ok(tls)
}

/// `true` when `url` uses the `https` scheme, so the caller must attach a
/// [`tonic_client`] config. tonic refuses an `https` URI on a channel with no
/// `ClientTlsConfig` ("Connecting to HTTPS without TLS enabled"), which is
/// exactly how finding 603 manifested.
pub fn is_https_url(url: &str) -> bool {
    url.trim_start().len() >= 8 && url.trim_start()[..8].eq_ignore_ascii_case("https://")
}

/// Host component of `url`, for use as the SNI / verification domain.
///
/// IPv6 literals come back without the URL brackets, which is the form rustls
/// wants.
pub fn sni_domain(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url.trim()).ok()?;
    let host = parsed.host_str()?.to_string();
    Some(
        host.strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .map(str::to_string)
            .unwrap_or(host),
    )
}

/// `true` when `host` is a bare IP literal rather than a name.
///
/// A certificate issued for the coordinator cannot validate for
/// `https://10.1.2.3:50052` unless it carries every worker IP in its SAN list,
/// so a worker that advertises an IP under TLS is a boot-time misconfiguration
/// (finding 603).
pub fn host_is_ip_literal(host: &str) -> bool {
    let bare = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    bare.parse::<std::net::IpAddr>().is_ok()
}

/// Boot-time check behind `SqeConfig::validate`: with TLS on and `https://`
/// peers configured, the client side needs a trust root or every dispatch
/// fails at first query instead of at boot.
///
/// Returns `Some(message)` when the configuration must be refused.
pub fn https_peers_need_trust_root(
    tls_enabled: bool,
    peer_urls: &[String],
    ca_file: &str,
    native_roots_available: bool,
) -> Option<String> {
    if !tls_enabled || !ca_file.is_empty() || native_roots_available {
        return None;
    }
    let https: Vec<&str> = peer_urls
        .iter()
        .filter(|u| is_https_url(u))
        .map(String::as_str)
        .collect();
    if https.is_empty() {
        return None;
    }
    Some(format!(
        "coordinator.tls is enabled and coordinator.worker_urls contains https entries ({}), \
         but no client trust root is available: coordinator.tls.ca_file is empty and this build \
         has no platform trust store (the sqe-core `tls-native-roots` feature is off). Every \
         dispatch to those workers would fail the TLS handshake at first query. Set \
         [coordinator.tls] ca_file to the CA that issued the worker certificates.",
        https.join(", ")
    ))
}

/// Install ring as the rustls process default before `main` runs.
///
/// Why pre-main and not in each `main`: reqwest 0.13 is built with
/// `rustls-no-provider` (its `rustls` feature hardwires the aws-lc provider
/// and with it the aws-lc-sys C build, see the workspace `Cargo.toml`), and
/// under that feature reqwest panics at `Client` construction unless a
/// process default provider exists. Clients are built in about 25 places
/// across a dozen crates and in their test binaries. Every one of those
/// links this crate, so this is the one place that guarantees the provider
/// is there first. The binaries additionally call
/// [`ensure_crypto_provider`] from `main`, which then finds it installed and
/// says so at debug level.
///
/// No logging here: this runs before any subscriber exists. A failed install
/// can only mean another pre-main initialiser won the race, in which case
/// the provider that is installed is the one rustls will use.
#[ctor::ctor]
fn install_ring_before_main() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Install the ring crypto provider as the rustls process default, once.
///
/// The old call sites did `let _ = rustls::crypto::ring::default_provider()
/// .install_default();`, which silently no-ops when something else installed a
/// provider first. This reports which case happened. Paths that build their
/// config through [`ServerTlsMaterial`] do not depend on the process default
/// at all; this exists for the ones that still call rustls helpers which do
/// (for example `RustlsConfig::reload_from_pem_file`). Since the pre-main
/// install above, the usual outcome is the first branch.
pub fn ensure_crypto_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        tracing::debug!("rustls crypto provider already installed; keeping the existing one");
        return;
    }
    match rustls::crypto::ring::default_provider().install_default() {
        Ok(()) => tracing::debug!("installed the rustls ring crypto provider"),
        Err(_) => tracing::info!(
            "another component installed a rustls crypto provider first; SQE will use it. \
             Cipher suite selection on TLS listeners may differ from the ring defaults."
        ),
    }
}

/// The one place in the workspace that turns a PEM pair into a tonic
/// `Identity`. Server side and client side share it so a change to key
/// handling (a passphrase, a different encoding) cannot land in one and be
/// forgotten in the other; that drift is finding 210.
fn tonic_identity(cert_pem: &[u8], key_pem: &[u8]) -> Identity {
    Identity::from_pem(cert_pem, key_pem)
}

fn read_pem(path: &str, kind: &str) -> Result<Vec<u8>> {
    std::fs::read(path)
        .map_err(|e| SqeError::Config(format!("Failed to read TLS {kind} '{path}': {e}")))
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Throwaway PEM material generated once with openssl (RSA-2048, 20 year
    /// validity, SAN `worker-1.sqe.test` / `localhost` / `127.0.0.1`). It
    /// secures nothing; regenerate with the recipe in the git history of this
    /// file if it ever expires.
    fn fixture(name: &str) -> String {
        format!("{}/tests/fixtures/tls/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    fn server_tls() -> TlsConfig {
        TlsConfig {
            cert_file: fixture("server.crt"),
            key_file: fixture("server.key"),
            ..TlsConfig::default()
        }
    }

    fn mtls() -> TlsConfig {
        TlsConfig {
            ca_file: fixture("ca.crt"),
            ..server_tls()
        }
    }

    #[test]
    fn tls_material_absent_when_not_configured() {
        assert!(ServerTlsMaterial::load_optional(&TlsConfig::default())
            .unwrap()
            .is_none());
    }

    #[test]
    fn tls_server_config_floor_is_tls12() {
        let material = ServerTlsMaterial::load(&server_tls()).unwrap();
        assert_eq!(
            material.min_protocol_version(),
            rustls::ProtocolVersion::TLSv1_2
        );
        let versions = material.protocol_versions();
        assert_eq!(versions.len(), 2, "exactly TLS1.2 and TLS1.3");
        assert!(versions
            .iter()
            .any(|v| v.version == rustls::ProtocolVersion::TLSv1_2));
        assert!(versions
            .iter()
            .any(|v| v.version == rustls::ProtocolVersion::TLSv1_3));
        assert!(
            !versions
                .iter()
                .any(|v| v.version == rustls::ProtocolVersion::TLSv1_1
                    || v.version == rustls::ProtocolVersion::TLSv1_0),
            "TLS 1.1 and below must never be offered"
        );
        // A TLS 1.2 suite is actually present in the provider, so the floor is
        // reachable rather than nominal.
        assert!(
            material
                .rustls_server_config()
                .crypto_provider()
                .cipher_suites
                .iter()
                .any(|s| s.version().version == rustls::ProtocolVersion::TLSv1_2),
            "no TLS 1.2 cipher suite available"
        );
    }

    #[test]
    fn tls_server_without_ca_does_not_ask_for_client_cert() {
        let material = ServerTlsMaterial::load(&server_tls()).unwrap();
        assert!(!material.requires_client_cert());
    }

    #[test]
    fn tls_ca_file_requires_a_client_certificate() {
        let material = ServerTlsMaterial::load(&mtls()).unwrap();
        assert!(
            material.requires_client_cert(),
            "ca_file must make the listener demand a client certificate"
        );
        // And the tonic listener carries the same CA, so the Flight port and
        // the axum ports cannot disagree about mTLS.
        let _ = material.tonic_server();
    }

    #[test]
    fn tls_material_reports_a_stable_cert_fingerprint() {
        let a = ServerTlsMaterial::load(&server_tls()).unwrap();
        let b = ServerTlsMaterial::load(&server_tls()).unwrap();
        assert_eq!(a.cert_fingerprint(), b.cert_fingerprint());
        assert_eq!(a.cert_fingerprint().len(), 64);

        let rotated = TlsConfig {
            cert_file: fixture("server-rotated.crt"),
            key_file: fixture("server-rotated.key"),
            ..TlsConfig::default()
        };
        let rotated = ServerTlsMaterial::load(&rotated).unwrap();
        assert_ne!(
            a.cert_fingerprint(),
            rotated.cert_fingerprint(),
            "a rotated certificate must be distinguishable"
        );
    }

    #[test]
    fn tls_load_reports_a_missing_cert_file() {
        let config = TlsConfig {
            cert_file: "/nonexistent/cert.pem".to_string(),
            key_file: "/nonexistent/key.pem".to_string(),
            ..TlsConfig::default()
        };
        let err = ServerTlsMaterial::load(&config).unwrap_err().to_string();
        assert!(err.contains("Failed to read TLS cert"), "got: {err}");
    }

    #[test]
    fn tls_load_rejects_garbage_pem() {
        // nosemgrep: opt.aikido.semgrep-rules.rust.lang.security.temp-dir -- test-only scratch dir holding a garbage PEM, pid suffixed
        let dir = std::env::temp_dir().join(format!("sqe-tls-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert = dir.join("bad.crt");
        let key = dir.join("bad.key");
        std::fs::write(&cert, b"not a certificate").unwrap();
        std::fs::write(&key, b"not a key").unwrap();
        let config = TlsConfig {
            cert_file: cert.display().to_string(),
            key_file: key.display().to_string(),
            ..TlsConfig::default()
        };
        let err = ServerTlsMaterial::load(&config).unwrap_err().to_string();
        // rustls-pki-types simply yields no sections for non-PEM input, so
        // "no CERTIFICATE block" is the honest message here; a malformed but
        // recognisable PEM section reports "not valid PEM".
        assert!(
            err.contains("no CERTIFICATE block") || err.contains("not valid PEM"),
            "got: {err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tls_client_config_built_for_https_with_ca_file() {
        let config = TlsConfig {
            ca_file: fixture("ca.crt"),
            ..TlsConfig::default()
        };
        let built = tonic_client(&config, Some("worker-1.sqe.test")).unwrap();
        let rendered = format!("{built:?}");
        assert!(
            rendered.contains("worker-1.sqe.test"),
            "SNI domain must reach the client config: {rendered}"
        );
        assert!(
            rendered.contains("certs: [Certificate"),
            "ca_file must become the trust root: {rendered}"
        );
    }

    #[test]
    fn tls_client_config_attaches_our_identity_for_mtls() {
        let built = tonic_client(&mtls(), Some("worker-1.sqe.test")).unwrap();
        let rendered = format!("{built:?}");
        assert!(
            rendered.contains("identity: Some"),
            "mTLS peer needs our client certificate: {rendered}"
        );
    }

    #[test]
    fn tls_client_config_falls_back_to_native_roots() {
        let built = tonic_client(&TlsConfig::default(), None).unwrap();
        let rendered = format!("{built:?}");
        assert!(!rendered.contains("Certificate"), "got: {rendered}");
        if NATIVE_ROOTS_AVAILABLE {
            assert!(
                rendered.contains("with_native_roots: true"),
                "no ca_file must mean the platform trust store: {rendered}"
            );
        }
    }

    #[test]
    fn tls_client_config_surfaces_a_missing_ca() {
        let config = TlsConfig {
            ca_file: "/nonexistent/ca.pem".to_string(),
            ..TlsConfig::default()
        };
        let err = tonic_client(&config, None).unwrap_err().to_string();
        assert!(err.contains("Failed to read TLS CA"), "got: {err}");
    }

    #[test]
    fn tls_is_https_url_classifies_schemes() {
        assert!(is_https_url("https://worker-1:50052"));
        assert!(is_https_url("HTTPS://worker-1:50052"));
        assert!(!is_https_url("http://worker-1:50052"));
        assert!(!is_https_url("http://https.example.com:50052"));
        assert!(!is_https_url(""));
        assert!(!is_https_url("https:/"));
    }

    #[test]
    fn tls_sni_domain_extracts_the_host() {
        assert_eq!(
            sni_domain("https://worker-1.svc:50052").as_deref(),
            Some("worker-1.svc")
        );
        assert_eq!(
            sni_domain("https://10.1.2.3:50052").as_deref(),
            Some("10.1.2.3")
        );
        assert_eq!(
            sni_domain("https://[2001:db8::1]:50052").as_deref(),
            Some("2001:db8::1")
        );
        assert_eq!(sni_domain("not a url"), None);
    }

    #[test]
    fn tls_host_is_ip_literal_classifies_hosts() {
        assert!(host_is_ip_literal("10.1.2.3"));
        assert!(host_is_ip_literal("[2001:db8::1]"));
        assert!(host_is_ip_literal("2001:db8::1"));
        assert!(!host_is_ip_literal("worker-1.svc.cluster.local"));
        assert!(!host_is_ip_literal("localhost"));
    }

    #[test]
    fn tls_validate_rejects_https_worker_urls_without_ca() {
        let urls = vec![
            "http://worker-1:50052".to_string(),
            "https://worker-2:50052".to_string(),
        ];
        // No CA and no platform trust store: refuse at boot.
        let err = https_peers_need_trust_root(true, &urls, "", false)
            .expect("must refuse https workers with no trust root");
        assert!(
            err.contains("ca_file"),
            "error must name the missing CA: {err}"
        );
        assert!(err.contains("https://worker-2:50052"), "got: {err}");

        // A CA is a trust root.
        assert!(https_peers_need_trust_root(true, &urls, "/etc/sqe/ca.crt", false).is_none());
        // So is the platform store.
        assert!(https_peers_need_trust_root(true, &urls, "", true).is_none());
        // Plaintext workers are not affected.
        assert!(https_peers_need_trust_root(
            true,
            &["http://worker-1:50052".to_string()],
            "",
            false
        )
        .is_none());
        // Neither is a coordinator with TLS off.
        assert!(https_peers_need_trust_root(false, &urls, "", false).is_none());
    }

    #[test]
    fn tls_material_debug_redacts_the_private_key() {
        let material = ServerTlsMaterial::load(&server_tls()).unwrap();
        let rendered = format!("{material:?}");
        assert!(rendered.contains("<redacted>"), "got: {rendered}");
        assert!(!rendered.contains("PRIVATE KEY"), "key leaked into Debug");
    }

    #[test]
    fn tls_ensure_crypto_provider_is_idempotent() {
        ensure_crypto_provider();
        ensure_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }
}
