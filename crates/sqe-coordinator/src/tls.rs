//! TLS configuration for tonic gRPC servers (coordinator + worker).

use sqe_core::config::TlsConfig;
use tonic::transport::{Identity, ServerTlsConfig};

/// Build a [`ServerTlsConfig`] from the engine's TLS configuration.
///
/// Returns `None` when TLS is not configured (plaintext mode).
///
/// # Errors
/// Returns an error if configured cert/key files cannot be read.
pub fn build_server_tls_config(config: &TlsConfig) -> sqe_core::Result<Option<ServerTlsConfig>> {
    if !config.is_enabled() {
        return Ok(None);
    }

    let cert = std::fs::read(&config.cert_file).map_err(|e| {
        sqe_core::SqeError::Config(format!(
            "Failed to read TLS cert '{}': {e}",
            config.cert_file
        ))
    })?;
    let key = std::fs::read(&config.key_file).map_err(|e| {
        sqe_core::SqeError::Config(format!(
            "Failed to read TLS key '{}': {e}",
            config.key_file
        ))
    })?;

    let identity = Identity::from_pem(cert, key);
    let mut tls_config = ServerTlsConfig::new().identity(identity);

    // Optional client CA for mTLS verification
    if !config.ca_file.is_empty() {
        let ca = std::fs::read(&config.ca_file).map_err(|e| {
            sqe_core::SqeError::Config(format!(
                "Failed to read TLS CA '{}': {e}",
                config.ca_file
            ))
        })?;
        let ca_cert = tonic::transport::Certificate::from_pem(ca);
        tls_config = tls_config.client_ca_root(ca_cert);
        tracing::info!(
            ca_file = config.ca_file,
            "TLS enabled with client CA verification (mTLS)"
        );
    } else {
        tracing::info!("TLS enabled (server-side only, no client CA)");
    }

    Ok(Some(tls_config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_when_no_cert() {
        let config = TlsConfig::default();
        let result = build_server_tls_config(&config).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn disabled_when_cert_only() {
        let config = TlsConfig {
            cert_file: "/tmp/cert.pem".to_string(),
            key_file: String::new(),
            ca_file: String::new(),
        };
        assert!(!config.is_enabled());
        let result = build_server_tls_config(&config).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn error_when_cert_file_missing() {
        let config = TlsConfig {
            cert_file: "/nonexistent/cert.pem".to_string(),
            key_file: "/nonexistent/key.pem".to_string(),
            ca_file: String::new(),
        };
        let err = build_server_tls_config(&config).unwrap_err();
        assert!(err.to_string().contains("Failed to read TLS cert"));
    }
}
