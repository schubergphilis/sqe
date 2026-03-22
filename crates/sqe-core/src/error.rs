use thiserror::Error;

#[derive(Error, Debug)]
pub enum SqeError {
    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Query execution error: {0}")]
    Execution(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl SqeError {
    /// Return a short, sanitised message safe for sending to clients.
    ///
    /// Internal details (stack traces, file paths, connection strings) are
    /// stripped.  `NotImplemented` messages are passed through because they
    /// are user-facing by design.
    pub fn client_message(&self) -> String {
        match self {
            SqeError::Auth(_) => "Authentication failed".to_string(),
            SqeError::Catalog(_) => "Catalog operation failed".to_string(),
            SqeError::Execution(_) => "Query execution failed".to_string(),
            SqeError::Config(_) => "Configuration error".to_string(),
            SqeError::NotImplemented(msg) => msg.clone(),
            SqeError::Internal(_) => "Internal error".to_string(),
        }
    }

    /// Build the error string returned to a client.
    ///
    /// * **debug = true** (dev mode): return the full `Display` representation
    ///   including the error chain.
    /// * **debug = false** (production): return only the sanitised
    ///   [`client_message`](Self::client_message).
    pub fn to_client_error(&self, debug: bool) -> String {
        if debug {
            self.to_string()
        } else {
            self.client_message()
        }
    }
}

pub type Result<T> = std::result::Result<T, SqeError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_message_hides_auth_details() {
        let err = SqeError::Auth("JWT expired at 2026-01-01T00:00:00Z".into());
        assert_eq!(err.client_message(), "Authentication failed");
        assert!(!err.client_message().contains("JWT"));
    }

    #[test]
    fn client_message_hides_catalog_details() {
        let err = SqeError::Catalog("connection refused: polaris:8181".into());
        assert_eq!(err.client_message(), "Catalog operation failed");
        assert!(!err.client_message().contains("polaris"));
    }

    #[test]
    fn client_message_hides_execution_details() {
        let err = SqeError::Execution("column 'secret_col' not found in s3://bucket/path".into());
        assert_eq!(err.client_message(), "Query execution failed");
        assert!(!err.client_message().contains("s3://"));
    }

    #[test]
    fn client_message_hides_internal_details() {
        let err = SqeError::Internal(anyhow::anyhow!("segfault at 0xdeadbeef"));
        assert_eq!(err.client_message(), "Internal error");
        assert!(!err.client_message().contains("segfault"));
    }

    #[test]
    fn client_message_hides_config_details() {
        let err = SqeError::Config("missing field 'client_secret'".into());
        assert_eq!(err.client_message(), "Configuration error");
        assert!(!err.client_message().contains("client_secret"));
    }

    #[test]
    fn client_message_passes_through_not_implemented() {
        let msg = "MERGE INTO is not yet supported";
        let err = SqeError::NotImplemented(msg.into());
        assert_eq!(err.client_message(), msg);
    }

    #[test]
    fn to_client_error_production_hides_details() {
        let err = SqeError::Auth("token invalid: audience mismatch".into());
        let output = err.to_client_error(false);
        assert_eq!(output, "Authentication failed");
        assert!(!output.contains("audience"));
    }

    #[test]
    fn to_client_error_debug_exposes_details() {
        let err = SqeError::Auth("token invalid: audience mismatch".into());
        let output = err.to_client_error(true);
        assert!(output.contains("audience mismatch"));
        assert!(output.contains("Authentication failed"));
    }

    #[test]
    fn to_client_error_internal_production_vs_debug() {
        let err = SqeError::Internal(anyhow::anyhow!("disk full: /var/data"));
        // Production: sanitised
        assert_eq!(err.to_client_error(false), "Internal error");
        // Debug: full chain
        let debug_output = err.to_client_error(true);
        assert!(debug_output.contains("disk full"));
    }
}
