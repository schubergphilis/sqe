use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use thiserror::Error;
use zeroize::Zeroize;

/// Structured error type for [`SecretStore`] operations (issue #107).
///
/// The previous `Result<_, String>` shape flattened every failure into a
/// freeform message, so callers that wanted to distinguish "already exists"
/// from "in use by attached catalogs" had to substring-match. Worse, the
/// internal `RwLock` poisoning case turned into the same opaque string as
/// any other error, hiding a panic-recovery boundary that should never be
/// silently swallowed.
#[derive(Error, Debug)]
pub enum SecretStoreError {
    /// `CREATE SECRET` on a name that is already registered. Maps to a
    /// `DuplicateTable`-style classification at the SQL layer so the
    /// client receives the right gRPC code.
    #[error("secret already exists: {0}")]
    AlreadyExists(String),
    /// `DROP SECRET` on a name that one or more attached catalogs still
    /// reference. The carried tuple is `(secret_name, catalogs)` so the
    /// rendered error message tells the user which secret was rejected and
    /// which catalogs need to be detached first.
    #[error("secret '{0}' is in use by catalogs: {1:?}")]
    InUseBy(String, Vec<String>),
    /// Lookup or drop on a name that was never registered.
    #[error("secret not found: {0}")]
    NotFound(String),
    /// The backing `RwLock` was poisoned by a panic in another thread. This
    /// is a separate variant rather than a flattened string so process-wide
    /// recovery code can react to it explicitly.
    #[error("secret store poisoned")]
    Poisoned,
}

impl From<SecretStoreError> for crate::error::SqeError {
    fn from(err: SecretStoreError) -> Self {
        match err {
            SecretStoreError::AlreadyExists(_) => {
                // Routing through `Catalog` keeps the existing
                // `error_code() == DuplicateTable` classifier working.
                crate::error::SqeError::Catalog(err.to_string())
            }
            SecretStoreError::InUseBy(..) | SecretStoreError::NotFound(_) => {
                crate::error::SqeError::Execution(err.to_string())
            }
            SecretStoreError::Poisoned => crate::error::SqeError::Internal(anyhow::anyhow!(err)),
        }
    }
}

/// Credential material, scoped to the running process.
///
/// Stored in [`SecretStore`] keyed by name. Memory only; not persisted.
/// Sensitive bytes are zeroized on drop.
///
/// `Debug` is hand-implemented so a stray `{:?}` in a panic handler or
/// `anyhow!` chain prints only the variant name and field presence — never
/// the raw token, access key, or password (issue #16).
#[derive(Clone)]
pub enum Secret {
    /// AWS credentials. Any subset can be `None` to defer to the AWS
    /// credential chain (env, shared credentials, IMDS, ECS, EKS Pod
    /// Identity).
    Aws {
        access_key: Option<String>,
        secret_key: Option<String>,
        session_token: Option<String>,
        region: Option<String>,
        profile: Option<String>,
    },
    /// Bearer token, e.g. for an Iceberg REST endpoint.
    Bearer { token: String },
    /// Basic auth (HMS over SASL/PLAIN, JDBC, etc.).
    Basic { username: String, password: String },
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // For Aws, identifying metadata (region, profile) is safe to log;
            // anything that smells like a credential becomes a presence flag.
            Self::Aws {
                access_key,
                secret_key,
                session_token,
                region,
                profile,
            } => f
                .debug_struct("Secret::Aws")
                .field("access_key", &presence(access_key))
                .field("secret_key", &presence(secret_key))
                .field("session_token", &presence(session_token))
                .field("region", region)
                .field("profile", profile)
                .finish(),
            Self::Bearer { token } => f
                .debug_struct("Secret::Bearer")
                .field("token", &if token.is_empty() { "None" } else { "<set>" })
                .finish(),
            Self::Basic { username, password } => f
                .debug_struct("Secret::Basic")
                .field("username", username)
                .field("password", &if password.is_empty() { "None" } else { "<set>" })
                .finish(),
        }
    }
}

fn presence(value: &Option<String>) -> &'static str {
    match value {
        Some(s) if !s.is_empty() => "<set>",
        _ => "None",
    }
}

impl Secret {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Aws { .. } => "aws",
            Self::Bearer { .. } => "bearer",
            Self::Basic { .. } => "basic",
        }
    }
}

impl Drop for Secret {
    fn drop(&mut self) {
        match self {
            Self::Aws {
                access_key,
                secret_key,
                session_token,
                ..
            } => {
                if let Some(s) = access_key.as_mut() {
                    s.zeroize();
                }
                if let Some(s) = secret_key.as_mut() {
                    s.zeroize();
                }
                if let Some(s) = session_token.as_mut() {
                    s.zeroize();
                }
            }
            Self::Bearer { token } => token.zeroize(),
            Self::Basic { password, .. } => password.zeroize(),
        }
    }
}

/// Process-global, in-memory secret store.
///
/// Cloning a `SecretStore` shares the same backing map; the store is
/// designed to be cloned cheaply into [`crate::QueryHandler`] and
/// [`crate::EmbeddedClient`] alike.
#[derive(Debug, Default, Clone)]
pub struct SecretStore {
    inner: Arc<RwLock<HashMap<String, Secret>>>,
}

impl SecretStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new secret. Errors if the name already exists.
    pub fn create(&self, name: &str, secret: Secret) -> Result<(), SecretStoreError> {
        let mut w = self.inner.write().map_err(|_| SecretStoreError::Poisoned)?;
        if w.contains_key(name) {
            return Err(SecretStoreError::AlreadyExists(name.to_string()));
        }
        w.insert(name.to_string(), secret);
        Ok(())
    }

    /// Drop a secret. Errors if the secret does not exist or if any
    /// catalog in `in_use_by` references it.
    pub fn drop_secret(
        &self,
        name: &str,
        in_use_by: &[String],
    ) -> Result<(), SecretStoreError> {
        if !in_use_by.is_empty() {
            return Err(SecretStoreError::InUseBy(
                name.to_string(),
                in_use_by.to_vec(),
            ));
        }
        let mut w = self.inner.write().map_err(|_| SecretStoreError::Poisoned)?;
        w.remove(name)
            .ok_or_else(|| SecretStoreError::NotFound(name.to_string()))?;
        Ok(())
    }

    /// Fetch a secret by name. The returned value is a clone; the store
    /// retains its own copy. Errors if the name is not registered.
    pub fn get(&self, name: &str) -> Result<Secret, SecretStoreError> {
        let r = self.inner.read().map_err(|_| SecretStoreError::Poisoned)?;
        r.get(name)
            .cloned()
            .ok_or_else(|| SecretStoreError::NotFound(name.to_string()))
    }

    /// List all secrets as `(name, type_name)`. Never returns values.
    pub fn list(&self) -> Vec<(String, &'static str)> {
        let r = self.inner.read().expect("secret store poisoned");
        let mut out: Vec<_> = r.iter().map(|(n, s)| (n.clone(), s.type_name())).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aws_full() -> Secret {
        Secret::Aws {
            access_key: Some("AKIAEXAMPLE".to_string()),
            secret_key: Some("supersecret".to_string()),
            session_token: Some("session-token-value".to_string()),
            region: Some("us-east-1".to_string()),
            profile: Some("prod".to_string()),
        }
    }

    #[test]
    fn create_then_get_round_trips() {
        let store = SecretStore::new();
        store.create("aws_prod", aws_full()).unwrap();
        let got = store.get("aws_prod").expect("should fetch");
        match &got {
            Secret::Aws {
                access_key,
                secret_key,
                session_token,
                region,
                profile,
            } => {
                assert_eq!(access_key.as_deref(), Some("AKIAEXAMPLE"));
                assert_eq!(secret_key.as_deref(), Some("supersecret"));
                assert_eq!(session_token.as_deref(), Some("session-token-value"));
                assert_eq!(region.as_deref(), Some("us-east-1"));
                assert_eq!(profile.as_deref(), Some("prod"));
            }
            other => panic!("expected Aws, got {}", other.type_name()),
        }
    }

    // --- Issue #16: Debug never leaks secret material ---

    #[test]
    fn bearer_debug_does_not_leak_token() {
        let s = Secret::Bearer {
            token: "ey-very-secret-jwt".to_string(),
        };
        let d = format!("{:?}", s);
        assert!(!d.contains("ey-very-secret-jwt"), "leaked: {d}");
        assert!(d.contains("<set>"), "presence sentinel missing: {d}");
        assert!(d.contains("Bearer"), "variant tag missing: {d}");
    }

    #[test]
    fn aws_debug_redacts_credentials_but_keeps_region() {
        let s = Secret::Aws {
            access_key: Some("AKIAEXAMPLEXXXXXXXXX".to_string()),
            secret_key: Some("supersecret-key-value".to_string()),
            session_token: Some("session-token-payload".to_string()),
            region: Some("eu-central-1".to_string()),
            profile: Some("prod".to_string()),
        };
        let d = format!("{:?}", s);
        assert!(!d.contains("AKIAEXAMPLEXXXXXXXXX"), "access_key leaked: {d}");
        assert!(!d.contains("supersecret-key-value"), "secret_key leaked: {d}");
        assert!(!d.contains("session-token-payload"), "session_token leaked: {d}");
        // Non-credential fields are still useful for diagnostics.
        assert!(d.contains("eu-central-1"), "region missing: {d}");
        assert!(d.contains("prod"), "profile missing: {d}");
    }

    #[test]
    fn basic_debug_does_not_leak_password() {
        let s = Secret::Basic {
            username: "alice".to_string(),
            password: "hunter2-very-private".to_string(),
        };
        let d = format!("{:?}", s);
        assert!(!d.contains("hunter2-very-private"), "password leaked: {d}");
        // Username appears (not a secret).
        assert!(d.contains("alice"), "username missing: {d}");
    }

    #[test]
    fn create_duplicate_errors() {
        let store = SecretStore::new();
        store.create("dup", aws_full()).unwrap();
        let err = store
            .create("dup", aws_full())
            .expect_err("second create should fail");
        match err {
            SecretStoreError::AlreadyExists(ref name) => assert_eq!(name, "dup"),
            other => panic!("expected AlreadyExists(\"dup\"), got: {other:?}"),
        }
        assert!(err.to_string().contains("dup"));
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn drop_unused_succeeds() {
        let store = SecretStore::new();
        store.create("ephemeral", aws_full()).unwrap();
        store.drop_secret("ephemeral", &[]).expect("drop succeeds");
        let err = store.get("ephemeral").expect_err("should be gone");
        match err {
            SecretStoreError::NotFound(ref name) => assert_eq!(name, "ephemeral"),
            other => panic!("expected NotFound(\"ephemeral\"), got: {other:?}"),
        }
    }

    #[test]
    fn drop_in_use_errors() {
        let store = SecretStore::new();
        store.create("locked", aws_full()).unwrap();
        let err = store
            .drop_secret("locked", &["mycat".to_string()])
            .expect_err("in-use drop should fail");
        match err {
            SecretStoreError::InUseBy(ref name, ref cats) => {
                assert_eq!(name, "locked");
                assert_eq!(cats, &vec!["mycat".to_string()]);
            }
            other => panic!("expected InUseBy(\"locked\", [\"mycat\"]), got: {other:?}"),
        }
        // Secret should still exist.
        store.get("locked").expect("still present");
    }

    #[test]
    fn drop_missing_errors() {
        let store = SecretStore::new();
        let err = store
            .drop_secret("nope", &[])
            .expect_err("dropping missing should fail");
        match err {
            SecretStoreError::NotFound(ref name) => assert_eq!(name, "nope"),
            other => panic!("expected NotFound(\"nope\"), got: {other:?}"),
        }
    }

    #[test]
    fn error_conversion_preserves_classifier_for_already_exists() {
        // AlreadyExists must convert into a `SqeError::Catalog` whose
        // string contains the right phrase so `error_code()` still classifies
        // it as DuplicateTable (issue #12 alignment).
        let err: crate::error::SqeError =
            SecretStoreError::AlreadyExists("x".to_string()).into();
        match err {
            crate::error::SqeError::Catalog(ref m) => {
                assert!(m.contains("already exists"), "got: {m}");
            }
            other => panic!("expected SqeError::Catalog, got: {other:?}"),
        }
    }

    #[test]
    fn error_conversion_routes_poisoned_to_internal() {
        let err: crate::error::SqeError = SecretStoreError::Poisoned.into();
        assert!(matches!(err, crate::error::SqeError::Internal(_)));
    }

    #[test]
    fn list_returns_names_and_types_sorted_no_values() {
        let store = SecretStore::new();
        store
            .create(
                "zeta",
                Secret::Bearer {
                    token: "t".to_string(),
                },
            )
            .unwrap();
        store.create("alpha", aws_full()).unwrap();
        store
            .create(
                "mid",
                Secret::Basic {
                    username: "u".to_string(),
                    password: "p".to_string(),
                },
            )
            .unwrap();
        let listed = store.list();
        assert_eq!(
            listed,
            vec![
                ("alpha".to_string(), "aws"),
                ("mid".to_string(), "basic"),
                ("zeta".to_string(), "bearer"),
            ]
        );
    }

    #[test]
    fn bearer_secret_zeroize_does_not_panic() {
        let store = SecretStore::new();
        store
            .create(
                "bearer_test",
                Secret::Bearer {
                    token: "0123456789abcdef".to_string(),
                },
            )
            .unwrap();
        store.drop_secret("bearer_test", &[]).unwrap();
        // If Drop misbehaves, the test process aborts. Reaching this line is
        // the assertion.
    }

    #[test]
    fn clone_shares_backing_map() {
        let original = SecretStore::new();
        let cloned = original.clone();
        cloned
            .create(
                "shared",
                Secret::Bearer {
                    token: "tok".to_string(),
                },
            )
            .unwrap();
        let got = original
            .get("shared")
            .expect("original should see the clone's insert");
        assert_eq!(got.type_name(), "bearer");
    }
}
