use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use zeroize::Zeroize;

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
    pub fn create(&self, name: &str, secret: Secret) -> Result<(), String> {
        let mut w = self
            .inner
            .write()
            .map_err(|_| "secret store poisoned".to_string())?;
        if w.contains_key(name) {
            return Err(format!("secret '{name}' already exists"));
        }
        w.insert(name.to_string(), secret);
        Ok(())
    }

    /// Drop a secret. Errors if the secret does not exist or if any
    /// catalog in `in_use_by` references it.
    pub fn drop_secret(&self, name: &str, in_use_by: &[String]) -> Result<(), String> {
        if !in_use_by.is_empty() {
            return Err(format!(
                "secret '{name}' is referenced by attached catalogs: {}",
                in_use_by.join(", ")
            ));
        }
        let mut w = self
            .inner
            .write()
            .map_err(|_| "secret store poisoned".to_string())?;
        w.remove(name)
            .ok_or_else(|| format!("secret '{name}' not found"))?;
        Ok(())
    }

    /// Fetch a secret by name. The returned value is a clone; the store
    /// retains its own copy. Errors if the name is not registered.
    pub fn get(&self, name: &str) -> Result<Secret, String> {
        let r = self
            .inner
            .read()
            .map_err(|_| "secret store poisoned".to_string())?;
        r.get(name)
            .cloned()
            .ok_or_else(|| format!("secret '{name}' not found"))
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
        assert!(err.contains("dup"));
        assert!(err.contains("already exists"));
    }

    #[test]
    fn drop_unused_succeeds() {
        let store = SecretStore::new();
        store.create("ephemeral", aws_full()).unwrap();
        store.drop_secret("ephemeral", &[]).expect("drop succeeds");
        let err = store.get("ephemeral").expect_err("should be gone");
        assert!(err.contains("not found"));
    }

    #[test]
    fn drop_in_use_errors() {
        let store = SecretStore::new();
        store.create("locked", aws_full()).unwrap();
        let err = store
            .drop_secret("locked", &["mycat".to_string()])
            .expect_err("in-use drop should fail");
        assert!(err.contains("locked"));
        assert!(err.contains("mycat"));
        // Secret should still exist.
        store.get("locked").expect("still present");
    }

    #[test]
    fn drop_missing_errors() {
        let store = SecretStore::new();
        let err = store
            .drop_secret("nope", &[])
            .expect_err("dropping missing should fail");
        assert!(err.contains("nope"));
        assert!(err.contains("not found"));
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
