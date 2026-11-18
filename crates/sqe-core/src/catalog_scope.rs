//! A session narrowed to a signed list of catalogs (#908).
//!
//! Tokens of one configured OAuth client (`[session.catalog_scope]
//! client_id`, matched against the bearer's `azp`) must present a catalog
//! scope: a JWS signed by a trusted issuer that names the catalogs the
//! session may use, each `read_write` or `read_only`. The verified scope is
//! carried on the [`Session`](crate::Session) for one request; the session
//! then sees only those catalogs, and writes only into the `read_write`
//! ones. Grants still decide access inside the scope: the scope only
//! narrows.
//!
//! This module holds the types and the checks. Verifying the signature is
//! `sqe-auth`'s job; enforcing visibility and writes is the coordinator's.

use std::collections::BTreeMap;
use std::sync::RwLock;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::error::{SqeError, SqeErrorCode};

/// What a scoped session may do in one catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CatalogAccess {
    ReadWrite,
    ReadOnly,
}

impl CatalogAccess {
    /// Parse the claim value (`read_write` / `read_only`).
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "read_write" => Some(Self::ReadWrite),
            "read_only" => Some(Self::ReadOnly),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadWrite => "read_write",
            Self::ReadOnly => "read_only",
        }
    }
}

/// The catalogs SQE itself keeps for every session, never part of a scope:
/// `datafusion` holds the DML helpers' temporary tables. SQE's virtual
/// `system` schemas are handled where the `system` catalog is registered.
pub const SCOPE_EXEMPT_CATALOGS: &[&str] = &["datafusion"];

/// Fold a catalog name the way an unquoted SQL identifier folds (ASCII
/// lowercase, like DataFusion), so `"Sales"` in a scope and `sales` in SQL
/// agree.
pub fn fold_catalog_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// A verified catalog scope, bound to one bearer token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogScope {
    catalogs: BTreeMap<String, CatalogAccess>,
    subject: String,
    token_id: String,
    expires_at: DateTime<Utc>,
}

impl CatalogScope {
    /// Build a scope from verified claims. Catalog names are folded; if a
    /// name appears twice after folding, the narrower access wins.
    pub fn new(
        subject: impl Into<String>,
        token_id: impl Into<String>,
        expires_at: DateTime<Utc>,
        catalogs: impl IntoIterator<Item = (String, CatalogAccess)>,
    ) -> Self {
        let mut folded: BTreeMap<String, CatalogAccess> = BTreeMap::new();
        for (name, access) in catalogs {
            let entry = folded.entry(fold_catalog_name(&name)).or_insert(access);
            if access == CatalogAccess::ReadOnly {
                *entry = CatalogAccess::ReadOnly;
            }
        }
        Self {
            catalogs: folded,
            subject: subject.into(),
            token_id: token_id.into(),
            expires_at,
        }
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }

    /// The access this scope grants in `catalog`, `None` when the catalog
    /// is not in scope.
    pub fn access(&self, catalog: &str) -> Option<CatalogAccess> {
        self.catalogs.get(&fold_catalog_name(catalog)).copied()
    }

    /// The scoped catalog names, folded and sorted.
    pub fn catalog_names(&self) -> impl Iterator<Item = &str> {
        self.catalogs.keys().map(String::as_str)
    }

    /// `(catalog, access)` pairs, folded and sorted.
    pub fn entries(&self) -> impl Iterator<Item = (&str, CatalogAccess)> {
        self.catalogs.iter().map(|(n, a)| (n.as_str(), *a))
    }

    /// Stable digest of the catalog list, for cache keys. Two scopes with
    /// the same catalogs and modes digest the same, whatever their order.
    pub fn digest(&self) -> String {
        let mut h = Sha256::new();
        for (name, access) in &self.catalogs {
            h.update((name.len() as u64).to_le_bytes());
            h.update(name.as_bytes());
            h.update([match access {
                CatalogAccess::ReadWrite => 1u8,
                CatalogAccess::ReadOnly => 2u8,
            }]);
        }
        hex::encode(h.finalize())[..16].to_string()
    }
}

/// The bearer claims a catalog scope binds to, read from the token the
/// session was authenticated with (`azp`, `jti`, `sub`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TokenBinding {
    /// OAuth `azp` (authorized party): the client the token was issued to.
    pub client_id: Option<String>,
    /// JWT `jti`: the token's unique id.
    pub token_id: Option<String>,
    /// JWT `sub`.
    pub subject: Option<String>,
}

impl TokenBinding {
    pub fn is_empty(&self) -> bool {
        self.client_id.is_none() && self.token_id.is_none() && self.subject.is_none()
    }
}

/// The OAuth client whose sessions must carry a scope, set once at
/// coordinator startup from `[session.catalog_scope] client_id`. `None`
/// (the default) turns the feature off. Process-wide so the fail-closed
/// check in [`Session::effective_scope`](crate::Session::effective_scope)
/// holds on every protocol, including those that never see the config.
static SCOPED_CLIENT: RwLock<Option<String>> = RwLock::new(None);

pub fn set_scoped_client(client_id: Option<String>) {
    if let Ok(mut slot) = SCOPED_CLIENT.write() {
        *slot = client_id.filter(|c| !c.is_empty());
    }
}

pub fn scoped_client() -> Option<String> {
    SCOPED_CLIENT.read().ok().and_then(|c| c.clone())
}

/// The refusal for a session of the scoped client that carries no valid
/// scope. Same code as a failed login: the session never starts.
pub fn scope_required(reason: &str) -> SqeError {
    SqeError::Auth(format!("catalog scope refused: {reason}"))
}

/// The error a scoped session gets for a catalog outside its scope: the
/// same answer as a catalog that does not exist, so the reply does not tell
/// a hidden catalog from a missing one.
pub fn catalog_not_found(catalog: &str) -> SqeError {
    SqeError::with_code(
        SqeErrorCode::CatalogNotFound,
        format!("Catalog '{catalog}' not found"),
    )
}

/// The error for a write into a catalog the scope marks `read_only`.
pub fn catalog_read_only(catalog: &str) -> SqeError {
    SqeError::with_code(
        SqeErrorCode::AccessDenied,
        format!("Catalog '{catalog}' is read-only in this session"),
    )
}

/// The error for a statement a scoped session may not run at all.
pub fn refused_in_scoped_session(what: &str) -> SqeError {
    SqeError::with_code(
        SqeErrorCode::AccessDenied,
        format!("{what} is not allowed in a session limited to a catalog scope"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn scope() -> CatalogScope {
        CatalogScope::new(
            "alice",
            "jti-1",
            Utc::now() + Duration::minutes(5),
            [
                ("A".to_string(), CatalogAccess::ReadWrite),
                ("b".to_string(), CatalogAccess::ReadOnly),
            ],
        )
    }

    #[test]
    fn names_fold_like_identifiers() {
        let s = scope();
        assert_eq!(s.access("a"), Some(CatalogAccess::ReadWrite));
        assert_eq!(s.access("A"), Some(CatalogAccess::ReadWrite));
        assert_eq!(s.access("B"), Some(CatalogAccess::ReadOnly));
        assert_eq!(s.access("c"), None);
        assert_eq!(s.catalog_names().collect::<Vec<_>>(), vec!["a", "b"]);
    }

    #[test]
    fn duplicate_after_fold_takes_the_narrower_access() {
        let s = CatalogScope::new(
            "alice",
            "jti-1",
            Utc::now(),
            [
                ("Sales".to_string(), CatalogAccess::ReadWrite),
                ("sales".to_string(), CatalogAccess::ReadOnly),
            ],
        );
        assert_eq!(s.access("sales"), Some(CatalogAccess::ReadOnly));
        let s = CatalogScope::new(
            "alice",
            "jti-1",
            Utc::now(),
            [
                ("sales".to_string(), CatalogAccess::ReadOnly),
                ("Sales".to_string(), CatalogAccess::ReadWrite),
            ],
        );
        assert_eq!(s.access("sales"), Some(CatalogAccess::ReadOnly));
    }

    #[test]
    fn digest_ignores_order_and_follows_modes() {
        let now = Utc::now();
        let ab = CatalogScope::new(
            "s",
            "j",
            now,
            [
                ("a".to_string(), CatalogAccess::ReadWrite),
                ("b".to_string(), CatalogAccess::ReadOnly),
            ],
        );
        let ba = CatalogScope::new(
            "s",
            "j",
            now,
            [
                ("b".to_string(), CatalogAccess::ReadOnly),
                ("a".to_string(), CatalogAccess::ReadWrite),
            ],
        );
        let both_rw = CatalogScope::new(
            "s",
            "j",
            now,
            [
                ("a".to_string(), CatalogAccess::ReadWrite),
                ("b".to_string(), CatalogAccess::ReadWrite),
            ],
        );
        assert_eq!(ab.digest(), ba.digest());
        assert_ne!(ab.digest(), both_rw.digest());
    }

    #[test]
    fn access_parses_only_the_two_modes() {
        assert_eq!(
            CatalogAccess::parse("read_write"),
            Some(CatalogAccess::ReadWrite)
        );
        assert_eq!(
            CatalogAccess::parse("read_only"),
            Some(CatalogAccess::ReadOnly)
        );
        assert_eq!(CatalogAccess::parse("READ_WRITE"), None);
        assert_eq!(CatalogAccess::parse("write"), None);
    }
}
