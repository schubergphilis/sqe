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

/// Why a scoped session was refused a catalog (#295). Carried as the source
/// of the refusal so the audit record names it, with the same wire names as
/// the Spark side of the workspace binding; the client still sees only
/// "not found" for a catalog outside the scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeReason {
    /// The catalog is not in the session's scope.
    NotInWorkspace,
    /// A write into a catalog the scope marks `read_only`.
    ReadOnly,
}

impl ScopeReason {
    pub fn as_str(self) -> &'static str {
        match self {
            ScopeReason::NotInWorkspace => "catalog_not_in_workspace",
            ScopeReason::ReadOnly => "catalog_read_only",
        }
    }

    /// The reason `error` (or any error in its source chain) carries, if it
    /// is a scope refusal.
    pub fn of(error: &(dyn std::error::Error + 'static)) -> Option<ScopeReason> {
        let mut next = Some(error);
        while let Some(e) = next {
            if let Some(reason) = e.downcast_ref::<ScopeReason>() {
                return Some(*reason);
            }
            next = e.source();
        }
        None
    }
}

/// `fallback`, unless `source` carries a catalog scope refusal: then the
/// fallback's message under the refusal's code, with the reason kept as the
/// source. For call sites that reword a lower error into text, which would
/// otherwise drop the reason the audit record names (#295).
pub fn keep_scope_reason(
    source: &(dyn std::error::Error + 'static),
    fallback: SqeError,
) -> SqeError {
    match ScopeReason::of(source) {
        Some(reason) => {
            let code = match reason {
                ScopeReason::NotInWorkspace => SqeErrorCode::CatalogNotFound,
                ScopeReason::ReadOnly => SqeErrorCode::AccessDenied,
            };
            SqeError::sourced(code, fallback.to_string(), reason)
        }
        None => fallback,
    }
}

impl std::fmt::Display for ScopeReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::error::Error for ScopeReason {}

/// The error a scoped session gets for a catalog outside its scope: the
/// same answer as a catalog that does not exist, so the reply does not tell
/// a hidden catalog from a missing one. The audit record names the reason.
pub fn catalog_not_found(catalog: &str) -> SqeError {
    SqeError::sourced(
        SqeErrorCode::CatalogNotFound,
        format!("Catalog '{catalog}' not found"),
        ScopeReason::NotInWorkspace,
    )
}

/// The error for a write into a catalog the scope marks `read_only`.
pub fn catalog_read_only(catalog: &str) -> SqeError {
    SqeError::sourced(
        SqeErrorCode::AccessDenied,
        format!("Catalog '{catalog}' is read-only in this session (catalog_read_only)"),
        ScopeReason::ReadOnly,
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

    #[test]
    fn refusals_carry_their_reason_but_not_in_the_message() {
        let hidden = catalog_not_found("sales");
        assert_eq!(hidden.error_code(), SqeErrorCode::CatalogNotFound);
        assert_eq!(hidden.client_message(), "Catalog 'sales' not found");
        assert_eq!(ScopeReason::of(&hidden), Some(ScopeReason::NotInWorkspace));
        let read_only = catalog_read_only("sales");
        assert_eq!(read_only.error_code(), SqeErrorCode::AccessDenied);
        assert_eq!(
            read_only.client_message(),
            "Catalog 'sales' is read-only in this session (catalog_read_only)"
        );
        assert_eq!(ScopeReason::of(&read_only), Some(ScopeReason::ReadOnly));
        assert_eq!(ScopeReason::ReadOnly.as_str(), "catalog_read_only");
        assert_eq!(
            ScopeReason::NotInWorkspace.as_str(),
            "catalog_not_in_workspace"
        );
        // Found through a wrapper, as when DataFusion carries it.
        let wrapped = SqeError::sourced(SqeErrorCode::ExecutionFailed, "outer", hidden);
        assert_eq!(ScopeReason::of(&wrapped), Some(ScopeReason::NotInWorkspace));
        assert_eq!(ScopeReason::of(&SqeError::Execution("x".into())), None);
    }
}
