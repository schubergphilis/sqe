//! Verification of a signed catalog scope (issue #908).
//!
//! A scope is a compact JWS (ES256) minted by a configured issuer for one
//! bearer token. It names the catalogs a session of that token may see and
//! whether each may be written. The verifier reuses [`BearerTokenProvider`]
//! for the JWKS fetch, key rotation, per-key algorithm pinning, `iss`
//! (required, equal to the configured issuer), `exp` and `nbf`, then checks
//! what is specific to a scope: the header algorithm is ES256, the header
//! `typ` is one this verifier knows, `sub` and `jti` equal the bearer's, and
//! `catalogs` is a map of catalog name to `read_write` or `read_only`.
//! Other claims, such as `iat`, are accepted and not checked.
//!
//! The scope only narrows. The bearer token still goes to Polaris and S3,
//! so the user stays the identity end to end.

use chrono::{DateTime, Utc};
use jsonwebtoken::{decode_header, Algorithm};
use sqe_core::catalog_scope::{CatalogAccess, CatalogScope, TokenBinding};
use sqe_core::config::CatalogScopeConfig;

use crate::bearer_token::{BearerTokenProvider, BearerTokenProviderConfig};
use crate::provider::AuthError;

/// Why a scope was refused. The text is safe to return to the client and to
/// write to the audit log: it never contains the token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeRefusal(pub String);

impl std::fmt::Display for ScopeRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog scope refused: {}", self.0)
    }
}

impl std::error::Error for ScopeRefusal {}

fn refuse(reason: impl Into<String>) -> ScopeRefusal {
    ScopeRefusal(reason.into())
}

/// Verifies catalog scopes for the one configured client.
pub struct CatalogScopeVerifier {
    client_id: String,
    header_name: String,
    /// The configured scope `typ`, normalised by [`media_type`].
    token_type: String,
    keys: BearerTokenProvider,
}

/// A JOSE `typ` as a comparable media type: lowercased, without the optional
/// `application/` prefix (RFC 7515 section 4.1.9).
fn media_type(typ: &str) -> String {
    let typ = typ.trim().to_ascii_lowercase();
    match typ.strip_prefix("application/") {
        Some(rest) => rest.to_string(),
        None => typ,
    }
}

impl std::fmt::Debug for CatalogScopeVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogScopeVerifier")
            .field("client_id", &self.client_id)
            .field("header_name", &self.header_name)
            .finish_non_exhaustive()
    }
}

impl CatalogScopeVerifier {
    pub fn new(config: &CatalogScopeConfig) -> Result<Self, AuthError> {
        config
            .validate()
            .map_err(|e| AuthError::Internal(anyhow::anyhow!(e)))?;
        let keys = BearerTokenProvider::new(BearerTokenProviderConfig {
            jwks_url: config.jwks_url.clone(),
            issuer: Some(config.issuer.clone()),
            // A scope has no audience: it is bound to one bearer by `sub`
            // and `jti`, which is narrower than any audience string.
            audience: None,
            allow_unbounded_audience: true,
            allow_unbounded_issuer: false,
            allow_insecure_jwks: config.allow_insecure_jwks,
            accept_invalid_certs: config.accept_invalid_certs,
            leeway_secs: config.leeway_secs,
            ..Default::default()
        })?;
        Ok(Self {
            client_id: config.client_id.clone(),
            header_name: config.header_name.to_ascii_lowercase(),
            token_type: media_type(&config.token_type),
            keys,
        })
    }

    /// The OAuth client (`azp`) whose sessions need a scope.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Header / metadata key carrying the scope, lowercased.
    pub fn header_name(&self) -> &str {
        &self.header_name
    }

    /// True when sessions of this bearer must carry a scope.
    pub fn applies_to(&self, binding: Option<&TokenBinding>) -> bool {
        binding
            .and_then(|b| b.client_id.as_deref())
            .is_some_and(|azp| azp == self.client_id)
    }

    /// Verify `scope_token` for the bearer described by `binding`.
    pub async fn verify(
        &self,
        scope_token: &str,
        binding: &TokenBinding,
    ) -> Result<CatalogScope, ScopeRefusal> {
        let scope_token = scope_token.trim();
        if scope_token.is_empty() {
            return Err(refuse("the scope is empty"));
        }
        let header =
            decode_header(scope_token).map_err(|_| refuse("the scope is not a compact JWS"))?;
        if header.alg != Algorithm::ES256 {
            return Err(refuse("the scope must be signed with ES256"));
        }
        self.check_type(header.typ.as_deref())?;
        let claims = self
            .keys
            .verify_jwt_claims(scope_token)
            .await
            .map_err(|e| match e {
                AuthError::Internal(_) => refuse("the scope issuer's keys are unavailable"),
                other => refuse(format!("the signature or lifetime is invalid ({other})")),
            })?;
        scope_from_claims(&claims, binding)
    }

    /// Read a scope only in a format this verifier knows. The configured
    /// `token_type` names the format and its version. An absent `typ` and
    /// the generic `JWT` name no version, so a producer that sets neither is
    /// still read. Any other `typ`, such as a later version or an access
    /// token's `at+jwt`, is refused.
    fn check_type(&self, typ: Option<&str>) -> Result<(), ScopeRefusal> {
        let Some(typ) = typ else {
            return Ok(());
        };
        let typ = media_type(typ);
        if typ == "jwt" || typ == self.token_type {
            Ok(())
        } else {
            // The presented `typ` is unauthenticated input, so it stays out
            // of the reason, which reaches the client and the audit log.
            Err(refuse(format!(
                "the scope type is not known (expected `{}`)",
                self.token_type
            )))
        }
    }
}

/// Check the claims of a signature-verified scope against the bearer.
fn scope_from_claims(
    claims: &serde_json::Value,
    binding: &TokenBinding,
) -> Result<CatalogScope, ScopeRefusal> {
    let str_claim = |name: &str| {
        claims
            .get(name)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
    };
    let sub = str_claim("sub").ok_or_else(|| refuse("the scope has no `sub`"))?;
    let jti = str_claim("jti").ok_or_else(|| refuse("the scope has no `jti`"))?;
    let bearer_sub = binding
        .subject
        .as_deref()
        .ok_or_else(|| refuse("the bearer token has no `sub`"))?;
    let bearer_jti = binding
        .token_id
        .as_deref()
        .ok_or_else(|| refuse("the bearer token has no `jti`"))?;
    if sub != bearer_sub {
        return Err(refuse("the scope is for another subject"));
    }
    if jti != bearer_jti {
        return Err(refuse("the scope is for another token (`jti`)"));
    }
    let exp = claims
        .get("exp")
        .and_then(|v| v.as_i64())
        .and_then(|secs| DateTime::<Utc>::from_timestamp(secs, 0))
        .ok_or_else(|| refuse("the scope has no valid `exp`"))?;
    let catalogs = claims
        .get("catalogs")
        .and_then(|v| v.as_object())
        .ok_or_else(|| refuse("the scope has no `catalogs` object"))?;
    let mut entries = Vec::with_capacity(catalogs.len());
    for (name, access) in catalogs {
        if name.trim().is_empty() {
            return Err(refuse("the scope names an empty catalog"));
        }
        let access = access
            .as_str()
            .and_then(CatalogAccess::parse)
            .ok_or_else(|| {
                refuse(format!(
                    "catalog '{name}' must be `read_write` or `read_only`"
                ))
            })?;
        entries.push((name.clone(), access));
    }
    Ok(CatalogScope::new(sub, jti, exp, entries))
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;

    // Static EC P-256 test keypair, the same public test material as the
    // bearer provider's tests. NOT for production use.
    const EC_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgcb/PT1Xj+xW0FvVc
0YZLwJCPra8OSopguEXNw3d3TbihRANCAASkMxNpk8e4ypVo60ZUB3Q4TIHOcXSJ
Gsk+TB4BOZOZvvRKOPj1Dj5tcKc/tv2Hx8kiS4egVEC2qWmKaBDbv9kN
-----END PRIVATE KEY-----";
    const EC_X: &str = "pDMTaZPHuMqVaOtGVAd0OEyBznF0iRrJPkweATmTmb4";
    const EC_Y: &str = "9Eo4-PUOPm1wpz-2_YfHySJLh6BUQLapaYpoENu_2Q0";
    const KID: &str = "scope-key-1";
    const ISSUER: &str = "https://portal.example";

    fn binding() -> TokenBinding {
        TokenBinding {
            client_id: Some("portal-data-runs".into()),
            token_id: Some("jti-1".into()),
            subject: Some("user-1".into()),
        }
    }

    fn sign(claims: &serde_json::Value, kid: &str) -> String {
        sign_typed(claims, kid, Some("JWT"))
    }

    fn sign_typed(claims: &serde_json::Value, kid: &str, typ: Option<&str>) -> String {
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(kid.to_string());
        header.typ = typ.map(str::to_string);
        encode(
            &header,
            claims,
            &EncodingKey::from_ec_pem(EC_PRIVATE_KEY_PEM.as_bytes()).unwrap(),
        )
        .unwrap()
    }

    fn claims(jti: &str, exp_offset: i64) -> serde_json::Value {
        let now = Utc::now().timestamp();
        json!({
            "iss": ISSUER,
            "sub": "user-1",
            "jti": jti,
            "exp": now + exp_offset,
            "catalogs": { "Sales": "read_write", "ref": "read_only" }
        })
    }

    async fn jwks_server() -> String {
        let body = json!({ "keys": [{
            "kty": "EC", "kid": KID, "alg": "ES256", "crv": "P-256",
            "use": "sig", "x": EC_X, "y": EC_Y
        }]})
        .to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let _ = stream.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }
        });
        format!("http://127.0.0.1:{port}/jwks")
    }

    async fn verifier() -> CatalogScopeVerifier {
        verifier_with("").await
    }

    async fn verifier_with(extra_config: &str) -> CatalogScopeVerifier {
        let jwks_url = jwks_server().await;
        let config: CatalogScopeConfig = toml::from_str(&format!(
            "client_id = \"portal-data-runs\"\nissuer = \"{ISSUER}\"\njwks_url = \"{jwks_url}\"\nallow_insecure_jwks = true\n{extra_config}"
        ))
        .unwrap();
        CatalogScopeVerifier::new(&config).unwrap()
    }

    /// The claims of the producer's version 1 contract: item 7's `sub`,
    /// `jti`, `catalogs` and `exp`, plus `iss` and `iat`.
    fn contract_claims() -> serde_json::Value {
        let mut c = claims("jti-1", 600);
        c["iat"] = json!(Utc::now().timestamp());
        c
    }

    #[tokio::test]
    async fn configured_type_with_iss_and_iat_is_read() {
        let v = verifier_with("token_type = \"portal-catalog-scope+jwt\"").await;
        for typ in [
            "portal-catalog-scope+jwt",
            "Portal-Catalog-Scope+JWT",
            "application/portal-catalog-scope+jwt",
        ] {
            let scope = v
                .verify(&sign_typed(&contract_claims(), KID, Some(typ)), &binding())
                .await
                .unwrap_or_else(|e| panic!("typ {typ}: {e}"));
            assert_eq!(scope.access("sales"), Some(CatalogAccess::ReadWrite));
        }
    }

    #[tokio::test]
    async fn absent_or_generic_type_is_read() {
        let v = verifier_with("token_type = \"portal-catalog-scope+jwt\"").await;
        for typ in [None, Some("JWT"), Some("jwt")] {
            assert!(
                v.verify(&sign_typed(&contract_claims(), KID, typ), &binding())
                    .await
                    .is_ok(),
                "typ {typ:?}"
            );
        }
    }

    #[tokio::test]
    async fn unknown_type_is_refused() {
        let v = verifier_with("token_type = \"portal-catalog-scope+jwt\"").await;
        for typ in [
            "portal-catalog-scope-v2+jwt",
            "at+jwt",
            "sqe-catalog-scope+jwt",
            "",
        ] {
            let err = v
                .verify(&sign_typed(&contract_claims(), KID, Some(typ)), &binding())
                .await
                .expect_err(typ);
            assert!(err.0.contains("type is not known"), "{typ}: {err}");
            assert!(
                typ.is_empty() || !err.0.contains(typ),
                "the presented typ must not be echoed: {err}"
            );
        }
    }

    #[tokio::test]
    async fn default_type_is_the_neutral_name() {
        let v = verifier().await;
        assert!(v
            .verify(
                &sign_typed(&contract_claims(), KID, Some("sqe-catalog-scope+jwt")),
                &binding()
            )
            .await
            .is_ok());
        assert!(v
            .verify(
                &sign_typed(&contract_claims(), KID, Some("portal-catalog-scope+jwt")),
                &binding()
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn missing_issuer_is_refused() {
        let v = verifier().await;
        let mut c = contract_claims();
        c.as_object_mut().unwrap().remove("iss");
        assert!(v.verify(&sign(&c, KID), &binding()).await.is_err());
    }

    #[tokio::test]
    async fn valid_scope_parses_catalogs() {
        let v = verifier().await;
        let scope = v
            .verify(&sign(&claims("jti-1", 600), KID), &binding())
            .await
            .expect("valid scope");
        assert_eq!(scope.access("sales"), Some(CatalogAccess::ReadWrite));
        assert_eq!(scope.access("REF"), Some(CatalogAccess::ReadOnly));
        assert_eq!(scope.access("other"), None);
        assert_eq!(scope.subject(), "user-1");
        assert_eq!(scope.token_id(), "jti-1");
    }

    #[tokio::test]
    async fn wrong_jti_is_refused() {
        let v = verifier().await;
        let err = v
            .verify(&sign(&claims("jti-other", 600), KID), &binding())
            .await
            .unwrap_err();
        assert!(err.0.contains("jti"), "{err}");
    }

    #[tokio::test]
    async fn expired_scope_is_refused() {
        let v = verifier().await;
        assert!(v
            .verify(&sign(&claims("jti-1", -600), KID), &binding())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn unknown_key_is_refused() {
        let v = verifier().await;
        assert!(v
            .verify(&sign(&claims("jti-1", 600), "unknown-kid"), &binding())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn wrong_issuer_is_refused() {
        let v = verifier().await;
        let mut c = claims("jti-1", 600);
        c["iss"] = json!("https://evil.example");
        assert!(v.verify(&sign(&c, KID), &binding()).await.is_err());
    }

    #[tokio::test]
    async fn garbage_is_refused() {
        let v = verifier().await;
        assert!(v.verify("not-a-jws", &binding()).await.is_err());
        assert!(v.verify("", &binding()).await.is_err());
    }

    #[test]
    fn claim_checks() {
        let b = binding();
        let mut c = claims("jti-1", 600);
        assert!(scope_from_claims(&c, &b).is_ok());

        c["sub"] = json!("someone-else");
        assert!(scope_from_claims(&c, &b).is_err());

        let mut c = claims("jti-1", 600);
        c["catalogs"] = json!({ "a": "admin" });
        assert!(scope_from_claims(&c, &b).is_err());

        let mut c = claims("jti-1", 600);
        c.as_object_mut().unwrap().remove("catalogs");
        assert!(scope_from_claims(&c, &b).is_err());

        let no_jti = TokenBinding {
            token_id: None,
            ..binding()
        };
        assert!(scope_from_claims(&claims("jti-1", 600), &no_jti).is_err());
    }

    #[test]
    fn applies_only_to_configured_client() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let v = rt.block_on(verifier());
        assert!(v.applies_to(Some(&binding())));
        let other = TokenBinding {
            client_id: Some("trino-cli".into()),
            ..binding()
        };
        assert!(!v.applies_to(Some(&other)));
        assert!(!v.applies_to(None));
    }
}
