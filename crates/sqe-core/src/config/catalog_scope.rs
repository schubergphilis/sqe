use serde::Deserialize;

/// `[session.catalog_scope]`: a signed catalog scope for one OAuth client
/// (issue #908).
///
/// When set, a session whose bearer token has `azp == client_id` must
/// present a compact JWS (ES256) signed by `issuer`, whose `sub` and `jti`
/// equal the bearer's. The session then sees only the catalogs the scope
/// lists, and writes only into those marked `read_write`. The bearer itself
/// still goes to Polaris and S3: the scope narrows the user's access, it
/// never replaces the user. Unset (the default) leaves every client as it
/// was.
#[derive(Debug, Deserialize, Clone)]
pub struct CatalogScopeConfig {
    /// The OAuth client (`azp` claim of the bearer) whose sessions are scoped.
    pub client_id: String,
    /// Expected `iss` of the scope token.
    pub issuer: String,
    /// JWKS URL holding the issuer's ES256 keys. Must be https unless
    /// `allow_insecure_jwks` is set.
    pub jwks_url: String,
    /// HTTP header (Trino/HTTP) and Flight SQL metadata key carrying the
    /// scope. Matched case-insensitively.
    #[serde(default = "default_catalog_scope_header")]
    pub header_name: String,
    /// Accept a plain-http JWKS URL. Development only.
    #[serde(default)]
    pub allow_insecure_jwks: bool,
    /// Skip TLS verification when fetching the JWKS. Development only.
    #[serde(default)]
    pub accept_invalid_certs: bool,
    /// Clock skew tolerated on `exp`, in seconds.
    #[serde(default = "default_catalog_scope_leeway")]
    pub leeway_secs: u64,
}

pub const DEFAULT_CATALOG_SCOPE_HEADER: &str = "x-sqe-catalog-scope";

fn default_catalog_scope_header() -> String {
    DEFAULT_CATALOG_SCOPE_HEADER.to_string()
}

fn default_catalog_scope_leeway() -> u64 {
    30
}

impl CatalogScopeConfig {
    /// Configuration errors that must stop startup: an empty client, issuer
    /// or JWKS URL would scope nobody or verify against nothing.
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("client_id", &self.client_id),
            ("issuer", &self.issuer),
            ("jwks_url", &self.jwks_url),
            ("header_name", &self.header_name),
        ] {
            if value.trim().is_empty() {
                return Err(format!("[session.catalog_scope] {field} must not be empty"));
            }
        }
        if !self.allow_insecure_jwks && !self.jwks_url.starts_with("https://") {
            return Err(
                "[session.catalog_scope] jwks_url must be https (set allow_insecure_jwks for development)"
                    .to_string(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(toml_src: &str) -> CatalogScopeConfig {
        toml::from_str(toml_src).expect("parse")
    }

    #[test]
    fn defaults_header_and_leeway() {
        let c = parse(
            r#"client_id = "portal-data-runs"
issuer = "https://portal.example"
jwks_url = "https://portal.example/jwks.json""#,
        );
        assert_eq!(c.header_name, DEFAULT_CATALOG_SCOPE_HEADER);
        assert_eq!(c.leeway_secs, 30);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn refuses_http_jwks_and_empty_fields() {
        let mut c = parse(
            r#"client_id = "c"
issuer = "i"
jwks_url = "http://x/jwks""#,
        );
        assert!(c.validate().is_err());
        c.allow_insecure_jwks = true;
        assert!(c.validate().is_ok());
        c.client_id = " ".into();
        assert!(c.validate().is_err());
    }
}
