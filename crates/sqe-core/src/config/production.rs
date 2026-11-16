//! The production-mode rule table (#637 item 2).
//!
//! `coordinator.production_mode = true` turns a set of dev-only defaults into
//! startup errors. Those checks used to be `if self.coordinator.production_mode
//! && ...` branches spread through `validate()` and `validate_production()`,
//! so nobody could answer "what does production mode enforce?" from one
//! place, and the Helm `values-production.yaml` and the production guide
//! reproduced the list by hand. [`PRODUCTION_RULES`] is that list.
//! [`SqeConfig::validate_production`] iterates it, and a test checks that the
//! production guide's table names every rule and nothing else.
//!
//! Adding a rule: add a [`ProductionRule`] here, then add its row to
//! `docs/site/book/src/operations/production-guide.md` (the test will tell
//! you which is missing).

use super::{
    AccessControlBackend, AuthProviderConfig, CatalogAuthConfig, CatalogBackend, CatalogConfig,
    DistributedCredentialMode, McpAuthMode, PolicyEngine, SqeConfig,
};

/// One thing production mode refuses.
pub struct ProductionRule {
    /// Stable identifier; the row key in the production guide's table.
    pub name: &'static str,
    /// One line for the docs table: what the rule refuses and why.
    pub summary: &'static str,
    /// `Some(message)` when `config` violates the rule. The message is what
    /// the operator reads at startup, so it names the field and the fix.
    pub check: fn(&SqeConfig) -> Option<String>,
}

/// Every rule `production_mode` enforces, in the order the errors are reported.
pub const PRODUCTION_RULES: &[ProductionRule] = &[
    ProductionRule {
        name: "no-dev-auth-providers",
        summary: "`auth.providers` must not include `anonymous` or `bearer_passthrough` (dev-only; accept any connection or skip local JWT validation).",
        check: |c| {
            c.auth
                .providers
                .iter()
                .any(|p| {
                    matches!(
                        p,
                        AuthProviderConfig::Anonymous { .. }
                            | AuthProviderConfig::BearerPassthrough { .. }
                    )
                })
                .then(|| {
                    "production_mode: auth.providers must not include anonymous or \
                     bearer_passthrough providers (dev-only; accept any connection \
                     or skip local JWT validation)"
                        .to_string()
                })
        },
    },
    ProductionRule {
        name: "rate-limit-enabled",
        summary: "`rate_limit.enabled` must be true (disabled rate limiting allows query and auth flooding).",
        check: |c| {
            (!c.rate_limit.enabled).then(|| {
                "production_mode: rate_limit.enabled must be true (disabled rate \
                 limiting allows query and auth flooding)"
                    .to_string()
            })
        },
    },
    ProductionRule {
        name: "no-insecure-transport",
        summary: "`security.allow_insecure_transport` must be false (cleartext distributed hops expose bearer tokens, worker secrets and vended S3 credentials).",
        check: |c| {
            c.security.allow_insecure_transport.then(|| {
                "production_mode: security.allow_insecure_transport must be false \
                 (cleartext distributed hops expose bearer tokens, worker secrets, \
                 and vended S3 credentials)"
                    .to_string()
            })
        },
    },
    // The Trino HTTP listener, the Quack listener without `[coordinator.tls]`
    // and MCP without TLS (or riding the web port) serve cleartext on every
    // interface; user passwords and bearer tokens cross it. Allowed only when
    // a TLS-terminating proxy is acknowledged in front.
    ProductionRule {
        name: "no-plaintext-client-listeners",
        summary: "An enabled plaintext Trino HTTP, Quack or MCP listener on a non-loopback address is refused unless `security.allow_plaintext_behind_tls_proxy = true` acknowledges a TLS-terminating proxy in front (client credentials would cross it in cleartext).",
        check: |c| {
            if c.security.allow_plaintext_behind_tls_proxy {
                return None;
            }
            let tls = c.coordinator.tls.is_enabled();
            let mcp_plain = c.mcp.is_enabled()
                && (c.mcp.port == 0
                    || (!tls && c.mcp.bind_ip().is_ok_and(|ip| !ip.is_loopback())));
            let listeners = [
                ("coordinator.trino_http_port", c.coordinator.trino_http_port > 0),
                ("coordinator.quack_port", c.coordinator.quack_port > 0 && !tls),
                ("mcp", mcp_plain),
            ];
            let offenders: Vec<&str> = listeners
                .iter()
                .filter(|(_, plain)| *plain)
                .map(|(name, _)| *name)
                .collect();
            (!offenders.is_empty()).then(|| {
                format!(
                    "production_mode: {} serve plaintext on a non-loopback address; \
                     enable TLS (trino_https_port with trino_http_port = 0, \
                     [coordinator.tls] for Quack and MCP) or set \
                     security.allow_plaintext_behind_tls_proxy = true when a \
                     TLS-terminating proxy fronts them",
                    offenders.join(", ")
                )
            })
        },
    },
    // SEC-03: a config-held `client_credentials` grant hands one server-baked
    // service token to every connection, collapsing all users to a single
    // identity at Polaris/S3 and defeating per-user authorization. Two
    // sibling paths carry this risk and must be gated with parity:
    //   1. `[auth].providers`: the Flight user-auth `ClientCredentials`
    //      provider (shared identity for every connecting user).
    //   2. `[catalog.auth]` / `[catalogs.*.auth]`: a catalog whose auth
    //      method is `ClientCredentials` fetches one token per session and
    //      reuses it, so that catalog sees one identity for all users.
    // The per-connection `client_credentials_passthrough` provider (own
    // creds per connection) is NOT flagged.
    ProductionRule {
        name: "no-shared-service-identity",
        summary: "No config-held `client_credentials` in `[auth].providers` or `[catalog(s).auth]` unless `security.allow_shared_service_identity = true` (one identity for every user at Polaris/S3).",
        check: |c| {
            let shared_identity_flight = c
                .auth
                .providers
                .iter()
                .any(|p| matches!(p, AuthProviderConfig::ClientCredentials { .. }));
            let shared_identity_catalog = std::iter::once(&c.catalog)
                .chain(c.catalogs.values())
                .any(|cat| matches!(cat.auth, Some(CatalogAuthConfig::ClientCredentials { .. })));
            (!c.security.allow_shared_service_identity
                && (shared_identity_flight || shared_identity_catalog))
                .then(|| {
                    "production_mode: a shared-service-identity auth mode is configured \
                     (config-held client_credentials in [auth].providers and/or \
                     [catalog(s).auth]); every connection would use one identity at \
                     Polaris/S3, losing per-user authorization. Use oidc_password \
                     (per-user) or client_credentials_passthrough (per-connection) for \
                     user auth and SessionBearer for catalog auth, or set \
                     security.allow_shared_service_identity = true to acknowledge a \
                     deliberate single-service deployment."
                        .to_string()
                })
        },
    },
    ProductionRule {
        name: "worker-secret-required",
        summary: "An empty `coordinator.worker_secret` with `allow_unauthenticated_workers = true` is refused regardless of `worker_urls` (the waiver alone makes heartbeat discovery live, so any TCP-reachable client may register as a worker; #719).",
        check: |c| {
            // #719: `distributed_enabled` (sqe-server) and
            // `mode::warns_unauthenticated_workers` both treat the waiver alone
            // as live heartbeat discovery, so this rule must not key off
            // `worker_urls`.
            (c.coordinator.worker_secret.is_empty() && c.coordinator.allow_unauthenticated_workers)
                .then(|| {
                    "production_mode: coordinator.worker_secret is empty with \
                     coordinator.allow_unauthenticated_workers = true (heartbeat discovery \
                     is live on the waiver alone, so any TCP-reachable client may register \
                     as a worker and receive user bearer tokens)"
                        .to_string()
                })
        },
    },
    ProductionRule {
        name: "no-unauthenticated-worker",
        summary: "`worker.allow_unauthenticated` must be false (a worker without a secret accepts scan tickets and credential refreshes from any TCP-reachable client).",
        check: |c| {
            c.worker.allow_unauthenticated.then(|| {
                "production_mode: worker.allow_unauthenticated must be false (a worker \
                 without a secret accepts scan tickets and credential refreshes from any \
                 TCP-reachable client, leaking user S3 credentials)"
                    .to_string()
            })
        },
    },
    ProductionRule {
        name: "max-sql-bytes",
        summary: "`query.max_sql_bytes` must be > 0 (0 disables the pre-parse statement size cap).",
        check: |c| {
            (c.query.max_sql_bytes == 0).then(|| {
                "production_mode: query.max_sql_bytes must be > 0 \
                 (0 disables the pre-parse statement size cap and lets a \
                 client burn coordinator memory/CPU on an arbitrarily large \
                 SQL string)"
                    .to_string()
            })
        },
    },
    ProductionRule {
        name: "explicit-distributed-credentials",
        summary: "`coordinator.distributed_credentials` must be set explicitly when workers are configured (`auto` ships the shared `[storage]` keys to workers for tables without vended credentials).",
        check: |c| {
            (c.coordinator.distributed_credentials == DistributedCredentialMode::Auto
                && !c.coordinator.worker_urls.is_empty())
                .then(|| {
                    "production_mode: coordinator.distributed_credentials must be set \
                     explicitly (vended, static or worker_default); `auto` silently \
                     ships the shared [storage] key pair to workers when a table has \
                     no vended credentials (finding 401)"
                        .to_string()
                })
        },
    },
    ProductionRule {
        name: "vended-credentials-required",
        summary: "`require_vended_credentials = true` on `[catalog]` and every REST `[catalogs.*]` block (otherwise FileIO falls back to the shared `[storage]` S3 keys).",
        check: |c| {
            let rest_missing_vended = |cat: &CatalogConfig| {
                matches!(cat.backend, CatalogBackend::Rest) && !cat.require_vended_credentials
            };
            (rest_missing_vended(&c.catalog) || c.catalogs.values().any(rest_missing_vended)).then(
                || {
                    "production_mode: catalog.require_vended_credentials must be true \
                     on every REST catalog (otherwise FileIO falls back to the shared \
                     [storage] S3 keys after LOAD_TABLE and Polaris table-data-read \
                     is not the data gate). Set it on [catalog] and every REST \
                     [catalogs.*] block."
                        .to_string()
                },
            )
        },
    },
    // Architecture review 212 (#565): the default engine enforces nothing.
    // Every other dev-only default is refused here or sits behind an
    // `allow_*` flag; this one used to start silently. `in_memory` is a test
    // store that starts empty and is lost on restart, so it counts as none.
    ProductionRule {
        name: "policy-engine-configured",
        summary: "`policy.engine = passthrough` or `in_memory` is refused unless `security.allow_no_policy_engine = true` (no row filters, masks or column restrictions).",
        check: |c| {
            let engine = match c.policy.engine {
                PolicyEngine::Passthrough => "passthrough",
                PolicyEngine::InMemory => "in_memory",
                PolicyEngine::Ranger => return None,
            };
            (!c.security.allow_no_policy_engine).then(|| {
                format!(
                    "production_mode: policy.engine is {engine} (no persistent row filters, \
                     masks or column restrictions; only catalog-level authorization \
                     applies). Set policy.engine = \"ranger\", or set \
                     security.allow_no_policy_engine = true to acknowledge running without \
                     a policy engine."
                )
            })
        },
    },
    ProductionRule {
        name: "grant-backend-configured",
        summary: "`access_control.backend = none` is refused unless `security.allow_no_grant_backend = true` (GRANT / REVOKE would be rejected).",
        check: |c| {
            (c.access_control.backend == AccessControlBackend::None
                && !c.security.allow_no_grant_backend)
                .then(|| {
                    "production_mode: access_control.backend is none (GRANT / REVOKE are \
                     rejected; privileges can only be managed outside SQE). Set \
                     access_control.backend to chameleon, polaris or ranger, or set \
                     security.allow_no_grant_backend = true to acknowledge managing \
                     grants elsewhere."
                        .to_string()
                })
        },
    },
    ProductionRule {
        name: "ranger-mask-key",
        summary: "With `policy.engine = ranger`, `policy.mask_key` must be set unless `security.allow_unkeyed_hash_masks = true` (MASK_HASH would fall back to unsalted SHA-256).",
        check: |c| {
            (c.policy.engine == PolicyEngine::Ranger
                && c.policy.mask_key.is_empty()
                && !c.security.allow_unkeyed_hash_masks)
                .then(|| {
                    "production_mode: policy.engine is ranger but policy.mask_key is empty \
                     (Ranger MASK_HASH would fall back to unsalted SHA-256). Set \
                     policy.mask_key (or SQE_POLICY__MASK_KEY), or set \
                     security.allow_unkeyed_hash_masks = true to acknowledge unsalted hashes."
                        .to_string()
                })
        },
    },
    ProductionRule {
        name: "ranger-project-tags",
        summary: "With `policy.engine = ranger`, `policy.ranger.project-tags` must be true unless `security.allow_unprojected_tags = true` (Spark would return columns SQE masks).",
        check: |c| {
            (c.policy.engine == PolicyEngine::Ranger
                && !c.policy.ranger.project_tags
                && !c.security.allow_unprojected_tags)
                .then(|| {
                    "production_mode: policy.engine is ranger but policy.ranger.project-tags \
                     is false (SET TAG writes Iceberg properties that Spark/Kyuubi cannot \
                     see, so Spark returns raw columns SQE masks). Mixed SQE+Spark must set \
                     policy.ranger.project-tags = true, or set \
                     security.allow_unprojected_tags = true for SQE-only Ranger deploys."
                        .to_string()
                })
        },
    },
    ProductionRule {
        name: "ranger-groups-claim",
        summary: "With `policy.engine = ranger`, every `oidc_password` / `bearer_token` provider must set `groups_claim`, and the legacy `[auth]` block (no `auth.providers`) is refused, unless `security.allow_tokens_without_groups = true` (group grants and masks would match nobody).",
        check: |c| {
            if c.policy.engine != PolicyEngine::Ranger || c.security.allow_tokens_without_groups {
                return None;
            }
            let offenders = groupless_providers(c);
            (!offenders.is_empty()).then(|| {
                format!(
                    "production_mode: policy.engine is ranger but {} read no groups from the \
                     token, so every Ranger group grant, mask and row filter matches nobody. \
                     Set groups_claim (for example \"groups\") on each oidc_password / \
                     bearer_token provider, or set security.allow_tokens_without_groups = true \
                     when every Ranger item names users.",
                    offenders.join(", ")
                )
            })
        },
    },
    // Finding 751: three more dev-only switches that used to start silently.
    ProductionRule {
        name: "no-hmac-tickets",
        summary: "`worker.accept_hmac_tickets` must be false (the HMAC key is the shared worker secret, so it proves nothing about who authored a scan ticket).",
        check: |c| {
            c.worker.accept_hmac_tickets.then(|| {
                "production_mode: worker.accept_hmac_tickets must be false (the HMAC \
                 key is the shared worker_secret, so any secret holder can forge scan \
                 tickets; finish the Ed25519 rollout instead)"
                    .to_string()
            })
        },
    },
    ProductionRule {
        name: "verified-worker-advertise",
        summary: "`coordinator.allow_unverified_advertise` must be false (a secret holder could register an attacker URL and receive every user's scan tickets).",
        check: |c| {
            c.coordinator.allow_unverified_advertise.then(|| {
                "production_mode: coordinator.allow_unverified_advertise must be false \
                 (a worker_secret holder could advertise an attacker URL and receive \
                 every user's scan tickets and vended credentials)"
                    .to_string()
            })
        },
    },
    ProductionRule {
        name: "no-inline-tvf-credentials",
        summary: "`storage.tvf.allow_inline_credentials` must be false (caller-supplied object-store keys would bypass `allowed_object_store_prefixes`, turning the coordinator into an arbitrary fetcher).",
        check: |c| {
            c.storage.tvf.allow_inline_credentials.then(|| {
                "production_mode: storage.tvf.allow_inline_credentials must be false \
                 (it defaults to true; caller-supplied object-store keys bypass \
                 allowed_object_store_prefixes, so any user can turn the coordinator \
                 into an arbitrary fetcher)"
                    .to_string()
            })
        },
    },
    // The three MCP rules apply only when the MCP listener is enabled; the
    // shape checks on the same fields (absolute URL, path /mcp, issuer has a
    // host) run in every mode and live in `validate()`.
    ProductionRule {
        name: "mcp-https-resource-uri",
        summary: "`mcp.resource_uri` must use https when MCP is enabled.",
        check: |c| {
            if !c.mcp.is_enabled() {
                return None;
            }
            let url = url::Url::parse(&c.mcp.resource_uri).ok()?;
            (url.has_host()
                && url.path() == "/mcp"
                && url.query().is_none()
                && url.fragment().is_none()
                && url.scheme() != "https")
                .then(|| "mcp.resource_uri must use https in production_mode".to_string())
        },
    },
    ProductionRule {
        name: "mcp-https-authorization-servers",
        summary: "Every `mcp.authorization_servers` issuer must use https when MCP is enabled.",
        check: |c| {
            if !c.mcp.is_enabled() {
                return None;
            }
            let offenders: Vec<String> = c
                .mcp
                .authorization_servers
                .iter()
                .filter(|issuer| match url::Url::parse(issuer) {
                    Ok(url) => url.has_host() && url.fragment().is_none() && url.scheme() != "https",
                    Err(_) => false,
                })
                .map(|issuer| {
                    format!("mcp authorization server must use https in production_mode: {issuer}")
                })
                .collect();
            (!offenders.is_empty()).then(|| offenders.join("; "))
        },
    },
    ProductionRule {
        name: "https-telemetry-endpoints",
        summary: "OTLP, audit-export and OpenLineage endpoints must use https unless they are loopback (a sidecar collector): audit events and lineage carry user identities and SQL text.",
        check: |c| {
            let m = &c.metrics;
            let endpoints = [
                ("metrics.otlp_endpoint", m.otlp_endpoint.as_str(), true),
                ("metrics.traces_otlp_endpoint", m.traces_otlp_endpoint.as_str(), true),
                (
                    "metrics.audit_export.otlp_endpoint",
                    m.audit_export.otlp_endpoint.as_str(),
                    m.audit_export.enabled,
                ),
                (
                    "metrics.openlineage.http_endpoint",
                    m.openlineage.http_endpoint.as_str(),
                    m.openlineage.enabled,
                ),
            ];
            let offenders: Vec<&str> = endpoints
                .iter()
                .filter(|(_, url, active)| *active && is_plaintext_remote(url))
                .map(|(name, _, _)| *name)
                .collect();
            (!offenders.is_empty()).then(|| {
                format!(
                    "production_mode: {} must use https (plain http only to a loopback sidecar collector)",
                    offenders.join(", ")
                )
            })
        },
    },
    ProductionRule {
        name: "ranger-grant-backend-url",
        summary: "`access_control.backend = ranger` with an empty `access_control.url` (the grant backend would silently be absent and GRANT / REVOKE NotImplemented).",
        check: |c| {
            (c.access_control.backend == AccessControlBackend::Ranger
                && c.access_control.url.trim().is_empty())
            .then(|| {
                "production_mode: access_control.backend is ranger but access_control.url is \
                 empty (no grant backend would be built; GRANT / REVOKE would be \
                 NotImplemented). Set access_control.url to the Ranger Admin base URL."
                    .to_string()
            })
        },
    },
    ProductionRule {
        name: "mcp-no-anonymous-auth",
        summary: "`mcp.auth.mode = anonymous` is refused when MCP is enabled.",
        check: |c| {
            (c.mcp.is_enabled() && c.mcp.auth.mode == McpAuthMode::Anonymous).then(|| {
                "mcp.auth.mode = \"anonymous\" is not allowed under production_mode".to_string()
            })
        },
    },
];

/// The auth sources that leave a session with no groups (issue #899): each
/// `oidc_password` / `bearer_token` provider with an empty `groups_claim`, as
/// `auth.providers[i] (<type>)`, or the legacy `[auth]` block when
/// `auth.providers` is empty (it never reads groups). Providers without a
/// `groups_claim` field are not counted; they cannot be fixed by setting one.
fn groupless_providers(c: &SqeConfig) -> Vec<String> {
    if c.auth.providers.is_empty() {
        return vec!["the legacy [auth] block (no auth.providers)".to_string()];
    }
    c.auth
        .providers
        .iter()
        .enumerate()
        .filter_map(|(i, p)| match p {
            AuthProviderConfig::OidcPassword { groups_claim, .. } if groups_claim.is_empty() => {
                Some(format!("auth.providers[{i}] (oidc_password)"))
            }
            AuthProviderConfig::BearerToken { groups_claim, .. } if groups_claim.is_empty() => {
                Some(format!("auth.providers[{i}] (bearer_token)"))
            }
            _ => None,
        })
        .collect()
}

/// Every rule `config` violates, in table order. Empty means production mode
/// is satisfied.
/// `http://` to anything but loopback. Empty or unparseable values are left to
/// the other validators; `https://` and a sidecar on `localhost` pass.
fn is_plaintext_remote(endpoint: &str) -> bool {
    let Ok(url) = url::Url::parse(endpoint) else {
        return false;
    };
    if url.scheme() != "http" {
        return false;
    }
    match url.host() {
        Some(url::Host::Domain(d)) => d != "localhost",
        Some(url::Host::Ipv4(ip)) => !ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => !ip.is_loopback(),
        None => false,
    }
}

/// Every rule `config` breaks, each message prefixed with its rule name in
/// brackets, so a startup error names the rule an operator can look up in
/// the production guide.
pub fn violations(config: &SqeConfig) -> Vec<String> {
    PRODUCTION_RULES
        .iter()
        .filter_map(|rule| (rule.check)(config).map(|msg| format!("[{}] {msg}", rule.name)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// The production guide is generated by hand from this table; keep the
    /// two in step. Every rule name must appear as a `` `name` `` row key in
    /// the guide's "What production mode enforces" table, and the table must
    /// not name a rule that no longer exists.
    #[test]
    fn production_guide_lists_every_rule_and_nothing_else() {
        let guide = include_str!(
            "../../../../docs/site/book/src/operations/production-guide.md"
        );
        let section = guide
            .split("## What production mode enforces")
            .nth(1)
            .expect("production-guide.md has a 'What production mode enforces' section");
        let section = section.split("\n## ").next().unwrap_or(section);
        let documented: BTreeSet<&str> = section
            .lines()
            .filter(|line| line.starts_with("| `"))
            .filter_map(|line| line.trim_start_matches("| `").split('`').next())
            .collect();
        let coded: BTreeSet<&str> = PRODUCTION_RULES.iter().map(|r| r.name).collect();
        assert_eq!(
            documented, coded,
            "production-guide.md rows and PRODUCTION_RULES differ (left: docs, right: code)"
        );
    }

    #[test]
    fn telemetry_endpoints_need_https_unless_loopback() {
        // `load_from_str` reads the `SQE_*` env overlay; tests in
        // `config::tests` set those variables under this lock.
        let _g = super::super::tests::ENV_LOCK
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let rule = PRODUCTION_RULES
            .iter()
            .find(|r| r.name == "https-telemetry-endpoints")
            .unwrap();
        let mut c = SqeConfig::load_from_str(
            "[coordinator]\n[auth]\nclient_id = \"sqe-client\"\nkeycloak_url = \"https://kc.example.com\"\n\
             [catalog]\ncatalog_url = \"https://polaris.example.com\"\nwarehouse = \"wh\"\n",
        )
        .expect("minimal config");
        assert!((rule.check)(&c).is_none(), "empty endpoints pass");

        c.metrics.otlp_endpoint = "http://otel-collector:4317".into();
        let msg = (rule.check)(&c).expect("remote http is refused");
        assert!(msg.contains("metrics.otlp_endpoint"), "{msg}");

        c.metrics.otlp_endpoint = "https://otel-collector:4317".into();
        assert!((rule.check)(&c).is_none());
        c.metrics.otlp_endpoint = "http://localhost:4317".into();
        assert!((rule.check)(&c).is_none(), "sidecar on localhost passes");
        c.metrics.otlp_endpoint = "http://127.0.0.1:4317".into();
        assert!((rule.check)(&c).is_none());

        // Audit export only counts when it is on.
        c.metrics.audit_export.otlp_endpoint = "http://siem:4317".into();
        c.metrics.audit_export.enabled = false;
        assert!((rule.check)(&c).is_none());
        c.metrics.audit_export.enabled = true;
        assert!((rule.check)(&c)
            .unwrap()
            .contains("metrics.audit_export.otlp_endpoint"));
    }

    /// The guide's minimal Ranger config passes every rule, and the same file
    /// with `passthrough` fails naming `policy-engine-configured` (#898).
    #[test]
    fn production_guide_minimal_ranger_config_validates() {
        let _g = super::super::tests::ENV_LOCK
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let guide = include_str!("../../../../docs/site/book/src/operations/production-guide.md");
        let toml = guide
            .split("## Minimal production config with Ranger")
            .nth(1)
            .and_then(|s| s.split("```toml\n").nth(1))
            .and_then(|s| s.split("```").next())
            .expect("production-guide.md has the minimal Ranger config block");
        // validate() checks the certificate files exist; point them at real ones.
        let dir = std::env::temp_dir().join(format!("sqe-898-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        for f in ["tls.crt", "tls.key"] {
            std::fs::write(dir.join(f), "").unwrap();
        }
        let toml = toml.replace("/etc/sqe/tls", &dir.to_string_lossy());

        let c = SqeConfig::load_from_str(&toml).expect("the documented config parses");
        assert!(c.coordinator.production_mode);
        assert!(c.policy.ranger.catalog_scoped);
        if let Err(e) = c.validate() {
            panic!("the documented config must start in production_mode: {e}");
        }

        let passthrough = toml.replace("engine = \"ranger\"", "engine = \"passthrough\"");
        let c = SqeConfig::load_from_str(&passthrough).expect("passthrough variant parses");
        let err = c
            .validate()
            .expect_err("passthrough is refused")
            .to_string();
        assert!(
            err.contains(
                "[policy-engine-configured] production_mode: policy.engine is passthrough"
            ),
            "{err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rule_names_are_unique() {
        let names: BTreeSet<&str> = PRODUCTION_RULES.iter().map(|r| r.name).collect();
        assert_eq!(names.len(), PRODUCTION_RULES.len());
    }
}
