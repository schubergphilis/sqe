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
    // `allow_*` flag; this one used to start silently.
    ProductionRule {
        name: "policy-engine-configured",
        summary: "`policy.engine = passthrough` is refused unless `security.allow_no_policy_engine = true` (no row filters, masks or column restrictions).",
        check: |c| {
            (c.policy.engine == PolicyEngine::Passthrough && !c.security.allow_no_policy_engine)
                .then(|| {
                    "production_mode: policy.engine is passthrough (no row filters, masks or \
                     column restrictions; only catalog-level authorization applies). Set \
                     policy.engine = \"ranger\" (or \"in_memory\" for tests), or set \
                     security.allow_no_policy_engine = true to acknowledge running without \
                     a policy engine."
                        .to_string()
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

/// Every rule `config` violates, in table order. Empty means production mode
/// is satisfied.
pub fn violations(config: &SqeConfig) -> Vec<String> {
    PRODUCTION_RULES
        .iter()
        .filter_map(|rule| (rule.check)(config))
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
    fn rule_names_are_unique() {
        let names: BTreeSet<&str> = PRODUCTION_RULES.iter().map(|r| r.name).collect();
        assert_eq!(names.len(), PRODUCTION_RULES.len());
    }
}
