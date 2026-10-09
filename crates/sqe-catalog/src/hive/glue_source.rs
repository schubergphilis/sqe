//! [`HiveMetadataSource`] backed by the AWS Glue Data Catalog.
//!
//! Glue already is the metastore, so SQE reads it rather than keeping a copy.
//! The mapping is mechanical because `manifest.rs` models Glue's `TableInput`
//! and `StorageDescriptor` field for field, which is what it was built for.
//!
//! Distinct from `--catalog-backend glue`, which builds an *Iceberg* Glue
//! catalog. That one resolves a table through `Parameters.metadata_location`
//! and cannot see a Hive external table at all; against a catalog of them it
//! fails the whole attach rather than returning nothing.
//!
//! ## Read-only, and lazy
//!
//! No `put`/`delete`: SQE does not own this catalog. And a table is fetched
//! only when something names it. Enumerating a namespace and loading every
//! table in it is what makes the Iceberg provider fail whole-catalog on one
//! unreadable table, and a real catalog always has one.

use std::collections::BTreeMap;

use async_trait::async_trait;
use aws_sdk_glue::types::{Column as GlueSdkColumn, StorageDescriptor as GlueSdkSd, Table};

use super::manifest::{
    GlueColumn, HiveManifest, ManifestLimits, SerdeInfo, SqeExtensions, StorageDescriptor,
    MANIFEST_VERSION,
};
use super::source::HiveMetadataSource;
use super::HiveError;

/// Reads table metadata straight out of Glue.
pub struct GlueMetadataSource {
    client: aws_sdk_glue::Client,
    /// Glue `CatalogId`, i.e. the owning account. `None` means the caller's
    /// own account, which is what almost every deployment wants.
    catalog_id: Option<String>,
    /// When non-empty, the root is restricted to these databases: only they
    /// are listed, and a table in any other database is not found. Empty
    /// exposes the whole catalog.
    databases: Vec<String>,
    limits: ManifestLimits,
    /// The region the client signs for, named in credential errors so an
    /// operator can tell "no credentials" from "credentials for the wrong
    /// account or region" without reading the SDK's source chain.
    region: Option<String>,
}

impl std::fmt::Debug for GlueMetadataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlueMetadataSource")
            .field("catalog_id", &self.catalog_id)
            .finish()
    }
}

impl GlueMetadataSource {
    pub fn new(
        client: aws_sdk_glue::Client,
        catalog_id: Option<String>,
        databases: Vec<String>,
        limits: ManifestLimits,
    ) -> Self {
        let region = client.config().region().map(|r| r.to_string());
        // Glue stores database names lower-cased, whatever case the console
        // or the DDL used, so a scope written as `Claude` would list a
        // schema no query could resolve and hide the real `claude`. Folded
        // once here; every comparison below is exact against these.
        let mut databases: Vec<String> = databases
            .into_iter()
            .map(|d| d.trim().to_ascii_lowercase())
            .filter(|d| !d.is_empty())
            .collect();
        databases.sort();
        databases.dedup();
        Self {
            client,
            catalog_id,
            databases,
            limits,
            region,
        }
    }

    /// Map one failed SDK call. Credential failures get their own variant
    /// and a message that names the cause and the region; everything else
    /// keeps the operation name and the SDK's full source chain, because
    /// `SdkError`'s own `Display` is the bare kind ("dispatch failure") and
    /// the reason lives two levels down.
    fn glue_err<E, R>(&self, op: &str, e: aws_sdk_glue::error::SdkError<E, R>) -> HiveError
    where
        E: std::error::Error + 'static,
        R: std::fmt::Debug + 'static,
    {
        classify_glue_error(op, self.region.as_deref(), &format!("{e:?}"), &error_chain(&e))
    }

    /// `true` when `database` is outside the configured allow-list. An empty
    /// list allows everything.
    fn database_blocked(&self, database: &str) -> bool {
        !self.databases.is_empty()
            && !self
                .databases
                .iter()
                .any(|d| d.eq_ignore_ascii_case(database))
    }

    /// The configured scope, lower-cased and deduplicated. Empty means the
    /// whole catalog.
    pub fn scope(&self) -> &[String] {
        &self.databases
    }

    /// One paginated `GetTables` for `database`, mapped to manifests. Backs
    /// both `list_tables` and `list_manifests`.
    async fn list_glue_manifests(&self, database: &str) -> Result<Vec<HiveManifest>, HiveError> {
        // A database outside the allow-list lists no tables, the same empty
        // answer an unknown database gives.
        if self.database_blocked(database) {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let mut pages = self
            .client
            .get_tables()
            .set_catalog_id(self.catalog_id.clone())
            .database_name(database)
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            let page = match page {
                Ok(p) => p,
                // DataFusion asks about schemas it has only seen named, so an
                // unknown database is an empty list. Erroring here fails the
                // whole query over a namespace nobody selected from.
                Err(e) if is_not_found(&e) => return Ok(Vec::new()),
                Err(e) => return Err(self.glue_err("get_tables", e)),
            };
            out.extend(
                page.table_list()
                    .iter()
                    .map(|t| manifest_from_glue_table(database, t)),
            );
        }
        Ok(out)
    }

    /// Build a client from the ambient AWS configuration: the same provider
    /// chain the AWS CLI uses, so `AWS_PROFILE`, SSO, an instance profile and
    /// a task role all work, and credentials refresh rather than being
    /// captured once.
    pub async fn from_env(
        region: Option<&str>,
        catalog_id: Option<String>,
        databases: Vec<String>,
        limits: ManifestLimits,
        timeout: Option<std::time::Duration>,
    ) -> Self {
        // The shared loader carries the ring HTTPS client; a bare
        // `aws_config::defaults` has none and panics on the first request.
        let mut loader = crate::aws_config::sdk_config_loader();
        if let Some(region) = region.filter(|r| !r.is_empty()) {
            loader = loader.region(aws_config::Region::new(region.to_string()));
        }
        let conf = loader.load().await;
        let mut builder = aws_sdk_glue::config::Builder::from(&conf);
        // Bound the SDK's own connect and operation clocks too (issue #666):
        // the `TimeoutSource` wrapper cuts the await, but an SDK call with
        // no timeout of its own would keep a connection and a retry loop
        // alive underneath it.
        if let Some(t) = timeout {
            builder = builder.timeout_config(
                aws_sdk_glue::config::timeout::TimeoutConfig::builder()
                    .connect_timeout(t.min(std::time::Duration::from_secs(5)))
                    .operation_attempt_timeout(t)
                    .operation_timeout(t)
                    .build(),
            );
        }
        Self::new(
            aws_sdk_glue::Client::from_conf(builder.build()),
            catalog_id,
            databases,
            limits,
        )
    }
}

fn column_of(c: &GlueSdkColumn) -> GlueColumn {
    GlueColumn {
        name: c.name().to_string(),
        r#type: c.r#type().unwrap_or("string").to_string(),
        comment: c.comment().map(str::to_string),
        // Carries `sqe.column-tags`; see `HiveManifest::column_tags`.
        parameters: c
            .parameters()
            .map(|p| {
                p.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default(),
    }
}

fn storage_descriptor_of(sd: Option<&GlueSdkSd>) -> StorageDescriptor {
    let Some(sd) = sd else {
        return StorageDescriptor {
            location: String::new(),
            input_format: None,
            output_format: None,
            compressed: false,
            serde_info: SerdeInfo::default(),
            columns: Vec::new(),
            sort_columns: Vec::new(),
        };
    };
    let serde_info = sd
        .serde_info()
        .map(|si| SerdeInfo {
            serialization_library: si.serialization_library().map(str::to_string),
            parameters: si
                .parameters()
                .map(|p| {
                    p.iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<BTreeMap<_, _>>()
                })
                .unwrap_or_default(),
        })
        .unwrap_or_default();

    StorageDescriptor {
        // A trailing slash, so a prefix listing does not also match a
        // sibling whose name merely starts with this one.
        location: match sd.location() {
            Some(l) if !l.is_empty() => format!("{}/", l.trim_end_matches('/')),
            _ => String::new(),
        },
        input_format: sd.input_format().map(str::to_string),
        output_format: sd.output_format().map(str::to_string),
        compressed: sd.compressed(),
        serde_info,
        columns: sd.columns().iter().map(column_of).collect(),
        sort_columns: Vec::new(),
    }
}

/// Glue's `Table` as a [`HiveManifest`].
///
/// Partition keys stay in their own list rather than being folded into
/// `columns`: Glue never puts them in the `StorageDescriptor`, and the
/// provider appends them at the tail of the query schema itself.
pub fn manifest_from_glue_table(database: &str, t: &Table) -> HiveManifest {
    HiveManifest {
        manifest_version: MANIFEST_VERSION,
        table_type: t
            .table_type()
            .unwrap_or("EXTERNAL_TABLE")
            .to_string(),
        database_name: database.to_string(),
        name: t.name().to_string(),
        owner: t.owner().map(str::to_string),
        created_at: t.create_time().map(|d| d.to_string()),
        description: t.description().map(str::to_string),
        view_original_text: t.view_original_text().map(str::to_string),
        partition_keys: t.partition_keys().iter().map(column_of).collect(),
        // Carries projection.* verbatim, which is what makes a projected
        // table readable without any partitions registered in Glue.
        parameters: t
            .parameters()
            .map(|p| {
                p.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default(),
        storage_descriptor: storage_descriptor_of(t.storage_descriptor()),
        // Glue has no SQE block. Column tags are not copied in here: they
        // stay in the table and column `parameters` carried above, and
        // `HiveManifest::column_tags` reads them from there.
        sqe: SqeExtensions::default(),
    }
}

/// Every message in an error's `source()` chain, outermost first. The SDK
/// renders `SdkError::DispatchFailure` as "dispatch failure" and puts the
/// reason ("no credentials in the property bag", "the SSO session has
/// expired") in the sources, so this is what the operator has to see.
fn error_chain(e: &dyn std::error::Error) -> Vec<String> {
    let mut out = vec![e.to_string()];
    let mut cur = e.source();
    while let Some(next) = cur {
        out.push(next.to_string());
        cur = next.source();
    }
    out
}

/// The SDK error shapes that mean "the engine's AWS credentials are not
/// usable", matched on the `Debug` rendering (the service error code) and
/// the source-chain text (the credential provider's own message).
///
/// - `DispatchFailure` from a `User` connector error whose chain talks about
///   credentials, a token or a provider: no credentials at all, an SSO
///   session that ended, a `credential_process` that failed.
/// - `UnrecognizedClientException` / `InvalidSignatureException` /
///   `ExpiredTokenException` / `InvalidClientTokenId`: the request was signed
///   with a key STS no longer knows, which is what a stale
///   `AWS_SESSION_TOKEN` produces.
///
/// `AccessDeniedException` is deliberately not here: valid credentials that
/// lack `glue:GetTables` is a permission answer, not a credential one.
fn is_credential_failure(debug: &str, chain: &[String]) -> bool {
    let lower = debug.to_ascii_lowercase();
    if lower.contains("unrecognizedclientexception")
        || lower.contains("invalidsignatureexception")
        || lower.contains("expiredtokenexception")
        || lower.contains("invalidclienttokenid")
        || lower.contains("credentialsstale")
    {
        return true;
    }
    let dispatch = lower.starts_with("dispatchfailure");
    let text = chain.join(" | ").to_ascii_lowercase();
    dispatch
        && (text.contains("credential")
            || text.contains("no providers in chain")
            || text.contains("token")
            || text.contains("sso")
            || text.contains("expired"))
}

/// One failed Glue call as a [`HiveError`]. Pure over strings so the
/// classification has tests without an SDK client.
fn classify_glue_error(
    op: &str,
    region: Option<&str>,
    debug: &str,
    chain: &[String],
) -> HiveError {
    let detail = chain.join(": ");
    if is_credential_failure(debug, chain) {
        return HiveError::Credentials(format!(
            "region={}, op={op}: {detail}. Renew the engine's AWS session (aws sso login / \
             fresh AWS_* keys) and restart, or fix the credential chain it starts with",
            region.unwrap_or("unset")
        ));
    }
    HiveError::Store(format!("glue {op} failed: {detail}"))
}

#[async_trait]
impl HiveMetadataSource for GlueMetadataSource {
    async fn list_databases(&self) -> Result<Vec<String>, HiveError> {
        // With an allow-list, skip the catalog scan: the databases the
        // operator named are the answer. Each is checked against Glue once
        // per refresh, so a misspelt scope entry is a WARN at the first
        // listing rather than a schema that lists and answers nothing; the
        // check is skipped, not fatal, when Glue cannot be asked (the scope
        // is still served and the tables call surfaces the real error).
        if !self.databases.is_empty() {
            let mut present = Vec::with_capacity(self.databases.len());
            for name in &self.databases {
                match self
                    .client
                    .get_database()
                    .set_catalog_id(self.catalog_id.clone())
                    .name(name)
                    .send()
                    .await
                {
                    Ok(_) => present.push(name.clone()),
                    Err(e) if e.as_service_error().is_some_and(|s| s.is_entity_not_found_exception()) => {
                        tracing::warn!(
                            database = %name,
                            "glue: a database named in the scope (--glue-database / hive.roots.databases) \
                             does not exist in this catalog and is not listed"
                        );
                    }
                    Err(e) => {
                        let err = self.glue_err("get_database", e);
                        tracing::warn!(database = %name, error = %err, "glue: could not verify a scoped database; serving it unverified");
                        present.push(name.clone());
                    }
                }
            }
            return Ok(present);
        }
        let mut out = Vec::new();
        let mut pages = self
            .client
            .get_databases()
            .set_catalog_id(self.catalog_id.clone())
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|e| self.glue_err("get_databases", e))?;
            out.extend(page.database_list().iter().map(|d| d.name().to_string()));
        }
        Ok(out)
    }

    async fn list_tables(&self, database: &str) -> Result<Vec<String>, HiveError> {
        // `GetTables` returns whole `Table` objects either way, so the names
        // are the manifests' names; the paginated call is the cost, not the
        // mapping.
        Ok(self
            .list_glue_manifests(database)
            .await?
            .into_iter()
            .map(|m| m.name)
            .collect())
    }

    /// `GetTables` already carries every table's full definition, so the
    /// listing IS the bulk fetch: one paginated call per database instead of
    /// one `GetTable` per table (issue #666).
    async fn list_manifests(
        &self,
        database: &str,
    ) -> Result<Option<Vec<HiveManifest>>, HiveError> {
        self.list_glue_manifests(database).await.map(Some)
    }

    async fn get(&self, database: &str, table: &str) -> Result<HiveManifest, HiveError> {
        // A database outside the allow-list is not found, so a direct
        // `SELECT FROM glue.other_db.t` cannot reach past the scoping.
        if self.database_blocked(database) {
            return Err(HiveError::NotFound(format!("{database}.{table}")));
        }
        let resp = self
            .client
            .get_table()
            .set_catalog_id(self.catalog_id.clone())
            .database_name(database)
            .name(table)
            .send()
            .await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) if is_not_found(&e) => {
                return Err(HiveError::NotFound(format!("{database}.{table}")))
            }
            Err(e) => return Err(self.glue_err("get_table", e)),
        };
        let t = resp
            .table()
            .ok_or_else(|| HiveError::NotFound(format!("{database}.{table}")))?;
        Ok(manifest_from_glue_table(database, t))
    }

    /// Glue exposes no change token cheaper than the fetch itself, so this
    /// always reports "cannot tell" and freshness comes from the cache TTL.
    /// `UpdateTime` would serve, but reading it means `GetTable`, which is
    /// the very call the token exists to avoid.
    async fn etag(&self, _database: &str, _table: &str) -> Result<Option<String>, HiveError> {
        Ok(None)
    }

    fn limits(&self) -> &ManifestLimits {
        &self.limits
    }
}

fn is_not_found<E: std::fmt::Debug>(e: &E) -> bool {
    // The SDK models EntityNotFound per-operation, so matching the rendered
    // error keeps one predicate for every call rather than three.
    format!("{e:?}").contains("EntityNotFound")
}

#[cfg(test)]
mod scope_tests {
    use super::*;

    fn source(databases: &[&str]) -> GlueMetadataSource {
        // No HTTP client and no credentials: nothing here sends a request.
        let conf = aws_sdk_glue::Config::builder()
            .behavior_version_latest()
            .region(aws_config::Region::new("eu-central-1"))
            .build();
        GlueMetadataSource::new(
            aws_sdk_glue::Client::from_conf(conf),
            None,
            databases.iter().map(|d| d.to_string()).collect(),
            ManifestLimits::default(),
        )
    }

    /// Glue lower-cases database names; the scope is folded to match, so
    /// `--glue-database Claude,afas_raw` scopes to `claude` and `afas_raw`
    /// and a lookup in either case is answered.
    #[test]
    fn the_scope_is_lower_cased_trimmed_and_deduplicated() {
        let s = source(&["Claude", " afas_raw ", "claude", ""]);
        assert_eq!(s.scope(), ["afas_raw", "claude"]);
        assert!(!s.database_blocked("claude"));
        assert!(!s.database_blocked("CLAUDE"));
        assert!(!s.database_blocked("afas_raw"));
        assert!(s.database_blocked("usage_data_raw"));
    }

    #[test]
    fn an_empty_scope_blocks_nothing() {
        let s = source(&[]);
        assert!(s.scope().is_empty());
        assert!(!s.database_blocked("anything"));
    }
}

#[cfg(test)]
mod column_tag_tests {
    use super::*;
    use crate::hive::manifest::COLUMN_TAGS_KEY;

    fn column(name: &str, tags: Option<&str>) -> GlueSdkColumn {
        let mut b = GlueSdkColumn::builder().name(name).r#type("string");
        if let Some(tags) = tags {
            b = b.parameters(COLUMN_TAGS_KEY, tags);
        }
        b.build().expect("column")
    }

    fn table(columns: Vec<GlueSdkColumn>, partition_keys: Vec<GlueSdkColumn>) -> Table {
        Table::builder()
            .name("customers")
            .storage_descriptor(
                GlueSdkSd::builder()
                    .location("s3://bucket/crm/customers")
                    .set_columns(Some(columns))
                    .build(),
            )
            .set_partition_keys(Some(partition_keys))
            .build()
            .expect("table")
    }

    /// The #702 bypass: the Glue mapping dropped column parameters and set
    /// an empty SQE block, so a column tagged in Glue read as untagged and
    /// its tag mask never applied.
    #[test]
    fn a_glue_column_tag_reaches_the_manifest() {
        let t = table(
            vec![column("id", None), column("ssn", Some(r#"["PII","GDPR"]"#))],
            vec![column("region", Some(r#"["RESIDENCY"]"#))],
        );
        let tags = manifest_from_glue_table("crm", &t)
            .column_tags()
            .expect("well-formed tags");
        assert_eq!(
            tags,
            BTreeMap::from([
                (
                    "ssn".to_string(),
                    vec!["PII".to_string(), "GDPR".to_string()]
                ),
                ("region".to_string(), vec!["RESIDENCY".to_string()]),
            ])
        );
    }

    #[test]
    fn a_glue_table_parameter_tag_map_is_read_like_an_iceberg_property() {
        let t = Table::builder()
            .name("customers")
            .storage_descriptor(GlueSdkSd::builder().columns(column("ssn", None)).build())
            .parameters(COLUMN_TAGS_KEY, r#"{"ssn":["PII"]}"#)
            .build()
            .expect("table");
        let tags = manifest_from_glue_table("crm", &t)
            .column_tags()
            .expect("ok");
        assert_eq!(tags.get("ssn"), Some(&vec!["PII".to_string()]));
    }

    #[test]
    fn an_untagged_glue_table_is_known_untagged() {
        let t = table(vec![column("id", None)], Vec::new());
        assert!(manifest_from_glue_table("crm", &t)
            .column_tags()
            .expect("known")
            .is_empty());
    }

    /// A tag value that does not parse is unknown tag state, not "no tags":
    /// reading it as untagged would unmask the very column it tags.
    #[test]
    fn a_malformed_glue_column_tag_is_unknown_not_untagged() {
        for bad in ["PII", "PII,GDPR", r#"{"ssn":["PII"]}"#, "[1]"] {
            let t = table(vec![column("ssn", Some(bad))], Vec::new());
            assert!(
                manifest_from_glue_table("crm", &t).column_tags().is_err(),
                "`{bad}` must be unknown tag state"
            );
        }
    }
}

#[cfg(test)]
mod error_classification_tests {
    use super::*;

    fn chain(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_dispatch_failure_about_credentials_names_the_cause_and_region() {
        // What the SDK produces when the SSO session is over: Display is
        // "dispatch failure", the reason is two sources down.
        let err = classify_glue_error(
            "get_databases",
            Some("eu-central-1"),
            "DispatchFailure(DispatchFailure { source: ConnectorError { kind: User, .. } })",
            &chain(&[
                "dispatch failure",
                "user error",
                "failed to load credentials: the SSO session has expired",
            ]),
        );
        let text = err.to_string();
        assert!(matches!(err, HiveError::Credentials(_)), "{text}");
        assert!(text.contains("AWS credentials missing or expired for the Glue catalog"), "{text}");
        assert!(text.contains("region=eu-central-1"), "{text}");
        assert!(text.contains("op=get_databases"), "{text}");
        assert!(text.contains("SSO session has expired"), "{text}");
        assert!(text.contains("aws sso login"), "{text}");
    }

    #[test]
    fn no_credentials_in_the_chain_is_a_credential_failure_too() {
        let err = classify_glue_error(
            "get_table",
            None,
            "DispatchFailure(DispatchFailure { source: ConnectorError { kind: User, .. } })",
            &chain(&["dispatch failure", "user error", "no providers in chain provided credentials"]),
        );
        assert!(matches!(err, HiveError::Credentials(_)), "{err}");
        assert!(err.to_string().contains("region=unset"), "{err}");
    }

    #[test]
    fn a_stale_session_token_rejected_by_sts_is_a_credential_failure() {
        for code in [
            "UnrecognizedClientException",
            "ExpiredTokenException",
            "InvalidSignatureException",
        ] {
            let err = classify_glue_error(
                "get_tables",
                Some("eu-west-1"),
                &format!("ServiceError(ServiceError {{ source: Unhandled(Unhandled {{ code: {code} }}) }})"),
                &chain(&["service error", "unhandled error", "The security token included in the request is invalid"]),
            );
            assert!(matches!(err, HiveError::Credentials(_)), "{code}: {err}");
        }
    }

    #[test]
    fn a_network_dispatch_failure_and_a_permission_denial_stay_store_errors_with_the_chain() {
        let io = classify_glue_error(
            "get_databases",
            Some("eu-central-1"),
            "DispatchFailure(DispatchFailure { source: ConnectorError { kind: Io, .. } })",
            &chain(&["dispatch failure", "io error", "connection refused"]),
        );
        assert!(matches!(io, HiveError::Store(_)), "{io}");
        assert_eq!(
            io.to_string(),
            "hive_external: object store error: glue get_databases failed: dispatch failure: io error: connection refused"
        );

        // Valid credentials without glue:GetTables: a permission answer.
        let denied = classify_glue_error(
            "get_tables",
            Some("eu-central-1"),
            "ServiceError(ServiceError { source: AccessDeniedException(AccessDeniedException { .. }) })",
            &chain(&["service error", "User is not authorized to perform: glue:GetTables"]),
        );
        assert!(matches!(denied, HiveError::Store(_)), "{denied}");
        assert!(denied.to_string().contains("not authorized to perform"), "{denied}");
    }
}
