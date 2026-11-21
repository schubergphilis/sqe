//! The access-control family: GRANT / REVOKE (including `MASKED WITH` and
//! `ROWS WHERE`), `SHOW GRANTS`, `SHOW EFFECTIVE GRANTS`, `CHECK ACCESS`,
//! `SHOW EFFECTIVE POLICY`, `SHOW MASKING POLICIES` and the `CREATE POLICY` /
//! `DROP POLICY` DDL, plus the policy-cache flush they share.

use super::*;

impl QueryHandler {
    // ── Access control handlers ──────────────────────────────────────────

    /// Flush the policy decision cache after a policy-mutating statement so
    /// the change takes effect on the next query instead of lingering until
    /// the cache TTL elapses (default 60s, `[policy] cache_ttl_secs`).
    /// Issue #207.
    ///
    /// No-op when no `PolicyStore` is wired (the current production default;
    /// the OPA enforcer is gated behind AUTH-01). When OPA IS wired, the
    /// SAME `Arc<dyn PolicyStore>` must back both the `PolicyPlanRewriter`
    /// enforcer on the read path AND this `policy_store` field, otherwise the
    /// flush hits a different cache than the one the read path consults.
    pub(super) fn invalidate_policy_cache(&self) {
        if let Some(store) = &self.policy_store {
            store.invalidate_all();
        }
    }

    /// Return the grant backend or a `NotImplemented` error.
    fn require_grant_backend(&self) -> sqe_core::Result<&dyn GrantBackend> {
        self.grant_backend
            .as_deref()
            .ok_or_else(|| {
                SqeError::NotImplemented(
                    "Access control is not configured. Set [access_control] backend and url in the config."
                        .to_string(),
                )
            })
    }

    /// Extract a `GrantStatement` from a sqlparser `Statement::Grant` or `Statement::Revoke`.
    pub(super) fn extract_grant_statement(stmt: &Statement) -> sqe_core::Result<GrantStatement> {
        // sqlparser 0.62: ObjectName holds `Vec<ObjectNamePart>` rather than
        // `Vec<Ident>`; pull the bare identifier value out of each part.
        fn object_name_parts(name: &sqlparser::ast::ObjectName) -> Vec<String> {
            name.0
                .iter()
                .filter_map(|p| p.as_ident())
                .map(|id| id.value.clone())
                .collect()
        }

        let with_grant_option = match stmt {
            Statement::Grant(g) => g.with_grant_option,
            // REVOKE has no WITH GRANT OPTION; `REVOKE GRANT OPTION FOR` is a
            // separate form sqlparser models elsewhere and SQE does not map it.
            _ => false,
        };
        // DENY carries `objects` unconditionally where GRANT/REVOKE make it
        // optional, so it is normalised to Some here.
        let deny_objects = match stmt {
            Statement::Deny(d) => Some(d.objects.clone()),
            _ => None,
        };
        let (privileges, objects, grantees) = match stmt {
            Statement::Grant(g) => (&g.privileges, &g.objects, &g.grantees),
            Statement::Revoke(r) => (&r.privileges, &r.objects, &r.grantees),
            Statement::Deny(d) => (&d.privileges, &deny_objects, &d.grantees),
            other => {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Expected GRANT/REVOKE/DENY statement, got: {other}"),
                ));
            }
        };

        let privilege = format!("{privileges}");

        // Views use the SAME resource shape as tables (the view name goes in the
        // `table` slot); only the access-type set differs. Verified against a
        // live Polaris 1.6: `view-properties-read` + `view-list` on
        // `{catalog, namespace, table: <view>}` lets the grantee load the view.
        let object = match objects {
            Some(
                sqlparser::ast::GrantObjects::Views(_)
                | sqlparser::ast::GrantObjects::AllViewsInSchema { .. }
                | sqlparser::ast::GrantObjects::FutureViewsInSchema { .. },
            ) => sqe_policy::grants::GrantObjectKind::View,
            _ => sqe_policy::grants::GrantObjectKind::Table,
        };

        let (catalog, namespace, table) = match objects {
            Some(
                sqlparser::ast::GrantObjects::Tables(tables)
                | sqlparser::ast::GrantObjects::Views(tables),
            ) if !tables.is_empty() => {
                let name = &tables[0];
                let parts: Vec<String> = object_name_parts(name);
                match parts.len() {
                    1 => (None, None, Some(parts[0].clone())),
                    2 => (None, Some(parts[0].clone()), Some(parts[1].clone())),
                    3 => (
                        Some(parts[0].clone()),
                        Some(parts[1].clone()),
                        Some(parts[2].clone()),
                    ),
                    _ => (None, None, Some(name.to_string())),
                }
            }
            // `ON DATABASE cat` is a CATALOG-level resource. Without this arm it
            // fell through to (None, None, None) and errored with "requires a
            // catalog" despite naming one.
            Some(sqlparser::ast::GrantObjects::Databases(dbs)) if !dbs.is_empty() => {
                let parts: Vec<String> = object_name_parts(&dbs[0]);
                (parts.first().cloned(), None, None)
            }
            Some(sqlparser::ast::GrantObjects::Schemas(schemas)) if !schemas.is_empty() => {
                let name = &schemas[0];
                let parts: Vec<String> = object_name_parts(name);
                match parts.len() {
                    1 => (None, Some(parts[0].clone()), None),
                    2 => (Some(parts[0].clone()), Some(parts[1].clone()), None),
                    _ => (None, Some(name.to_string()), None),
                }
            }
            // ON ALL TABLES IN SCHEMA and ON FUTURE TABLES IN SCHEMA both map
            // to a TABLE-level resource with the wildcard "*".
            //
            // `ON ALL` used to map to a namespace-level resource (table: None),
            // which made the statement a silent no-op: namespace-level SELECT
            // is namespace-list + namespace-properties-read and deliberately
            // carries NO table-data-read, and Ranger does not widen a
            // namespace policy to the tables beneath it (no implicit
            // isRecursive, no defaulted table wildcard). Granting that shape
            // parsed, reported success, and conferred nothing on any table.
            //
            // The fix belongs here and NOT in the grant profile: adding
            // table-data-read at the namespace level would silently widen
            // every namespace-scoped grant.
            //
            // Parity limit, accepted deliberately: Ranger has no future-only
            // resource, so a "*" table policy covers existing AND future
            // tables and the two statements necessarily collapse to the same
            // policy. Snowflake distinguishes them. SQE treats ON FUTURE as a
            // superset that also covers existing tables rather than rejecting
            // it, and the docs say so.
            Some(
                sqlparser::ast::GrantObjects::AllTablesInSchema { schemas }
                | sqlparser::ast::GrantObjects::FutureTablesInSchema { schemas }
                | sqlparser::ast::GrantObjects::AllViewsInSchema { schemas }
                | sqlparser::ast::GrantObjects::FutureViewsInSchema { schemas },
            ) if !schemas.is_empty() => {
                let name = &schemas[0];
                let parts: Vec<String> = object_name_parts(name);
                match parts.len() {
                    1 => (None, Some(parts[0].clone()), Some("*".to_string())),
                    2 => (
                        Some(parts[0].clone()),
                        Some(parts[1].clone()),
                        Some("*".to_string()),
                    ),
                    _ => (None, Some(name.to_string()), Some("*".to_string())),
                }
            }
            _ => (None, None, None),
        };

        let raw_grantee = grantees.first().ok_or_else(|| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                "GRANT/REVOKE requires at least one grantee".to_string(),
            )
        })?;

        // Extract the raw grantee name without surrounding quotes.
        // GranteeName::ObjectName.to_string() would include quotes for quoted identifiers
        // such as `TO ROLE "analysts"`. We want the bare value instead.
        // In sqlparser 0.54, ObjectName is Vec<Ident>; each Ident.value is the raw string.
        let grantee_name = match raw_grantee.name.as_ref() {
            Some(sqlparser::ast::GranteeName::ObjectName(obj)) => object_name_parts(obj).join("."),
            Some(other) => other.to_string(),
            None => String::new(),
        };

        let grantee = match &raw_grantee.grantee_type {
            sqlparser::ast::GranteesType::User => Grantee::User(grantee_name),
            sqlparser::ast::GranteesType::None => Grantee::User(grantee_name),
            sqlparser::ast::GranteesType::Role => Grantee::Role(grantee_name),
            sqlparser::ast::GranteesType::Group => Grantee::Group(grantee_name),
            sqlparser::ast::GranteesType::DatabaseRole => Grantee::Role(grantee_name),
            other => {
                return Err(SqeError::NotImplemented(format!(
                    "Unsupported grantee type: {other:?}. Use USER, ROLE, or GROUP"
                )));
            }
        };

        Ok(GrantStatement {
            privilege,
            catalog,
            namespace,
            table,
            grantee,
            // Filled in by the caller from the session: the extractor is a pure
            // function of the parsed statement and has no identity.
            grantor: None,
            with_grant_option,
            object,
            grantor_is_admin: false,
        })
    }

    /// Handle GRANT by delegating to the configured grant backend.
    ///
    /// `policy` carries the `MASKED WITH` / `ROWS WHERE` clauses when present;
    /// they are written to the policy store before the grant, so a grant that
    /// then fails leaves only restrictions behind, never an unmasked SELECT.
    pub(super) async fn handle_grant(
        &self,
        session: &Session,
        stmt: &Statement,
        policy: Option<&sqe_sql::grant_policy::GrantWithPolicy>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Gate BEFORE the backend is asked to do anything (issue #204). In
        // production the Polaris backend swaps the caller's token for a service
        // token scoped PRINCIPAL_ROLE:ALL, so an ungated GRANT let any
        // authenticated user self-escalate. `require_grant_backend` only reads the
        // configured backend -- no network, no token swap -- so resolving it first
        // to ask what it enforces keeps that path unreachable for non-admins.
        let backend = self.require_grant_backend()?;
        self.require_grant_authority(session, "GRANT", backend)?;
        let mut grant_stmt = Self::extract_grant_statement(stmt)?;
        // WITH GRANT OPTION confers ownership, and only an admin confers it
        // (#817). Under `ranger-delegate` the gate above stood down, and Ranger
        // lets any holder of delegateAdmin grant it again, so without this a
        // delegate could pass ownership on, and its grantee after it, by chaining.
        if grant_stmt.with_grant_option {
            self.require_admin(session, "GRANT ... WITH GRANT OPTION")?;
        }
        // Always act as the authenticated caller, never as SQE's service
        // identity. Ranger authorizes the grant against this name, so a caller
        // can only grant what it holds delegate authority over, and the Ranger
        // audit record names the human rather than "admin". The `require_admin`
        // role gate above stays as defence in depth: it is coarse (role-based),
        // this is resource-scoped, and the Polaris backend still swaps in a
        // service token (issue #204) so it has no equivalent check of its own.
        grant_stmt.grantor = Some(session.user.username.clone());
        grant_stmt.grantor_is_admin = self.config.auth.has_admin_role(&session.user.roles);
        if let Some(policy) = policy {
            // Admin only, whatever `grant_authority` says: an item written for
            // a role goes first in the column's policy, so for a user holding
            // that role and a broader principal it decides. A delegate could
            // shadow a stricter mask with a looser one (the #817 class).
            self.require_admin(session, "GRANT ... MASKED WITH / ROWS WHERE")?;
            let store = self.policy_store.as_ref().ok_or_else(|| {
                SqeError::Execution(
                    "GRANT ... MASKED WITH / ROWS WHERE is not available: no policy \
                     backend is configured. It needs [policy] engine = \"ranger\"."
                        .to_string(),
                )
            })?;
            let spec = sqe_sql::grant_policy::GrantPolicySpec {
                table: grant_table_name(&grant_stmt)?,
                principal: policy_principal(&grant_stmt.grantee),
                masks: policy.masks.clone(),
                row_filter: policy.row_filter.clone(),
            };
            store.set_grant_policies(&spec).await?;
            // The masks are live from here, so drop cached decisions now: if
            // the grant below fails, the grantee's next read is already masked.
            self.invalidate_policy_cache();
            if let Some(cache) = &self.query_cache {
                cache.invalidate_all();
            }
        }
        backend
            .grant(session.access_token().expose(), &grant_stmt)
            .await?;
        // Flush the policy cache only after the mutation succeeds so the new
        // grant is visible on the next query (issue #207).
        self.invalidate_policy_cache();
        if let Some(cache) = &self.query_cache {
            cache.invalidate_all();
        }
        Ok(vec![])
    }

    /// Handle REVOKE by delegating to the configured grant backend.
    pub(super) async fn handle_revoke(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Same gate as GRANT (issue #204): REVOKE mutates grants under the service
        // token too. Ranger authorizes the revoke endpoint against the grantor
        // independently of the grant endpoint (verified against 2.8: 403 "User
        // doesn't have necessary permission to revoke access"), so it earns the
        // same relaxation.
        let backend = self.require_grant_backend()?;
        self.require_grant_authority(session, "REVOKE", backend)?;
        let grant_stmt = Self::extract_grant_statement(stmt)?;
        // The masks and row filter a GRANT ... MASKED WITH wrote stay: the
        // grantee may still read the table through a wider grant (a schema
        // grant, a group), and lifting them would show it the real values. A
        // mask behind no SELECT does nothing.
        let revoke_stmt = RevokeStatement {
            privilege: grant_stmt.privilege,
            catalog: grant_stmt.catalog,
            namespace: grant_stmt.namespace,
            table: grant_stmt.table,
            grantee: grant_stmt.grantee,
            // Same reasoning as GRANT: revoking is authorized against the
            // grantor too, so a caller cannot revoke what it has no authority
            // over.
            grantor: Some(session.user.username.clone()),
            object: grant_stmt.object,
            grantor_is_admin: self.config.auth.has_admin_role(&session.user.roles),
        };
        backend
            .revoke(session.access_token().expose(), &revoke_stmt)
            .await?;
        // Flush the policy cache only after the mutation succeeds so the
        // revoked grant stops working immediately (issue #207).
        self.invalidate_policy_cache();
        if let Some(cache) = &self.query_cache {
            cache.invalidate_all();
        }
        Ok(vec![])
    }

    /// Handle DENY by delegating to the configured grant backend.
    ///
    /// Always requires an admin role, and `[access_control] grant_authority`
    /// deliberately does not apply. The asymmetry is documented on
    /// `RangerGrantStore::deny`: Ranger's policy API (the only way to write a deny
    /// item) authorizes the REST user SQE connects with rather than a `grantor`, so
    /// there is no per-resource check to hand over to. Relaxing this one would
    /// leave the write unauthorized rather than authorized more finely.
    pub(super) async fn handle_deny(
        &self,
        session: &Session,
        stmt: &Statement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        self.require_admin(session, "DENY")?;
        let backend = self.require_grant_backend()?;
        let mut deny_stmt = Self::extract_grant_statement(stmt)?;
        deny_stmt.grantor = Some(session.user.username.clone());
        backend
            .deny(session.access_token().expose(), &deny_stmt)
            .await?;
        self.invalidate_policy_cache();
        if let Some(cache) = &self.query_cache {
            cache.invalidate_all();
        }
        Ok(vec![])
    }

    /// Handle SHOW GRANTS by delegating to the configured grant backend.
    pub(super) async fn handle_show_grants(
        &self,
        session: &Session,
        target: &sqe_sql::ShowGrantsTarget,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Same self-or-admin gate as SHOW EFFECTIVE GRANTS (issue #260): the
        // Ranger and Polaris backends list as the service identity, so an
        // ungated read let any user enumerate anyone's grants. `TO USER <self>`
        // is self-service; a resource, role, group or other user needs admin.
        let target_user = match target {
            sqe_sql::ShowGrantsTarget::ToGrantee {
                grantee_type,
                grantee_name,
            } if !matches!(grantee_type.to_uppercase().as_str(), "ROLE" | "GROUP") => {
                grantee_name.as_str()
            }
            _ => "",
        };
        self.require_self_or_admin(session, target_user, "SHOW GRANTS")?;
        let backend = self.require_grant_backend()?;

        let filter = match target {
            sqe_sql::ShowGrantsTarget::OnResource {
                catalog,
                namespace,
                table,
            } => GrantFilter::OnResource {
                catalog: catalog.clone(),
                namespace: namespace.clone(),
                table: table.clone(),
            },
            sqe_sql::ShowGrantsTarget::ToGrantee {
                grantee_type,
                grantee_name,
            } => {
                let grantee = match grantee_type.to_uppercase().as_str() {
                    "ROLE" => Grantee::Role(grantee_name.clone()),
                    "GROUP" => Grantee::Group(grantee_name.clone()),
                    _ => Grantee::User(grantee_name.clone()),
                };
                GrantFilter::ToGrantee(grantee)
            }
        };

        let entries = backend
            .show_grants(session.access_token().expose(), &filter)
            .await?;
        Self::grants_to_record_batch(&entries)
    }

    /// Authorise a read-path grant introspection request (issue #260).
    ///
    /// A caller may always introspect THEIR OWN effective grants/access
    /// (legitimate self-service). Targeting another principal requires an
    /// admin role: in production the Polaris backend swaps the caller's bearer
    /// for a service token scoped PRINCIPAL_ROLE:ALL, so an ungated read let
    /// any authenticated user enumerate anyone's effective privileges. The
    /// check sits ahead of `require_grant_backend`, so the service-token path
    /// is unreachable for a non-admin targeting someone else.
    fn require_self_or_admin(
        &self,
        session: &Session,
        target_user: &str,
        statement: &str,
    ) -> sqe_core::Result<()> {
        if target_user == session.user.username {
            return Ok(());
        }
        self.require_admin(session, statement)
    }

    /// Handle SHOW EFFECTIVE GRANTS by delegating to the configured grant backend.
    pub(super) async fn handle_show_effective_grants(
        &self,
        session: &Session,
        user: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Self-introspection is allowed; introspecting another principal
        // requires admin (issue #260). Gate BEFORE touching the backend so
        // the service-token path is unreachable for an unauthorised caller.
        self.require_self_or_admin(session, user, "SHOW EFFECTIVE GRANTS")?;
        let backend = self.require_grant_backend()?;
        let entries = backend
            .show_effective(session.access_token().expose(), user)
            .await?;
        Self::grants_to_record_batch(&entries)
    }

    /// Handle CHECK ACCESS by delegating to the configured grant backend.
    pub(super) async fn handle_check_access(
        &self,
        session: &Session,
        params: &sqe_sql::CheckAccessParams,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Same self-or-admin gate as SHOW EFFECTIVE GRANTS (issue #260):
        // checking your own access is self-service, checking another
        // principal's is admin-only.
        self.require_self_or_admin(session, &params.user, "CHECK ACCESS")?;
        let backend = self.require_grant_backend()?;

        let check = AccessCheck {
            user: params.user.clone(),
            privilege: params.privilege.clone(),
            catalog: params.catalog.clone(),
            namespace: params.namespace.clone(),
            table: params.table.clone(),
        };

        let resp = backend
            .check_access(session.access_token().expose(), &check)
            .await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("allowed", DataType::Boolean, false),
            Field::new("reason", DataType::Utf8, true),
        ]));

        let allowed_array: ArrayRef = Arc::new(BooleanArray::from(vec![resp.allowed]));
        let mut reason_builder = StringBuilder::new();
        match resp.reason {
            Some(ref r) => reason_builder.append_value(r),
            None => reason_builder.append_null(),
        }
        let reason_array: ArrayRef = Arc::new(reason_builder.finish());

        let batch =
            RecordBatch::try_new(schema, vec![allowed_array, reason_array]).map_err(|e| {
                SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    format!("Failed to build result batch: {e}"),
                )
            })?;

        Ok(vec![batch])
    }

    /// Handle `SHOW EFFECTIVE POLICY [FOR USER "u"] ON <table>`.
    ///
    /// Runs the SAME policy resolution the plan rewriter applies for (user,
    /// table): resolves the resource policy via the wired `PolicyStore`, then
    /// merges tag-derived masks and filters using the table's own
    /// `sqe.column-tags`. Returns a redacted, row-per-effect description.
    ///
    /// Gating mirrors SHOW EFFECTIVE GRANTS: self-introspection is always
    /// allowed; `FOR USER other` requires admin (`require_self_or_admin`).
    ///
    /// Redaction: only the mask *type* name is surfaced (never `Redact`'s
    /// replacement, `Custom`'s expression body, or any row-filter body) so the
    /// diagnostic cannot leak the literals embedded in a policy expression.
    pub(super) async fn handle_show_effective_policy(
        &self,
        session: &Session,
        params: &sqe_sql::ShowEffectivePolicyParams,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        // Default target is the session user. `FOR USER other` is gated BEFORE
        // any catalog/policy work so an unauthorised caller cannot probe another
        // principal's policy (issue #260, same gate as SHOW EFFECTIVE GRANTS).
        let target_user = params.user.as_deref().unwrap_or(&session.user.username);
        self.require_self_or_admin(session, target_user, "SHOW EFFECTIVE POLICY")?;

        let store = self.policy_store.as_deref().ok_or_else(|| {
            SqeError::NotImplemented(
                "Policy enforcement is not configured. Set [policy] backend in the config."
                    .to_string(),
            )
        })?;

        // Load the table's column tags via the caller's token (catalog gates
        // read access). The returned TableIdent gives the policy key without a
        // second parse.
        let (table_ident, col_tags) = self
            .catalog_ops
            .load_column_tags(session, &params.table)
            .await?;

        // Derive the (namespace, table) policy key through the SAME function
        // the rewriter uses (`resolve_policy_key_scoped`): the full dotted
        // namespace, prefixed with the catalog when the store is
        // `catalog_scoped`. This used to take the LAST namespace component,
        // which the rewriter stopped doing in #205, so a multi-level namespace
        // reported a different policy than enforcement applied.
        let parts = split_qualified_identifier(&params.table);
        let catalog_name = match parts.as_slice() {
            [c, _, _, ..] => c.clone(),
            _ => self.session_catalog_name(session),
        };
        let default_hive = sqe_policy::plan_rewriter::HiveCatalogs::new();
        let hive_catalogs = self.hive_catalogs.as_deref().unwrap_or(&default_hive);
        let (namespace, table_name) =
            sqe_policy::plan_rewriter::resolve_policy_key_for_ident_scoped(
                Some(&catalog_name),
                table_ident.namespace().as_ref(),
                table_ident.name(),
                hive_catalogs,
                store.catalog_scoped(),
            )
            .ok_or_else(|| {
                SqeError::with_code(
                    SqeErrorCode::TableNotFound,
                    format!("table not found: {}", params.table),
                )
            })?;

        // The target user's roles are only known for the self case. For
        // `FOR USER other` we have no role lookup wired (the session holds only
        // the caller's roles), so role-based policy for another principal is
        // resolved with an empty role set. See the report's known-gaps note.
        let target = if params.user.is_none()
            || params.user.as_deref() == Some(session.user.username.as_str())
        {
            session.user.clone()
        } else {
            sqe_core::session::SessionUser {
                username: target_user.to_string(),
                roles: Vec::new(),
                subject: None,
                email: None,
                groups: Vec::new(),
            }
        };

        // SHOW EFFECTIVE POLICY must answer with the precedence the rewriter
        // will actually apply, or the introspection statement becomes a second,
        // silently divergent implementation of the contract.
        let policy = sqe_policy::plan_rewriter::resolve_effective_policy(
            store,
            &target,
            Some(&catalog_name),
            &table_name,
            &namespace,
            &col_tags,
            self.config.policy.mask_precedence,
        )
        .await;

        Self::effective_policy_to_record_batch(&policy)
    }

    /// Build the `SHOW EFFECTIVE POLICY` result batch from a `ResolvedPolicy`.
    ///
    /// Columns: `kind` (`row_filter` | `column_mask` | `column_restriction` |
    /// `denied`), `column` (null for row filters / denied), `detail` (redacted),
    /// `source` (best-effort origin; `policy` for now). Rows are emitted in a
    /// deterministic order: denied, then row filters, then masks (sorted by
    /// column), then restrictions (sorted).
    pub(super) fn effective_policy_to_record_batch(
        policy: &sqe_policy::ResolvedPolicy,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("kind", DataType::Utf8, false),
            Field::new("column", DataType::Utf8, true),
            Field::new("detail", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, false),
        ]));

        let mut kind_b = StringBuilder::new();
        let mut col_b = StringBuilder::new();
        let mut detail_b = StringBuilder::new();
        let mut source_b = StringBuilder::new();

        let mut push = |kind: &str, column: Option<&str>, detail: &str, source: &str| {
            kind_b.append_value(kind);
            match column {
                Some(c) => col_b.append_value(c),
                None => col_b.append_null(),
            }
            detail_b.append_value(detail);
            source_b.append_value(source);
        };

        // Deny-all sentinel: the resolver signals a fully-denied table by
        // pushing `lit(false)` into row_filters. Surface it as a single `denied`
        // row rather than counting it as a normal filter. Same check
        // `HiveDdlHandler`'s SHOW TABLES / SHOW CREATE TABLE gate uses, and
        // `summarise_policies`'s own audit-summary fold, all funneled through
        // one function so "denied" means the same thing everywhere.
        let denied = sqe_policy::plan_rewriter::policy_denies_all(policy);

        if denied {
            push("denied", None, "access denied (deny-all policy)", "policy");
        } else {
            let n = policy.row_filters.len();
            if n > 0 {
                // Never print the filter expression body: it can embed literals.
                push(
                    "row_filter",
                    None,
                    &format!("{n} row filter(s) applied"),
                    "policy",
                );
            }
        }

        // Column masks, sorted by column for deterministic output. Only the
        // mask TYPE name is surfaced (redaction).
        let mut masks: Vec<(&String, &sqe_policy::MaskType)> = policy.column_masks.iter().collect();
        masks.sort_by(|a, b| a.0.cmp(b.0));
        for (col, mask) in masks {
            push(
                "column_mask",
                Some(col),
                Self::mask_type_name(mask),
                "policy",
            );
        }

        // Restricted columns, sorted.
        let mut restricted = policy.restricted_columns.clone();
        restricted.sort();
        for col in &restricted {
            push("column_restriction", Some(col), "restricted", "policy");
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(kind_b.finish()) as ArrayRef,
                Arc::new(col_b.finish()) as ArrayRef,
                Arc::new(detail_b.finish()) as ArrayRef,
                Arc::new(source_b.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to build result batch: {e}"),
            )
        })?;

        Ok(vec![batch])
    }

    /// Redaction-safe label for a `MaskType`: the variant name only, never the
    /// `Redact` replacement string or the `Custom` expression body.
    pub(super) fn mask_type_name(mask: &sqe_policy::MaskType) -> &'static str {
        match mask {
            sqe_policy::MaskType::Nullify => "Nullify",
            sqe_policy::MaskType::Redact(_) => "Redact",
            sqe_policy::MaskType::Hash => "Hash",
            sqe_policy::MaskType::Custom(_) => "Custom",
            sqe_policy::MaskType::PartialMask { .. } => "PartialMask",
            sqe_policy::MaskType::DateShowYear => "DateShowYear",
        }
    }

    /// Handle `SHOW TAGS ON <table>` — read back the `sqe.column-tags` property
    /// as (column, tag) rows. No extra SQE gate: `load_column_tags` loads the
    /// table with the caller's token, so the catalog enforces read access.
    /// Empty result when the table carries no tags.
    /// `SHOW MASKING POLICIES` / `SHOW MASKING POLICY <name>`.
    ///
    /// ADMIN-GATED. The listing names the columns someone thought worth masking,
    /// which is a map of where the sensitive data is. A non-admin who cannot read
    /// `orders.ssn` should not learn that `orders.ssn` is the column under a mask.
    ///
    /// Reports what is CONFIGURED, not what applies to any given user. Precedence,
    /// deny items and role membership are `SHOW EFFECTIVE POLICY`'s job, and the
    /// column comments here say so because conflating them is the natural mistake.
    pub(super) async fn handle_show_masking_policies(
        &self,
        session: &Session,
        name_filter: Option<&str>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        self.require_admin(session, "SHOW MASKING POLICIES")?;
        // Two ways this is unavailable, and both must SAY so rather than return an
        // empty result. `[policy] engine = "passthrough"` (the default) has no store
        // at all; `in-memory` has one that cannot list policies and refuses via the
        // trait default. An empty listing would read as "nothing is masked here".
        let store = self.policy_store.as_ref().ok_or_else(|| {
            SqeError::Execution(
                "SHOW MASKING POLICIES is not available: no policy backend is \
                 configured. It needs [policy] engine = \"ranger\"."
                    .to_string(),
            )
        })?;
        let mut policies = store.list_mask_policies().await?;
        if let Some(want) = name_filter {
            policies.retain(|p| p.name.eq_ignore_ascii_case(want));
        }
        Self::mask_policies_to_record_batch(&policies)
    }

    /// Author Ranger masks and row filters through SQL. Ranger remains the
    /// source of truth, so Spark/Kyuubi observes the same policy on its next
    /// policy refresh.
    pub(super) async fn handle_policy_ddl(
        &self,
        session: &Session,
        stmt: &sqe_sql::policy_ddl::PolicyDdlStatement,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        use sqe_sql::policy_ddl::PolicyDdlStatement;

        self.require_admin(
            session,
            match stmt {
                PolicyDdlStatement::Create(_) => "CREATE POLICY",
                PolicyDdlStatement::Drop(_) | PolicyDdlStatement::DropForPrincipal(_) => {
                    "DROP POLICY"
                }
            },
        )?;
        let store = self.policy_store.as_ref().ok_or_else(|| {
            SqeError::Execution(
                "Policy DDL is not available: no policy backend is configured. \
                 It needs [policy] engine = \"ranger\"."
                    .to_string(),
            )
        })?;
        match stmt {
            PolicyDdlStatement::Create(create) => store.create_policy(create).await?,
            PolicyDdlStatement::Drop(drop) => store.drop_policy(drop).await?,
            // The one way to lift what GRANT ... MASKED WITH wrote: REVOKE
            // leaves it (see handle_revoke), so a later plain GRANT brings
            // the same masks back until an admin drops them here.
            PolicyDdlStatement::DropForPrincipal(drop) => {
                store.drop_principal_policies(drop).await?;
            }
        }
        self.invalidate_policy_cache();
        if let Some(cache) = &self.query_cache {
            cache.invalidate_all();
        }
        Ok(vec![])
    }

    /// Turn `SET MASKING POLICY p` into the equivalent column-tag operation.
    ///
    /// A named Ranger masking policy is not a free-floating definition you bind to a
    /// column: it already carries its own resource. There are two shapes, and only
    /// one can be attached to a column safely.
    ///
    /// A TAG policy is keyed on a tag and names no table, so "attach p to c" means
    /// "give c the tag p is keyed on". One rule, many columns, no rewriting of the
    /// policy. That is what this does, and it is also exactly Snowflake's semantics
    /// for a masking policy: a reusable definition attached in many places.
    ///
    /// A RESOURCE policy names `database`/`table`/`column` explicitly. Attaching
    /// would mean appending to those lists, and Ranger evaluates them as a CROSS
    /// PRODUCT: adding `(db2, tbl2, col2)` to a policy holding `(db1, tbl1, col1)`
    /// starts masking `db1.tbl2.col2` too. So the statement REFUSES for a resource
    /// policy rather than silently widening a mask to tables nobody named. The
    /// refusal explains the alternative instead of just saying no.
    pub(super) async fn masking_policy_to_tag_op(
        &self,
        op: &sqe_sql::tags::MaskingPolicyOp,
    ) -> sqe_core::Result<sqe_sql::tags::SetTagsStatement> {
        use sqe_sql::tags::{ColumnTagOp, SetTagsStatement, TagAction};

        // UNSET needs no lookup: it clears every tag on the column, which is what
        // `UNSET TAGS` with an empty list already means. Doing this before the store
        // check keeps a detach working on a deployment whose backend cannot list
        // policies, because taking protection OFF must never be blocked by an
        // inability to enumerate what is ON.
        let Some(policy_name) = op.policy.as_deref() else {
            return Ok(SetTagsStatement {
                table: op.table.clone(),
                ops: vec![ColumnTagOp {
                    column: op.column.clone(),
                    tags: vec![],
                    values: Default::default(),
                    action: TagAction::Unset,
                }],
            });
        };

        let store = self.policy_store.as_ref().ok_or_else(|| {
            SqeError::Execution(
                "SET MASKING POLICY is not available: no policy backend is \
                 configured. It needs [policy] engine = \"ranger\"."
                    .to_string(),
            )
        })?;
        let policies = store.list_mask_policies().await?;
        let tag = resolve_masking_policy_tag(&policies, policy_name)?;

        Ok(SetTagsStatement {
            table: op.table.clone(),
            ops: vec![ColumnTagOp {
                column: op.column.clone(),
                tags: vec![tag],
                values: Default::default(),
                action: TagAction::Set,
            }],
        })
    }

    fn mask_policies_to_record_batch(
        policies: &[sqe_policy::MaskPolicyInfo],
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("tag", DataType::Utf8, false),
            Field::new("database", DataType::Utf8, false),
            Field::new("table", DataType::Utf8, false),
            Field::new("column", DataType::Utf8, false),
            Field::new("mask_type", DataType::Utf8, false),
            Field::new("expression", DataType::Utf8, false),
            Field::new("grantees", DataType::Utf8, false),
            Field::new("enabled", DataType::Boolean, false),
        ]));

        let mut name_b = StringBuilder::new();
        let mut tag_b = StringBuilder::new();
        let mut db_b = StringBuilder::new();
        let mut tbl_b = StringBuilder::new();
        let mut col_b = StringBuilder::new();
        let mut type_b = StringBuilder::new();
        let mut expr_b = StringBuilder::new();
        let mut grantee_b = StringBuilder::new();
        let mut enabled_b = arrow_array::builder::BooleanBuilder::new();
        for p in policies {
            name_b.append_value(&p.name);
            tag_b.append_value(&p.tag);
            db_b.append_value(&p.database);
            tbl_b.append_value(&p.table);
            col_b.append_value(&p.column);
            type_b.append_value(&p.mask_type);
            expr_b.append_value(&p.expression);
            grantee_b.append_value(&p.grantees);
            enabled_b.append_value(p.enabled);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(name_b.finish()) as ArrayRef,
                Arc::new(tag_b.finish()) as ArrayRef,
                Arc::new(db_b.finish()) as ArrayRef,
                Arc::new(tbl_b.finish()) as ArrayRef,
                Arc::new(col_b.finish()) as ArrayRef,
                Arc::new(type_b.finish()) as ArrayRef,
                Arc::new(expr_b.finish()) as ArrayRef,
                Arc::new(grantee_b.finish()) as ArrayRef,
                Arc::new(enabled_b.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to build result batch: {e}"),
            )
        })?;
        Ok(vec![batch])
    }

    pub(super) async fn handle_show_tags(
        &self,
        session: &Session,
        table: &str,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let (_table_ident, col_tags) = self.catalog_ops.load_column_tags(session, table).await?;
        Self::tags_to_record_batch(&col_tags)
    }

    /// Build the `SHOW TAGS` result batch: one row per (column, tag), sorted by
    /// (column, tag) for deterministic output.
    pub(super) fn tags_to_record_batch(
        col_tags: &std::collections::HashMap<String, Vec<String>>,
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("column", DataType::Utf8, false),
            Field::new("tag", DataType::Utf8, false),
        ]));

        let mut rows: Vec<(&str, &str)> = Vec::new();
        for (col, tags) in col_tags {
            for tag in tags {
                rows.push((col.as_str(), tag.as_str()));
            }
        }
        rows.sort_unstable();

        let mut col_b = StringBuilder::new();
        let mut tag_b = StringBuilder::new();
        for (col, tag) in rows {
            col_b.append_value(col);
            tag_b.append_value(tag);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(col_b.finish()) as ArrayRef,
                Arc::new(tag_b.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to build result batch: {e}"),
            )
        })?;

        Ok(vec![batch])
    }

    /// Convert a list of `GrantEntry` values into a `RecordBatch` for the client.
    fn grants_to_record_batch(
        entries: &[sqe_policy::grants::GrantEntry],
    ) -> sqe_core::Result<Vec<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("privilege", DataType::Utf8, false),
            Field::new("resource", DataType::Utf8, false),
            Field::new("grantee_type", DataType::Utf8, false),
            Field::new("grantee_name", DataType::Utf8, false),
            Field::new("effect", DataType::Utf8, false),
            Field::new("granted_by", DataType::Utf8, true),
            Field::new("granted_at", DataType::Utf8, true),
        ]));

        let mut priv_builder = StringBuilder::new();
        let mut resource_builder = StringBuilder::new();
        let mut type_builder = StringBuilder::new();
        let mut name_builder = StringBuilder::new();
        let mut effect_builder = StringBuilder::new();
        let mut by_builder = StringBuilder::new();
        let mut at_builder = StringBuilder::new();

        for entry in entries {
            priv_builder.append_value(&entry.privilege);
            resource_builder.append_value(&entry.resource);
            type_builder.append_value(&entry.grantee_type);
            name_builder.append_value(&entry.grantee_name);
            effect_builder.append_value(&entry.effect);
            match entry.granted_by {
                Some(ref v) => by_builder.append_value(v),
                None => by_builder.append_null(),
            }
            match entry.granted_at {
                Some(ref v) => at_builder.append_value(v),
                None => at_builder.append_null(),
            }
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(priv_builder.finish()) as ArrayRef,
                Arc::new(resource_builder.finish()) as ArrayRef,
                Arc::new(type_builder.finish()) as ArrayRef,
                Arc::new(name_builder.finish()) as ArrayRef,
                Arc::new(effect_builder.finish()) as ArrayRef,
                Arc::new(by_builder.finish()) as ArrayRef,
                Arc::new(at_builder.finish()) as ArrayRef,
            ],
        )
        .map_err(|e| {
            SqeError::with_code(
                SqeErrorCode::ExecutionFailed,
                format!("Failed to build result batch: {e}"),
            )
        })?;

        Ok(vec![batch])
    }
}

/// The dotted table a GRANT / REVOKE names, for the policy store.
fn grant_table_name(stmt: &GrantStatement) -> sqe_core::Result<String> {
    let table = stmt.table.as_deref().ok_or_else(|| {
        SqeError::Execution("MASKED WITH and ROWS WHERE apply to exactly one table".to_string())
    })?;
    Ok([
        stmt.catalog.as_deref(),
        stmt.namespace.as_deref(),
        Some(table),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("."))
}

fn policy_principal(grantee: &Grantee) -> sqe_sql::policy_ddl::PolicyPrincipal {
    use sqe_sql::policy_ddl::{PolicyPrincipal, PolicyPrincipalKind};
    let (kind, name) = match grantee {
        Grantee::User(n) => (PolicyPrincipalKind::User, n),
        Grantee::Role(n) => (PolicyPrincipalKind::Role, n),
        Grantee::Group(n) => (PolicyPrincipalKind::Group, n),
    };
    PolicyPrincipal {
        kind,
        name: name.clone(),
    }
}

/// Which tag a named masking policy is keyed on.
///
/// Pure, because every branch here is a REFUSAL and the refusals are the safety
/// property. Two of them are the reason `SET MASKING POLICY` is not simply "append
/// this column to that policy":
///
/// * a RESOURCE policy already names its columns, and Ranger evaluates a policy's
///   database/table/column lists as a cross product, so appending would widen the
///   mask to combinations nobody asked for
/// * a policy keyed on several tags gives no single answer
///
/// Neither can be exercised through a live Ranger without deliberately building the
/// bad policy, which is why this is tested here rather than end to end.
fn resolve_masking_policy_tag(
    policies: &[sqe_policy::MaskPolicyInfo],
    policy_name: &str,
) -> sqe_core::Result<String> {
    let matching: Vec<&sqe_policy::MaskPolicyInfo> = policies
        .iter()
        .filter(|p| p.name.eq_ignore_ascii_case(policy_name))
        .collect();

    if matching.is_empty() {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!(
                "no masking policy named `{policy_name}`. \
             Run SHOW MASKING POLICIES to see what is available."
            ),
        ));
    }
    // One policy yields one row per grantee item; they share the resource, so any
    // row answers "which tag". They must AGREE, or the name is ambiguous.
    let tags: std::collections::BTreeSet<&str> = matching
        .iter()
        .map(|p| p.tag.as_str())
        .filter(|t| !t.is_empty())
        .collect();

    if tags.is_empty() {
        let target = matching
            .iter()
            .map(|p| format!("{}.{}.{}", p.database, p.table, p.column))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(", ");
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!(
                "`{policy_name}` is a resource masking policy: it already names the \
             columns it covers ({target}), so it cannot be attached to another one. \
             Ranger evaluates a policy's database/table/column lists as a cross \
             product, so adding a column here would also mask unrelated combinations \
             of the values already listed.\n\
             Use a TAG-based masking policy for something you attach in several \
             places, or write a policy for this column directly."
            ),
        ));
    }
    if tags.len() > 1 {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!(
                "`{policy_name}` is keyed on more than one tag ({}), so attaching it is \
             ambiguous. Attach the tag you want with \
             ALTER TABLE ... MODIFY COLUMN ... SET TAG.",
                tags.iter().copied().collect::<Vec<_>>().join(", ")
            ),
        ));
    }
    Ok(tags.iter().next().expect("exactly one tag").to_string())
}

#[cfg(test)]
mod masking_policy_resolution_tests {
    use super::resolve_masking_policy_tag;
    use sqe_policy::MaskPolicyInfo;

    fn tag_policy(name: &str, tag: &str) -> MaskPolicyInfo {
        MaskPolicyInfo {
            name: name.to_string(),
            enabled: true,
            tag: tag.to_string(),
            database: String::new(),
            table: String::new(),
            column: String::new(),
            mask_type: "hive:CUSTOM".to_string(),
            expression: "concat('x', {col})".to_string(),
            grantees: "ROLE engineer".to_string(),
        }
    }

    fn resource_policy(name: &str, db: &str, table: &str, column: &str) -> MaskPolicyInfo {
        MaskPolicyInfo {
            name: name.to_string(),
            enabled: true,
            tag: String::new(),
            database: db.to_string(),
            table: table.to_string(),
            column: column.to_string(),
            mask_type: "CUSTOM".to_string(),
            expression: "concat('x', {col})".to_string(),
            grantees: "ROLE engineer".to_string(),
        }
    }

    #[test]
    fn a_tag_policy_resolves_to_its_tag() {
        let policies = vec![tag_policy("mask_pii", "PII")];
        assert_eq!(
            resolve_masking_policy_tag(&policies, "mask_pii").expect("resolves"),
            "PII"
        );
    }

    /// Ranger policy names are case-insensitive in practice and operators type
    /// whatever the console shows.
    #[test]
    fn the_lookup_is_case_insensitive() {
        let policies = vec![tag_policy("Mask_PII", "PII")];
        assert_eq!(
            resolve_masking_policy_tag(&policies, "mask_pii").expect("resolves"),
            "PII"
        );
    }

    /// THE safety refusal. A resource policy names its own columns, and Ranger
    /// treats database/table/column as a CROSS PRODUCT: appending a column to a
    /// policy that already lists `(sales, orders, ssn)` would also mask
    /// `sales.<other table>.<the new column>`.
    ///
    /// Attaching is the obvious reading of the statement, and it is the dangerous
    /// one, so the refusal is the feature. The message must name the columns
    /// already covered, or an operator cannot tell why they were refused.
    #[test]
    fn a_resource_policy_cannot_be_attached() {
        let policies = vec![resource_policy("mask_ssn", "sales", "orders", "ssn")];
        let err = resolve_masking_policy_tag(&policies, "mask_ssn")
            .expect_err("a resource policy must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("sales.orders.ssn"),
            "must name what it covers: {msg}"
        );
        assert!(msg.contains("cross product"), "must say WHY: {msg}");
        assert!(
            msg.contains("TAG-based"),
            "must offer the alternative: {msg}"
        );
    }

    /// Several grantee items on ONE policy are not ambiguity: they share the tag.
    #[test]
    fn several_rows_of_the_same_policy_are_not_ambiguous() {
        let mut second = tag_policy("mask_pii", "PII");
        second.grantees = "USER dave".to_string();
        let policies = vec![tag_policy("mask_pii", "PII"), second];
        assert_eq!(
            resolve_masking_policy_tag(&policies, "mask_pii").expect("resolves"),
            "PII"
        );
    }

    /// Genuinely conflicting tags are refused, not guessed. Picking one would
    /// silently apply a mask the operator did not choose.
    #[test]
    fn conflicting_tags_are_refused() {
        let policies = vec![
            tag_policy("mask_pii", "PII"),
            tag_policy("mask_pii", "SENSITIVE"),
        ];
        let err =
            resolve_masking_policy_tag(&policies, "mask_pii").expect_err("ambiguous must refuse");
        let msg = err.to_string();
        assert!(
            msg.contains("PII") && msg.contains("SENSITIVE"),
            "list both: {msg}"
        );
    }

    #[test]
    fn an_unknown_policy_points_at_show_masking_policies() {
        let policies = vec![tag_policy("mask_pii", "PII")];
        let err = resolve_masking_policy_tag(&policies, "nope").expect_err("unknown");
        assert!(err.to_string().contains("SHOW MASKING POLICIES"));
    }

    /// A resource policy and a tag policy sharing a name resolves to the TAG.
    ///
    /// Deliberate: the tag one is attachable and the resource one is not, so there
    /// is exactly one workable answer. Refusing here would block a legitimate
    /// attach over a name collision the operator may not control.
    #[test]
    fn a_name_shared_by_both_kinds_resolves_to_the_tag_policy() {
        let policies = vec![
            resource_policy("mask_ssn", "sales", "orders", "ssn"),
            tag_policy("mask_ssn", "PII"),
        ];
        assert_eq!(
            resolve_masking_policy_tag(&policies, "mask_ssn").expect("resolves"),
            "PII"
        );
    }
}
