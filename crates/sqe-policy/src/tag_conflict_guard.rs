//! Refuse a tag write that creates a policy conflict (#918). A read already
//! fails on a conflict; this refuses the write that would cause one, for every
//! principal the table's policies name. A row-filter conflict (TA-13, #931: one
//! row filter per table and user) is refused under every `mask_precedence`. A
//! mask conflict is refused under `refuse` (any two different masks) and under
//! `tag` and `resource` when the column's tag masks cannot be joined.
//!
//! Design: `openspec/changes/policy-conflict-refuse-at-write/design.md`.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use sqe_core::config::MaskPrecedence;
use sqe_core::{SessionUser, SqeError, SqeErrorCode};

use crate::plan_rewriter::{merge_tag_masks, PolicyConflict};
use crate::session_udf::SessionIdentity;
use crate::{PolicyStore, ResolvedPolicy};

/// Column name -> tag keys, the shape the read path merges.
pub type ColumnTagKeys = HashMap<String, Vec<String>>;

/// The check for one table, keyed as the read path keys it.
pub struct TagConflictGuard {
    store: Arc<dyn PolicyStore>,
    namespace: String,
    table: String,
    display: String,
    catalog: Option<String>,
    precedence: MaskPrecedence,
}

impl TagConflictGuard {
    /// `namespace` and `table` are the policy key enforcement uses
    /// (`resolve_policy_key_for_ident_scoped`); `display` names the table in
    /// the error.
    pub fn new(
        store: Arc<dyn PolicyStore>,
        namespace: String,
        table: String,
        display: String,
    ) -> Self {
        Self {
            store,
            namespace,
            table,
            display,
            catalog: None,
            precedence: MaskPrecedence::Refuse,
        }
    }

    /// The configured `mask_precedence`; `refuse` when unset. The guard finds
    /// the conflicts a read under this precedence would refuse.
    pub fn with_precedence(mut self, precedence: MaskPrecedence) -> Self {
        self.precedence = precedence;
        self
    }

    /// The table's catalog, for tag policies scoped to it (#919): tags then
    /// resolve as the read path resolves them for that catalog.
    pub fn with_catalog(mut self, catalog: String) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// The tag names whose policies can bind: each tag, plus its
    /// catalog-qualified name when the store scopes tag policies by catalog.
    fn policy_tag_names(&self, col_tags: &ColumnTagKeys) -> HashSet<String> {
        let mut names = tag_set(col_tags);
        if let Some(catalog) = self.scoped_catalog() {
            let qualified: Vec<String> = names
                .iter()
                .map(|t| crate::tag_scope::qualify_tag(catalog, t))
                .collect();
            names.extend(qualified);
        }
        names
    }

    fn scoped_catalog(&self) -> Option<&str> {
        self.catalog
            .as_deref()
            .filter(|c| self.store.catalog_scoped() && !c.is_empty())
    }

    /// Refuse when the tags `after` resolve a conflict, for some principal,
    /// that the tags `before` did not. A change that adds no tag is not
    /// checked, so a tag can always be removed. A store error refuses.
    pub async fn check(
        &self,
        before: &ColumnTagKeys,
        after: &ColumnTagKeys,
    ) -> sqe_core::Result<()> {
        if !adds_a_tag(before, after) {
            return Ok(());
        }
        let tags = self.policy_tag_names(after);
        let Some(principals) = self
            .store
            .conflict_principals(&self.table, &self.namespace, &tags)
            .await?
        else {
            return Ok(());
        };
        for user in &principals {
            let base = self
                .store
                .resolve(user, &self.table, &self.namespace)
                .await?;
            let old = self.conflicts(user, &base, before).await;
            let new = self.conflicts(user, &base, after).await;
            if let Some(conflict) = new.difference(&old).next() {
                return Err(self.refusal(conflict));
            }
        }
        Ok(())
    }

    /// The conflicts `col_tags` resolves for `user` on top of `base`, as the
    /// read path's merge under the configured precedence finds them.
    async fn conflicts(
        &self,
        user: &SessionUser,
        base: &ResolvedPolicy,
        col_tags: &ColumnTagKeys,
    ) -> BTreeSet<PolicyConflict> {
        let tags = tag_set(col_tags);
        if tags.is_empty() {
            return BTreeSet::new();
        }
        let (masks, filters, unmappable) = crate::tag_scope::resolve_tags_in_catalog(
            self.store.as_ref(),
            user,
            &tags,
            self.catalog.as_deref(),
        )
        .await;
        let identity = SessionIdentity {
            username: user.username.clone(),
            roles: user.roles.clone(),
            database: None,
            schema: None,
        };
        let mut policy = base.clone();
        merge_tag_masks(
            &mut policy,
            col_tags,
            &masks,
            filters,
            &unmappable,
            &identity,
            self.precedence,
        )
        .into_iter()
        .collect()
    }

    /// Names the table and column, not the principal: a column steward is
    /// not entitled to learn whom a policy binds.
    fn refusal(&self, conflict: &PolicyConflict) -> SqeError {
        let what = match conflict {
            PolicyConflict::Mask(column) => format!(
                "a mask conflict on {}.{column} under mask_precedence = {} \
                 (masks that cannot be joined)",
                self.display,
                match self.precedence {
                    MaskPrecedence::Tag => "tag",
                    MaskPrecedence::Resource => "resource",
                    MaskPrecedence::Refuse => "refuse",
                }
            ),
            PolicyConflict::RowFilter => format!(
                "a row filter conflict on {} (one row filter per table and user)",
                self.display
            ),
        };
        SqeError::with_code(
            SqeErrorCode::AccessDenied,
            format!("tag change refused: it would create {what}"),
        )
    }
}

fn tag_set(col_tags: &ColumnTagKeys) -> HashSet<String> {
    col_tags.values().flatten().cloned().collect()
}

/// True when some column carries a tag after the change it did not before.
fn adds_a_tag(before: &ColumnTagKeys, after: &ColumnTagKeys) -> bool {
    after.iter().any(|(column, tags)| {
        let old = before.get(column);
        tags.iter().any(|t| !old.is_some_and(|o| o.contains(t)))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MaskType, TagMaskSpec};
    use async_trait::async_trait;
    use datafusion::logical_expr::{col, lit, Expr};
    use std::sync::Mutex;

    /// Resource mask `MASK_HASH` on `amount` for role `eng`; tag `null` masks
    /// `MASK_NULL`, tag `hash` masks `MASK_HASH` and tag `pm` masks
    /// `MASK_SHOW_LAST_4` for `eng`; tag `f` adds a
    /// row filter for `eng` that differs from the resource one.
    /// With `scoped`, tag `c.x` (catalog `c`'s policy on `x`) masks
    /// `MASK_NULL` for `eng`, and `conflict_principals` records its tags.
    struct Stub {
        principals: Option<Vec<SessionUser>>,
        fail: bool,
        resolved: Mutex<usize>,
        scoped: bool,
        asked: Mutex<HashSet<String>>,
    }

    fn eng() -> SessionUser {
        SessionUser {
            username: "\0p".into(),
            roles: vec!["eng".into()],
            subject: None,
            email: None,
            groups: vec![],
        }
    }

    fn stub(principals: Option<Vec<SessionUser>>) -> Arc<Stub> {
        Arc::new(Stub {
            principals,
            fail: false,
            resolved: Mutex::new(0),
            scoped: false,
            asked: Mutex::new(HashSet::new()),
        })
    }

    fn failing() -> Arc<Stub> {
        Arc::new(Stub {
            principals: Some(vec![eng()]),
            fail: true,
            resolved: Mutex::new(0),
            scoped: false,
            asked: Mutex::new(HashSet::new()),
        })
    }

    #[async_trait]
    impl PolicyStore for Stub {
        fn catalog_scoped(&self) -> bool {
            self.scoped
        }

        async fn resolve(
            &self,
            user: &SessionUser,
            _: &str,
            _: &str,
        ) -> sqe_core::Result<ResolvedPolicy> {
            *self.resolved.lock().unwrap() += 1;
            if self.fail {
                return Err(SqeError::Execution("ranger down".into()));
            }
            let mut p = ResolvedPolicy::default();
            if user.roles.iter().any(|r| r == "eng") {
                p.column_masks.insert("amount".into(), MaskType::Hash);
                p.row_filters.push(col("region").eq(lit("eu")));
            }
            Ok(p)
        }

        async fn resolve_tags(
            &self,
            user: &SessionUser,
            tags: &HashSet<String>,
        ) -> (HashMap<String, TagMaskSpec>, Vec<Expr>, HashSet<String>) {
            let mut masks = HashMap::new();
            let mut filters = vec![];
            if user.roles.iter().any(|r| r == "eng") {
                if tags.contains("null") {
                    masks.insert("null".into(), TagMaskSpec::Ready(MaskType::Nullify));
                }
                if tags.contains("hash") {
                    masks.insert("hash".into(), TagMaskSpec::Ready(MaskType::Hash));
                }
                if tags.contains("pm") {
                    masks.insert(
                        "pm".into(),
                        TagMaskSpec::Ready(MaskType::PartialMask {
                            show_first: 0,
                            show_last: 4,
                            upper: 'X',
                            lower: 'x',
                            digit: '#',
                        }),
                    );
                }
                if tags.contains("f") {
                    filters.push(col("region").eq(lit("us")));
                }
                if self.scoped && tags.contains("c.x") {
                    masks.insert("c.x".into(), TagMaskSpec::Ready(MaskType::Nullify));
                }
            }
            (masks, filters, HashSet::new())
        }

        async fn conflict_principals(
            &self,
            _: &str,
            _: &str,
            tags: &HashSet<String>,
        ) -> sqe_core::Result<Option<Vec<SessionUser>>> {
            if self.fail {
                return Err(SqeError::Execution("ranger down".into()));
            }
            self.asked.lock().unwrap().extend(tags.iter().cloned());
            Ok(self.principals.clone())
        }
    }

    fn tags(entries: &[(&str, &[&str])]) -> ColumnTagKeys {
        entries
            .iter()
            .map(|(c, t)| (c.to_string(), t.iter().map(|s| s.to_string()).collect()))
            .collect()
    }

    fn guard(store: Arc<Stub>) -> TagConflictGuard {
        TagConflictGuard::new(store, "ac".into(), "orders".into(), "c.ac.orders".into())
    }

    async fn run(
        store: Arc<Stub>,
        before: ColumnTagKeys,
        after: ColumnTagKeys,
    ) -> sqe_core::Result<()> {
        guard(store).check(&before, &after).await
    }

    #[tokio::test]
    async fn a_new_mask_conflict_is_refused_and_named() {
        let err = run(
            stub(Some(vec![eng()])),
            tags(&[]),
            tags(&[("amount", &["null"])]),
        )
        .await
        .unwrap_err()
        .to_string();
        // The portal's governance_service.py matches this phrase.
        let want = "it would create a mask conflict on c.ac.orders.amount under mask_precedence";
        assert!(
            err.contains(&format!("tag change refused: {want}")),
            "{err}"
        );
        assert!(!err.contains("eng"), "the principal is not named: {err}");
    }

    #[tokio::test]
    async fn the_same_mask_from_two_sources_is_allowed() {
        run(
            stub(Some(vec![eng()])),
            tags(&[]),
            tags(&[("amount", &["hash"])]),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_new_row_filter_conflict_is_refused() {
        let err = run(
            stub(Some(vec![eng()])),
            tags(&[]),
            tags(&[("region", &["f"])]),
        )
        .await
        .unwrap_err()
        .to_string();
        // The portal's governance_service.py matches this phrase.
        let want = "it would create a row filter conflict on c.ac.orders (one row filter";
        assert!(
            err.contains(&format!("tag change refused: {want}")),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_principal_the_tag_does_not_bind_is_allowed() {
        let mut other = eng();
        other.roles = vec!["analyst".into()];
        run(
            stub(Some(vec![other])),
            tags(&[]),
            tags(&[("amount", &["null"])]),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn an_existing_conflict_does_not_block_an_unrelated_tag() {
        run(
            stub(Some(vec![eng()])),
            tags(&[("amount", &["null"])]),
            tags(&[("amount", &["null"]), ("note", &["hash"])]),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_change_that_adds_no_tag_is_not_checked() {
        let store = failing();
        run(
            store.clone(),
            tags(&[("amount", &["null", "x"])]),
            tags(&[("amount", &["null"])]),
        )
        .await
        .unwrap();
        assert_eq!(
            *store.resolved.lock().unwrap(),
            0,
            "not even the store is asked"
        );
    }

    #[tokio::test]
    async fn a_store_error_refuses() {
        run(failing(), tags(&[]), tags(&[("amount", &["hash"])]))
            .await
            .unwrap_err();
    }

    fn scoped() -> Arc<Stub> {
        Arc::new(Stub {
            principals: Some(vec![eng()]),
            fail: false,
            resolved: Mutex::new(0),
            scoped: true,
            asked: Mutex::new(HashSet::new()),
        })
    }

    #[tokio::test]
    async fn a_catalog_scoped_tag_policy_is_checked_for_its_catalog() {
        let store = scoped();
        let err = guard(store.clone())
            .with_catalog("c".into())
            .check(&tags(&[]), &tags(&[("amount", &["x"])]))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("mask conflict on c.ac.orders.amount"), "{err}");
        let asked = store.asked.lock().unwrap().clone();
        assert!(asked.contains("x") && asked.contains("c.x"), "{asked:?}");
    }

    #[tokio::test]
    async fn another_catalogs_scoped_tag_policy_is_not_applied() {
        guard(scoped())
            .with_catalog("d".into())
            .check(&tags(&[]), &tags(&[("amount", &["x"])]))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_store_that_cannot_list_principals_allows() {
        run(stub(None), tags(&[]), tags(&[("amount", &["null"])]))
            .await
            .unwrap();
    }

    async fn run_under(precedence: MaskPrecedence, after: ColumnTagKeys) -> sqe_core::Result<()> {
        guard(stub(Some(vec![eng()])))
            .with_precedence(precedence)
            .check(&tags(&[]), &after)
            .await
    }

    /// TA-13 (#931): one row filter per table and user holds in every
    /// precedence, so the write that would break it is refused in every one.
    #[tokio::test]
    async fn a_new_row_filter_conflict_is_refused_under_every_precedence() {
        for precedence in [
            MaskPrecedence::Tag,
            MaskPrecedence::Resource,
            MaskPrecedence::Refuse,
        ] {
            let err = run_under(precedence, tags(&[("region", &["f"])]))
                .await
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("row filter conflict on c.ac.orders"),
                "{precedence:?}: {err}"
            );
            assert!(!err.contains("mask_precedence"), "{precedence:?}: {err}");
        }
    }

    /// Outside `refuse` the precedence decides which mask wins, so two masks
    /// on a column are no conflict and the write goes through.
    #[tokio::test]
    async fn a_mask_conflict_is_allowed_outside_refuse() {
        for precedence in [MaskPrecedence::Tag, MaskPrecedence::Resource] {
            run_under(precedence, tags(&[("amount", &["null"])]))
                .await
                .unwrap_or_else(|e| panic!("{precedence:?}: {e}"));
        }
    }

    /// Tag masks that cannot be joined conflict under every precedence, and the
    /// write that creates the conflict is refused (#942).
    #[tokio::test]
    async fn tag_conflict_guard_most_restrictive_refuses_a_tag_mask_that_cannot_be_joined() {
        for precedence in [MaskPrecedence::Tag, MaskPrecedence::Resource] {
            let err = guard(stub(Some(vec![eng()])))
                .with_precedence(precedence)
                .check(
                    &tags(&[("ssn", &["hash"])]),
                    &tags(&[("ssn", &["hash", "pm"])]),
                )
                .await
                .unwrap_err()
                .to_string();
            assert!(err.contains("mask conflict on c.ac.orders.ssn"), "{err}");
            assert!(err.contains("cannot be joined"), "{err}");
            assert!(!err.contains("= refuse"), "{precedence:?}: {err}");
        }
    }

    /// A weaker mask next to `MASK_NULL` is allowed, and the column still reads
    /// NULL.
    #[tokio::test]
    async fn tag_conflict_guard_most_restrictive_allows_a_weaker_tag_next_to_nullify() {
        let before = tags(&[("ssn", &["null"])]);
        let after = tags(&[("ssn", &["pm", "null"])]);
        guard(stub(Some(vec![eng()])))
            .with_precedence(MaskPrecedence::Tag)
            .check(&before, &after)
            .await
            .unwrap();
        let (masks, filters, unmappable) = crate::tag_scope::resolve_tags_in_catalog(
            stub(None).as_ref(),
            &eng(),
            &tag_set(&after),
            None,
        )
        .await;
        let mut policy = ResolvedPolicy::default();
        let conflicts = merge_tag_masks(
            &mut policy,
            &after,
            &masks,
            filters,
            &unmappable,
            &SessionIdentity {
                username: "u".into(),
                roles: vec![],
                database: None,
                schema: None,
            },
            MaskPrecedence::Tag,
        );
        assert!(conflicts.is_empty());
        assert!(matches!(policy.mask_for("ssn"), Some(MaskType::Nullify)));
    }
}
