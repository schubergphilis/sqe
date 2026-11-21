//! Tag policies scoped to a catalog (#919).
//!
//! Under `[policy.ranger] catalog_scoped`, a tag policy written for
//! `<catalog>.<tag>` (`CREATE POLICY ... ON TAG sales.pii`) governs only the
//! tables of that catalog, while column tags stay bare (`SET TAGS ... 'pii'`).
//! A column of catalog `c` tagged `t` is resolved against both `c.t` and the
//! bare `t`:
//!
//! - a mask written for `c.t` wins over one written for `t`;
//! - row filters of both apply (ANDed), which only ever hides more rows;
//! - an unmappable tag of either restricts the column.
//!
//! A bare tag policy keeps applying in every catalog, as it did before #919, so
//! switching the feature on never unmasks a column. The projector writes both
//! names into Ranger's tag store, so Spark through Ranger's own evaluator
//! applies the same two policies.
//!
//! Without `catalog_scoped` (or without a catalog to scope by), resolution is
//! the store's plain `resolve_tags`, unchanged.

use std::collections::{HashMap, HashSet};

use datafusion::logical_expr::Expr;
use sqe_core::session::SessionUser;
use sqe_policy_api::{PolicyStore, TagMaskSpec};

/// The tag name a catalog-scoped tag policy carries for `tag` in `catalog`.
pub fn qualify_tag(catalog: &str, tag: &str) -> String {
    format!("{catalog}.{tag}")
}

/// [`PolicyStore::resolve_tags`] for the tags of a table in `catalog`, with the
/// catalog-scoped rules in the module doc. The result is keyed by the bare tag
/// names in `tags`, the names the table's columns carry, so callers merge it
/// exactly as before.
// The 3-tuple is `PolicyStore::resolve_tags`'s documented contract.
#[allow(clippy::type_complexity)]
pub async fn resolve_tags_in_catalog(
    store: &dyn PolicyStore,
    user: &SessionUser,
    tags: &HashSet<String>,
    catalog: Option<&str>,
) -> (HashMap<String, TagMaskSpec>, Vec<Expr>, HashSet<String>) {
    let catalog = match catalog {
        Some(c) if store.catalog_scoped() && !c.is_empty() => c,
        _ => return store.resolve_tags(user, tags).await,
    };
    // qualified name -> the bare tag it stands for
    let qualified: HashMap<String, &str> = tags
        .iter()
        .map(|t| (qualify_tag(catalog, t), t.as_str()))
        .collect();
    let mut lookup = tags.clone();
    lookup.extend(qualified.keys().cloned());

    let (masks, filters, unmappable) = store.resolve_tags(user, &lookup).await;
    (
        scope_masks(tags, &qualified, masks),
        filters,
        scope_unmappable(tags, &qualified, unmappable),
    )
}

/// Bare-keyed masks: the bare tag's own mask, replaced by the catalog's.
fn scope_masks(
    tags: &HashSet<String>,
    qualified: &HashMap<String, &str>,
    mut masks: HashMap<String, TagMaskSpec>,
) -> HashMap<String, TagMaskSpec> {
    let mut scoped: HashMap<String, TagMaskSpec> = HashMap::new();
    for tag in tags {
        if let Some(spec) = masks.get(tag) {
            scoped.insert(tag.clone(), spec.clone());
        }
    }
    for (name, bare) in qualified {
        if let Some(spec) = masks.remove(name) {
            scoped.insert((*bare).to_string(), spec);
        }
    }
    scoped
}

/// Bare-keyed unmappable tags: unmappable under either name.
fn scope_unmappable(
    tags: &HashSet<String>,
    qualified: &HashMap<String, &str>,
    unmappable: HashSet<String>,
) -> HashSet<String> {
    let mut scoped = HashSet::new();
    for name in &unmappable {
        if tags.contains(name) {
            scoped.insert(name.clone());
        }
        if let Some(bare) = qualified.get(name) {
            scoped.insert((*bare).to_string());
        }
    }
    scoped
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use datafusion::logical_expr::{col, lit};
    use sqe_policy_api::{MaskType, ResolvedPolicy};

    /// A tag store with fixed answers; `scoped` is what `catalog_scoped` says.
    struct TagStore {
        scoped: bool,
        masks: Vec<(&'static str, MaskType)>,
        filters: Vec<(&'static str, Expr)>,
        unmappable: Vec<&'static str>,
    }

    #[async_trait]
    impl PolicyStore for TagStore {
        fn catalog_scoped(&self) -> bool {
            self.scoped
        }
        async fn resolve(
            &self,
            _user: &SessionUser,
            _table: &str,
            _namespace: &str,
        ) -> sqe_core::Result<ResolvedPolicy> {
            Ok(ResolvedPolicy::default())
        }
        async fn resolve_tags(
            &self,
            _user: &SessionUser,
            tags: &HashSet<String>,
        ) -> (HashMap<String, TagMaskSpec>, Vec<Expr>, HashSet<String>) {
            let masks = self
                .masks
                .iter()
                .filter(|(t, _)| tags.contains(*t))
                .map(|(t, m)| ((*t).to_string(), TagMaskSpec::Ready(m.clone())))
                .collect();
            let filters = self
                .filters
                .iter()
                .filter(|(t, _)| tags.contains(*t))
                .map(|(_, e)| e.clone())
                .collect();
            let unmappable = self
                .unmappable
                .iter()
                .filter(|t| tags.contains(**t))
                .map(|t| (*t).to_string())
                .collect();
            (masks, filters, unmappable)
        }
    }

    fn user() -> SessionUser {
        SessionUser {
            username: "bob".to_string(),
            roles: Vec::new(),
            subject: None,
            email: None,
            groups: Vec::new(),
        }
    }

    fn tag_set(tags: &[&str]) -> HashSet<String> {
        tags.iter().map(|t| (*t).to_string()).collect()
    }

    fn ready(spec: Option<&TagMaskSpec>) -> Option<&MaskType> {
        match spec {
            Some(TagMaskSpec::Ready(m)) => Some(m),
            _ => None,
        }
    }

    /// The acceptance case: `a.pii` masks catalog `a`'s `pii` columns and not
    /// catalog `b`'s, which `b.pii` masks its own way.
    #[tokio::test]
    async fn scoped_tag_policy_governs_only_its_catalog() {
        let store = TagStore {
            scoped: true,
            masks: vec![("a.pii", MaskType::Nullify), ("b.pii", MaskType::Hash)],
            filters: vec![],
            unmappable: vec![],
        };
        let tags = tag_set(&["pii"]);
        let (a, _, _) = resolve_tags_in_catalog(&store, &user(), &tags, Some("a")).await;
        let (b, _, _) = resolve_tags_in_catalog(&store, &user(), &tags, Some("b")).await;
        let (c, _, _) = resolve_tags_in_catalog(&store, &user(), &tags, Some("c")).await;
        assert!(matches!(ready(a.get("pii")), Some(MaskType::Nullify)));
        assert!(matches!(ready(b.get("pii")), Some(MaskType::Hash)));
        assert!(c.is_empty(), "no policy names catalog c: {c:?}");
    }

    /// A bare policy still applies in every catalog; the catalog's own mask
    /// wins over it, its row filter adds to the bare one, and an unmappable
    /// tag of either name restricts.
    #[tokio::test]
    async fn scoped_tag_policy_keeps_bare_policies() {
        let store = TagStore {
            scoped: true,
            masks: vec![
                ("pii", MaskType::Hash),
                ("a.pii", MaskType::Nullify),
                ("ssn", MaskType::Redact("***".into())),
            ],
            filters: vec![
                ("region", col("r").eq(lit("eu"))),
                ("a.region", col("r").eq(lit("nl"))),
            ],
            unmappable: vec!["a.secret"],
        };
        let tags = tag_set(&["pii", "ssn", "region", "secret"]);
        let (masks, filters, unmappable) =
            resolve_tags_in_catalog(&store, &user(), &tags, Some("a")).await;
        assert!(matches!(ready(masks.get("pii")), Some(MaskType::Nullify)));
        assert!(matches!(ready(masks.get("ssn")), Some(MaskType::Redact(_))));
        assert_eq!(filters.len(), 2, "both row filters apply: {filters:?}");
        assert_eq!(unmappable, tag_set(&["secret"]));
        assert!(masks.keys().all(|k| tags.contains(k)), "keys stay bare");
    }

    /// Without `catalog_scoped`, or without a catalog, a qualified policy is
    /// not consulted: resolution is the store's own, by bare name.
    #[tokio::test]
    async fn scoped_tag_policy_needs_the_flag_and_a_catalog() {
        let unscoped = TagStore {
            scoped: false,
            masks: vec![("a.pii", MaskType::Nullify)],
            filters: vec![],
            unmappable: vec![],
        };
        let tags = tag_set(&["pii"]);
        let (m, _, _) = resolve_tags_in_catalog(&unscoped, &user(), &tags, Some("a")).await;
        assert!(m.is_empty(), "{m:?}");
        let scoped = TagStore {
            scoped: true,
            ..unscoped
        };
        let (m, _, _) = resolve_tags_in_catalog(&scoped, &user(), &tags, None).await;
        assert!(m.is_empty(), "{m:?}");
    }
}
