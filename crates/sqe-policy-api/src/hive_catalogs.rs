//! The live set of catalog names that hold Hive external tables. Defined here
//! so `sqe-catalog` (which registers roots on ATTACH/DETACH) and `sqe-policy`
//! (whose plan rewriter consults it on every scan) share it without the
//! catalog depending on the policy crate.


/// Live set of catalog names that hold Hive external tables rather than
/// Iceberg tables. Consulted by `resolve_policy_key` on every scan; empty in
/// every Iceberg-only deployment, which is why the Hive branch there cannot
/// change an existing key.
///
/// Read through a lock on every key resolution rather than snapshotted at
/// construction. `build_policy_enforcer` runs the rewriter's constructor once,
/// process-wide, at coordinator startup; `ATTACH ... TYPE hive_external`
/// happens later, per session. A snapshot taken at construction would go
/// stale the moment a root was attached: the Iceberg branch would keep
/// winning, and `lake.hr.employees` would silently key as `("hr",
/// "employees")`, inheriting whatever grant governs an Iceberg namespace
/// named `hr`. That is the exact escalation this type exists to prevent, so
/// it must not be reachable through staleness.
#[derive(Debug, Default)]
pub struct HiveCatalogs(std::sync::RwLock<std::collections::HashSet<String>>);

impl HiveCatalogs {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, root: &str) {
        // A poisoned lock means some other thread panicked while holding it.
        // Failing closed is not an option here: the caller (ATTACH/DETACH)
        // has no error path to report it through. The set is a name
        // registry, not state that can be left half-written, so clearing
        // the poison and continuing is safe.
        self.0
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(root.to_string());
    }

    pub fn remove(&self, root: &str) {
        self.0
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(root);
    }

    pub fn contains_name(&self, catalog: Option<&str>) -> bool {
        let Some(c) = catalog else { return false };
        self.0.read().unwrap_or_else(|e| e.into_inner()).contains(c)
    }

    pub fn is_empty(&self) -> bool {
        self.0.read().unwrap_or_else(|e| e.into_inner()).is_empty()
    }
}
