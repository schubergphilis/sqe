//! A hook on every table SQE plans against (#903).
//!
//! A tag committed to Polaris by another client reaches SQE's own masks on the
//! next statement, through the per-statement revalidation. Ranger's tag store,
//! which Spark reads, only learned of tags SQE itself wrote. The observer lets
//! the write layer see each loaded table and project a tag change it finds, so
//! the two engines converge on the same tags.
//!
//! The catalog crate cannot depend on the projector (it lives above it), so
//! the dependency is inverted: the coordinator installs an observer into the
//! shared [`crate::TableMetadataCache`], and the schema provider reports every
//! successful table load to it.

use std::sync::{Arc, OnceLock};

use iceberg::table::Table;
use iceberg::TableIdent;

use crate::schema_tags::SchemaTagState;

/// Told about every table a schema provider loads for planning.
///
/// Called on the planning path: an implementation must return quickly and do
/// any network work in a spawned task.
pub trait TableLoadObserver: Send + Sync {
    /// `catalog` is the planner's name for the table's catalog, the name a
    /// catalog-scoped Ranger key carries (#899), not the warehouse.
    /// `schema_tags` is the state the table's scan provider is built with
    /// (#935), read once for both.
    fn table_loaded(
        &self,
        catalog: &str,
        ident: &TableIdent,
        table: &Table,
        schema_tags: &SchemaTagState,
    );
}

/// The observer slot a [`crate::TableMetadataCache`] carries. Every clone of
/// the cache shares one slot, so an observer installed after the cache was
/// handed to the session factory still sees every load.
#[derive(Clone, Default)]
pub struct LoadObserverSlot(Arc<OnceLock<Arc<dyn TableLoadObserver>>>);

impl LoadObserverSlot {
    /// Install `observer`. Returns false when one is already installed; the
    /// first stays.
    pub fn set(&self, observer: Arc<dyn TableLoadObserver>) -> bool {
        self.0.set(observer).is_ok()
    }

    /// Report a load to the installed observer, if any.
    pub fn notify(
        &self,
        catalog: &str,
        ident: &TableIdent,
        table: &Table,
        schema_tags: &SchemaTagState,
    ) {
        if let Some(observer) = self.0.get() {
            observer.table_loaded(catalog, ident, table, schema_tags);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Never;
    impl TableLoadObserver for Never {
        fn table_loaded(&self, _: &str, _: &TableIdent, _: &Table, _: &SchemaTagState) {}
    }

    #[test]
    fn clones_share_the_slot_and_the_first_observer_stays() {
        let slot = LoadObserverSlot::default();
        let clone = slot.clone();
        assert!(clone.set(Arc::new(Never)));
        assert!(!slot.set(Arc::new(Never)), "the clone installed it first");
        assert!(slot.0.get().is_some());
    }
}
