//! Projects a column-tag change SQE finds on table load into Ranger (#903).
//!
//! `SET TAG` projects its own change. A tag committed to Polaris by another
//! client (Spark, a raw REST commit) masks in SQE on the next statement, but
//! Ranger's tag store, which Spark's plugin reads, never heard of it: Spark
//! kept reading a column SQE masks. This observer sees every table the
//! planner loads and, when the metadata changed since it last looked,
//! projects the difference.
//!
//! What it remembers is per process: after a restart, the first load of a
//! tagged table projects its tags once (an idempotent add-or-update), and a
//! tag removed while the coordinator was down stays in Ranger until
//! `CALL system.reproject_column_tags` repairs it.
//!
//! The table's own tags (`sqe.table-tags`) go the same way, onto the
//! table-level Ranger resource (#935). When they cannot be read, the
//! table-level resource keeps what was last projected: Spark masks only
//! what Ranger holds, so clearing it on an error would unmask every column
//! there. The column tags still project.
//!
//! The projector writes to Ranger with the coordinator's Ranger credentials,
//! on behalf of whoever committed the tag to Polaris. That client could
//! already change what SQE masks; this makes Spark agree.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use iceberg::table::Table;
use iceberg::TableIdent;
use sqe_catalog::load_observer::TableLoadObserver;
use sqe_policy::tag_projector::{
    project_table_level_tags, project_table_tags, ColumnTags, TagProjector,
};

use crate::catalog_ops::{keyed_lock, TableLockMap};

/// The table state a projection last succeeded for.
#[derive(Clone)]
struct Projected {
    location: String,
    last_updated_ms: i64,
    tags: ColumnTags,
    /// The table's own tag keys on the table-level resource (#935).
    table_tags: Vec<String>,
}

/// What the observer last projected, per `{catalog}|{ns}.{table}`.
///
/// `CatalogOps` owns it and hands it to the observer, so a DDL statement that
/// projects a table itself can [`forget`](Self::forget) it (#928). Without
/// that, a dropped table's remembered tags would make the observer skip a
/// table recreated under the same name with the same tags.
#[derive(Clone, Default)]
pub(crate) struct TagLoadMemory(Arc<Mutex<HashMap<String, Projected>>>);

impl TagLoadMemory {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Projected>> {
        self.0.lock().expect("projected tags mutex poisoned")
    }

    /// Drop what was projected for `ident` in every catalog, so its next
    /// load projects its tags afresh.
    pub(crate) fn forget(&self, ident: &TableIdent) {
        let suffix = format!("|{ident}");
        self.lock().retain(|key, _| !key.ends_with(&suffix));
    }
}

/// One table load, reduced to what the projection needs.
struct Observation {
    /// `{catalog}|{ns}.{table}`: the same table in two catalogs is two
    /// Ranger resources under `catalog_scoped`.
    key: String,
    catalog: String,
    ident: TableIdent,
    location: String,
    last_updated_ms: i64,
    tags: ColumnTags,
    /// The keys of `sqe.table-tags`; `None` when the value could not be
    /// read, which leaves the table-level resource as it is.
    table_tags: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct TagLoadObserver {
    projector: Arc<dyn TagProjector>,
    catalog_scoped: bool,
    /// `CatalogOps`' per-table locks, so a projection from here never
    /// interleaves with `SET TAG`'s own.
    table_locks: TableLockMap,
    projected: TagLoadMemory,
}

impl TagLoadObserver {
    pub(crate) fn new(
        projector: Arc<dyn TagProjector>,
        catalog_scoped: bool,
        table_locks: TableLockMap,
        projected: TagLoadMemory,
    ) -> Self {
        Self {
            projector,
            catalog_scoped,
            table_locks,
            projected,
        }
    }

    fn is_current(&self, key: &str, location: &str) -> bool {
        let projected = self.projected.lock();
        projected.get(key).is_some_and(|p| p.location == location)
    }

    /// Bring Ranger in line with one observation. Serialised per table; an
    /// observation older than the one already projected is dropped, and a
    /// failed projection is not recorded, so the next load retries it.
    async fn converge(&self, obs: Observation) {
        let lock_key = format!("{}.{}", obs.ident.namespace(), obs.ident.name());
        let lock = keyed_lock(&self.table_locks, lock_key);
        let _guard = lock.lock().await;
        let previous = {
            let projected = self.projected.lock();
            match projected.get(&obs.key) {
                Some(p)
                    if p.location == obs.location || p.last_updated_ms > obs.last_updated_ms =>
                {
                    return;
                }
                Some(p) => (p.tags.clone(), p.table_tags.clone()),
                None => (ColumnTags::new(), Vec::new()),
            }
        };
        let (previous, previous_table) = previous;
        let (ns, name) = (obs.ident.namespace().as_ref(), obs.ident.name());
        // A data commit leaves the tags alone; nothing to send.
        if previous != obs.tags {
            let (prev, tags, scoped) = (&previous, &obs.tags, self.catalog_scoped);
            let projector = self.projector.as_ref();
            let done = project_table_tags(projector, &obs.catalog, ns, name, scoped, prev, tags);
            if let Err(e) = done.await {
                tracing::warn!(
                    table = %obs.ident,
                    error = %e,
                    "could not project a column-tag change found on load into Ranger; \
                     other engines keep the old tags until the next load retries"
                );
                return;
            }
        }
        let table_tags = match obs.table_tags {
            // Never shrink on an error: what Ranger holds stays, and is
            // remembered, until a load reads the tags again (#935).
            None => {
                tracing::error!(
                    table = %obs.ident,
                    "malformed sqe.table-tags; the table-level tags last projected into \
                     Ranger are kept as they are, and a change to them is not projected"
                );
                previous_table
            }
            Some(now) if now == previous_table => now,
            Some(now) => {
                let (prev, scoped) = (&previous_table, self.catalog_scoped);
                let projector = self.projector.as_ref();
                let done =
                    project_table_level_tags(projector, &obs.catalog, ns, name, scoped, prev, &now);
                if let Err(e) = done.await {
                    tracing::warn!(
                        table = %obs.ident,
                        error = %e,
                        "could not project a table-tag change found on load into Ranger; \
                         other engines keep the old tags until the next load retries"
                    );
                    return;
                }
                now
            }
        };
        let state = Projected {
            location: obs.location,
            last_updated_ms: obs.last_updated_ms,
            tags: obs.tags,
            table_tags,
        };
        self.projected.lock().insert(obs.key, state);
    }
}

impl TableLoadObserver for TagLoadObserver {
    fn table_loaded(&self, catalog: &str, ident: &TableIdent, table: &Table) {
        let Some(location) = table.metadata_location() else {
            return;
        };
        let key = format!("{catalog}|{ident}");
        if self.is_current(&key, location) {
            return;
        }
        let metadata = table.metadata();
        let tags = match crate::tag_source_impl::table_column_tags(table) {
            Ok(tags) => tags.into_iter().collect(),
            Err(e) => {
                tracing::warn!(table = %ident, error = %e, "malformed sqe.column-tags; not projected");
                return;
            }
        };
        let table_tags = crate::catalog_ops::table_tag_keys(table).ok();
        let obs = Observation {
            key,
            catalog: catalog.to_string(),
            ident: ident.clone(),
            location: location.to_string(),
            last_updated_ms: metadata.last_updated_ms(),
            tags,
            table_tags,
        };
        let this = self.clone();
        tokio::spawn(async move { this.converge(obs).await });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::NamespaceIdent;
    use sqe_core::{SqeError, SqeErrorCode};
    use sqe_policy::tag_projector::TagTableKey;

    /// (table, previous, tags) per call.
    type Calls = Vec<(String, ColumnTags, ColumnTags)>;
    /// (table, previous, tags) per table-level call.
    type TableCalls = Vec<(String, Vec<String>, Vec<String>)>;

    #[derive(Default)]
    struct Recording {
        calls: Mutex<Calls>,
        table_calls: Mutex<TableCalls>,
        fail: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl TagProjector for Recording {
        async fn project(
            &self,
            table: &TagTableKey,
            previous: &ColumnTags,
            tags: &ColumnTags,
        ) -> sqe_core::Result<()> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    "ranger down",
                ));
            }
            let call = (table.table.clone(), previous.clone(), tags.clone());
            self.calls.lock().unwrap().push(call);
            Ok(())
        }

        async fn project_table_level(
            &self,
            table: &TagTableKey,
            previous: &[String],
            tags: &[String],
        ) -> sqe_core::Result<()> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    "ranger down",
                ));
            }
            let call = (table.table.clone(), previous.to_vec(), tags.to_vec());
            self.table_calls.lock().unwrap().push(call);
            Ok(())
        }

        fn enabled(&self) -> bool {
            true
        }
    }

    fn setup() -> (Arc<Recording>, TagLoadObserver) {
        let rec = Arc::new(Recording::default());
        let locks = Arc::new(Mutex::new(HashMap::new()));
        let memory = TagLoadMemory::default();
        (rec.clone(), TagLoadObserver::new(rec, false, locks, memory))
    }

    fn obs(location: &str, updated: i64, tags: &[(&str, &str)]) -> Observation {
        let tags = tags
            .iter()
            .map(|(c, t)| (c.to_string(), vec![t.to_string()]))
            .collect();
        Observation {
            key: "cat|ns.t".into(),
            catalog: "cat".into(),
            ident: TableIdent::new(NamespaceIdent::new("ns".into()), "t".into()),
            location: location.into(),
            last_updated_ms: updated,
            tags,
            table_tags: Some(Vec::new()),
        }
    }

    fn with_table_tags(mut o: Observation, tags: Option<&[&str]>) -> Observation {
        o.table_tags = tags.map(|ts| ts.iter().map(ToString::to_string).collect());
        o
    }

    fn table_calls(rec: &Recording) -> TableCalls {
        rec.table_calls.lock().unwrap().clone()
    }

    fn calls(rec: &Recording) -> Calls {
        rec.calls.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn cold_start_untagged_table_sends_nothing() {
        let (rec, o) = setup();
        o.converge(obs("m1", 1, &[])).await;
        assert!(calls(&rec).is_empty());
        assert!(
            o.is_current("cat|ns.t", "m1"),
            "recorded, so the next load is a no-op"
        );
    }

    #[tokio::test]
    async fn an_added_then_removed_tag_is_projected_with_its_previous_map() {
        let (rec, o) = setup();
        o.converge(obs("m1", 1, &[("ssn", "PII")])).await;
        o.converge(obs("m1", 1, &[("ssn", "PII")])).await;
        o.converge(obs("m2", 2, &[])).await;
        let c = calls(&rec);
        assert_eq!(c.len(), 2, "the repeat of m1 sends nothing: {c:?}");
        assert!(c[0].1.is_empty() && c[0].2.contains_key("ssn"));
        assert!(
            c[1].1.contains_key("ssn") && c[1].2.is_empty(),
            "the removal names ssn"
        );
    }

    #[tokio::test]
    async fn a_data_commit_that_keeps_the_tags_sends_nothing() {
        let (rec, o) = setup();
        o.converge(obs("m1", 1, &[("ssn", "PII")])).await;
        o.converge(obs("m2", 2, &[("ssn", "PII")])).await;
        assert_eq!(calls(&rec).len(), 1);
        assert!(o.is_current("cat|ns.t", "m2"));
    }

    #[tokio::test]
    async fn an_older_observation_is_dropped() {
        let (rec, o) = setup();
        o.converge(obs("m2", 2, &[])).await;
        o.converge(obs("m1", 1, &[("ssn", "PII")])).await;
        assert!(
            calls(&rec).is_empty(),
            "stale m1 must not re-add the removed tag"
        );
    }

    #[tokio::test]
    async fn a_failed_projection_is_retried_on_the_next_load() {
        let (rec, o) = setup();
        rec.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        o.converge(obs("m1", 1, &[("ssn", "PII")])).await;
        assert!(!o.is_current("cat|ns.t", "m1"));
        rec.fail.store(false, std::sync::atomic::Ordering::SeqCst);
        o.converge(obs("m1", 1, &[("ssn", "PII")])).await;
        assert_eq!(calls(&rec).len(), 1);
    }

    /// #928: a table dropped and recreated under its name with the same tags
    /// is projected again once DDL has forgotten it.
    #[tokio::test]
    async fn a_forgotten_table_is_projected_again_with_the_same_tags() {
        let (rec, o) = setup();
        o.converge(obs("m1", 1, &[("ssn", "PII")])).await;
        o.converge(obs("m2", 2, &[("ssn", "PII")])).await;
        assert_eq!(calls(&rec).len(), 1, "same tags, nothing re-sent");

        let other = TableIdent::new(NamespaceIdent::new("ns2".into()), "t".into());
        o.projected.forget(&other);
        assert!(o.is_current("cat|ns.t", "m2"), "another table is kept");

        o.projected.forget(&obs("m3", 3, &[]).ident);
        assert!(!o.is_current("cat|ns.t", "m2"));
        o.converge(obs("m3", 3, &[("ssn", "PII")])).await;
        let c = calls(&rec);
        assert_eq!(c.len(), 2, "the recreated table is projected: {c:?}");
        assert!(c[1].1.is_empty() && c[1].2.contains_key("ssn"));
    }

    #[tokio::test]
    async fn table_tags_are_projected_on_the_table_level_resource() {
        let (rec, o) = setup();
        o.converge(with_table_tags(obs("m1", 1, &[]), Some(&["pii"])))
            .await;
        o.converge(with_table_tags(obs("m2", 2, &[]), Some(&["pii"])))
            .await;
        o.converge(with_table_tags(obs("m3", 3, &[]), Some(&[])))
            .await;
        let pii = vec!["pii".to_string()];
        let want = vec![
            ("t".to_string(), vec![], pii.clone()),
            ("t".to_string(), pii, vec![]),
        ];
        assert_eq!(table_calls(&rec), want, "m2 kept the tags, so sent nothing");
        assert!(calls(&rec).is_empty(), "no column tags, no column call");
    }

    /// #935: a parent read fails and the existing projected tags stay. The
    /// table's tags cannot be read on m2, so the table-level resource is not
    /// touched (Spark keeps masking), the memory keeps them, and the column
    /// tag still projects. When m3 reads them again, only the real change is
    /// sent.
    #[tokio::test]
    async fn a_failed_parent_read_keeps_the_projected_table_tags() {
        let (rec, o) = setup();
        o.converge(with_table_tags(obs("m1", 1, &[]), Some(&["pii"])))
            .await;
        assert_eq!(table_calls(&rec).len(), 1);

        o.converge(with_table_tags(obs("m2", 2, &[("ssn", "PII")]), None))
            .await;
        assert_eq!(table_calls(&rec).len(), 1, "nothing deleted or shrunk");
        assert_eq!(calls(&rec).len(), 1, "the column's own tag still projects");
        let kept = o
            .projected
            .lock()
            .get("cat|ns.t")
            .map(|p| p.table_tags.clone());
        assert_eq!(kept, Some(vec!["pii".to_string()]));

        o.converge(with_table_tags(
            obs("m3", 3, &[("ssn", "PII")]),
            Some(&["pii", "gold"]),
        ))
        .await;
        let last = table_calls(&rec).pop().unwrap();
        assert_eq!(
            last.1,
            vec!["pii".to_string()],
            "previous is what Ranger holds"
        );
        assert_eq!(last.2, vec!["pii".to_string(), "gold".to_string()]);
    }
}
