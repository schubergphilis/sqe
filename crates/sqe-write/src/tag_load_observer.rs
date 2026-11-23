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
//! The memory only saves work while it is fresh ([`TABLE_MEMORY_TTL`],
//! [`SCHEMA_MEMORY_TTL`]). Another coordinator, or an edit in Ranger's UI,
//! can change what Ranger holds behind it, so an expired entry is not
//! trusted: a table's tags are sent again as a replace, and a schema's are
//! read back from Ranger before a write is skipped.
//!
//! The table's own tags (`sqe.table-tags`) go the same way, onto the
//! table-level Ranger resource (#935). When they cannot be read, the
//! table-level resource keeps what was last projected: Spark masks only
//! what Ranger holds, so clearing it on an error would unmask every column
//! there. The column tags still project.
//!
//! With `policy.inherit_schema_tags` on, the schema's tags go onto the
//! schema-wide resource (`table:*`, #935), which Ranger applies to every
//! table and column of the schema. From a load this only ever grows: a key
//! the schema carries that Ranger does not hold yet is added, and nothing is
//! removed. Whether Ranger holds a key is read back from Ranger unless a
//! fresh memory already says so; an unreadable Ranger or schema writes
//! nothing and keeps what was projected. `ALTER SCHEMA ... SET TAGS`,
//! `DROP SCHEMA` and `CALL system.reproject_column_tags` are what shrink it.
//!
//! The projector writes to Ranger with the coordinator's Ranger credentials,
//! on behalf of whoever committed the tag to Polaris. That client could
//! already change what SQE masks; this makes Spark agree.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use iceberg::table::Table;
use iceberg::TableIdent;
use sqe_catalog::load_observer::TableLoadObserver;
use sqe_catalog::schema_tags::SchemaTagState;
use sqe_policy::tag_projector::{
    held_table_level_tags, project_table_level_tags, project_table_tags, ColumnTags, TagProjector,
};

/// How long a table's remembered projection is trusted. After it a load
/// sends the table's tags again, as a replace, in case Ranger was changed
/// behind this process.
pub(crate) const TABLE_MEMORY_TTL: Duration = Duration::from_secs(300);

/// How long a schema's remembered projection is trusted. After it a load
/// reads Ranger before it skips a write: the memory may hold a key another
/// coordinator has since removed from Ranger.
pub(crate) const SCHEMA_MEMORY_TTL: Duration = Duration::from_secs(120);

use crate::catalog_ops::{keyed_lock, TableLockMap};

/// The table state a projection last succeeded for.
#[derive(Clone)]
struct Projected {
    location: String,
    last_updated_ms: i64,
    tags: ColumnTags,
    /// The table's own tag keys on the table-level resource (#935).
    table_tags: Vec<String>,
    at: Instant,
}

impl Projected {
    fn fresh(&self) -> bool {
        self.at.elapsed() < TABLE_MEMORY_TTL
    }
}

/// What the observer last projected, per `{catalog}|{ns}.{table}`, and the
/// schema tag keys the schema-wide resource holds, per
/// [`schema_key`].
///
/// `CatalogOps` owns it and hands it to the observer, so a DDL statement that
/// projects a table itself can [`forget`](Self::forget) it (#928). Without
/// that, a dropped table's remembered tags would make the observer skip a
/// table recreated under the same name with the same tags.
#[derive(Clone, Default)]
pub(crate) struct TagLoadMemory {
    tables: Arc<Mutex<HashMap<String, Projected>>>,
    schemas: Arc<Mutex<HashMap<String, SchemaHeld>>>,
}

/// A schema-wide resource's tag keys, and when they were written or read.
type SchemaHeld = (Vec<String>, Instant);

/// The [`TagLoadMemory`] key of a schema's schema-wide resource.
pub(crate) fn schema_key(catalog: &str, namespace: &iceberg::NamespaceIdent) -> String {
    format!("{catalog}|{}", namespace.to_url_string())
}

/// The lock a schema-wide projection takes, so the observer and `ALTER
/// SCHEMA` never interleave their writes to it.
pub(crate) fn schema_lock_key(catalog: &str, namespace: &iceberg::NamespaceIdent) -> String {
    format!("schema-projection|{}", schema_key(catalog, namespace))
}

impl TagLoadMemory {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Projected>> {
        self.tables.lock().expect("projected tags mutex poisoned")
    }

    /// The schema tag keys Ranger held under `key` when this process last
    /// wrote or read them, while that is fresh ([`SCHEMA_MEMORY_TTL`]);
    /// `None` when unknown or expired.
    pub(crate) fn schema_held(&self, key: &str) -> Option<Vec<String>> {
        let schemas = self.schemas.lock().expect("projected tags mutex poisoned");
        schemas
            .get(key)
            .filter(|(_, at)| at.elapsed() < SCHEMA_MEMORY_TTL)
            .map(|(tags, _)| tags.clone())
    }

    /// Record what Ranger now holds under `key`.
    pub(crate) fn record_schema(&self, key: String, tags: Vec<String>) {
        let mut schemas = self.schemas.lock().expect("projected tags mutex poisoned");
        schemas.insert(key, (tags, Instant::now()));
    }

    /// Drop what was remembered for the schema-wide resource under `key`.
    pub(crate) fn forget_schema(&self, key: &str) {
        let mut schemas = self.schemas.lock().expect("projected tags mutex poisoned");
        schemas.remove(key);
    }

    /// Age every entry by `by`, as if that much time had passed.
    #[cfg(test)]
    pub(crate) fn age(&self, by: Duration) {
        let back = |at: &mut Instant| *at = at.checked_sub(by).expect("clock too young to age");
        self.lock().values_mut().for_each(|p| back(&mut p.at));
        let mut schemas = self.schemas.lock().expect("projected tags mutex poisoned");
        schemas.values_mut().for_each(|(_, at)| back(at));
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
    metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
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
            metrics: None,
        }
    }

    /// Count tags that could not be read on load in
    /// `sqe_tag_projection_failures_total{op="source"}`.
    pub(crate) fn with_metrics(
        mut self,
        metrics: Option<Arc<sqe_metrics::MetricsRegistry>>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    fn count_unreadable(&self) {
        if let Some(m) = &self.metrics {
            m.tag_projection_failures_total
                .with_label_values(&["source"])
                .inc();
        }
    }

    fn is_current(&self, key: &str, location: &str) -> bool {
        let projected = self.projected.lock();
        projected
            .get(key)
            .is_some_and(|p| p.location == location && p.fresh())
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
                Some(p) if p.last_updated_ms > obs.last_updated_ms => return,
                Some(p) if p.location == obs.location && p.fresh() => return,
                // An expired memory is a guess: send the tags again.
                Some(p) => (p.tags.clone(), p.table_tags.clone(), !p.fresh()),
                None => (ColumnTags::new(), Vec::new(), false),
            }
        };
        let (previous, previous_table, refresh) = previous;
        let (ns, name) = (obs.ident.namespace().as_ref(), obs.ident.name());
        // A data commit leaves the tags alone; nothing to send while the
        // memory of what Ranger holds is fresh.
        if refresh || previous != obs.tags {
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
                self.count_unreadable();
                previous_table
            }
            Some(now) if now == previous_table && !refresh => now,
            Some(now) if now.is_empty() && previous_table.is_empty() => now,
            Some(now) => {
                // A non-empty set replaces what the resource holds; an empty
                // one deletes what `previous_table` says was projected.
                let prev = &previous_table[..];
                let scoped = self.catalog_scoped;
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
            at: Instant::now(),
        };
        self.projected.lock().insert(obs.key, state);
    }

    /// Add the schema tag keys in `now` that Ranger does not hold yet to the
    /// schema-wide resource. Never removes one: the set sent is what Ranger
    /// holds plus `now`, as one replace. What Ranger holds is read back from
    /// it unless a fresh memory already covers `now`; when it cannot be read,
    /// nothing is written. Serialised with `ALTER SCHEMA`'s own.
    async fn converge_schema(
        &self,
        catalog: String,
        ns: iceberg::NamespaceIdent,
        now: Vec<String>,
    ) {
        let lock = keyed_lock(&self.table_locks, schema_lock_key(&catalog, &ns));
        let _guard = lock.lock().await;
        let key = schema_key(&catalog, &ns);
        let covered = |held: &[String]| now.iter().all(|k| held.contains(k));
        if self
            .projected
            .schema_held(&key)
            .is_some_and(|h| covered(&h))
        {
            return;
        }
        let (projector, scoped) = (self.projector.as_ref(), self.catalog_scoped);
        let held = match held_table_level_tags(projector, &catalog, ns.as_ref(), "*", scoped).await
        {
            Ok(held) => held,
            Err(e) => {
                tracing::error!(
                    schema = %ns.to_url_string(),
                    error = %e,
                    "cannot read the schema tags Ranger holds; nothing is written, and a \
                     schema tag Ranger lacks reaches other engines on a later load"
                );
                return;
            }
        };
        let want = crate::catalog_ops::object_tags::union(&held, &now);
        if covered(&held) {
            self.projected.record_schema(key, held);
            return;
        }
        let done =
            project_table_level_tags(projector, &catalog, ns.as_ref(), "*", scoped, &[], &want);
        match done.await {
            Ok(()) => self.projected.record_schema(key, want),
            Err(e) => tracing::warn!(
                schema = %ns.to_url_string(),
                error = %e,
                "could not project a schema tag found on load into Ranger; \
                 other engines miss it until the next load retries"
            ),
        }
    }

    /// The schema half of a load: the keys to converge on when the schema
    /// carries one the schema-wide resource lacks; `None` on an unreadable
    /// schema, which keeps what is projected.
    fn schema_change(
        &self,
        catalog: &str,
        ns: &iceberg::NamespaceIdent,
        schema_tags: &SchemaTagState,
    ) -> Option<Vec<String>> {
        let now: Vec<String> = match schema_tags {
            SchemaTagState::NotInherited => return None,
            SchemaTagState::Unknown => {
                tracing::error!(
                    schema = %ns.to_url_string(),
                    "cannot read the schema's tags; the schema tags last projected into \
                     Ranger are kept as they are, and a change to them is not projected"
                );
                self.count_unreadable();
                return None;
            }
            SchemaTagState::Known(tags) => tags.iter().map(|t| t.key.clone()).collect(),
        };
        if now.is_empty() {
            return None;
        }
        let held = self.projected.schema_held(&schema_key(catalog, ns));
        let covered = held.is_some_and(|h| now.iter().all(|k| h.contains(k)));
        (!covered).then_some(now)
    }
}

impl TableLoadObserver for TagLoadObserver {
    fn table_loaded(
        &self,
        catalog: &str,
        ident: &TableIdent,
        table: &Table,
        schema_tags: &SchemaTagState,
    ) {
        // Before the table's own early return: a schema tag changes no
        // table metadata.
        let ns = ident.namespace();
        if let Some(now) = self.schema_change(catalog, ns, schema_tags) {
            let (this, catalog, ns) = (self.clone(), catalog.to_string(), ns.clone());
            tokio::spawn(async move { this.converge_schema(catalog, ns, now).await });
        }
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
                self.count_unreadable();
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

    /// Records every call, and keeps a tag store for the table-level
    /// resources with the semantics probed on Ranger 2.8: a non-empty set
    /// replaces, an empty one with a `previous` deletes. Two observers on one
    /// `Recording` are two coordinators on one Ranger.
    #[derive(Default)]
    struct Recording {
        calls: Mutex<Calls>,
        table_calls: Mutex<TableCalls>,
        store: Mutex<HashMap<String, Vec<String>>>,
        fail: std::sync::atomic::AtomicBool,
        fail_read: std::sync::atomic::AtomicBool,
        reads: std::sync::atomic::AtomicUsize,
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
            let key = format!("{}|{}", table.database, table.table);
            let mut store = self.store.lock().unwrap();
            if !tags.is_empty() {
                store.insert(key, tags.to_vec());
            } else if !previous.is_empty() {
                store.remove(&key);
            }
            Ok(())
        }

        async fn held_table_level(&self, table: &TagTableKey) -> sqe_core::Result<Vec<String>> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fail_read.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(SqeError::with_code(
                    SqeErrorCode::ExecutionFailed,
                    "ranger unreadable",
                ));
            }
            let key = format!("{}|{}", table.database, table.table);
            Ok(self
                .store
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or_default())
        }

        fn enabled(&self) -> bool {
            true
        }
    }

    impl Recording {
        fn held(&self, key: &str) -> Vec<String> {
            self.store
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .unwrap_or_default()
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

    fn schema(keys: &[&str]) -> SchemaTagState {
        use sqe_core::column_tags::ColumnTag;
        SchemaTagState::Known(keys.iter().map(|k| ColumnTag::key(*k)).collect())
    }

    fn strings(keys: &[&str]) -> Vec<String> {
        keys.iter().map(ToString::to_string).collect()
    }

    /// #935: the schema-wide resource is the table `*` of the namespace.
    #[test]
    fn the_schema_wide_resource_is_keyed_table_star() {
        let ns = ["ns".to_string()];
        let bare = TagTableKey::for_table("cat", &ns, "*", false).unwrap();
        assert_eq!((bare.database.as_str(), bare.table.as_str()), ("ns", "*"));
        let scoped = TagTableKey::for_table("cat", &ns, "*", true).unwrap();
        assert_eq!(
            (scoped.database.as_str(), scoped.table.as_str()),
            ("cat.ns", "*")
        );
    }

    #[tokio::test]
    async fn a_new_schema_key_sends_the_union_as_one_replace() {
        let (rec, o) = setup();
        let ns = NamespaceIdent::new("ns".into());
        let now = o.schema_change("cat", &ns, &schema(&["pii"])).unwrap();
        o.converge_schema("cat".into(), ns.clone(), now).await;
        let now = o.schema_change("cat", &ns, &schema(&["eu"])).unwrap();
        o.converge_schema("cat".into(), ns.clone(), now).await;
        let want = vec![
            ("*".to_string(), vec![], strings(&["pii"])),
            ("*".to_string(), vec![], strings(&["pii", "eu"])),
        ];
        assert_eq!(table_calls(&rec), want, "never a delete, never a shrink");
        let held = o.projected.schema_held("cat|ns");
        assert_eq!(held, Some(strings(&["pii", "eu"])));
    }

    #[tokio::test]
    async fn a_stale_schema_subset_does_not_shrink() {
        let (rec, o) = setup();
        let ns = NamespaceIdent::new("ns".into());
        o.projected
            .record_schema("cat|ns".into(), strings(&["pii", "eu"]));
        assert!(o.schema_change("cat", &ns, &schema(&["pii"])).is_none());
        assert!(o.schema_change("cat", &ns, &schema(&[])).is_none());
        o.converge_schema("cat".into(), ns, strings(&["pii"])).await;
        assert!(table_calls(&rec).is_empty());
        let held = o.projected.schema_held("cat|ns");
        assert_eq!(held, Some(strings(&["pii", "eu"])));
    }

    #[tokio::test]
    async fn not_inherited_projects_no_schema_tags() {
        let (_, o) = setup();
        let ns = NamespaceIdent::new("ns".into());
        assert!(o
            .schema_change("cat", &ns, &SchemaTagState::NotInherited)
            .is_none());
    }

    /// #935, fail closed: the schema cannot be read, so the schema-wide
    /// resource keeps what was projected, while the table's column and
    /// table-level tags still project.
    #[tokio::test]
    async fn an_unreadable_schema_keeps_its_projection_and_the_table_still_projects() {
        let (rec, o) = setup();
        let ns = NamespaceIdent::new("ns".into());
        o.projected
            .record_schema("cat|ns".into(), strings(&["pii"]));
        assert!(o
            .schema_change("cat", &ns, &SchemaTagState::Unknown)
            .is_none());
        assert_eq!(o.projected.schema_held("cat|ns"), Some(strings(&["pii"])));

        o.converge(with_table_tags(
            obs("m1", 1, &[("ssn", "PII")]),
            Some(&["gold"]),
        ))
        .await;
        assert_eq!(calls(&rec).len(), 1, "the column tag projects");
        let want = vec![("t".to_string(), vec![], strings(&["gold"]))];
        assert_eq!(
            table_calls(&rec),
            want,
            "the table tag projects, the schema is untouched"
        );
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
            "replaced from what was held"
        );
        assert_eq!(last.2, vec!["pii".to_string(), "gold".to_string()]);
        assert_eq!(
            rec.held("ns|t"),
            vec!["pii".to_string(), "gold".to_string()]
        );
    }

    /// #935, the fail-open this guards against: coordinator B remembers the
    /// schema-wide resource holding `pii`, coordinator A has since removed it
    /// from Ranger, and the schema carries `pii` again. Once B's memory has
    /// expired, B reads Ranger rather than trust it, and writes `pii` back.
    #[tokio::test]
    async fn an_expired_memory_is_checked_against_ranger_before_a_skip() {
        let rec = Arc::new(Recording::default());
        let locks: TableLockMap = Arc::new(Mutex::new(HashMap::new()));
        let a = TagLoadObserver::new(rec.clone(), false, locks.clone(), TagLoadMemory::default());
        let b = TagLoadObserver::new(rec.clone(), false, locks, TagLoadMemory::default());
        let ns = NamespaceIdent::new("ns".into());

        let now = b.schema_change("cat", &ns, &schema(&["pii"])).unwrap();
        b.converge_schema("cat".into(), ns.clone(), now).await;
        assert_eq!(rec.held("ns|*"), strings(&["pii"]));

        // A unsets pii: Ranger no longer holds it; B's memory still says so.
        let pii = strings(&["pii"]);
        let unset =
            project_table_level_tags(rec.as_ref(), "cat", ns.as_ref(), "*", false, &pii, &[]);
        unset.await.unwrap();
        assert!(rec.held("ns|*").is_empty());
        drop(a);

        assert!(
            b.schema_change("cat", &ns, &schema(&["pii"])).is_none(),
            "a fresh memory skips: the window the TTL bounds"
        );
        b.projected.age(SCHEMA_MEMORY_TTL);
        let now = b.schema_change("cat", &ns, &schema(&["pii"])).unwrap();
        b.converge_schema("cat".into(), ns.clone(), now).await;
        assert_eq!(rec.held("ns|*"), strings(&["pii"]), "pii is masked again");
    }

    /// #935: a key Ranger already holds (another coordinator wrote it) is
    /// recorded from the read, not written again; and the read never shrinks
    /// what Ranger holds.
    #[tokio::test]
    async fn a_key_ranger_holds_is_recorded_not_written() {
        let (rec, o) = setup();
        rec.store
            .lock()
            .unwrap()
            .insert("ns|*".into(), strings(&["pii", "eu"]));
        let ns = NamespaceIdent::new("ns".into());
        let now = o.schema_change("cat", &ns, &schema(&["pii"])).unwrap();
        o.converge_schema("cat".into(), ns.clone(), now).await;
        assert!(table_calls(&rec).is_empty(), "nothing written");
        assert_eq!(
            o.projected.schema_held("cat|ns"),
            Some(strings(&["pii", "eu"]))
        );

        let now = o.schema_change("cat", &ns, &schema(&["gold"])).unwrap();
        o.converge_schema("cat".into(), ns, now).await;
        assert_eq!(rec.held("ns|*"), strings(&["pii", "eu", "gold"]));
    }

    /// #935, fail closed: when Ranger cannot be read, nothing is written and
    /// nothing is remembered, so the next load tries again.
    #[tokio::test]
    async fn an_unreadable_ranger_writes_nothing() {
        let (rec, o) = setup();
        rec.fail_read
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let ns = NamespaceIdent::new("ns".into());
        let now = o.schema_change("cat", &ns, &schema(&["pii"])).unwrap();
        o.converge_schema("cat".into(), ns.clone(), now).await;
        assert!(table_calls(&rec).is_empty());
        assert_eq!(o.projected.schema_held("cat|ns"), None);

        rec.fail_read
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let now = o.schema_change("cat", &ns, &schema(&["pii"])).unwrap();
        o.converge_schema("cat".into(), ns, now).await;
        assert_eq!(rec.held("ns|*"), strings(&["pii"]));
    }

    /// Loads that queue behind one expired schema read Ranger once: the
    /// first records what it read, the rest find a fresh memory.
    #[tokio::test]
    async fn queued_loads_read_ranger_once() {
        let (rec, o) = setup();
        let ns = NamespaceIdent::new("ns".into());
        for _ in 0..3 {
            o.converge_schema("cat".into(), ns.clone(), strings(&["pii"]))
                .await;
        }
        assert_eq!(rec.reads.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(table_calls(&rec).len(), 1);
    }

    /// #935: a table whose memory has expired is sent again as a replace,
    /// though neither its metadata nor its tags changed, in case Ranger was
    /// changed behind this process. Fresh, the same load sends nothing.
    #[tokio::test]
    async fn an_expired_table_memory_is_sent_again() {
        let (rec, o) = setup();
        let tagged = || with_table_tags(obs("m1", 1, &[("ssn", "PII")]), Some(&["gold"]));
        o.converge(tagged()).await;
        o.converge(tagged()).await;
        assert_eq!((calls(&rec).len(), table_calls(&rec).len()), (1, 1));
        assert!(o.is_current("cat|ns.t", "m1"));

        o.projected.age(TABLE_MEMORY_TTL);
        assert!(
            !o.is_current("cat|ns.t", "m1"),
            "an expired memory is not current"
        );
        o.converge(tagged()).await;
        assert_eq!((calls(&rec).len(), table_calls(&rec).len()), (2, 2));
        assert_eq!(calls(&rec)[1].2, calls(&rec)[0].2, "the same tags again");
    }

    /// Unreadable tags on load count in
    /// `sqe_tag_projection_failures_total{op="source"}`.
    #[tokio::test]
    async fn unreadable_tags_are_counted() {
        let (_, o) = setup();
        let metrics = Arc::new(sqe_metrics::MetricsRegistry::new().unwrap());
        let o = o.with_metrics(Some(metrics.clone()));
        let ns = NamespaceIdent::new("ns".into());
        assert!(o
            .schema_change("cat", &ns, &SchemaTagState::Unknown)
            .is_none());
        o.converge(with_table_tags(obs("m1", 1, &[]), None)).await;
        let n = metrics
            .tag_projection_failures_total
            .with_label_values(&["source"])
            .get();
        assert_eq!(n, 2);
    }
}
