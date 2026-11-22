//! Engine credential identity (#934): what a session catalog needs, and what
//! a loaded table carries, when a catalog reads through the engine's keys.
//!
//! With `[catalogs.<name>.credentials]` set, a person's `loadTable` carries no
//! delegation header. After it succeeds and the policy engine allows the
//! person `select`, SQE fetches read keys from the catalog's `/credentials`
//! endpoint as the engine and puts them in the table's FileIO. Three things
//! keep that from becoming a service account:
//!
//! * [`EngineIdentity`] bundles the token source, the select gate and the
//!   audit sink, so a session catalog cannot get engine keys without a gate;
//! * the FileIO marker [`KEY_SOURCE_PROP`] makes every write path refuse a
//!   table that was filled with engine keys ([`refuse_engine_keyed`]);
//! * the FileIO marker [`READ_DENIED_PROP`] turns a denied table into an
//!   empty scan before any manifest or data file is read.

use std::sync::Arc;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::TableIdent;
use sqe_core::column_tags::ColumnTag;

/// FileIO property naming where a table's storage keys came from.
pub const KEY_SOURCE_PROP: &str = "sqe.key-source";
/// [`KEY_SOURCE_PROP`] value for keys fetched with the engine identity.
pub const KEY_SOURCE_ENGINE: &str = "engine";
/// FileIO property on a table the policy engine denied the person `select`
/// on. Such a table has its schema and no keys.
pub const READ_DENIED_PROP: &str = "sqe.read-denied";
/// `error_type` of the audit event for a failed engine key fetch.
pub const ENGINE_CREDENTIALS_ERROR_TYPE: &str = "engine_credentials";

fn file_io_prop<'a>(table: &'a Table, key: &str) -> Option<&'a str> {
    table
        .file_io()
        .config()
        .props()
        .get(key)
        .map(String::as_str)
}

/// True when `table`'s FileIO holds keys fetched with the engine identity.
pub fn is_engine_keyed(table: &Table) -> bool {
    file_io_prop(table, KEY_SOURCE_PROP) == Some(KEY_SOURCE_ENGINE)
}

/// True when the policy engine denied the person `select` on `table`: the
/// table has no keys and a scan of it is empty.
pub fn is_read_denied(table: &Table) -> bool {
    file_io_prop(table, READ_DENIED_PROP) == Some("true")
}

/// Refuse a table filled with engine keys. Called by the writer and the DML
/// storage probe, so a write path that loaded through the read client fails
/// instead of writing with the engine's keys.
pub fn refuse_engine_keyed(table: &Table, what: &str) -> sqe_core::Result<()> {
    if is_engine_keyed(table) {
        return Err(sqe_core::SqeError::Execution(format!(
            "refusing to {what} {}: the table was loaded with the engine's read keys; \
             writes load the table with the person's own credentials",
            table.identifier()
        )));
    }
    Ok(())
}

/// Whether the policy engine allows the person `select` on a table. The
/// coordinator implements it on the policy store.
#[async_trait]
pub trait ReadGate: Send + Sync {
    /// `Ok(true)` when the person may read `table`; `Ok(false)` when the
    /// plan rewriter would return no rows for it. `Err` fails the load: a
    /// gate that cannot decide never grants keys.
    ///
    /// `loaded` is the table as the person's own load returned it: tag-based
    /// policies resolve from its properties and schema, plus `schema_tags`
    /// (its schema's tags, empty unless `policy.inherit_schema_tags` is on).
    async fn may_read(
        &self,
        table: &TableIdent,
        loaded: &Table,
        schema_tags: &[ColumnTag],
    ) -> sqe_core::Result<bool>;
}

/// The engine's catalog token. The coordinator implements it on
/// `sqe_auth::per_catalog`, which caches the `client_credentials` token.
#[async_trait]
pub trait EngineTokenSource: Send + Sync {
    /// OAuth client id of the engine, named as `app_name` in audit events.
    fn client_id(&self) -> &str;
    /// A current bearer token for the engine.
    async fn token(&self) -> sqe_core::Result<String>;
}

/// One engine key fetch, for the audit log.
#[derive(Debug, Clone)]
pub struct EngineFetchEvent {
    /// The table the keys were fetched for.
    pub table: TableIdent,
    /// OAuth client id of the engine.
    pub client_id: String,
    /// `None` on success, otherwise the failure (never a key or token).
    pub failure: Option<String>,
}

/// Sink for [`EngineFetchEvent`]. `sqe-catalog` cannot reach the audit
/// logger, so the coordinator wires one per session, bound to the person.
pub trait EngineAudit: Send + Sync {
    /// Record one fetch.
    fn record(&self, event: EngineFetchEvent);
}

/// The three things a session catalog needs to read as the engine, required
/// together: no token source without a gate, no fetch without an audit sink.
#[derive(Clone)]
pub struct EngineIdentity {
    pub(crate) token: Arc<dyn EngineTokenSource>,
    pub(crate) gate: Arc<dyn ReadGate>,
    pub(crate) audit: Arc<dyn EngineAudit>,
}

impl EngineIdentity {
    /// Bundle the engine token source, the select gate and the audit sink.
    pub fn new(
        token: Arc<dyn EngineTokenSource>,
        gate: Arc<dyn ReadGate>,
        audit: Arc<dyn EngineAudit>,
    ) -> Self {
        Self { token, gate, audit }
    }
}

impl std::fmt::Debug for EngineIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineIdentity")
            .field("client_id", &self.token.client_id())
            .finish_non_exhaustive()
    }
}
