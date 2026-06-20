mod event;
mod logger;
mod ocsf;
mod redact;
mod sink;

pub use event::{
    Actor, AuditEvent, AuditKind, Integrity, ObjectType, Outcome, PolicyAudit, QueryInfo,
    QueryStats, Resource, Timing,
};
pub use logger::{query_hash, AuditEntry, AuditLogger};
pub use ocsf::to_ocsf;
pub use redact::{redact_pii, strip_sql_literals};
pub use sink::{AuditFormat, AuditSink, NativeJsonlSink, OcsfJsonlSink};

#[cfg(test)]
pub(crate) use event::sample_query_event;
