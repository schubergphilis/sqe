//! OCSF-aligned audit logging: a structured event model, tamper-evident hash
//! chaining, PII/GDPR redaction, pluggable sinks, and async export to
//! OTLP/SIEM backends.

mod chain;
mod event;
pub mod export;
mod logger;
mod ocsf;
mod redact;
mod sink;
mod tag_lookup;

pub use chain::{verify_chain, ChainError, HashChain};
pub use event::{
    Actor, AuditEvent, AuditKind, Integrity, ObjectType, Outcome, PolicyAudit, QueryInfo,
    QueryStats, Resource, Timing,
};
pub use export::SeqCursor;
pub use logger::{query_hash, AuditEntry, AuditLogger};
pub use ocsf::to_ocsf;
pub use redact::{mask_gdpr_columns, redact_pii, strip_sql_literals, GdprIdentifierMode};
pub use sink::{AuditFormat, AuditSink, NativeJsonlSink, OcsfJsonlSink};
pub use tag_lookup::{NoopTagLookup, TagLookup};

#[cfg(test)]
pub(crate) use event::sample_query_event;
