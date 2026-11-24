//! OpenLineage emitter for SQE coordinator.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md`.

pub mod ddl;
pub mod emitter;
pub mod event;
pub mod extract;
pub mod observer;
pub mod sink;
pub mod sinks;

// The moved DDL hint builders name this crate `sqe_lineage::`, as callers do.
extern crate self as sqe_lineage;

pub use emitter::{spawn_emitter, EmitterConfig};
pub use observer::{
    ChannelObserver, LineageHint, LineageMsg, LineageScope, PlanOrHint, QueryCompleteCtx,
    QueryFailCtx, QueryStartCtx, UserCtx,
};
pub use sink::{MultiSink, Sink, SinkError};
