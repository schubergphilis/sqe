//! OpenLineage emitter for SQE coordinator.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md`.

pub mod event;
pub mod extract;
pub mod observer;
pub mod emitter;
pub mod sink;
pub mod sinks;

pub use observer::{
    ChannelObserver, LineageHint, LineageMsg, LineageObserver, PlanOrHint, QueryCompleteCtx,
    QueryFailCtx, QueryStartCtx, UserCtx,
};
pub use sink::{MultiSink, Sink, SinkError};
