//! SQE metrics, OpenTelemetry, and the OCSF audit-logging subsystem.
//!
//! Exposes the Prometheus registry + `/metrics` server ([`server`]), OTel
//! init ([`otel`]), and the structured audit pipeline ([`audit`]).
//!
//! The registry types ([`MetricsRegistry`], [`WorkerMetricsRegistry`],
//! [`HasRegistry`]) and the trace-context [`propagation`] helpers live in
//! `sqe-metrics-core` and are re-exported here unchanged (review 803 item 2):
//! the policy, catalog, compaction and write crates depend on that crate
//! alone, so they no longer link the OTLP exporters, the OpenTelemetry SDK,
//! the tracing subscriber or the axum server that this crate carries.

pub mod audit;
pub mod otel;
pub mod server;

pub use sqe_metrics_core::propagation;
pub use sqe_metrics_core::{HasRegistry, MetricsRegistry, WorkerMetricsRegistry};
