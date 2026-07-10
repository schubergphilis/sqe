//! Sink trait + MultiSink combinator.
//!
//! See `docs/superpowers/specs/2026-05-08-openlineage-emitter-design.md` §6.

use crate::event::RunEvent;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("http error: {0}")]
    Http(String),
    #[error("serialise error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("other: {0}")]
    Other(String),
}

#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    async fn send(&self, event: &RunEvent) -> Result<(), SinkError>;
    fn name(&self) -> &'static str;
}

/// Fans out one event to multiple sinks. Failures are isolated: a sink failing
/// never blocks another. Each failure increments the per-sink Prometheus counter.
pub struct MultiSink {
    sinks: Vec<Arc<dyn Sink>>,
    errors: prometheus::IntCounterVec,
}

impl MultiSink {
    pub fn new(sinks: Vec<Arc<dyn Sink>>) -> Self {
        let errors = prometheus::IntCounterVec::new(
            prometheus::Opts::new("sqe_lineage_sink_errors_total", "OL sink failures"),
            &["sink"],
        )
        .expect("static Prometheus opts cannot fail to validate");
        Self { sinks, errors }
    }

    /// Send an event to every configured sink in parallel.
    /// Logs and counts each failure; never returns an error to the caller.
    pub async fn send(&self, ev: &RunEvent) {
        let futs = self.sinks.iter().map(|s| {
            let s = s.clone();
            async move { (s.name(), s.send(ev).await) }
        });
        let results = futures::future::join_all(futs).await;
        for (name, r) in results {
            if let Err(e) = r {
                self.errors.with_label_values(&[name]).inc();
                tracing::warn!(sink = name, error = %e, "OL sink failed");
            }
        }
    }
}
