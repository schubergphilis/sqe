//! JSONL-appender file sink.
//!
//! Mirrors the `sqe_metrics::audit::AuditLogger` pattern: single mutex-guarded
//! `BufWriter<File>` opened in append mode. Flushes after each event so an
//! abrupt shutdown loses at most the last line in the OS write buffer.

use crate::event::RunEvent;
use crate::sink::{Sink, SinkError};
use std::io::Write;
use std::sync::Mutex;

pub struct FileSink {
    writer: Mutex<std::io::BufWriter<std::fs::File>>,
}

impl FileSink {
    pub fn new(path: &str) -> Result<Self, SinkError> {
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            writer: Mutex::new(std::io::BufWriter::new(f)),
        })
    }
}

#[async_trait::async_trait]
impl Sink for FileSink {
    async fn send(&self, ev: &RunEvent) -> Result<(), SinkError> {
        let line = serde_json::to_string(ev)?;
        // Recover from poison: an earlier panic should not silence future writes.
        let mut w = self
            .writer
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        writeln!(w, "{line}")?;
        w.flush()?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "file"
    }
}
