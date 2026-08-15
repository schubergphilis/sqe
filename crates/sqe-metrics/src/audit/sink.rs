//! [`AuditSink`] trait and output [`AuditFormat`]s — the pluggable destination
//! audit events are written to.

use std::io::Write;

use serde::{Deserialize, Serialize};

use super::event::AuditEvent;
use super::ocsf::to_ocsf;

/// Output format selector for audit sinks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AuditFormat {
    #[default]
    Native,
    Ocsf,
    Both,
}

/// Trait for writing audit events to a sink. Implementations must be `Send`
/// so they can be moved across thread boundaries.
pub trait AuditSink: Send {
    fn write_line(&mut self, event: &AuditEvent) -> std::io::Result<()>;
    fn flush(&mut self) -> std::io::Result<()>;
}

/// Writes canonical `AuditEvent` JSON as one newline-terminated line.
pub struct NativeJsonlSink {
    w: Box<dyn Write + Send>,
}

impl NativeJsonlSink {
    pub fn from_writer(w: Box<dyn Write + Send>) -> Self {
        Self { w }
    }

    /// Write a pre-formatted line (no serialization). Used by the legacy
    /// `log()` path so both legacy flat-JSON and canonical events flow through
    /// the same single writer, preventing torn lines on the native file.
    pub fn write_raw_line(&mut self, line: &str) -> std::io::Result<()> {
        writeln!(self.w, "{line}")
    }
}

impl AuditSink for NativeJsonlSink {
    fn write_line(&mut self, event: &AuditEvent) -> std::io::Result<()> {
        let line = serde_json::to_string(event).map_err(std::io::Error::other)?;
        writeln!(self.w, "{line}")
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

/// Writes the OCSF representation of an `AuditEvent` as one newline-terminated line.
pub struct OcsfJsonlSink {
    w: Box<dyn Write + Send>,
}

impl OcsfJsonlSink {
    pub fn from_writer(w: Box<dyn Write + Send>) -> Self {
        Self { w }
    }
}

impl AuditSink for OcsfJsonlSink {
    fn write_line(&mut self, event: &AuditEvent) -> std::io::Result<()> {
        let line = serde_json::to_string(&to_ocsf(event)).map_err(std::io::Error::other)?;
        writeln!(self.w, "{line}")
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::sample_query_event;
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    // VecWriter uses Arc<Mutex<...>> so it is Send, satisfying Box<dyn Write + Send>.
    struct VecWriter(Arc<Mutex<Vec<u8>>>);
    impl Write for VecWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn native_sink_writes_canonical_json_line() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let mut sink = NativeJsonlSink::from_writer(Box::new(VecWriter(buf.clone())));
        sink.write_line(&sample_query_event()).unwrap();
        let s = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(s.contains("\"kind\":\"query\""));
        assert!(s.ends_with('\n'));
    }

    #[test]
    fn ocsf_sink_writes_class_uid_line() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let mut sink = OcsfJsonlSink::from_writer(Box::new(VecWriter(buf.clone())));
        sink.write_line(&sample_query_event()).unwrap();
        let s = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(s.contains("\"class_uid\":6005"));
    }
}
