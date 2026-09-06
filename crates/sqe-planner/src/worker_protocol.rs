//! Coordinator/worker protocol handshake (finding 408 / #588).
//!
//! The wire between coordinator and worker is a [`crate::ScanTask`] JSON plus
//! an optional `datafusion_proto` logical `Expr`. `ScanTask::version` guards
//! the JSON layout only; the `Expr` bytes are coupled to the exact DataFusion
//! release on both sides, and a skew used to fail at decode time inside the
//! worker with a generic error. The heartbeat now carries both an integer
//! protocol version (`sqe.proto.version`) and the DataFusion version string so
//! the coordinator can refuse a mismatched worker at registration.

use serde::{Deserialize, Serialize};

/// `sqe.proto.version`: bump when the coordinator/worker wire contract changes
/// in a way an older peer cannot honour (ticket layout, action names, response
/// bodies). Independent of [`crate::SCAN_TASK_VERSION_CURRENT`], which only
/// versions the `ScanTask` JSON.
pub const PROTO_VERSION: u32 = 1;

/// Version a heartbeat is treated as when it carries no protocol fields at
/// all: the legacy body is the bare worker URL.
pub const PROTO_VERSION_UNKNOWN: u32 = 0;

/// Body of the worker `heartbeat` action.
///
/// Serialised as JSON. Older workers send the bare advertise URL instead;
/// [`HeartbeatPayload::parse`] accepts both and reports the legacy shape as
/// [`PROTO_VERSION_UNKNOWN`] with an empty DataFusion version, so the
/// coordinator can decide (warn or refuse) rather than the decoder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatPayload {
    /// The worker's routable Flight URL.
    pub worker_url: String,
    /// [`PROTO_VERSION`] of the sending worker; `0` when absent.
    #[serde(default)]
    pub proto_version: u32,
    /// `datafusion::DATAFUSION_VERSION` of the sending worker; empty when absent.
    #[serde(default)]
    pub datafusion_version: String,
}

impl HeartbeatPayload {
    /// Build the payload the current worker build sends.
    pub fn current(worker_url: impl Into<String>, datafusion_version: &str) -> Self {
        Self {
            worker_url: worker_url.into(),
            proto_version: PROTO_VERSION,
            datafusion_version: datafusion_version.to_string(),
        }
    }

    /// JSON body for the `heartbeat` action.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("HeartbeatPayload serialises")
    }

    /// Parse a heartbeat body: JSON (current workers) or a bare URL (legacy).
    ///
    /// Returns `Err` only when the body is not UTF-8 or is a JSON object
    /// without a `worker_url`. A body that is not a JSON object is taken as a
    /// bare URL, so a legacy worker keeps registering.
    pub fn parse(body: &[u8]) -> Result<Self, String> {
        let text = std::str::from_utf8(body).map_err(|e| format!("Invalid heartbeat body: {e}"))?;
        let trimmed = text.trim();
        if trimmed.starts_with('{') {
            return serde_json::from_str::<Self>(trimmed)
                .map_err(|e| format!("Invalid heartbeat body: {e}"));
        }
        Ok(Self {
            worker_url: trimmed.to_string(),
            proto_version: PROTO_VERSION_UNKNOWN,
            datafusion_version: String::new(),
        })
    }

    /// True when the payload predates the handshake (no version fields).
    pub fn is_legacy(&self) -> bool {
        self.proto_version == PROTO_VERSION_UNKNOWN && self.datafusion_version.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_roundtrip_carries_versions() {
        let p = HeartbeatPayload::current("http://w1:50052", "55.0.0");
        let parsed = HeartbeatPayload::parse(p.to_json().as_bytes()).unwrap();
        assert_eq!(parsed, p);
        assert_eq!(parsed.proto_version, PROTO_VERSION);
        assert!(!parsed.is_legacy());
    }

    #[test]
    fn bare_url_is_legacy_version_zero() {
        let parsed = HeartbeatPayload::parse(b"http://w1:50052").unwrap();
        assert_eq!(parsed.worker_url, "http://w1:50052");
        assert_eq!(parsed.proto_version, PROTO_VERSION_UNKNOWN);
        assert!(parsed.is_legacy());
    }

    #[test]
    fn json_without_versions_is_legacy() {
        let parsed = HeartbeatPayload::parse(br#"{"worker_url":"http://w1:50052"}"#).unwrap();
        assert!(parsed.is_legacy());
    }

    #[test]
    fn json_without_url_is_rejected() {
        assert!(HeartbeatPayload::parse(br#"{"proto_version":1}"#).is_err());
        assert!(HeartbeatPayload::parse(&[0xff, 0xfe]).is_err());
    }
}
