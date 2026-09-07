//! Coordinator/worker registration protocol (findings 408 / #588 and 609 / #616).
//!
//! The wire between coordinator and worker is a [`crate::ScanTask`] JSON plus
//! an optional `datafusion_proto` logical `Expr`. `ScanTask::version` guards
//! the JSON layout only; the `Expr` bytes are coupled to the exact DataFusion
//! release on both sides, and a skew used to fail at decode time inside the
//! worker with a generic error. The heartbeat therefore carries an integer
//! protocol version (`sqe.proto.version`) and the DataFusion version string so
//! the coordinator can refuse a mismatched worker at registration.
//!
//! # Two generations of heartbeat body, one decoder
//!
//! - **Bare URL** (pre-#588): the body is the advertise URL as UTF-8.
//! - **[`HeartbeatPayload`]** (#588 / work package #655): JSON
//!   `{worker_url, proto_version, datafusion_version}`.
//! - **[`WorkerHello`]** (#616, this module): JSON
//!   `{v, url, instance_id, build, scan_task_version, proto_version,
//!   datafusion_version, capacity, failures_since_last}`.
//!
//! Design choice: `WorkerHello` is a strict SUPERSET of `HeartbeatPayload`
//! rather than a replacement. `HeartbeatPayload` stays as the legacy type
//! (soft-deprecated: new code should not construct it) because the
//! coordinator's `check_worker_protocol` and the worker's heartbeat loop
//! still name it and other agents migrate those in the same work package;
//! adding `#[deprecated]` now would fail their `-D warnings` gates before they
//! can move. [`WorkerHello::heartbeat_payload`] and
//! `From<HeartbeatPayload> for WorkerHello` bridge the two so the existing
//! version check runs unchanged on a hello, and [`WorkerHello::parse`] accepts
//! all three body generations, reporting the older two as `v == 0`
//! ([`HELLO_VERSION_LEGACY`]) with `is_legacy() == true`.
//!
//! The coordinator's reply follows the same pattern: today's body is
//! `{"status":"ok","ticket_public_key":...}` (finding 601). [`CoordinatorHello`]
//! keeps both keys so an old worker's pin logic keeps finding
//! `ticket_public_key`, and [`CoordinatorHello::parse`] accepts the bare `ok`
//! that pre-601 coordinators sent.
//!
//! Every field on both hellos is `#[serde(default)]`, so a peer one release
//! ahead can add fields without breaking a peer one release behind.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::scan_morsel::SCAN_TASK_VERSION_CURRENT;

/// `sqe.proto.version`: bump when the coordinator/worker wire contract changes
/// in a way an older peer cannot honour (ticket layout, action names, response
/// bodies). Independent of [`crate::SCAN_TASK_VERSION_CURRENT`], which only
/// versions the `ScanTask` JSON.
pub const PROTO_VERSION: u32 = 1;

/// Version a heartbeat is treated as when it carries no protocol fields at
/// all: the legacy body is the bare worker URL.
pub const PROTO_VERSION_UNKNOWN: u32 = 0;

/// Layout version of [`WorkerHello`] / [`CoordinatorHello`] (`v`). Bump when a
/// field changes MEANING; adding a defaulted field does not need a bump.
pub const HELLO_VERSION: u32 = 1;

/// `v` reported for a body that predates the hello (bare URL or
/// [`HeartbeatPayload`]).
pub const HELLO_VERSION_LEGACY: u32 = 0;

/// Body of the worker `heartbeat` action (legacy shape, #588).
///
/// Serialised as JSON. Older workers send the bare advertise URL instead;
/// [`HeartbeatPayload::parse`] accepts both and reports the legacy shape as
/// [`PROTO_VERSION_UNKNOWN`] with an empty DataFusion version, so the
/// coordinator can decide (warn or refuse) rather than the decoder.
///
/// Superseded by [`WorkerHello`] (finding 609); kept so existing call sites
/// compile while they migrate. `worker_url` also accepts the hello's `url`
/// key so a coordinator still on this type can decode a hello body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatPayload {
    /// The worker's routable Flight URL.
    #[serde(alias = "url")]
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

/// Load vector a worker reports on every heartbeat (finding 609), for the
/// scheduler and the `sqe_worker_reported_*` gauges. All fields default to
/// zero so a worker that cannot measure one of them still registers; the
/// coordinator treats zero as "unknown", not "full".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkerCapacity {
    /// Configured memory pool limit.
    pub memory_limit_bytes: u64,
    /// Bytes the scan admission governor would still admit right now.
    pub scan_budget_free_bytes: u64,
    /// Free bytes on the spill volume.
    pub spill_free_bytes: u64,
    /// Scans currently executing.
    pub inflight_scans: u32,
}

/// Body of the worker `heartbeat` action (finding 609).
///
/// Superset of [`HeartbeatPayload`]; see the module docs for why both exist.
/// Every field is defaulted so an older coordinator's decoder ignores what it
/// does not know and a newer one fills in what an older worker did not send.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkerHello {
    /// [`HELLO_VERSION`]; `0` for a body that predates the hello.
    pub v: u32,
    /// The worker's routable Flight URL. Accepts the legacy `worker_url` key.
    #[serde(alias = "worker_url")]
    pub url: String,
    /// Fresh per process. A known URL with a new id means the worker
    /// restarted and every fragment-scoped thing the coordinator remembered
    /// about it (credential channels, in-flight scans) is gone. `nil` when
    /// the worker predates the hello.
    pub instance_id: Uuid,
    /// Build identifier (git sha or crate version) for operator diagnostics.
    pub build: String,
    /// Newest [`crate::ScanTask`] version this worker implements. `0` when
    /// unknown (legacy body); the coordinator then assumes v1.
    pub scan_task_version: u32,
    /// [`PROTO_VERSION`] of the sending worker; `0` when absent.
    pub proto_version: u32,
    /// `datafusion::DATAFUSION_VERSION` of the sending worker; empty when absent.
    pub datafusion_version: String,
    /// Current load vector.
    pub capacity: WorkerCapacity,
    /// Scan tasks that failed with a worker-attributable fault
    /// ([`crate::WorkerFault::marks_worker_unhealthy`]) since the previous
    /// heartbeat. The registry only resets `consecutive_failures` on a
    /// heartbeat that reports zero here.
    pub failures_since_last: u32,
}

impl Default for WorkerHello {
    fn default() -> Self {
        Self {
            v: HELLO_VERSION_LEGACY,
            url: String::new(),
            instance_id: Uuid::nil(),
            build: String::new(),
            scan_task_version: 0,
            proto_version: PROTO_VERSION_UNKNOWN,
            datafusion_version: String::new(),
            capacity: WorkerCapacity::default(),
            failures_since_last: 0,
        }
    }
}

impl WorkerHello {
    /// Build the hello the current worker build sends. `instance_id` is the
    /// process-lifetime id the worker generated at boot.
    pub fn current(
        url: impl Into<String>,
        instance_id: Uuid,
        build: impl Into<String>,
        datafusion_version: &str,
    ) -> Self {
        Self {
            v: HELLO_VERSION,
            url: url.into(),
            instance_id,
            build: build.into(),
            scan_task_version: SCAN_TASK_VERSION_CURRENT,
            proto_version: PROTO_VERSION,
            datafusion_version: datafusion_version.to_string(),
            capacity: WorkerCapacity::default(),
            failures_since_last: 0,
        }
    }

    /// Set the load vector.
    pub fn with_capacity(mut self, capacity: WorkerCapacity) -> Self {
        self.capacity = capacity;
        self
    }

    /// Set the worker-attributable failure count since the last heartbeat.
    pub fn with_failures_since_last(mut self, failures: u32) -> Self {
        self.failures_since_last = failures;
        self
    }

    /// JSON body for the `heartbeat` action.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("WorkerHello serialises")
    }

    /// Strict decode of a hello body: must be a JSON object with a non-empty
    /// `url` (or legacy `worker_url`). Use [`WorkerHello::parse`] to also
    /// accept the pre-hello body shapes.
    pub fn decode(body: &[u8]) -> Result<Self, String> {
        let text = std::str::from_utf8(body).map_err(|e| format!("Invalid hello body: {e}"))?;
        let trimmed = text.trim();
        if !trimmed.starts_with('{') {
            return Err("Invalid hello body: not a JSON object".to_string());
        }
        let hello: Self =
            serde_json::from_str(trimmed).map_err(|e| format!("Invalid hello body: {e}"))?;
        if hello.url.is_empty() {
            return Err("Invalid hello body: missing url".to_string());
        }
        Ok(hello)
    }

    /// Compatibility shim: decode a body a pre-hello worker sends -- a
    /// [`HeartbeatPayload`] JSON object or a bare URL string -- into a hello
    /// with `v == 0`. Returns `None` for anything else (not UTF-8, empty, a
    /// JSON object without a URL), so the caller can produce its own error.
    pub fn from_legacy_body(body: &[u8]) -> Option<Self> {
        let payload = HeartbeatPayload::parse(body).ok()?;
        if payload.worker_url.is_empty() {
            return None;
        }
        Some(Self::from(payload))
    }

    /// Accept any body generation: hello, [`HeartbeatPayload`], or bare URL.
    ///
    /// A JSON object with a `v` key is decoded as a hello; without one it is
    /// a `HeartbeatPayload` and reported as `v == 0`. `Err` only when no
    /// generation can make sense of the body.
    pub fn parse(body: &[u8]) -> Result<Self, String> {
        let text = std::str::from_utf8(body).map_err(|e| format!("Invalid heartbeat body: {e}"))?;
        let trimmed = text.trim();
        if trimmed.starts_with('{') {
            let hello = Self::decode(body)?;
            return Ok(hello);
        }
        Self::from_legacy_body(body)
            .ok_or_else(|| "Invalid heartbeat body: empty worker URL".to_string())
    }

    /// True when the body predates the hello (bare URL or `HeartbeatPayload`).
    pub fn is_legacy(&self) -> bool {
        self.v == HELLO_VERSION_LEGACY
    }

    /// True when the body carries no protocol version at all (bare URL).
    /// Same meaning as [`HeartbeatPayload::is_legacy`].
    pub fn is_pre_handshake(&self) -> bool {
        self.proto_version == PROTO_VERSION_UNKNOWN && self.datafusion_version.is_empty()
    }

    /// The `ScanTask` version to plan for this worker: what it reported, or
    /// v1 when it reported nothing.
    pub fn effective_scan_task_version(&self) -> u32 {
        if self.scan_task_version == 0 {
            crate::scan_morsel::SCAN_TASK_VERSION_V1
        } else {
            self.scan_task_version
        }
    }

    /// View as the legacy payload, so `check_worker_protocol` runs unchanged.
    pub fn heartbeat_payload(&self) -> HeartbeatPayload {
        HeartbeatPayload {
            worker_url: self.url.clone(),
            proto_version: self.proto_version,
            datafusion_version: self.datafusion_version.clone(),
        }
    }

    /// Compare this worker's versions against what the coordinator runs.
    ///
    /// Pure: it reports, the coordinator decides. A legacy body yields
    /// [`HelloCompat::Legacy`] so strict/lenient policy stays in one place.
    pub fn check_compat(&self, coordinator: &CoordinatorVersions) -> HelloCompat {
        if self.is_pre_handshake() {
            return HelloCompat::Legacy;
        }
        let mut reasons = Vec::new();
        if self.proto_version != coordinator.proto_version {
            reasons.push(format!(
                "sqe.proto.version {} (worker) != {} (coordinator)",
                self.proto_version, coordinator.proto_version
            ));
        }
        if self.datafusion_version != coordinator.datafusion_version {
            reasons.push(format!(
                "DataFusion {} (worker) != {} (coordinator)",
                self.datafusion_version, coordinator.datafusion_version
            ));
        }
        // A worker that implements FEWER ScanTask versions than the
        // coordinator plans is the rolling-upgrade hazard from finding 109: it
        // would accept only part of what the coordinator sends. A worker that
        // implements MORE is fine; it accepts everything the coordinator
        // produces. A legacy body reports 0 = unknown and is not compared.
        if self.scan_task_version != 0 && self.scan_task_version < coordinator.scan_task_version {
            reasons.push(format!(
                "ScanTask version {} (worker) < {} (coordinator)",
                self.scan_task_version, coordinator.scan_task_version
            ));
        }
        if reasons.is_empty() {
            HelloCompat::Compatible
        } else {
            HelloCompat::Mismatch(reasons.join("; "))
        }
    }
}

impl From<HeartbeatPayload> for WorkerHello {
    fn from(p: HeartbeatPayload) -> Self {
        Self {
            v: HELLO_VERSION_LEGACY,
            url: p.worker_url,
            proto_version: p.proto_version,
            datafusion_version: p.datafusion_version,
            ..Self::default()
        }
    }
}

/// What the coordinator runs, for [`WorkerHello::check_compat`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorVersions {
    pub proto_version: u32,
    pub scan_task_version: u32,
    pub datafusion_version: String,
}

impl CoordinatorVersions {
    /// This build's versions.
    pub fn current() -> Self {
        Self {
            proto_version: PROTO_VERSION,
            scan_task_version: SCAN_TASK_VERSION_CURRENT,
            datafusion_version: datafusion::DATAFUSION_VERSION.to_string(),
        }
    }
}

/// Result of [`WorkerHello::check_compat`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HelloCompat {
    /// Every version the worker reported matches.
    Compatible,
    /// The worker sent no versions (pre-handshake body); policy decides.
    Legacy,
    /// At least one version differs; the reason names each.
    Mismatch(String),
}

impl HelloCompat {
    pub fn is_compatible(&self) -> bool {
        matches!(self, HelloCompat::Compatible)
    }
}

/// The coordinator's reply to a heartbeat (finding 609).
///
/// Wire-compatible with the pre-hello reply `{"status":"ok","ticket_public_key":...}`:
/// `status` and `ticket_public_key` are kept as top-level keys so an old
/// worker's pin logic still finds the key, and [`CoordinatorHello::parse`]
/// accepts the bare `ok` older coordinators sent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CoordinatorHello {
    /// [`HELLO_VERSION`]; `0` for a legacy reply.
    pub v: u32,
    /// Identity of the cluster the worker just joined. A worker pointed at
    /// the wrong coordinator sees it here instead of a bare `ok`.
    pub cluster_id: String,
    /// Coordinator build identifier.
    pub build: String,
    /// Whether the worker was admitted to the registry. `false` with a
    /// `reason` replaces the silent `ok` a single-node coordinator with no
    /// registry used to return.
    pub accepted: bool,
    /// Why `accepted` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Legacy `"ok"` marker, kept for pre-hello workers. Derived from
    /// `accepted` on encode.
    pub status: String,
    /// base64 public half of the scan-ticket signing key (finding 601).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ticket_public_key: Option<String>,
}

impl Default for CoordinatorHello {
    fn default() -> Self {
        Self {
            v: HELLO_VERSION_LEGACY,
            cluster_id: String::new(),
            build: String::new(),
            accepted: false,
            reason: None,
            status: String::new(),
            ticket_public_key: None,
        }
    }
}

impl CoordinatorHello {
    /// An accepting reply.
    pub fn accepted(cluster_id: impl Into<String>, build: impl Into<String>) -> Self {
        Self {
            v: HELLO_VERSION,
            cluster_id: cluster_id.into(),
            build: build.into(),
            accepted: true,
            reason: None,
            status: "ok".to_string(),
            ticket_public_key: None,
        }
    }

    /// A rejecting reply. The transport-level `Status` should still be an
    /// error (`FailedPrecondition` etc.); this body is for the case where the
    /// coordinator answers `Ok` but wants the worker to know it is not in the
    /// pool (e.g. single-node mode without a registry).
    pub fn rejected(
        cluster_id: impl Into<String>,
        build: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            v: HELLO_VERSION,
            cluster_id: cluster_id.into(),
            build: build.into(),
            accepted: false,
            reason: Some(reason.into()),
            status: "rejected".to_string(),
            ticket_public_key: None,
        }
    }

    /// Attach the ticket-signing public key (finding 601).
    pub fn with_ticket_public_key(mut self, key_b64: impl Into<String>) -> Self {
        self.ticket_public_key = Some(key_b64.into());
        self
    }

    /// JSON reply body.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("CoordinatorHello serialises")
    }

    /// Strict decode: must be a JSON object. Use [`CoordinatorHello::parse`]
    /// to also accept the bare `ok`.
    pub fn decode(body: &[u8]) -> Result<Self, String> {
        let text = std::str::from_utf8(body).map_err(|e| format!("Invalid hello reply: {e}"))?;
        let trimmed = text.trim();
        if !trimmed.starts_with('{') {
            return Err("Invalid hello reply: not a JSON object".to_string());
        }
        let mut hello: Self =
            serde_json::from_str(trimmed).map_err(|e| format!("Invalid hello reply: {e}"))?;
        // A pre-hello coordinator wrote only `status`; derive `accepted`.
        if hello.v == HELLO_VERSION_LEGACY && hello.status == "ok" {
            hello.accepted = true;
        }
        Ok(hello)
    }

    /// Accept any reply generation: hello JSON, the 601 `{"status":"ok",...}`
    /// object, or the bare `ok`. `None` when the body is none of those.
    pub fn parse(body: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(body).ok()?;
        let trimmed = text.trim();
        if trimmed.starts_with('{') {
            return Self::decode(body).ok();
        }
        if trimmed.eq_ignore_ascii_case("ok") {
            return Some(Self {
                accepted: true,
                status: "ok".to_string(),
                ..Self::default()
            });
        }
        None
    }

    /// True when the reply predates the hello (no cluster identity).
    pub fn is_legacy(&self) -> bool {
        self.v == HELLO_VERSION_LEGACY
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

    // ---- finding 609: WorkerHello ----

    fn hello() -> WorkerHello {
        WorkerHello::current("http://w1:50052", Uuid::now_v7(), "abc123", "55.0.0")
            .with_capacity(WorkerCapacity {
                memory_limit_bytes: 8 << 30,
                scan_budget_free_bytes: 2 << 30,
                spill_free_bytes: 100 << 30,
                inflight_scans: 3,
            })
            .with_failures_since_last(2)
    }

    #[test]
    fn worker_hello_round_trips() {
        let h = hello();
        let back = WorkerHello::decode(&h.encode()).unwrap();
        assert_eq!(back, h);
        assert_eq!(back.v, HELLO_VERSION);
        assert_eq!(back.scan_task_version, SCAN_TASK_VERSION_CURRENT);
        assert_eq!(back.capacity.inflight_scans, 3);
        assert_eq!(back.failures_since_last, 2);
        assert!(!back.is_legacy());
        assert!(!back.is_pre_handshake());
        // parse() takes the same body.
        assert_eq!(WorkerHello::parse(&h.encode()).unwrap(), h);
    }

    /// The pre-hello body a #655 worker sends must decode as a legacy hello
    /// with the URL and both protocol versions preserved.
    #[test]
    fn heartbeat_payload_json_decodes_as_legacy_hello() {
        let legacy = HeartbeatPayload::current("http://w1:50052", "55.0.0");
        let h = WorkerHello::from_legacy_body(legacy.to_json().as_bytes()).unwrap();
        assert_eq!(h.url, "http://w1:50052");
        assert_eq!(h.proto_version, PROTO_VERSION);
        assert_eq!(h.datafusion_version, "55.0.0");
        assert_eq!(h.v, HELLO_VERSION_LEGACY);
        assert!(h.is_legacy());
        assert!(!h.is_pre_handshake());
        assert_eq!(h.instance_id, Uuid::nil());
        assert_eq!(h.scan_task_version, 0);
        assert_eq!(
            h.effective_scan_task_version(),
            crate::scan_morsel::SCAN_TASK_VERSION_V1
        );
        // parse() reaches the same result through the JSON branch.
        assert_eq!(WorkerHello::parse(legacy.to_json().as_bytes()).unwrap(), h);
        // And the round trip back to the legacy type is lossless.
        assert_eq!(h.heartbeat_payload(), legacy);
    }

    #[test]
    fn bare_url_decodes_as_pre_handshake_hello() {
        let h = WorkerHello::from_legacy_body(b"  http://w1:50052\n").unwrap();
        assert_eq!(h.url, "http://w1:50052");
        assert!(h.is_legacy());
        assert!(h.is_pre_handshake());
        assert_eq!(WorkerHello::parse(b"http://w1:50052").unwrap(), h);
        assert!(WorkerHello::decode(b"http://w1:50052").is_err());
    }

    #[test]
    fn garbage_bodies_are_refused_by_every_decoder() {
        assert!(WorkerHello::from_legacy_body(&[0xff, 0xfe]).is_none());
        assert!(WorkerHello::from_legacy_body(b"").is_none());
        assert!(WorkerHello::from_legacy_body(br#"{"proto_version":1}"#).is_none());
        assert!(WorkerHello::decode(br#"{"v":1}"#).is_err(), "missing url");
        assert!(WorkerHello::parse(b"").is_err());
        assert!(WorkerHello::parse(&[0xff]).is_err());
    }

    /// A coordinator still decoding with the legacy type must understand a
    /// hello body: `url` is an alias of `worker_url`.
    #[test]
    fn legacy_decoder_reads_a_hello_body() {
        let h = hello();
        let p = HeartbeatPayload::parse(&h.encode()).unwrap();
        assert_eq!(p.worker_url, h.url);
        assert_eq!(p.proto_version, PROTO_VERSION);
        assert_eq!(p.datafusion_version, "55.0.0");
        assert!(!p.is_legacy());
    }

    /// Forward compatibility: a hello from a newer worker with fields this
    /// build does not know still decodes, and one missing fields this build
    /// expects fills defaults.
    #[test]
    fn hello_is_forward_and_backward_tolerant() {
        let newer = br#"{"v":2,"url":"http://w1:50052","instance_id":"0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b","gpu_count":4,"capacity":{"memory_limit_bytes":1,"tpu":9}}"#;
        let h = WorkerHello::decode(newer).unwrap();
        assert_eq!(h.v, 2);
        assert_eq!(h.capacity.memory_limit_bytes, 1);
        assert_eq!(h.build, "");
        assert_eq!(h.scan_task_version, 0);

        let older = br#"{"v":1,"url":"http://w1:50052"}"#;
        let h = WorkerHello::decode(older).unwrap();
        assert_eq!(h.instance_id, Uuid::nil());
        assert_eq!(h.capacity, WorkerCapacity::default());
    }

    #[test]
    fn check_compat_detects_each_kind_of_skew() {
        let coord = CoordinatorVersions {
            proto_version: PROTO_VERSION,
            scan_task_version: SCAN_TASK_VERSION_CURRENT,
            datafusion_version: "55.0.0".to_string(),
        };
        assert_eq!(hello().check_compat(&coord), HelloCompat::Compatible);
        assert!(hello().check_compat(&coord).is_compatible());

        let mut proto = hello();
        proto.proto_version = PROTO_VERSION + 1;
        match proto.check_compat(&coord) {
            HelloCompat::Mismatch(r) => assert!(r.contains("sqe.proto.version"), "{r}"),
            other => panic!("expected mismatch, got {other:?}"),
        }

        let mut df = hello();
        df.datafusion_version = "54.1.0".to_string();
        match df.check_compat(&coord) {
            HelloCompat::Mismatch(r) => assert!(r.contains("DataFusion 54.1.0"), "{r}"),
            other => panic!("expected mismatch, got {other:?}"),
        }

        // Acceptance criterion from the review, inverted to the hazardous
        // direction: a worker that implements FEWER ScanTask versions than
        // the coordinator plans is refused.
        let mut older_scan = hello();
        older_scan.scan_task_version = SCAN_TASK_VERSION_CURRENT - 1;
        match older_scan.check_compat(&coord) {
            HelloCompat::Mismatch(r) => assert!(r.contains("ScanTask version"), "{r}"),
            other => panic!("expected mismatch, got {other:?}"),
        }
        // A worker that implements MORE accepts everything we send.
        let mut newer_scan = hello();
        newer_scan.scan_task_version = SCAN_TASK_VERSION_CURRENT + 1;
        assert_eq!(newer_scan.check_compat(&coord), HelloCompat::Compatible);

        // Two skews are both named.
        let mut both = hello();
        both.proto_version = PROTO_VERSION + 1;
        both.datafusion_version = "54.1.0".to_string();
        match both.check_compat(&coord) {
            HelloCompat::Mismatch(r) => {
                assert!(
                    r.contains("sqe.proto.version") && r.contains("DataFusion"),
                    "{r}"
                )
            }
            other => panic!("expected mismatch, got {other:?}"),
        }
    }

    #[test]
    fn check_compat_reports_legacy_for_pre_handshake_bodies() {
        let coord = CoordinatorVersions::current();
        let bare = WorkerHello::parse(b"http://w1:50052").unwrap();
        assert_eq!(bare.check_compat(&coord), HelloCompat::Legacy);
        // A #655 payload carries versions, so it is compared, not waved through.
        let p = HeartbeatPayload::current("http://w1:50052", datafusion::DATAFUSION_VERSION);
        let h = WorkerHello::parse(p.to_json().as_bytes()).unwrap();
        assert_eq!(h.check_compat(&coord), HelloCompat::Compatible);
    }

    #[test]
    fn current_versions_match_the_constants() {
        let c = CoordinatorVersions::current();
        assert_eq!(c.proto_version, PROTO_VERSION);
        assert_eq!(c.scan_task_version, SCAN_TASK_VERSION_CURRENT);
        assert_eq!(c.datafusion_version, datafusion::DATAFUSION_VERSION);
    }

    // ---- finding 609: CoordinatorHello ----

    #[test]
    fn coordinator_hello_round_trips() {
        let h = CoordinatorHello::accepted("cluster-a", "abc123").with_ticket_public_key("cGs=");
        let back = CoordinatorHello::decode(&h.encode()).unwrap();
        assert_eq!(back, h);
        assert!(back.accepted);
        assert_eq!(back.status, "ok");
        assert_eq!(back.ticket_public_key.as_deref(), Some("cGs="));
        assert!(!back.is_legacy());

        let r = CoordinatorHello::rejected("cluster-a", "abc123", "single-node mode");
        let back = CoordinatorHello::decode(&r.encode()).unwrap();
        assert_eq!(back, r);
        assert!(!back.accepted);
        assert_eq!(back.reason.as_deref(), Some("single-node mode"));
    }

    /// An old worker reads `ticket_public_key` from the body by key name and
    /// nothing else. The hello must keep that key at the top level.
    #[test]
    fn coordinator_hello_keeps_the_601_keys_for_old_workers() {
        let body = CoordinatorHello::accepted("c", "b")
            .with_ticket_public_key("cGs=")
            .encode();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["ticket_public_key"], "cGs=");
    }

    #[test]
    fn coordinator_hello_parses_legacy_replies() {
        let ok = CoordinatorHello::parse(b"ok").unwrap();
        assert!(ok.accepted);
        assert!(ok.is_legacy());
        assert!(ok.cluster_id.is_empty());

        let with_key =
            CoordinatorHello::parse(br#"{"status":"ok","ticket_public_key":"cGs="}"#).unwrap();
        assert!(with_key.accepted);
        assert!(with_key.is_legacy());
        assert_eq!(with_key.ticket_public_key.as_deref(), Some("cGs="));

        assert!(CoordinatorHello::parse(b"nope").is_none());
        assert!(CoordinatorHello::parse(&[0xff]).is_none());
        assert!(CoordinatorHello::decode(b"ok").is_err());
    }
}
