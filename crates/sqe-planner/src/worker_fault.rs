//! Worker fault taxonomy (review finding 602 / #609).
//!
//! The worker used to collapse every failure into `Status::internal(string)`,
//! so the coordinator could not tell a user's expired S3 token from a worker
//! OOM and treated both the same way: mark the worker unhealthy, retry the
//! identical task elsewhere, repeat until the pool was empty. [`WorkerFault`]
//! is the shared vocabulary both sides use instead. The worker classifies
//! while the error is still typed ([`classify_datafusion_error`],
//! [`classify_object_store_error`]) and ships the fault in the gRPC `Status`;
//! the coordinator decodes it ([`WorkerFault::from_status`]) and consults the
//! retry/health predicates rather than guessing from the code.
//!
//! Wire encoding, two channels so nothing is lost and old peers still work:
//!
//! 1. Metadata key [`FAULT_METADATA_KEY`] (`x-sqe-fault`) carries the exact
//!    variant as its [`WorkerFault::metric_label`] (ASCII). This is the
//!    lossless primary channel.
//! 2. The gRPC [`tonic::Code`] ([`WorkerFault::grpc_code`]) is the secondary
//!    channel: an old coordinator without the decoder still gets a sensible
//!    code, and a new coordinator talking to an old worker that sends no
//!    metadata falls back to a best-effort mapping from the code alone.

use datafusion::error::DataFusionError;
use sqe_core::SqeErrorCode;
use tonic::metadata::MetadataValue;
use tonic::{Code, Status};

/// gRPC metadata key the fault label rides under. ASCII, lower-case, `x-`
/// prefixed so tonic accepts it as a custom header.
pub const FAULT_METADATA_KEY: &str = "x-sqe-fault";

/// Why a worker could not complete a scan task.
///
/// The variants are partitioned by WHO must act, which is what the
/// coordinator's retry loop needs to know:
///
/// - the coordinator sent something unusable ([`Self::TaskInvalid`]);
/// - the user's principal cannot read the data ([`Self::PrincipalDenied`],
///   [`Self::SourceMissing`]) -- no worker will do better;
/// - the source is pushing back ([`Self::SourceThrottled`]) -- same worker,
///   later;
/// - this worker is the problem ([`Self::WorkerResourceExhausted`],
///   [`Self::WorkerInternal`]) -- another worker may succeed;
/// - nobody: the client went away ([`Self::Cancelled`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerFault {
    /// The ticket itself is bad: signature, audience, expiry, version,
    /// undecodable predicate, unsupported file format. Retrying on any worker
    /// yields the same answer; the producer has a bug.
    TaskInvalid,
    /// The object store refused the caller's credentials: 401/403,
    /// `AccessDenied`, `ExpiredToken`, `InvalidAccessKeyId`. Every worker uses
    /// the same credentials, so retrying elsewhere only spreads the failure.
    PrincipalDenied,
    /// A data file the catalog listed is gone (404 / `NoSuchKey`). The
    /// snapshot is stale or the file was deleted underneath the plan.
    SourceMissing,
    /// The object store is pushing back: 429, 503, `SlowDown`, `Throttling`.
    /// Transient; the same worker should back off and retry.
    SourceThrottled,
    /// This worker ran out of a local resource: memory pool, spill space,
    /// scan budget, file descriptors. Another worker may have room.
    WorkerResourceExhausted,
    /// Anything else that went wrong inside this worker.
    WorkerInternal,
    /// The client or coordinator cancelled the stream. Not a failure of
    /// anyone; must not be retried or counted against the worker.
    Cancelled,
}

impl WorkerFault {
    /// Every variant, for metrics registration and exhaustive tests.
    pub const ALL: [WorkerFault; 7] = [
        WorkerFault::TaskInvalid,
        WorkerFault::PrincipalDenied,
        WorkerFault::SourceMissing,
        WorkerFault::SourceThrottled,
        WorkerFault::WorkerResourceExhausted,
        WorkerFault::WorkerInternal,
        WorkerFault::Cancelled,
    ];

    /// gRPC code used as the secondary channel on the wire.
    pub fn grpc_code(self) -> Code {
        match self {
            WorkerFault::TaskInvalid => Code::InvalidArgument,
            WorkerFault::PrincipalDenied => Code::PermissionDenied,
            WorkerFault::SourceMissing => Code::NotFound,
            WorkerFault::SourceThrottled => Code::Unavailable,
            WorkerFault::WorkerResourceExhausted => Code::ResourceExhausted,
            WorkerFault::WorkerInternal => Code::Internal,
            WorkerFault::Cancelled => Code::Cancelled,
        }
    }

    /// True when the identical task may succeed on a different worker.
    /// The coordinator should re-dispatch elsewhere.
    pub fn is_retryable_on_other_worker(self) -> bool {
        matches!(
            self,
            WorkerFault::WorkerResourceExhausted | WorkerFault::WorkerInternal
        )
    }

    /// True when the identical task may succeed on the SAME worker after a
    /// backoff. Moving it would only move the pressure.
    pub fn is_retryable_same_worker(self) -> bool {
        matches!(self, WorkerFault::SourceThrottled)
    }

    /// True when this fault is evidence against the worker's health and
    /// should count towards eviction. User-caused faults never are: a bad
    /// ticket or an expired token says nothing about the worker.
    pub fn marks_worker_unhealthy(self) -> bool {
        matches!(
            self,
            WorkerFault::WorkerResourceExhausted | WorkerFault::WorkerInternal
        )
    }

    /// Closest existing [`SqeErrorCode`], for the audit record and the error
    /// the coordinator returns to the client.
    pub fn sqe_error_code(self) -> SqeErrorCode {
        match self {
            WorkerFault::TaskInvalid => SqeErrorCode::InvalidArguments,
            WorkerFault::PrincipalDenied => SqeErrorCode::AccessDenied,
            WorkerFault::SourceMissing => SqeErrorCode::StorageError,
            WorkerFault::SourceThrottled => SqeErrorCode::StorageError,
            WorkerFault::WorkerResourceExhausted => SqeErrorCode::ResourceExhausted,
            WorkerFault::WorkerInternal => SqeErrorCode::ExecutionFailed,
            WorkerFault::Cancelled => SqeErrorCode::QueryCancelled,
        }
    }

    /// Stable snake_case label: the metrics `fault` label value and the wire
    /// encoding under [`FAULT_METADATA_KEY`]. Never rename a value; add.
    pub fn metric_label(self) -> &'static str {
        match self {
            WorkerFault::TaskInvalid => "task_invalid",
            WorkerFault::PrincipalDenied => "principal_denied",
            WorkerFault::SourceMissing => "source_missing",
            WorkerFault::SourceThrottled => "source_throttled",
            WorkerFault::WorkerResourceExhausted => "worker_resource_exhausted",
            WorkerFault::WorkerInternal => "worker_internal",
            WorkerFault::Cancelled => "cancelled",
        }
    }

    /// Inverse of [`Self::metric_label`].
    pub fn from_label(label: &str) -> Option<WorkerFault> {
        WorkerFault::ALL
            .into_iter()
            .find(|f| f.metric_label() == label.trim())
    }

    /// Best-effort mapping from a bare gRPC code, for workers that predate
    /// the metadata channel.
    ///
    /// `Unavailable`, `DeadlineExceeded` and `Unknown` deliberately map to
    /// `None`: tonic's own transport layer produces those for a dead
    /// connection, and an old worker never used `Unavailable` for throttling
    /// (it sent `Internal`). Returning `SourceThrottled` there would turn a
    /// crashed worker into a same-worker-retry loop. The caller keeps its
    /// existing transport handling for `None`.
    pub fn from_grpc_code(code: Code) -> Option<WorkerFault> {
        match code {
            Code::InvalidArgument | Code::FailedPrecondition | Code::Unimplemented => {
                Some(WorkerFault::TaskInvalid)
            }
            Code::PermissionDenied | Code::Unauthenticated => Some(WorkerFault::PrincipalDenied),
            Code::NotFound => Some(WorkerFault::SourceMissing),
            Code::ResourceExhausted => Some(WorkerFault::WorkerResourceExhausted),
            Code::Internal | Code::DataLoss | Code::Aborted => Some(WorkerFault::WorkerInternal),
            Code::Cancelled => Some(WorkerFault::Cancelled),
            _ => None,
        }
    }

    /// Decode from a `Status`: the [`FAULT_METADATA_KEY`] header first
    /// (lossless), then [`Self::from_grpc_code`] for old workers.
    pub fn from_status(status: &Status) -> Option<WorkerFault> {
        if let Some(value) = status.metadata().get(FAULT_METADATA_KEY) {
            if let Some(fault) = value.to_str().ok().and_then(WorkerFault::from_label) {
                return Some(fault);
            }
        }
        WorkerFault::from_grpc_code(status.code())
    }

    /// Encode into a `Status` carrying both channels.
    pub fn into_status(self, message: impl Into<String>) -> Status {
        let mut metadata = tonic::metadata::MetadataMap::new();
        // The labels are static lower-case ASCII, so this cannot fail; the
        // fallback is only there so a future typo cannot panic a worker.
        if let Ok(value) = MetadataValue::try_from(self.metric_label()) {
            metadata.insert(FAULT_METADATA_KEY, value);
        }
        Status::with_metadata(self.grpc_code(), message, metadata)
    }
}

impl std::fmt::Display for WorkerFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.metric_label())
    }
}

/// Classify a typed `object_store` error. Run this BEFORE the error is
/// stringified; once it is a `String` only [`classify_message`] is left.
pub fn classify_object_store_error(err: &object_store::Error) -> WorkerFault {
    use object_store::Error as E;
    match err {
        E::NotFound { .. } => WorkerFault::SourceMissing,
        E::PermissionDenied { .. } | E::Unauthenticated { .. } => WorkerFault::PrincipalDenied,
        // A path the coordinator produced that the store cannot even parse,
        // or an operation this store does not do: the task is wrong.
        E::InvalidPath { .. }
        | E::NotSupported { .. }
        | E::NotImplemented { .. }
        | E::UnknownConfigurationKey { .. } => WorkerFault::TaskInvalid,
        // `Generic` is where the AWS client puts HTTP-level failures (429,
        // 503, 403 with a body, connection resets). Read the chain.
        E::Generic { source, .. } => classify_error_chain(source.as_ref())
            .or_else(|| classify_message(&err.to_string()))
            .unwrap_or(WorkerFault::WorkerInternal),
        // Conditional-write and join failures are worker-side.
        _ => classify_message(&err.to_string()).unwrap_or(WorkerFault::WorkerInternal),
    }
}

/// Classify a `DataFusionError`, walking wrappers (`Context`, `Diagnostic`,
/// `Shared`, `Collection`, `External`, Arrow/Parquet/io externals) down to the
/// typed `object_store::Error` when there is one.
pub fn classify_datafusion_error(err: &DataFusionError) -> WorkerFault {
    classify_datafusion_inner(err).unwrap_or(WorkerFault::WorkerInternal)
}

fn classify_datafusion_inner(err: &DataFusionError) -> Option<WorkerFault> {
    match err {
        DataFusionError::ObjectStore(inner) => Some(classify_object_store_error(inner)),
        DataFusionError::ResourcesExhausted(_) => Some(WorkerFault::WorkerResourceExhausted),
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            classify_datafusion_inner(inner).or_else(|| classify_message(&err.to_string()))
        }
        DataFusionError::Shared(inner) => classify_datafusion_inner(inner),
        DataFusionError::Collection(errors) => {
            // The most specific classification wins; an internal error next
            // to a 403 is still a 403 for the user.
            let mut best: Option<WorkerFault> = None;
            for e in errors {
                match classify_datafusion_inner(e) {
                    Some(WorkerFault::WorkerInternal) if best.is_some() => {}
                    Some(f) => best = Some(f),
                    None => {}
                }
            }
            best
        }
        DataFusionError::External(source) => {
            classify_error_chain(source.as_ref()).or_else(|| classify_message(&err.to_string()))
        }
        DataFusionError::ArrowError(inner, _) => match inner.as_ref() {
            arrow_schema::ArrowError::ExternalError(source) => {
                classify_error_chain(source.as_ref())
            }
            _ => None,
        }
        .or_else(|| classify_message(&err.to_string())),
        DataFusionError::ParquetError(inner) => match inner.as_ref() {
            datafusion::parquet::errors::ParquetError::External(source) => {
                classify_error_chain(source.as_ref())
            }
            _ => None,
        }
        .or_else(|| classify_message(&err.to_string())),
        DataFusionError::IoError(io) => classify_io_error(io),
        // The coordinator handed us something DataFusion could not plan or
        // decode: wrong schema, unsupported expression, bad configuration.
        DataFusionError::Plan(_)
        | DataFusionError::SQL(_, _)
        | DataFusionError::SchemaError(_, _)
        | DataFusionError::Configuration(_)
        | DataFusionError::NotImplemented(_)
        | DataFusionError::Substrait(_) => Some(WorkerFault::TaskInvalid),
        DataFusionError::Execution(msg) | DataFusionError::Internal(msg) => classify_message(msg),
        _ => classify_message(&err.to_string()),
    }
}

fn classify_io_error(io: &std::io::Error) -> Option<WorkerFault> {
    if let Some(inner) = io.get_ref() {
        if let Some(f) = classify_error_chain(inner) {
            return Some(f);
        }
    }
    match io.kind() {
        std::io::ErrorKind::NotFound => Some(WorkerFault::SourceMissing),
        std::io::ErrorKind::PermissionDenied => Some(WorkerFault::PrincipalDenied),
        std::io::ErrorKind::OutOfMemory | std::io::ErrorKind::StorageFull => {
            Some(WorkerFault::WorkerResourceExhausted)
        }
        std::io::ErrorKind::Interrupted => Some(WorkerFault::Cancelled),
        _ => classify_message(&io.to_string()),
    }
}

/// Walk a `std::error::Error` chain looking for a typed error we know how to
/// classify, then fall back to message heuristics over the whole chain.
pub fn classify_error_chain(err: &(dyn std::error::Error + 'static)) -> Option<WorkerFault> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    let mut text = String::new();
    while let Some(e) = current {
        if let Some(os) = e.downcast_ref::<object_store::Error>() {
            return Some(classify_object_store_error(os));
        }
        if let Some(df) = e.downcast_ref::<DataFusionError>() {
            if let Some(f) = classify_datafusion_inner(df) {
                return Some(f);
            }
        }
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            if let Some(f) = classify_io_error(io) {
                return Some(f);
            }
        }
        if let Some(status) = e.downcast_ref::<Status>() {
            if let Some(f) = WorkerFault::from_status(status) {
                return Some(f);
            }
        }
        text.push_str(&e.to_string());
        text.push('\n');
        current = e.source();
    }
    classify_message(&text)
}

/// Classify an `anyhow::Error` by walking its chain.
pub fn classify_anyhow_error(err: &anyhow::Error) -> WorkerFault {
    for cause in err.chain() {
        if let Some(os) = cause.downcast_ref::<object_store::Error>() {
            return classify_object_store_error(os);
        }
        if let Some(df) = cause.downcast_ref::<DataFusionError>() {
            return classify_datafusion_error(df);
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            if let Some(f) = classify_io_error(io) {
                return f;
            }
        }
        if let Some(status) = cause.downcast_ref::<Status>() {
            if let Some(f) = WorkerFault::from_status(status) {
                return f;
            }
        }
    }
    classify_message(&format!("{err:#}")).unwrap_or(WorkerFault::WorkerInternal)
}

/// Last-resort classification from error text, for errors that were
/// stringified before reaching us (S3 SDK bodies, HTTP status lines).
///
/// Order matters: throttling first, because a `503 SlowDown` body can also
/// mention the bucket and "denied" in its help text; then credential
/// failures; then missing objects; then resource exhaustion. Returns `None`
/// when nothing matches so callers can keep their own default.
pub fn classify_message(text: &str) -> Option<WorkerFault> {
    let lower = text.to_ascii_lowercase();
    let has = |needles: &[&str]| needles.iter().any(|n| lower.contains(n));

    if has(&[
        "slowdown",
        "slow down",
        "throttl",
        "too many requests",
        "toomanyrequests",
        "requestlimitexceeded",
        "service unavailable",
        "status: 429",
        "status 429",
        "http 429",
        "(429)",
        " 429 ",
        "status: 503",
        "status 503",
        "http 503",
        "(503)",
        " 503 ",
    ]) {
        return Some(WorkerFault::SourceThrottled);
    }
    if has(&[
        "expiredtoken",
        "expired token",
        "token has expired",
        "accessdenied",
        "access denied",
        "permission denied",
        "invalidaccesskeyid",
        "signaturedoesnotmatch",
        "invalidtoken",
        "unauthenticated",
        "unauthorized",
        "forbidden",
        "status: 403",
        "status 403",
        "http 403",
        "(403)",
        " 403 ",
        "status: 401",
        "status 401",
        "http 401",
        "(401)",
        " 401 ",
    ]) {
        return Some(WorkerFault::PrincipalDenied);
    }
    if has(&[
        "nosuchkey",
        "nosuchbucket",
        "not found",
        "status: 404",
        "status 404",
        "http 404",
        "(404)",
        " 404 ",
    ]) {
        return Some(WorkerFault::SourceMissing);
    }
    if has(&[
        "resources exhausted",
        "resourcesexhausted",
        "out of memory",
        "memory limit",
        "memory pool",
        "no space left",
        "spill",
        "too many open files",
    ]) {
        return Some(WorkerFault::WorkerResourceExhausted);
    }
    if has(&["cancelled", "canceled"]) {
        return Some(WorkerFault::Cancelled);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generic(msg: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: Box::new(std::io::Error::other(msg.to_string())),
        }
    }

    #[test]
    fn every_variant_round_trips_through_status_metadata() {
        for fault in WorkerFault::ALL {
            let status = fault.into_status("boom");
            assert_eq!(status.code(), fault.grpc_code(), "{fault:?}");
            assert_eq!(status.message(), "boom");
            assert_eq!(
                status
                    .metadata()
                    .get(FAULT_METADATA_KEY)
                    .and_then(|v| v.to_str().ok()),
                Some(fault.metric_label()),
                "{fault:?}"
            );
            assert_eq!(WorkerFault::from_status(&status), Some(fault), "{fault:?}");
        }
    }

    #[test]
    fn labels_are_unique_and_parse_back() {
        let mut seen = std::collections::HashSet::new();
        for fault in WorkerFault::ALL {
            assert!(
                seen.insert(fault.metric_label()),
                "duplicate label {fault:?}"
            );
            assert_eq!(WorkerFault::from_label(fault.metric_label()), Some(fault));
            // Also the serde form, so a JSON-carried fault uses the same word.
            let json = serde_json::to_string(&fault).unwrap();
            assert_eq!(json, format!("\"{}\"", fault.metric_label()));
        }
        assert_eq!(WorkerFault::from_label("nonsense"), None);
    }

    /// Metadata wins over the code: a worker may deliberately pair a fault
    /// with a different code, and an unknown label must not poison decoding.
    #[test]
    fn metadata_takes_precedence_over_code_and_falls_back_when_unparseable() {
        let mut md = tonic::metadata::MetadataMap::new();
        md.insert(FAULT_METADATA_KEY, "source_throttled".parse().unwrap());
        let status = Status::with_metadata(Code::Internal, "x", md);
        assert_eq!(
            WorkerFault::from_status(&status),
            Some(WorkerFault::SourceThrottled)
        );

        let mut md = tonic::metadata::MetadataMap::new();
        md.insert(FAULT_METADATA_KEY, "from_the_future".parse().unwrap());
        let status = Status::with_metadata(Code::NotFound, "x", md);
        assert_eq!(
            WorkerFault::from_status(&status),
            Some(WorkerFault::SourceMissing),
            "unknown label falls back to the code channel"
        );
    }

    /// An old worker sends bare codes. The ambiguous transport codes must
    /// stay `None` so the coordinator keeps its channel-invalidation path.
    #[test]
    fn old_worker_codes_map_best_effort_and_transport_codes_stay_none() {
        assert_eq!(
            WorkerFault::from_status(&Status::internal("Scan execution failed: x")),
            Some(WorkerFault::WorkerInternal)
        );
        assert_eq!(
            WorkerFault::from_status(&Status::invalid_argument("bad ticket")),
            Some(WorkerFault::TaskInvalid)
        );
        assert_eq!(
            WorkerFault::from_status(&Status::permission_denied("no")),
            Some(WorkerFault::PrincipalDenied)
        );
        assert_eq!(
            WorkerFault::from_status(&Status::unauthenticated("no")),
            Some(WorkerFault::PrincipalDenied)
        );
        assert_eq!(
            WorkerFault::from_status(&Status::not_found("gone")),
            Some(WorkerFault::SourceMissing)
        );
        assert_eq!(
            WorkerFault::from_status(&Status::resource_exhausted("pool")),
            Some(WorkerFault::WorkerResourceExhausted)
        );
        assert_eq!(
            WorkerFault::from_status(&Status::cancelled("bye")),
            Some(WorkerFault::Cancelled)
        );
        for status in [
            Status::unavailable("connection refused"),
            Status::deadline_exceeded("timeout"),
            Status::unknown("h2 goaway"),
        ] {
            assert_eq!(WorkerFault::from_status(&status), None, "{status:?}");
        }
    }

    #[test]
    fn retry_and_health_policy_matches_the_review() {
        // User faults: fail fast, never touch the registry.
        for f in [
            WorkerFault::TaskInvalid,
            WorkerFault::PrincipalDenied,
            WorkerFault::SourceMissing,
            WorkerFault::Cancelled,
        ] {
            assert!(!f.is_retryable_on_other_worker(), "{f:?}");
            assert!(!f.is_retryable_same_worker(), "{f:?}");
            assert!(!f.marks_worker_unhealthy(), "{f:?}");
        }
        // Throttled: same worker, backoff, worker stays healthy.
        assert!(WorkerFault::SourceThrottled.is_retryable_same_worker());
        assert!(!WorkerFault::SourceThrottled.is_retryable_on_other_worker());
        assert!(!WorkerFault::SourceThrottled.marks_worker_unhealthy());
        // Worker faults: elsewhere, and count against the worker.
        for f in [
            WorkerFault::WorkerResourceExhausted,
            WorkerFault::WorkerInternal,
        ] {
            assert!(f.is_retryable_on_other_worker(), "{f:?}");
            assert!(!f.is_retryable_same_worker(), "{f:?}");
            assert!(f.marks_worker_unhealthy(), "{f:?}");
        }
    }

    #[test]
    fn sqe_error_codes_are_user_errors_exactly_for_user_faults() {
        assert_eq!(
            WorkerFault::PrincipalDenied.sqe_error_code(),
            SqeErrorCode::AccessDenied
        );
        assert!(WorkerFault::PrincipalDenied
            .sqe_error_code()
            .is_user_error());
        assert!(WorkerFault::TaskInvalid.sqe_error_code().is_user_error());
        assert!(WorkerFault::Cancelled.sqe_error_code().is_user_error());
        assert!(!WorkerFault::WorkerInternal.sqe_error_code().is_user_error());
        assert!(!WorkerFault::WorkerResourceExhausted
            .sqe_error_code()
            .is_user_error());
        assert_eq!(
            WorkerFault::WorkerResourceExhausted.sqe_error_code(),
            SqeErrorCode::ResourceExhausted
        );
    }

    #[test]
    fn object_store_typed_variants_classify_directly() {
        let not_found = object_store::Error::NotFound {
            path: "b/k".into(),
            source: Box::new(std::io::Error::other("404")),
        };
        assert_eq!(
            classify_object_store_error(&not_found),
            WorkerFault::SourceMissing
        );
        let denied = object_store::Error::PermissionDenied {
            path: "b/k".into(),
            source: Box::new(std::io::Error::other("403")),
        };
        assert_eq!(
            classify_object_store_error(&denied),
            WorkerFault::PrincipalDenied
        );
        let unauth = object_store::Error::Unauthenticated {
            path: "b/k".into(),
            source: Box::new(std::io::Error::other("401")),
        };
        assert_eq!(
            classify_object_store_error(&unauth),
            WorkerFault::PrincipalDenied
        );
        let bad_path = object_store::Error::NotImplemented {
            operation: "copy_if_not_exists".into(),
            implementer: "test".into(),
        };
        assert_eq!(
            classify_object_store_error(&bad_path),
            WorkerFault::TaskInvalid
        );
    }

    #[test]
    fn object_store_generic_is_classified_from_the_http_body() {
        assert_eq!(
            classify_object_store_error(&generic(
                "Client error with status 503 Service Unavailable: <Code>SlowDown</Code>"
            )),
            WorkerFault::SourceThrottled
        );
        assert_eq!(
            classify_object_store_error(&generic(
                "HTTP status client error (429 Too Many Requests)"
            )),
            WorkerFault::SourceThrottled
        );
        assert_eq!(
            classify_object_store_error(&generic(
                "Client error with status 403 Forbidden: <Code>ExpiredToken</Code>"
            )),
            WorkerFault::PrincipalDenied
        );
        assert_eq!(
            classify_object_store_error(&generic(
                "Client error with status 400 Bad Request: <Code>InvalidAccessKeyId</Code>"
            )),
            WorkerFault::PrincipalDenied
        );
        assert_eq!(
            classify_object_store_error(&generic("connection reset by peer")),
            WorkerFault::WorkerInternal
        );
    }

    #[test]
    fn datafusion_wrappers_are_walked_to_the_object_store_error() {
        let os = object_store::Error::NotFound {
            path: "b/k".into(),
            source: Box::new(std::io::Error::other("404")),
        };
        let df = DataFusionError::Context(
            "reading parquet".into(),
            Box::new(DataFusionError::ObjectStore(Box::new(os))),
        );
        assert_eq!(classify_datafusion_error(&df), WorkerFault::SourceMissing);

        let shared = DataFusionError::Shared(std::sync::Arc::new(
            DataFusionError::ResourcesExhausted("pool".into()),
        ));
        assert_eq!(
            classify_datafusion_error(&shared),
            WorkerFault::WorkerResourceExhausted
        );

        // External boxed chain: anyhow-style wrapper around an object_store
        // error two levels down.
        let inner = object_store::Error::PermissionDenied {
            path: "b/k".into(),
            source: Box::new(std::io::Error::other("403")),
        };
        let boxed: Box<dyn std::error::Error + Send + Sync> =
            anyhow::Error::new(inner).context("opening file").into();
        let external = DataFusionError::External(boxed);
        assert_eq!(
            classify_datafusion_error(&external),
            WorkerFault::PrincipalDenied
        );
    }

    #[test]
    fn datafusion_planning_errors_are_the_coordinators_fault() {
        assert_eq!(
            classify_datafusion_error(&DataFusionError::Plan("bad expr".into())),
            WorkerFault::TaskInvalid
        );
        assert_eq!(
            classify_datafusion_error(&DataFusionError::NotImplemented("x".into())),
            WorkerFault::TaskInvalid
        );
        assert_eq!(
            classify_datafusion_error(&DataFusionError::Execution("something odd".into())),
            WorkerFault::WorkerInternal
        );
        assert_eq!(
            classify_datafusion_error(&DataFusionError::Execution(
                "Failed to allocate: memory limit exceeded".into()
            )),
            WorkerFault::WorkerResourceExhausted
        );
    }

    #[test]
    fn collection_prefers_the_most_specific_fault() {
        let coll = DataFusionError::Collection(vec![
            DataFusionError::Internal("x".into()),
            DataFusionError::Execution("Client error 403 Forbidden AccessDenied".into()),
            DataFusionError::Internal("y".into()),
        ]);
        assert_eq!(
            classify_datafusion_error(&coll),
            WorkerFault::PrincipalDenied
        );
    }

    #[test]
    fn io_errors_classify_by_kind() {
        let nf = DataFusionError::IoError(std::io::Error::from(std::io::ErrorKind::NotFound));
        assert_eq!(classify_datafusion_error(&nf), WorkerFault::SourceMissing);
        let full = DataFusionError::IoError(std::io::Error::other("No space left on device"));
        assert_eq!(
            classify_datafusion_error(&full),
            WorkerFault::WorkerResourceExhausted
        );
    }

    #[test]
    fn anyhow_chain_finds_typed_errors_and_statuses() {
        let os = object_store::Error::NotFound {
            path: "b/k".into(),
            source: Box::new(std::io::Error::other("404")),
        };
        let err = anyhow::Error::new(os).context("scan failed");
        assert_eq!(classify_anyhow_error(&err), WorkerFault::SourceMissing);

        let status_err = anyhow::Error::new(WorkerFault::SourceThrottled.into_status("slow"))
            .context("upstream");
        assert_eq!(
            classify_anyhow_error(&status_err),
            WorkerFault::SourceThrottled
        );

        let plain = anyhow::anyhow!("some unrelated failure");
        assert_eq!(classify_anyhow_error(&plain), WorkerFault::WorkerInternal);
    }

    #[test]
    fn message_heuristics_order_throttle_before_denied_before_missing() {
        assert_eq!(
            classify_message("503 Service Unavailable: SlowDown, access to bucket denied?"),
            Some(WorkerFault::SourceThrottled)
        );
        assert_eq!(
            classify_message(
                "The AWS Access Key Id you provided does not exist: InvalidAccessKeyId"
            ),
            Some(WorkerFault::PrincipalDenied)
        );
        assert_eq!(
            classify_message("Object at location x not found: NoSuchKey"),
            Some(WorkerFault::SourceMissing)
        );
        assert_eq!(classify_message("all good"), None);
    }
}
