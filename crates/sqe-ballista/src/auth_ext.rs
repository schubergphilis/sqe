//! Per-query auth carried through ballista's session-config propagation.
//!
//! Ballista serializes every `SessionConfig` option as a `KeyValuePair` and
//! ships it client -> scheduler -> executor, merging it into the per-task
//! `TaskContext` (`execution_loop.rs` `update_from_key_value_pair`). Unknown
//! keys are silently dropped unless the option is *registered*, so we model
//! the per-query bearer as a DataFusion [`ConfigExtension`] under the
//! `sqe_auth` prefix. Registering the extension on the client, scheduler, and
//! executor `SessionConfig`s lets the key (`sqe_auth.bearer`) round-trip.
//!
//! Only the **bearer** travels — not S3 secrets. The executor reloads the
//! iceberg `Table` through the catalog with the user's bearer, and Polaris
//! vends per-user S3 credentials in the `LoadTableResponse`. This matches the
//! legacy distributed path's trust model (it already shipped bearer tokens to
//! workers) and limits secret exposure to the token itself.
//!
//! Security note (cutover design D7): the bearer rides in a session-config
//! value, which ballista logs at `trace` level. Keep cluster traffic internal
//! and avoid `trace` logging in production.

use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;

extensions_options! {
    /// Per-query auth options propagated to executors.
    pub struct SqeAuthOptions {
        /// The authenticated user's OIDC bearer token. Empty means "no
        /// per-query bearer" -> the executor falls back to its config-built
        /// service-token catalog (Phase 3 single-tenant behaviour).
        pub bearer: String, default = String::new()
    }
}

impl ConfigExtension for SqeAuthOptions {
    const PREFIX: &'static str = "sqe_auth";
}
