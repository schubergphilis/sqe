//! Worker protocol tests (review finding 611 / #618): registration, credential
//! refresh, error classification and cancellation, end to end through the
//! Flight service, with no external services.
//!
//! One test binary: the scenarios live as modules under `protocol/` so the
//! crate does not grow one binary per scenario (build-time program, binary
//! sprawl). Run with `cargo test -p sqe-worker --test protocol`.

// `mod protocol;` from a crate root named `protocol.rs` is ambiguous with
// `protocol/mod.rs`, hence the explicit path.
#[path = "protocol/mod.rs"]
mod scenarios;
