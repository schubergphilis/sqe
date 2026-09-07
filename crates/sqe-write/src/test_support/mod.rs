//! Test-only support code shared across this crate's unit tests and its
//! `tests/*.rs` integration binaries (architecture review 513).
//!
//! Compiled under `#[cfg(any(test, feature = "test-sqlite"))]` (see
//! `lib.rs`). Nothing in here is reachable from a production build.

pub mod sqlite_rig;
