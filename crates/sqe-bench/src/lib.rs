//! sqe-bench library surface.
//!
//! `sqe-bench` is primarily a CLI (`src/main.rs`). This library re-exports
//! the submodules that integration tests and other crates may want to
//! exercise directly. Keep the surface minimal and lean on `main.rs` for
//! the user-facing binary.

pub mod generate;
