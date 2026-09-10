// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! A binary-compatible Rust replication of SQLite: read (and, for WAL,
//! write) the same on-disk file format and SQL dialect as the C library,
//! targeting a memory-safe, extensible SQLite rather than a new engine.
//! See the [repository README](https://github.com/iheitlager/sqlite-rs)
//! for the full design rationale.
//!
//! ## Performance
//!
//! 5 of 7 benchmark queries beat or match the sqlite3 oracle (v3.53.4).
//! See [`docs/performance.md`](https://github.com/iheitlager/sqlite-rs/blob/main/docs/performance.md)
//! for the full V4→V7.3 progression.
//!
//! `include_str!` can't pull the README in directly here: `src/` is a
//! qualified Rust subset checked by `make check-mvl-limit` (mvl-rust rust-limit),
//! which doesn't allowlist that macro.
// `src/vfs/lock.rs` and `src/vfs/shm.rs` used to need a scoped
// `#![allow(unsafe_code)]` for raw `fcntl`/`mmap`/`fork` calls (#50), then
// went unsafe-free entirely under `nix`/`std` (#66). Vendoring `nix`'s
// `fcntl`/`termios` FFI (#563) reintroduces one, deliberately narrow,
// carve-out: `src/sys/` — see `.openspec/adr/0031-vendor-nix-subset.md`.
// `deny` (rather than `forbid`) is what makes that local
// `#![allow(unsafe_code)]` possible; every other module is still held to
// zero `unsafe` by this crate-wide default.
#![deny(unsafe_code)]
#![warn(missing_docs)]

// ## The supported surface
//
// [`api`] is the API this crate supports for embedding: `Connection`,
// `Statement`, `Rows`, `Transaction`, `Error`, and `Value`. An application
// should need nothing else, and spec 013 Requirement 6 makes that a
// testable claim rather than an aspiration —
// `tests/unit/api_surface_test.rs` runs the whole workload through
// `sqlite_rs::api` alone.
//
// Every other module below is the **engine**: the parser, code generator,
// virtual machine, b-tree, pager and VFS that `api` is built on. They are
// public because the CLI in `src/bin/` is a separate binary that links this
// crate like any other consumer, and because they are genuinely useful for
// inspecting a database file. They are *not* a stability promise. Their
// signatures change whenever the implementation needs them to, without a
// major version bump, and a consumer wiring `dump::open` to
// `execute_transaction_step` is building on items that carry no such
// promise.
//
// If something a consumer needs is only reachable through the engine, that
// is a gap in `api` and worth reporting as one.
pub mod api;
pub mod btree;
pub mod codegen;
pub mod dump;
pub mod format;
pub mod header;
pub mod integrity;
pub mod pager;
pub mod parser;
pub mod planner;
pub mod record;
pub mod schema;
pub mod sys;
pub mod vdbe;
pub mod vfs;
