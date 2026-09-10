// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Vendored subset of POSIX syscalls sqlite-rs needs and no pure-Rust/
//! `std` API covers: byte-range `fcntl` locking (SQLite's journal-mode
//! lock ladder) and `termios` raw mode (readline's raw keypress input).
//! Replaces the `nix` dependency (#563) — see
//! `.openspec/adr/0031-vendor-nix-subset.md` for the rationale. This is
//! the crate's sole `#![allow(unsafe_code)]` carve-out: `src/lib.rs`
//! `#![deny(unsafe_code)]`s everywhere else.

pub mod fcntl;
pub mod termios;
