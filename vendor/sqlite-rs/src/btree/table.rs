// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Table b-tree write path: insert and delete over table b-trees (page
//! types 0x05 interior / 0x0d leaf). See `insert.rs`'s and `delete.rs`'s
//! module docs for the byte-layout contract and simplifications shared
//! by both.

mod delete;
mod insert;

pub use delete::delete_row;
pub use insert::insert_row;
