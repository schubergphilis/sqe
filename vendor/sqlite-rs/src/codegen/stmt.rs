// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! DML statement codegen: `INSERT`/`UPDATE`/`DELETE` — the per-row
//! cursor-driven statements, as opposed to the single-opcode DDL
//! statements in the sibling `ddl` module. Grouped the same way
//! `btree/table.rs` groups its `insert`/`delete` submodules.

pub mod delete;
pub mod insert;
pub mod update;
