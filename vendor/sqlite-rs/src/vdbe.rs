// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! The value-semantics kernel beneath the future VDBE opcodes: type
//! affinity, cross-type comparison order, collations, NULL/three-valued
//! logic, and numeric coercion. Pure functions on `Value` — no expression
//! evaluation, no parser coupling. See spec 008.

mod affinity;
mod aggregate;
mod arithmetic;
mod cast;
mod coerce;
mod compare;
mod control;
mod cursor;
mod exec;
pub mod explain;
mod functions;
mod hash_agg;
mod pragma;
mod program;
mod result;
mod sorter;
mod value;

pub use crate::record::{compare_text, Collation};
pub use affinity::{affinity_of, apply_affinity, comparison_affinity, Affinity};
pub use cast::cast_to;
pub use coerce::{
    cast_to_integer, checked_add, checked_div, checked_mul, checked_rem, checked_sub,
    coerce_text_to_numeric,
};
pub use compare::compare;
pub use control::{
    TRANSACTION_MODE_DEFERRED, TRANSACTION_MODE_EXCLUSIVE, TRANSACTION_MODE_IMMEDIATE,
};
pub use exec::{
    execute, execute_transaction_step, execute_transaction_step_counted, execute_with_db,
    execute_with_db_and_params, execute_with_params, execute_with_writable_db, ExecError,
    Execution, Step, StepOutcome, Vm,
};
pub use explain::{explain, ExplainRow};
pub use functions::{call as call_function, like_match, FunctionError};
pub use pragma::{
    JOURNAL_MODE_DELETE, JOURNAL_MODE_WAL, SYNCHRONOUS_FULL, SYNCHRONOUS_NORMAL, SYNCHRONOUS_OFF,
    SYNCHRONOUS_QUERY,
};
pub use program::{
    AnalyzeIndexTarget, AnalyzeTarget, GroupKeyColumn, Instruction, Opcode, Program, SortKeyColumn,
    OPFLAG_LASTROWID, OPFLAG_NCHANGE, P4,
};
pub use value::{and, is, is_not, not, or, sql_eq, sql_lt};
