// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Aggregate accumulator state for `AggStep`/`AggFinal` (spec 009,
//! Requirement 12, #241). Mirrors `functions.rs`'s name-keyed registry
//! shape, but an aggregate additionally threads a mutable [`AggState`]
//! accumulator across repeated `AggStep` calls before `AggFinal` reads
//! it once (SQLite's own `sqlite3_aggregate_context()` role, here
//! modeled as a plain enum rather than an opaque blob since every
//! accumulator shape is known up front).
//!
//! `count`/`sum` were implemented first to prove the opcode mechanism
//! end-to-end; `avg`/`min`/`max` (#242) are added to this same registry
//! rather than a new mechanism.

use std::cmp::Ordering;

use crate::record::{Collation, Value};
use crate::vdbe::coerce::coerce_text_to_numeric;
use crate::vdbe::compare::compare;
use crate::vdbe::functions::FunctionError;

/// `sum()`/`avg()`'s per-row argument, normalized to `Integer`/`Real`
/// (`None` for a missing/`NULL` arg, which the caller skips exactly
/// like today). A TEXT/BLOB argument (R-29052-00975) is coerced via its
/// longest numeric prefix — the same rule `CAST(x AS NUMERIC)` applies
/// (`coerce_text_to_numeric`) — so a non-numeric-looking string becomes
/// `0` rather than being skipped like a `NULL` argument.
fn numeric_arg(arg: Option<&Value>) -> Option<Value> {
    match arg {
        None | Some(Value::Null) => None,
        Some(v @ (Value::Integer(_) | Value::Real(_))) => Some(v.clone()),
        Some(Value::Text(s)) => Some(coerce_text_to_numeric(s)),
        Some(Value::Blob(b)) => Some(coerce_text_to_numeric(&String::from_utf8_lossy(b))),
    }
}

/// One aggregate's running accumulator state, addressed by
/// `Vm::agg_context`/`Vm::set_agg_context` the same way a [`CursorSlot`](crate::vdbe::cursor::CursorSlot)
/// is addressed by `Vm::cursor` — a disjoint slot table keyed by an
/// opcode operand (`AggStep`/`AggFinal`'s `P1`).
#[derive(Debug, Clone, PartialEq)]
pub enum AggState {
    /// `count(x)` skips NULL args; `count(*)` (zero args) counts every
    /// row regardless of value.
    Count(i64),
    /// `sum(x)`: integer inputs accumulate exactly in `int_total` until
    /// a REAL input is seen, after which the whole running total moves
    /// to `real_total` — mirrors SQLite's own sum() promotion rule (an
    /// all-integer sum stays exact; one REAL input makes the result
    /// REAL).
    Sum {
        int_total: i128,
        real_total: f64,
        saw_real: bool,
        saw_any: bool,
    },
    /// `avg(x)`: same integer/real promotion as `Sum`, plus a row count
    /// so `finalize` can divide — SQLite's `avg()` is always REAL (or
    /// NULL on zero non-null rows), never an exact integer.
    Avg {
        int_total: i128,
        real_total: f64,
        saw_real: bool,
        count: i64,
    },
    /// `min(x)`/`max(x)`: the running extremum, compared with
    /// [`compare`] under SQLite's type-ordering rules (NULL <
    /// INTEGER/REAL < TEXT < BLOB). NULL args are skipped, matching
    /// count(x)'s NULL handling.
    Min(Option<Value>),
    Max(Option<Value>),
}

impl AggState {
    fn initial(name: &str) -> Result<Self, FunctionError> {
        match name.to_ascii_lowercase().as_str() {
            "count" => Ok(AggState::Count(0)),
            "sum" => Ok(AggState::Sum {
                int_total: 0,
                real_total: 0.0,
                saw_real: false,
                saw_any: false,
            }),
            "avg" => Ok(AggState::Avg {
                int_total: 0,
                real_total: 0.0,
                saw_real: false,
                count: 0,
            }),
            "min" => Ok(AggState::Min(None)),
            "max" => Ok(AggState::Max(None)),
            other => Err(FunctionError::Unknown {
                name: other.to_string(),
                arity: 1,
            }),
        }
    }
}

/// `AggStep`: folds `args` into `state` (creating a fresh accumulator
/// via `name` on the first call for this context), returning the
/// updated state. `name` is only consulted to build the *initial*
/// state — `state.is_some()` calls ignore it, matching how a real
/// VDBE program always steps the same aggregate for a given context
/// slot. `collation` governs `min`/`max`'s comparison only (#263) —
/// every other kind ignores it, same as `compare_jump`'s collation
/// argument is a no-op for non-comparison opcodes.
pub fn step(
    name: &str,
    state: Option<AggState>,
    args: &[Value],
    collation: Collation,
) -> Result<AggState, FunctionError> {
    let mut state = match state {
        Some(s) => s,
        None => AggState::initial(name)?,
    };
    match &mut state {
        AggState::Count(n) => {
            if args.first().is_none_or(|v| !matches!(v, Value::Null)) {
                *n = n.saturating_add(1);
            }
        }
        AggState::Sum {
            int_total,
            real_total,
            saw_real,
            saw_any,
        } => match numeric_arg(args.first()) {
            None => {}
            Some(Value::Integer(i)) => {
                *saw_any = true;
                // Once a REAL input has switched this accumulator over,
                // every later integer folds straight into `real_total`
                // (`int_total` stays frozen at 0) — matching SQLite's own
                // incremental int-to-double switchover, whose rounding at
                // extreme magnitudes (#folding order matters once the
                // running total exceeds a double's exact-integer range)
                // a single combine-at-the-end pass over `int_total`
                // doesn't reproduce bit-for-bit.
                if *saw_real {
                    *real_total += i as f64;
                } else {
                    *int_total = int_total.saturating_add(i128::from(i));
                }
            }
            Some(Value::Real(r)) => {
                *saw_any = true;
                if !*saw_real {
                    *real_total += *int_total as f64;
                    *int_total = 0;
                    *saw_real = true;
                }
                *real_total += r;
            }
            Some(Value::Null | Value::Text(_) | Value::Blob(_)) => {}
        },
        AggState::Avg {
            int_total,
            real_total,
            saw_real,
            count,
        } => match numeric_arg(args.first()) {
            None => {}
            Some(Value::Integer(i)) => {
                *count = count.saturating_add(1);
                if *saw_real {
                    *real_total += i as f64;
                } else {
                    *int_total = int_total.saturating_add(i128::from(i));
                }
            }
            Some(Value::Real(r)) => {
                *count = count.saturating_add(1);
                if !*saw_real {
                    *real_total += *int_total as f64;
                    *int_total = 0;
                    *saw_real = true;
                }
                *real_total += r;
            }
            Some(Value::Null | Value::Text(_) | Value::Blob(_)) => {}
        },
        AggState::Min(current) => {
            if let Some(v) = args.first().filter(|v| !matches!(v, Value::Null)) {
                if current
                    .as_ref()
                    .is_none_or(|c| compare(v, c, collation) == Ordering::Less)
                {
                    *current = Some(v.clone());
                }
            }
        }
        AggState::Max(current) => {
            if let Some(v) = args.first().filter(|v| !matches!(v, Value::Null)) {
                if current
                    .as_ref()
                    .is_none_or(|c| compare(v, c, collation) == Ordering::Greater)
                {
                    *current = Some(v.clone());
                }
            }
        }
    }
    Ok(state)
}

/// `AggFinal`: produces the result for a context that has seen zero or
/// more `AggStep` calls. `state = None` means zero rows were
/// aggregated (an empty group, or a context slot never stepped) —
/// `count` finalizes to 0, `sum` to NULL, matching SQLite's own
/// zero-row aggregate results.
pub fn finalize(name: &str, state: Option<&AggState>) -> Result<Value, FunctionError> {
    match state {
        None => match name.to_ascii_lowercase().as_str() {
            "count" => Ok(Value::Integer(0)),
            "sum" | "avg" | "min" | "max" => Ok(Value::Null),
            other => Err(FunctionError::Unknown {
                name: other.to_string(),
                arity: 1,
            }),
        },
        Some(AggState::Count(n)) => Ok(Value::Integer(*n)),
        Some(AggState::Sum {
            int_total,
            real_total,
            saw_real,
            saw_any,
        }) => {
            if !saw_any {
                return Ok(Value::Null);
            }
            #[allow(clippy::cast_precision_loss)]
            if *saw_real {
                Ok(Value::Real(*real_total + *int_total as f64))
            } else {
                i64::try_from(*int_total)
                    .map(Value::Integer)
                    .map_err(|_| FunctionError::IntegerOverflow)
            }
        }
        Some(AggState::Avg {
            int_total,
            real_total,
            saw_real,
            count,
        }) => {
            if *count == 0 {
                return Ok(Value::Null);
            }
            #[allow(clippy::cast_precision_loss)]
            let total = if *saw_real {
                *real_total + *int_total as f64
            } else {
                *int_total as f64
            };
            #[allow(clippy::cast_precision_loss)]
            Ok(Value::Real(total / *count as f64))
        }
        Some(AggState::Min(v)) | Some(AggState::Max(v)) => Ok(v.clone().unwrap_or(Value::Null)),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn count_star_counts_every_row_regardless_of_args() {
        let mut state = None;
        for _ in 0..4 {
            state = Some(step("count", state, &[], Collation::Binary).unwrap());
        }
        assert_eq!(
            finalize("count", state.as_ref()).unwrap(),
            Value::Integer(4)
        );
    }

    #[test]
    fn count_x_skips_null_args() {
        let mut state = None;
        for v in [Value::Integer(1), Value::Null, Value::Integer(2)] {
            state = Some(step("count", state, &[v], Collation::Binary).unwrap());
        }
        assert_eq!(
            finalize("count", state.as_ref()).unwrap(),
            Value::Integer(2)
        );
    }

    #[test]
    fn count_with_zero_rows_finalizes_to_zero() {
        assert_eq!(finalize("count", None).unwrap(), Value::Integer(0));
    }

    #[test]
    fn sum_of_all_integers_stays_exact_integer() {
        let mut state = None;
        for v in [1i64, 2, 3] {
            state = Some(step("sum", state, &[Value::Integer(v)], Collation::Binary).unwrap());
        }
        assert_eq!(finalize("sum", state.as_ref()).unwrap(), Value::Integer(6));
    }

    #[test]
    fn sum_promotes_to_real_once_any_real_input_seen() {
        let mut state = None;
        state = Some(step("sum", state, &[Value::Integer(1)], Collation::Binary).unwrap());
        state = Some(step("sum", state, &[Value::Real(0.5)], Collation::Binary).unwrap());
        assert_eq!(finalize("sum", state.as_ref()).unwrap(), Value::Real(1.5));
    }

    #[test]
    fn sum_skips_null_and_finalizes_null_on_zero_rows() {
        assert_eq!(finalize("sum", None).unwrap(), Value::Null);
        let state = step("sum", None, &[Value::Null], Collation::Binary).unwrap();
        assert_eq!(finalize("sum", Some(&state)).unwrap(), Value::Null);
    }

    #[test]
    fn avg_of_integers_divides_to_real() {
        let mut state = None;
        for v in [1i64, 2, 3] {
            state = Some(step("avg", state, &[Value::Integer(v)], Collation::Binary).unwrap());
        }
        assert_eq!(finalize("avg", state.as_ref()).unwrap(), Value::Real(2.0));
    }

    #[test]
    fn avg_promotes_to_real_once_any_real_input_seen() {
        let mut state = None;
        state = Some(step("avg", state, &[Value::Integer(1)], Collation::Binary).unwrap());
        state = Some(step("avg", state, &[Value::Real(3.0)], Collation::Binary).unwrap());
        assert_eq!(finalize("avg", state.as_ref()).unwrap(), Value::Real(2.0));
    }

    #[test]
    fn avg_skips_null_and_finalizes_null_on_zero_rows() {
        assert_eq!(finalize("avg", None).unwrap(), Value::Null);
        let state = step("avg", None, &[Value::Null], Collation::Binary).unwrap();
        assert_eq!(finalize("avg", Some(&state)).unwrap(), Value::Null);
    }

    #[test]
    fn min_tracks_running_minimum_and_skips_null() {
        let mut state = None;
        for v in [
            Value::Integer(5),
            Value::Null,
            Value::Integer(2),
            Value::Integer(9),
        ] {
            state = Some(step("min", state, &[v], Collation::Binary).unwrap());
        }
        assert_eq!(finalize("min", state.as_ref()).unwrap(), Value::Integer(2));
    }

    #[test]
    fn max_tracks_running_maximum_and_skips_null() {
        let mut state = None;
        for v in [
            Value::Integer(5),
            Value::Null,
            Value::Integer(2),
            Value::Integer(9),
        ] {
            state = Some(step("max", state, &[v], Collation::Binary).unwrap());
        }
        assert_eq!(finalize("max", state.as_ref()).unwrap(), Value::Integer(9));
    }

    #[test]
    fn min_max_use_type_ordering_for_mixed_types() {
        let mut state = None;
        for v in [Value::Integer(1), Value::Text("a".into())] {
            state = Some(step("max", state, &[v], Collation::Binary).unwrap());
        }
        assert_eq!(
            finalize("max", state.as_ref()).unwrap(),
            Value::Text("a".into())
        );
    }

    /// #263: `min`/`max` honour a non-BINARY collation — ASCII binary
    /// order puts every uppercase letter before every lowercase one,
    /// so `{'B', 'a'}` distinguishes BINARY (`min` picks `'B'`) from
    /// NOCASE (`min` picks `'a'`).
    #[test]
    fn min_max_honour_the_given_collation() {
        let mut state = None;
        for v in [Value::Text("B".into()), Value::Text("a".into())] {
            state = Some(step("min", state, &[v], Collation::Binary).unwrap());
        }
        assert_eq!(
            finalize("min", state.as_ref()).unwrap(),
            Value::Text("B".into())
        );

        let mut state = None;
        for v in [Value::Text("B".into()), Value::Text("a".into())] {
            state = Some(step("min", state, &[v], Collation::NoCase).unwrap());
        }
        assert_eq!(
            finalize("min", state.as_ref()).unwrap(),
            Value::Text("a".into())
        );
    }

    #[test]
    fn min_max_with_zero_rows_finalizes_null() {
        assert_eq!(finalize("min", None).unwrap(), Value::Null);
        assert_eq!(finalize("max", None).unwrap(), Value::Null);
    }

    #[test]
    fn unknown_aggregate_name_errors() {
        assert!(matches!(
            step("median", None, &[Value::Integer(1)], Collation::Binary),
            Err(FunctionError::Unknown { .. })
        ));
    }
}
