# Ranger Mask Vocabulary (Phase 2A) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Support the partial/full/date Hive mask types (`MASK`, `MASK_SHOW_LAST_4`, `MASK_SHOW_FIRST_4`, `MASK_DATE_SHOW_YEAR`) so SQE masks columns the way Apache Ranger/Kyuubi do, instead of fail-closing them to "restricted."

**Architecture:** Extend the existing pull/rewrite path. Add `MaskType::PartialMask {..}` and `MaskType::DateShowYear`; implement one `mask_partial` DataFusion UDF (Hive char-class masking with per-class mask chars, baked params like `sha256_udf`); realize `DateShowYear` with the built-in `date_trunc('year', col)`. `apply_mask` constructs both inline (carrying their own `Arc<ScalarUDF>`, so NO session registration needed, exactly like the sha256 mask). `map_mask` in `ranger_store.rs` maps the four Ranger `dataMaskType` strings to the new variants.

**Tech Stack:** Rust, DataFusion 54, arrow. Mirrors `crates/sqe-policy/src/sha256_udf.rs` (UDF with baked params + PartialEq/Eq/Hash) and the Phase-1 rewriter.

**Spec source (semantics are LOCKED from the live hive serviceDef transformer templates):**
| Ranger `dataMaskType` | transformer template | maps to |
|---|---|---|
| `MASK` | `mask({col})` (Hive defaults) | `PartialMask{show_first:0, show_last:0, upper:'X', lower:'x', digit:'n'}` |
| `MASK_SHOW_LAST_4` | `mask_show_last_n({col}, 4, 'x','x','x', -1, '1')` | `PartialMask{0, 4, 'x','x','x'}` |
| `MASK_SHOW_FIRST_4` | `mask_show_first_n({col}, 4, 'x','x','x', -1, '1')` | `PartialMask{4, 0, 'x','x','x'}` |
| `MASK_DATE_SHOW_YEAR` | `mask({col}, 'x','x','x', -1, '1', 1, 0, -1)` | `DateShowYear` (= `date_trunc('year', col)` → `YYYY-01-01`) |

Char-class rule: uppercase letters → `upper`, lowercase → `lower`, ASCII digits → `digit`, every other char (punctuation, space, dashes) → UNCHANGED. `show_first`/`show_last` characters are kept verbatim. Count by Unicode scalar (chars), not bytes. NULL → NULL.

**CRITICAL invariants (both are the `type_coercion` failure class from Phase 1 — get them wrong and the projection fails physical planning):**
1. Every mask arm's output type MUST equal the masked column's Arrow type. `PartialMask` is string-only and outputs Utf8; on a non-Utf8 column it MUST fall back to a typed NULL of the column type (see Task 4 decision). `DateShowYear` MUST cast the `date_trunc` result back to the column's exact type (`Date32`→`Date32`, preserve `Timestamp` unit/tz); on a non-temporal column it falls back to typed NULL.
2. The masked-column alias stays QUALIFIED (Phase 1 already does this via `alias_qualified`; do not regress it).

**Non-string / non-temporal mask decision (LOCKED):** fall back to typed `NULL` (column type preserved) rather than erroring the query or restricting. Rationale: keeps the data hidden (security intent met), keeps output type == column type (invariant 1), needs no Result-plumbing from `apply_mask` back to the projection builder, and never fails a query on a misconfigured numeric mask. Log a `warn!` when this fallback fires.

**Branch:** `feat/ranger-mask-vocabulary` off `feat/ranger-policy-store` (stacks on the Phase-1 MR !378). Never push to main; open an MR targeting `feat/ranger-policy-store`.

**Gates before MR:** `cargo build --all`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all` (the only pre-existing failures are the env-flaky network tests `sqe-auth oidc_m2m` and `sqe-coordinator channel_pool` — not ours).

---

## File Structure

| File | Responsibility | Action |
|---|---|---|
| `crates/sqe-policy/src/mask_udf.rs` | `mask_partial_udf(show_first, show_last, upper, lower, digit)` Hive-equivalent partial-mask UDF | Create |
| `crates/sqe-policy/src/lib.rs` | `pub mod mask_udf;` + extend `enum MaskType` | Modify |
| `crates/sqe-policy/src/plan_rewriter.rs` | `apply_mask` arms for `PartialMask` + `DateShowYear` (type-matched, NULL fallback) | Modify |
| `crates/sqe-policy/src/ranger_store.rs` | `map_mask` arms for the 4 Ranger types; fix stale "unsupported" test | Modify |
| `crates/sqe-policy/tests/rewriter_integration.rs` | executable mask tests over a qualified scan | Modify |
| `quickstart/polaris-ranger-keycloak/` | show-last-4 demo on a string column | Modify |

---

## Task 1: `mask_partial` UDF (Hive char-class masking)

**Files:**
- Create: `crates/sqe-policy/src/mask_udf.rs`
- Modify: `crates/sqe-policy/src/lib.rs` (add `pub mod mask_udf;`)

- [ ] **Step 1: Write the UDF + tests (TDD).** Create `crates/sqe-policy/src/mask_udf.rs`. Model it on `sha256_udf.rs`: a `MaskPartialFunc` struct holding `signature, show_first: u32, show_last: u32, upper: char, lower: char, digit: char`, with `PartialEq`/`Eq`/`Hash` that include ALL of `show_first/show_last/upper/lower/digit` (so DataFusion CSE does not collapse a show-first-4 and a show-last-4 UDF). SQL name: `"sqe_mask_partial"`. Signature: `Signature::exact(vec![DataType::Utf8], Volatility::Immutable)`, return `Utf8`. `invoke_with_args` handles `ColumnarValue::Array(StringArray)` and `ColumnarValue::Scalar(Utf8)`, NULL→NULL.

The masking core (pure fn, unit-tested directly):
```rust
/// Mask `s` Hive-style: keep the first `show_first` and last `show_last`
/// characters; for every other character, uppercase->`upper`, lowercase->`lower`,
/// ASCII digit->`digit`, anything else unchanged. Counts by Unicode scalar.
fn mask_str(s: &str, show_first: usize, show_last: usize, upper: char, lower: char, digit: char) -> String {
    let chars: Vec<char> = s.chars().collect();
    let n = chars.len();
    // Overlap-safe: a char is shown if it is within the first `show_first`
    // OR within the last `show_last`. If show_first+show_last >= n, all shown.
    chars
        .iter()
        .enumerate()
        .map(|(i, &c)| {
            let shown = i < show_first || i >= n.saturating_sub(show_last);
            if shown {
                c
            } else if c.is_ascii_uppercase() {
                upper
            } else if c.is_ascii_lowercase() {
                lower
            } else if c.is_ascii_digit() {
                digit
            } else {
                c
            }
        })
        .collect()
}
```

Tests to write FIRST (assert these exact outputs — they match the Ranger templates):
```rust
#[test] fn show_last_4_on_ssn() {
    // MASK_SHOW_LAST_4: mask upper/lower/digit all to 'x', keep punctuation + last 4.
    assert_eq!(mask_str("111-11-1111", 0, 4, 'x','x','x'), "xxx-xx-1111");
}
#[test] fn show_first_4() {
    assert_eq!(mask_str("abcdefgh", 4, 0, 'x','x','x'), "abcdxxxx");
}
#[test] fn full_mask_hive_defaults() {
    // MASK: upper->X, lower->x, digit->n, punctuation kept.
    assert_eq!(mask_str("Ab9-z", 0, 0, 'X','x','n'), "Xx9->"); // WRONG on purpose? verify below
}
#[test] fn show_n_longer_than_string_keeps_all() {
    assert_eq!(mask_str("ab", 0, 4, 'x','x','x'), "ab");
}
#[test] fn empty_string() {
    assert_eq!(mask_str("", 0, 4, 'x','x','x'), "");
}
```
NOTE: fix the `full_mask_hive_defaults` expectation to the TRUE output before committing: `"Ab9-z"` with upper→X, lower→x, digit→n, `-` kept → `"Xxn-x"`. Replace the placeholder assertion with `assert_eq!(mask_str("Ab9-z", 0, 0, 'X','x','n'), "Xxn-x");`. (This deliberate-error step forces you to compute the real value, per TDD.)

Then UDF-level tests mirroring `sha256_udf.rs` tests: array path (incl. a NULL element → NULL), scalar Utf8 path, scalar NULL path, and that two UDFs with different params are NOT equal (`MaskPartialFunc::new(0,4,..) != MaskPartialFunc::new(4,0,..)`).

- [ ] **Step 2: Public constructor.** `pub fn mask_partial_udf(show_first: u32, show_last: u32, upper: char, lower: char, digit: char) -> ScalarUDF { ScalarUDF::from(MaskPartialFunc::new(..)) }`.

- [ ] **Step 3: Register module.** Add `pub mod mask_udf;` to `crates/sqe-policy/src/lib.rs`.

- [ ] **Step 4: Run + commit.**
Run: `cargo test -p sqe-policy mask_udf 2>&1 | tail -20` (all pass) and `cargo clippy -p sqe-policy --all-targets -- -D warnings 2>&1 | tail -5` (clean).
```bash
git add crates/sqe-policy/src/mask_udf.rs crates/sqe-policy/src/lib.rs
git commit -m "feat(policy): mask_partial UDF (Hive show-first/last/full char-class masking)"
```

---

## Task 2: Extend `MaskType`

**Files:**
- Modify: `crates/sqe-policy/src/lib.rs` (the `enum MaskType`)

- [ ] **Step 1: Add the variants.** In `enum MaskType` (currently `Nullify, Redact(String), Hash, Custom(Expr)`), add:
```rust
    /// Hive-style partial mask. Keep the first `show_first` and last `show_last`
    /// characters; mask the rest by char class (upper/lower/digit). Realized by
    /// `mask_udf::mask_partial_udf`. String-only; non-string columns fall back
    /// to a typed NULL (see plan_rewriter::apply_mask).
    PartialMask {
        show_first: u32,
        show_last: u32,
        upper: char,
        lower: char,
        digit: char,
    },
    /// Show only the year of a date/timestamp (`YYYY-01-01`), via
    /// `date_trunc('year', col)`. Non-temporal columns fall back to typed NULL.
    DateShowYear,
```
Keep `#[derive(Debug, Clone)]` working (char/u32 are Clone+Debug).

- [ ] **Step 2: Update the existing `test_mask_type_variants_debug` test** in `lib.rs` to also construct the two new variants and assert their `{:?}` contains `"PartialMask"` / `"DateShowYear"`.

- [ ] **Step 3: Run + commit.**
Run: `cargo test -p sqe-policy --lib 2>&1 | tail -8` (pass).
```bash
git add crates/sqe-policy/src/lib.rs
git commit -m "feat(policy): MaskType::PartialMask + DateShowYear variants"
```

---

## Task 3: `apply_mask` arms (type-matched, NULL fallback)

**Files:**
- Modify: `crates/sqe-policy/src/plan_rewriter.rs` (`apply_mask`, ~line 277)

- [ ] **Step 1: Add the arms.** In `apply_mask(column_name, data_type, mask, mask_key)`, add match arms before the closing brace. Read the existing `Hash` arm first (it shows the cast-back-to-column-type pattern and how to build a UDF `ScalarFunction` inline).

```rust
        MaskType::PartialMask { show_first, show_last, upper, lower, digit } => {
            // String-only. On a non-string column, fall back to a typed NULL of
            // the column type: keeps output type == column type (avoids the
            // type_coercion projection failure) and still hides the value.
            if matches!(data_type, DataType::Utf8 | DataType::LargeUtf8) {
                Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    Arc::new(crate::mask_udf::mask_partial_udf(
                        *show_first, *show_last, *upper, *lower, *digit,
                    )),
                    vec![col(column_name)],
                ))
            } else {
                warn!(column = %column_name, ?data_type,
                    "PartialMask on non-string column; falling back to NULL");
                typed_null(data_type)
            }
        }
        MaskType::DateShowYear => {
            // date_trunc('year', col) -> YYYY-01-01. Cast back to the column's
            // exact type so the projection schema matches (Date32 stays Date32,
            // Timestamp keeps its unit/tz). Non-temporal -> typed NULL.
            if matches!(
                data_type,
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
            ) {
                let truncated = Expr::ScalarFunction(
                    datafusion::logical_expr::expr::ScalarFunction::new_udf(
                        datafusion::functions::datetime::date_trunc(),
                        vec![lit("year"), col(column_name)],
                    ),
                );
                Expr::Cast(Cast::new(Box::new(truncated), data_type.clone()))
            } else {
                warn!(column = %column_name, ?data_type,
                    "DateShowYear on non-temporal column; falling back to NULL");
                typed_null(data_type)
            }
        }
```
Factor the existing `Nullify` body into a helper `fn typed_null(data_type: &DataType) -> Expr` (the `ScalarValue::try_from(data_type)` / Utf8-NULL-cast logic already in the `Nullify` arm) and call it from `Nullify`, `PartialMask`, and `DateShowYear`. VERIFY the exact `date_trunc` accessor path: it is `datafusion::functions::datetime::date_trunc()` returning `Arc<ScalarUDF>` in DF 54 — if that path differs, grep the datafusion crate for `pub fn date_trunc` and use the correct one. The arg order is `date_trunc(granularity_str, timestamp)`.

- [ ] **Step 2: Run existing rewriter tests** to confirm no regression: `cargo test -p sqe-policy 2>&1 | grep "test result"` (all pass) + `cargo clippy -p sqe-policy --all-targets -- -D warnings 2>&1 | tail -5`.

- [ ] **Step 3: Commit.**
```bash
git add crates/sqe-policy/src/plan_rewriter.rs
git commit -m "feat(policy): apply_mask arms for PartialMask + DateShowYear (type-matched, NULL fallback)"
```

---

## Task 4: `map_mask` arms + fix stale Phase-1 test

**Files:**
- Modify: `crates/sqe-policy/src/ranger_store.rs` (`map_mask`, ~line 252; and the `unsupported_mask_restricts_column_failclosed` test)

- [ ] **Step 1: Add the arms.** In `map_mask`, replace the `// Phase 2: ...` comment + `_ => Err(())` tail with explicit arms then a real unknown fallback:
```rust
        "MASK" => Ok(Some(MaskType::PartialMask {
            show_first: 0, show_last: 0, upper: 'X', lower: 'x', digit: 'n',
        })),
        "MASK_SHOW_LAST_4" => Ok(Some(MaskType::PartialMask {
            show_first: 0, show_last: 4, upper: 'x', lower: 'x', digit: 'x',
        })),
        "MASK_SHOW_FIRST_4" => Ok(Some(MaskType::PartialMask {
            show_first: 4, show_last: 0, upper: 'x', lower: 'x', digit: 'x',
        })),
        "MASK_DATE_SHOW_YEAR" => Ok(Some(MaskType::DateShowYear)),
        // Genuinely unknown / unsupported types still fail closed (restrict).
        _ => Err(()),
```

- [ ] **Step 2: Fix the stale Phase-1 test.** The Phase-1 test `unsupported_mask_restricts_column_failclosed` uses `"MASK_SHOW_LAST_4"` as the "unsupported" example, which is now SUPPORTED. First `grep -n "MASK_SHOW_LAST_4\|MASK_SHOW_FIRST_4\|\"MASK\"\|MASK_DATE_SHOW_YEAR" crates/sqe-policy/src/ranger_store.rs` to find ALL stale references. Change the unsupported-type test to use a string that will never be a real Ranger type, e.g. `"MASK_FUTURE_UNSUPPORTED"`, and keep asserting it lands in `restricted_columns`.

- [ ] **Step 3: Add map_mask arm tests:**
```rust
    #[test]
    fn maps_show_last_4() {
        let info = DataMaskInfo { data_mask_type: "MASK_SHOW_LAST_4".into(), value_expr: None };
        match map_mask(&info, "ssn") {
            Ok(Some(MaskType::PartialMask { show_last: 4, show_first: 0, .. })) => {}
            other => panic!("expected show-last-4 PartialMask, got {other:?}"),
        }
    }
    #[test]
    fn maps_full_mask_uses_hive_default_chars() {
        let info = DataMaskInfo { data_mask_type: "MASK".into(), value_expr: None };
        match map_mask(&info, "name") {
            Ok(Some(MaskType::PartialMask { upper: 'X', lower: 'x', digit: 'n', show_first: 0, show_last: 0 })) => {}
            other => panic!("got {other:?}"),
        }
    }
    #[test]
    fn maps_date_show_year() {
        let info = DataMaskInfo { data_mask_type: "MASK_DATE_SHOW_YEAR".into(), value_expr: None };
        assert!(matches!(map_mask(&info, "hired_at"), Ok(Some(MaskType::DateShowYear))));
    }
    #[test]
    fn truly_unknown_mask_is_err() {
        let info = DataMaskInfo { data_mask_type: "MASK_FUTURE_UNSUPPORTED".into(), value_expr: None };
        assert!(map_mask(&info, "x").is_err());
    }
```
(Adjust `DataMaskInfo` field names/visibility to match the struct; they are `pub(crate)`, accessible from the in-module test.)

- [ ] **Step 4: Run + commit.**
Run: `cargo test -p sqe-policy ranger_store 2>&1 | tail -20` (all pass).
```bash
git add crates/sqe-policy/src/ranger_store.rs
git commit -m "feat(policy): map MASK/SHOW_LAST_4/SHOW_FIRST_4/DATE_SHOW_YEAR Ranger types"
```

---

## Task 5: Executable rewriter regression tests (qualified scan)

**Files:**
- Modify: `crates/sqe-policy/tests/rewriter_integration.rs`

- [ ] **Step 1: Add tests** mirroring the existing `row_filter_and_mask_execute_over_qualified_multilevel_scan` harness (build a user projection over the multilevel scan, rewrite, `execute_multilevel`, assert output). Add:
  - **show-last-4 on a string column** (`ssn` is Utf8 in `employee_schema`): policy `column_masks.insert("ssn", MaskType::PartialMask{0,4,'x','x','x'})`; after execute, assert the `ssn` column values equal the Hive-masked form of the seed SSNs (`"111-11-1111"`→`"xxx-xx-1111"`, etc.). This proves the UDF + qualified-alias + Utf8 output path end to end.
  - **date-show-year on `hired_at`** (Timestamp): policy `column_masks.insert("hired_at", MaskType::DateShowYear)`; assert the output column is the same Timestamp type and each value is the Jan-1 truncation of the seed timestamp (compute expected via `date_trunc` or assert month/day are January 1). Proves the cast-back-to-column-type invariant.
  - **non-string PartialMask falls back to NULL**: policy `PartialMask` on `customer_id` (Int64); assert the output `customer_id` column type is still Int64 and all values NULL (proves the NULL fallback keeps type).

- [ ] **Step 2: Run + commit.**
Run: `cargo test -p sqe-policy --test rewriter_integration 2>&1 | tail -15` (all pass).
```bash
git add crates/sqe-policy/tests/rewriter_integration.rs
git commit -m "test(policy): executable partial-mask + date-year mask tests over qualified scan"
```

---

## Task 6: Quickstart show-last-4 demo + gates + MR

**Files:**
- Modify: `quickstart/polaris-ranger-keycloak/test.sh` (add a string column + show-last-4 policy + assertion)
- Modify: `quickstart/polaris-ranger-keycloak/ranger/bootstrap-ranger.sh` (a `MASK_SHOW_LAST_4` policy)
- Modify: `quickstart/polaris-ranger-keycloak/OVERVIEW.md` (note the mask vocabulary)

- [ ] **Step 1: Data + policy.** The orders table is `(id, region, amount)` with no long string column. In `test.sh` data setup, add a `ssn VARCHAR` column to the orders CREATE/INSERT (e.g. `'111-11-1111'`, `'222-22-2222'`), OR create a small `sales_wh.sales.customers (id BIGINT, ssn VARCHAR)` table. Add a `MASK_SHOW_LAST_4` hive policy on `ssn` for role `engineer` in `bootstrap-ranger.sh` (mirror the existing `mask-sales-orders-amount` policy; `database=sales`, the correct table, `column=["ssn"]`, `dataMaskInfo.dataMaskType="MASK_SHOW_LAST_4"`).

- [ ] **Step 2: Assertion.** In `test.sh` section 5, add an assertion: as `bob` (engineer), `SELECT ssn FROM ...` returns values matching `xxx-xx-NNNN` (last 4 shown, digits→x, dashes kept) and NOT the raw SSN; as `alice` (analyst-only), the raw SSN is visible. Use the existing assert-helper style; include a positive check that a row was actually returned (no vacuity, per the Phase-1 lesson). Validate `sh -n test.sh`.

- [ ] **Step 3: Docs.** Add a line to OVERVIEW.md's fine-grained section listing the supported mask types (NULL, HASH, MASK, SHOW_FIRST_4, SHOW_LAST_4, DATE_SHOW_YEAR, CUSTOM) and noting char conventions match the hive serviceDef transformers (X/x/n for full MASK, x for SHOW_*). No emdash/endash/unicode-arrows in prose.

- [ ] **Step 4: Gates + project state.**
Run: `cargo build --all`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all` (only the known env-flaky failures). Tick the Phase-2A item in `docs/fine-grained-policy.md` / `nextsteps.md`.

- [ ] **Step 5: Commit + MR.**
```bash
git add quickstart/polaris-ranger-keycloak/ docs/ nextsteps.md
git commit -m "feat(quickstart): show-last-4 mask demo + mask-vocabulary docs"
git push -u origin feat/ranger-mask-vocabulary
```
Open an MR titled "feat: Ranger mask vocabulary (Phase 2A: partial/full/date masks)" targeting `feat/ranger-access-control-backend`-stacked `feat/ranger-policy-store`, noting the live show-last-4 result and that exact byte-for-byte Spark/Kyuubi parity is validated in Phase 2C.

---

## Live validation (controller, after build)
The stack is up. After the image rebuilds with this branch, verify against real Ranger: create a string column + a `MASK_SHOW_LAST_4` policy for `engineer`, query as `bob` (expect `xxx-xx-1111`) and `alice` (expect raw), exactly as Phase 1 was validated. This is the proof the new mask types work end to end; the Phase-1 type_coercion family of bugs lives in this same code path.

## Out of scope (later phases)
- Reading mask char args from the serviceDef `transformer` template dynamically (this plan hardcodes the four standard templates' args). Byte-exact Spark parity validation is Phase 2C.
- Session-context functions + role model (Phase 2B).
- Tag-based masking (Phase 3).
