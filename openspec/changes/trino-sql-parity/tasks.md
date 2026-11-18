## 1. Harness and baseline

- [x] 1.1 Shared client for Trino HTTP and Flight SQL (ADBC), Arrow-to-Trino type map, strict `compare`
- [x] 1.2 `parity_compare.py` and `iceberg_parity.py` take `--protocol http|flight|both`
- [x] 1.3 `function_coverage.py` over Trino's `SHOW FUNCTIONS`, UNTESTED counted, typed-NULL-only reported apart
- [x] 1.4 `scripts/parity-test.sh --all --protocol`
- [x] 1.5 Fix the `alter_set_properties` scenario (it downgraded v3 to v2; Trino refuses that too)
- [x] 1.6 Baseline recorded in design.md and `docs/evidence/trino-parity/`
- [ ] 1.7 Generator: `current_date` (no parentheses), `listagg` (WITHIN GROUP), `map_from_entries` / `multimap_from_entries` (row arrays), `normalize` (form keyword)

## 2. Result types at the protocol boundary

- [x] 2.1 Unsigned Arrow results become `Int64` on both protocols (`row_number`, `rank`, `dense_rank`, `ntile`, `cardinality`, `array_position`, `approx_distinct`)
- [x] 2.2 HTTP encoder sends NaN and infinities as Trino does
- [x] 2.3 HTTP type label of `cast(json AS varchar)` is `varchar`
- [ ] 2.4 Timestamp precision follows Trino: a literal's precision is its fraction digits, `timestamp` without a precision is `timestamp(3)`, and Flight reports the same precision as HTTP (today HTTP always says 6, Flight 9 for nanosecond values)
- [ ] 2.5 `TIMESTAMP '... UTC'` and `current_timestamp` keep their zone (today both come back as `timestamp(6)` without one)

## 3. Function return types

- [ ] 3.1 `length`, `strpos`, `position`, `extract` return `bigint`
- [ ] 3.2 `mod`, `sign`, `ceil`, `floor` keep an integer argument's type
- [ ] 3.3 `abs`, `avg` keep decimal precision and scale
- [ ] 3.4 Aggregates over `real` return `real` (`avg`, `corr`, `covar_pop`, `covar_samp`, ...)
- [ ] 3.5 Bitwise shifts keep the argument's width

## 4. Trino type syntax

- [x] 4.1 `array(T)`, `map(K, V)`, `row(name T, ...)` in CAST and DDL (CAST: `sqe-sql/src/cast_types.rs`; map via `arrow_cast`, no TRY_CAST to a map; no JSON-to-array cast)
- [ ] 4.2 `time(p)` and `time(p) with time zone` (CAST done: both become `TIME`, Time64(ns), zone dropped; DDL: `time(p<=6)` works, `time with time zone` is still refused as NotImplemented)

## 5. Function semantics (baseline DIFFs)

- [ ] 5.1 Work through every DIFF in `docs/evidence/trino-parity/2026-11-17-functions.md`, one themed branch per family (array, string, date/time, math, aggregate)

## 6. Missing functions

- [ ] 6.1 Math and statistics (`is_nan`, `is_finite`, `normal_cdf`, `width_bucket`, `beta_cdf`, ...)
- [ ] 6.2 String, binary and hash (`levenshtein_distance`, `crc32`, `xxhash64`, `hmac_*`, `to_base32`, ...)
  - [x] 6.2a Binary and hash (`fix/trino-binary-functions`; `spooky_hash_v2_*` open)
  - [ ] 6.2b String and URL
- [ ] 6.3 Date and time (`dow`, `doy`, `day_of_month`, `week_of_year`, `year_of_week`, `from_unixtime`, `parse_duration`, ...)
- [ ] 6.4 Array and map (`array_last`, `array_histogram`, `zip`, `zip_with`, `map_concat`, `map_filter`, `split_to_map`, ...)
- [ ] 6.5 Aggregates and signatures (`max(x, n)`, `min_by(x, y, n)`, `approx_distinct(x, e)`, `approx_most_frequent`, `reduce_agg`, `numeric_histogram`, ...)

## 7. Integer literals

- [ ] 7.1 Type integer literals as `integer` when they fit, as Trino does; ordinals, LIMIT/OFFSET and type parameters untouched
- [ ] 7.2 Overflow behaviour matches Trino (`2147483647 + 1`)
- [ ] 7.3 Access-control suite and a TPC-H compare before and after

## 8. Done

- [ ] 8.1 `scripts/parity-test.sh --all --protocol both` exits 0
