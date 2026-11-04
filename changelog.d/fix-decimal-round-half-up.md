### Changed

- Decimal division, `avg` over decimals and decimal-to-integer casts round half away from zero (#785)

    DataFusion truncated all three: `2.00 / 3.00` gave 0.666666, `avg` of 0.01, 0.01, 0.00 gave 0.006666 and `CAST(7.5 AS INT)` gave 7. They now give 0.666667, 0.006667 and 8, as Trino and PostgreSQL do. Result types do not change, only the last digit. A division gets one guard digit on its dividend and rounds back to its planned type (through `Decimal256` when the `Decimal128` guard could overflow). `avg` keeps DataFusion's count and sum state and rounds only the final division. Float-to-integer casts still truncate.

### Fixed

- `CAST(x AS DECIMAL(p,s))` to a smaller scale inside a comparison compared the unrounded value (#785)

    DataFusion's `unwrap_cast_in_comparison` dropped the cast, so `WHERE CAST(x AS DECIMAL(10,2)) = 1.23` ran as `x = 1.230` and missed 1.225, 1.226 and 1.234. A scale-reducing decimal cast now rounds through an internal `__sqe_rescale` function the optimizer leaves in place. Workers decode it in a pushed-down predicate.
