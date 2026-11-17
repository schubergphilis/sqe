### Added

- Trino math and statistics functions

    `is_nan`, `is_finite`, `is_infinite`, `width_bucket` (bounds and bins
    forms), `great_circle_distance`, `wilson_interval_lower` / `_upper`,
    `euclidean_distance`, `normal_cdf`, `inverse_normal_cdf`, `beta_cdf`,
    `inverse_beta_cdf`, `t_cdf`, `t_pdf`, `bit_count`,
    `bitwise_right_shift_arithmetic`, `random(n)` / `random(m, n)` and the
    `geometric_mean` aggregate.

### Fixed

- Math functions return Trino's types

    `DECIMAL '12.34'` is `decimal(4,2)`, typed from its digits, instead of
    `decimal(38,10)`. `ceil`, `floor`, `sign`, `truncate` and `mod` keep the
    integer or decimal type of their input instead of returning double; integer
    `mod` by zero is an error. The bit shifts return the first argument's
    integer type. `cosine_distance` of identical vectors is exactly 0. The
    real-typed results of `corr`, `covar_*` and `regr_*` are still double.
