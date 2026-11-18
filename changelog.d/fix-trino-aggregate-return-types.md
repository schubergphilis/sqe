### Fixed

- Trino result types for `corr`, `covar_pop`, `covar_samp`, `regr_slope`, `regr_intercept`, and DECIMAL `round`

    `corr`, `covar_pop`, `covar_samp`, `regr_slope` and `regr_intercept` over two `REAL` arguments now return `real`, as in Trino; double and mixed inputs still return `double`. `round(decimal(p,s))` returns `decimal(p-s+min(1,s),0)`, `round(decimal(p,s), d)` returns `decimal(p+1,s)`. Rounding is half away from zero.
