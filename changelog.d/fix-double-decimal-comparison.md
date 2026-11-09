### Fixed

- A double meets a decimal as a double, as in Trino

    `double_col > 9.0` compared in `decimal(30,15)`: `9.0` is a decimal
    literal, and DataFusion cast the double to it. A NaN row failed the
    query (`Cannot cast to Decimal128(30, 15). Overflowing on NaN`), and so
    did a value above 1e15. Comparisons, `BETWEEN`, `IN`, `IN (SELECT ..)`,
    `CASE`, `COALESCE`, `nvl`, `ifnull`, `nullif`, `nvl2`, `greatest`,
    `least`, array literals, `UNION` and `VALUES` lists now cast the
    decimal side to the float type instead. The double column stays bare, so a `variant_get(v, '$.d', 'double') > 9.0` filter
    now prunes files like `> 9`.
