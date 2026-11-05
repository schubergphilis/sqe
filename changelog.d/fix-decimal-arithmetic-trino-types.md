### Changed

- Decimal arithmetic and `sum` return Trino's result types (#785)

    DataFusion takes decimal result types from arrow: `a / b` has scale `s1 + 4`, `a * b` precision `p1 + p2 + 1`, an integer operand is `decimal(20,0)` and `sum(decimal(p,s))` is `decimal(p+10,s)`. SQE now follows Trino (and Spark): a division has at least six decimals, a result that needs more than 38 digits gives up scale (down to six) before integer digits, an integer is `decimal(10,0)` and a bigint `decimal(19,0)`, and `sum` returns `decimal(38,s)`. Every result rounds half away from zero, and a result past 38 digits raises an error instead of returning an invalid value. `2.00 / 3.00` stays `0.666667`; `CAST(2 AS DECIMAL(7,2)) / CAST(3 AS DECIMAL(7,2))` is `0.6666666667` as `decimal(17,10)` instead of `0.666667` as `decimal(13,6)`. `%` keeps arrow's type, which is Trino's.

### Fixed

- A prepared statement advertises the result types its stream carries, so ADBC accepts a decimal expression's result
