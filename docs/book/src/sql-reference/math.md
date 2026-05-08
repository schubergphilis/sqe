# Math functions

DataFusion contributes ~30 math functions. SQE adds a small set of Trino-named extras (`e()`, `mod()`, `truncate()`, `sign()`) plus base conversion and IEEE specials (`infinity`, `nan`).

## Sign and rounding

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `abs(x)` | `datafusion-builtin` | Absolute value. | yes | yes | yes | yes |
| `sign(x)` | `sqe-trino-functions` | -1 / 0 / +1 (and NaN -> NaN). `trino_functions.rs:100` | yes | yes | yes | yes |
| `ceil(x)` / `ceiling(x)` | `datafusion-builtin` | Round up to integer. | yes | yes | yes | yes |
| `floor(x)` | `datafusion-builtin` | Round down. | yes | yes | yes | yes |
| `round(x [, n])` | `datafusion-builtin` | Round to N decimal places. Banker's rounding by default. | yes | yes | yes | yes |
| `trunc(x [, n])` | `datafusion-builtin` | Round toward zero. | yes | yes | yes | yes |
| `truncate(x [, n])` | `sqe-trino-functions` | Trino-named alias of `trunc`. `trino_functions.rs:99` | yes | yes | partial | - |

## Powers, logs, roots

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `pow(x, y)` / `power(x, y)` | `datafusion-builtin` | x to the y. | yes | yes | yes | yes |
| `sqrt(x)` | `datafusion-builtin` | Square root. | yes | yes | yes | yes |
| `cbrt(x)` | `datafusion-builtin` | Cube root. | yes | yes | yes | yes |
| `exp(x)` | `datafusion-builtin` | e^x. | yes | yes | yes | yes |
| `ln(x)` | `datafusion-builtin` | Natural log. | yes | yes | yes | yes |
| `log(x [, base])` | `datafusion-builtin` | Log base 10 by default; or specified base. | yes | yes | yes | yes |
| `log2(x)` / `log10(x)` | `datafusion-builtin` | Specific bases. | yes | yes | yes | yes |
| `e()` | `sqe-trino-functions` | Euler's number. `trino_functions.rs:97` | yes | - | - | - |
| `pi()` | `datafusion-builtin` | Pi as a constant. | yes | yes | yes | yes |

## Trigonometry

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `sin(x)` / `cos(x)` / `tan(x)` | `datafusion-builtin` | Standard trig. Radians. | yes | yes | yes | yes |
| `asin(x)` / `acos(x)` / `atan(x)` | `datafusion-builtin` | Inverse trig. | yes | yes | yes | yes |
| `atan2(y, x)` | `datafusion-builtin` | Two-arg arctangent, full quadrant. | yes | yes | yes | yes |
| `sinh(x)` / `cosh(x)` / `tanh(x)` | `datafusion-builtin` | Hyperbolic. | yes | yes | partial | yes |
| `asinh(x)` / `acosh(x)` / `atanh(x)` | `datafusion-builtin` | Inverse hyperbolic. | yes | - | - | yes |
| `degrees(x)` | `datafusion-builtin` | Radians -> degrees. | yes | yes | yes | yes |
| `radians(x)` | `datafusion-builtin` | Degrees -> radians. | yes | yes | yes | yes |

## Modular and bit / base

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `mod(n, m)` | `sqe-trino-functions` | Modulo. Trino-named alias. `trino_functions.rs:98` | yes | yes | yes | yes |
| `n % m` | `datafusion-builtin` | SQL operator form. | yes | yes | yes | yes |
| `gcd(a, b)` | `datafusion-builtin` | Greatest common divisor. | yes | - | - | yes |
| `lcm(a, b)` | `datafusion-builtin` | Least common multiple. | yes | - | - | yes |
| `factorial(n)` | `datafusion-builtin` | n!. | - | - | yes | yes |
| `from_base(s, radix)` | `sqe-trino-functions (ext)` | Parse a base-N string to integer. `trino_functions_ext.rs:36` | yes | - | - | - |
| `to_base(n, radix)` | `sqe-trino-functions (ext)` | Convert integer to base-N string. `trino_functions_ext.rs:37` | yes | - | - | - |

```sql
SELECT to_base(255, 16);     -- 'ff'
SELECT from_base('ff', 16);  -- 255
SELECT to_base(8, 2);        -- '1000'
```

## Random

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `random()` | `datafusion-builtin` | Uniform `[0, 1)`. Volatile (re-evaluated per call). | yes | yes | yes | yes |
| `uuid()` | `datafusion-builtin` | RFC 4122 v4 random UUID. | yes | yes | yes | yes |

## IEEE specials

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `nanvl(x, y)` | `datafusion-builtin` | If `x` is NaN, return `y`; else `x`. | - | - | yes | - |
| `isnan(x)` | `datafusion-builtin` | True if `x` is NaN. | yes | - | yes | yes |
| `isinf(x)` | `datafusion-builtin` | True if `x` is infinite. | - | - | - | yes |
| `iszero(x)` | `datafusion-builtin` | True if `x` is exactly zero. | - | - | - | - |
| `infinity()` | `sqe-trino-functions (ext)` | Positive infinity (Double). `trino_functions_ext.rs:28` | yes | - | - | - |
| `nan()` | `sqe-trino-functions (ext)` | NaN (Double). `trino_functions_ext.rs:29` | yes | - | - | - |

## Statistical helpers

For aggregates (`stddev`, `variance`, `corr`, `covar_*`, `regr_*`), see [Aggregate functions](./aggregate.md). The math page covers scalars only.

## Examples

### Bucketing and binning

```sql
SELECT
    floor(amount / 100) * 100 AS bucket,
    count(*)
FROM orders
GROUP BY 1
ORDER BY 1;
```

### Geometric mean via logs

```sql
SELECT exp(avg(ln(price))) AS geo_mean FROM products WHERE price > 0;
```

DataFusion has no built-in `geo_mean`; the log identity is the standard workaround.

### Distance from a reference point (Pythagorean)

```sql
SELECT
    name,
    sqrt(pow(x - 100, 2) + pow(y - 200, 2)) AS distance
FROM points
ORDER BY distance
LIMIT 10;
```

### Hex and binary representations

```sql
SELECT
    n,
    to_base(n, 16) AS hex,
    to_base(n, 2)  AS bin,
    to_base(n, 8)  AS oct
FROM generate_series(0, 255) AS t(n);
```

## Numeric type promotion

`pow`, `log`, `exp` always return Double regardless of input. `abs`, `floor`, `ceil`, `round` preserve the input type. `+`, `-`, `*` follow SQL standard widening: integer + decimal -> decimal; integer + double -> double; decimal + decimal -> decimal with combined precision.

`/` between two integers in DataFusion returns Double, not integer. For integer division use `floor(a / b)` or `div(a, b)`.

## Decimal precision

`DECIMAL(p, s)` arithmetic widens precision per SQL standard. Two `DECIMAL(18, 2)` values multiplied produce `DECIMAL(36, 4)`. Going beyond `DECIMAL(38, ...)` overflows; CAST or use Double.
