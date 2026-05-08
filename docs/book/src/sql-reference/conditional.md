# Conditional and null-handling

Functions for choosing between values, replacing nulls, and inspecting types. Most are scalar UDFs; `CASE WHEN` is a SQL expression handled by the planner.

## Function table

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `if(cond, then, else)` | `sqe-trino-functions` | 3-arg conditional. NULL condition returns `else`. Result type is the type of `then`. `trino_functions.rs:955` | `if` | - | `if` | `if` |
| `iff(cond, then, else)` | `sqe-trino-functions` | Identical semantics to `if`. Snowflake spelling. NULL condition returns `else`. `trino_functions.rs:1039` | - | `iff` | - | - |
| `case when ... then ... [else ...] end` | `datafusion-builtin` | Searched form. Walks branches in order; first true `when` wins. | `case` | `case` | `case` | `case` |
| `case <expr> when <val> then ... end` | `datafusion-builtin` | Simple form. Compares `expr` to each `when val`; first equal wins. NULL never matches. | `case` | `case` | `case` | `case` |
| `coalesce(a, b, ...)` | `datafusion-builtin` | First non-NULL argument. Variadic. Returns NULL if every arg is NULL. | `coalesce` | `coalesce` | `coalesce` | `coalesce` |
| `nullif(a, b)` | `datafusion-builtin` | Returns NULL when `a = b`, else returns `a`. Inverse of `coalesce(nullif(...), default)` for "blank to null" patterns. | `nullif` | `nullif` | `nullif` | `nullif` |
| `nvl(a, b)` | `datafusion-builtin` | Two-arg `coalesce` shape. Returns `a` if non-NULL, else `b`. | - | `nvl` | `nvl` | - |
| `nvl2(expr, when_not_null, when_null)` | `datafusion-builtin` | Three-arg form: branches on whether `expr IS NULL`. | - | `nvl2` | `nvl2` | - |
| `greatest(a, b, ...)` | `datafusion-builtin` | Max of the arguments. NULLs ignored unless every argument is NULL. Variadic. | `greatest` | `greatest` | `greatest` | `greatest` |
| `least(a, b, ...)` | `datafusion-builtin` | Min of the arguments. NULLs ignored unless every argument is NULL. Variadic. | `least` | `least` | `least` | `least` |
| `typeof(expr)` | `sqe-trino-functions` | Returns the Arrow type as text (`"Int64"`, `"Utf8"`, `"Timestamp(Microsecond, None)"`). Trino spells the same way; result string differs by engine. `trino_functions.rs:1031` | `typeof` | - | - | `typeof` |
| `try(expr)` | `sqe-trino-functions (ext)` | Catches errors from `expr` and returns NULL on failure. Handy for casting strings of unknown shape. `trino_functions_ext.rs:76` | `try` | `try_cast` (different shape) | - | - |
| `arbitrary(col)` | `sqe-trino-functions (ext)` | Aggregate that returns one non-deterministic non-NULL value. Trino name. Equivalent to `any_value`. `trino_functions_ext.rs:68` | `arbitrary` | `any_value` | `any_value` | `any_value` |

## Patterns

### Replace NULL with a default

```sql
SELECT coalesce(comment, 'no comment') FROM orders;
SELECT nvl(comment, 'no comment') FROM orders;          -- two-arg shorthand
```

### Treat empty strings as NULL

```sql
SELECT coalesce(nullif(name, ''), 'unknown') FROM users;
```

`nullif(name, '')` returns NULL when name is the empty string, then `coalesce` substitutes the default.

### Branch on a boolean

```sql
SELECT
    iff(amount > 1000, 'large', 'small') AS bucket,    -- Snowflake
    if(amount > 1000, 'large', 'small') AS bucket_t    -- Trino
FROM orders;
```

Both calls produce the same result. Use whichever matches your team's existing dbt models. dbt-snowflake projects ported to SQE keep `iff()` working unmodified.

### Complex branching: prefer CASE

```sql
SELECT
    CASE
        WHEN amount < 100 THEN 'small'
        WHEN amount < 1000 THEN 'medium'
        ELSE 'large'
    END AS bucket
FROM orders;
```

Reach for `CASE` when there are more than two branches or the condition is not a single boolean expression.

### Take the safer cast

```sql
SELECT try(CAST(payload AS BIGINT)) AS amount FROM events;
```

`try()` swallows the conversion error and returns NULL for rows that fail. Without it, one bad row aborts the query.

## Type promotion

`coalesce`, `greatest`, `least`, `if`, `iff` all coerce arguments to a common supertype. The rules follow SQL standard widening: integer + decimal -> decimal; integer + double -> double; date + timestamp -> timestamp. If the arguments have no common supertype the planner returns an error before execution.

`case` is stricter: every branch must produce the same type, or the planner adds explicit casts when it can. Mixed types without an obvious supertype fail at plan time.

## NULL handling cheat sheet

| Construct | NULL input | Result |
|---|---|---|
| `if(NULL, x, y)` | NULL condition | `y` (NULL treated as false) |
| `iff(NULL, x, y)` | NULL condition | `y` (NULL treated as false) |
| `case when NULL then x else y end` | NULL condition | `y` |
| `coalesce(NULL, NULL, x)` | All but `x` are NULL | `x` |
| `nullif(NULL, x)` | First arg NULL | `NULL` |
| `nullif(x, NULL)` | Second arg NULL | `x` (NULL is not equal to anything) |
| `greatest(NULL, 1, 2)` | One NULL | `2` (NULLs skipped) |
| `greatest(NULL, NULL)` | All NULL | `NULL` |

## Why no `IIF` (T-SQL)

T-SQL's `IIF(cond, then, else)` is the same shape as `iff`. SQE registers `iff` (Snowflake) and `if` (Trino), both pointing at the same implementation, so a T-SQL `IIF` rename is the only change needed. We deliberately did not register a third name to keep the function table tight.

## Why no Oracle / Snowflake `DECODE`

Snowflake's `DECODE(expr, search1, result1, ..., default)` is a multi-way conditional with NULL = NULL match semantics. Two reasons it is not in SQE:

1. The name collides with DataFusion's built-in `decode(input, encoding)`, which decodes base64 / hex strings to binary. Registering a Snowflake-style DECODE under the same name would shadow the encoding helper and break any existing callsite.
2. `CASE WHEN expr IS NOT DISTINCT FROM s1 THEN r1 ... END` covers the same ground in standard SQL. (`IS NOT DISTINCT FROM` treats NULL = NULL as true.)

The audit row lives in [`features.md`](../../../features.md) so the conflict is visible.
