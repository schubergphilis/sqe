# Operators

Built-in SQL operators. All from DataFusion's parser; SQE adds none of its own. The list is here for completeness so users do not have to cross-reference the DataFusion docs for the basics.

## Arithmetic

| Operator | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|
| `+` | Add. Numeric or interval. | yes | yes | yes | yes |
| `-` | Subtract. Numeric, interval, or unary negation. | yes | yes | yes | yes |
| `*` | Multiply. | yes | yes | yes | yes |
| `/` | Divide. Integer / Integer returns Double in DataFusion. | yes | yes | yes | yes |
| `%` | Modulo. Integer or numeric. Same as `mod()`. | yes | yes | yes | yes |
| `^` | Not exponentiation in DataFusion. Use `pow(x, y)`. | - | - | - | yes |

```sql
SELECT 10 + 5,                  -- 15
       10 - 5,                  -- 5
       10 * 5,                  -- 50
       10 / 3,                  -- 3.333... (Double)
       10 % 3,                  -- 1
       -10                      -- unary minus
;
```

For integer division use `floor(a / b)` or `div(a, b)` (DataFusion).

## String

| Operator | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|
| `\|\|` | Concatenate. NULL propagates. | yes | yes | yes | yes |
| `LIKE` | Pattern with `_` and `%`. | yes | yes | yes | yes |
| `ILIKE` | Case-insensitive LIKE. | yes | yes | partial | yes |
| `NOT LIKE` / `NOT ILIKE` | Negated. | yes | yes | yes | yes |
| `SIMILAR TO` | SQL/POSIX-light regex. | yes | yes | - | yes |
| `~` / `~*` | Regex match (case-sensitive / insensitive). | - | yes | - | yes |
| `!~` / `!~*` | Negated regex. | - | yes | - | yes |

```sql
SELECT name FROM users WHERE email ILIKE '%@example.com';
SELECT * FROM logs WHERE message ~ '^ERROR:';
```

## Comparison

| Operator | Notes |
|---|---|
| `=`, `<>`, `!=` | Equal, not-equal. NULL propagates (returns NULL, not true / false). |
| `<`, `<=`, `>`, `>=` | Ordering. |
| `BETWEEN x AND y` | Inclusive range. NULL propagates. |
| `NOT BETWEEN x AND y` | Negated. |
| `IS DISTINCT FROM` | Like `<>` but treats NULL = NULL as false (i.e. NULLs are equal). |
| `IS NOT DISTINCT FROM` | Like `=` but treats NULL = NULL as true. |

```sql
-- These differ on NULLs
SELECT a = b      FROM (VALUES (1, NULL)) AS t(a, b);  -- NULL
SELECT a IS NOT DISTINCT FROM b FROM (VALUES (1, NULL)) AS t(a, b);  -- false
SELECT a IS NOT DISTINCT FROM b FROM (VALUES (NULL, NULL)) AS t(a, b);  -- true
```

`IS NOT DISTINCT FROM` covers what Snowflake `DECODE` does for NULL = NULL match without needing the conditional construct.

## NULL tests

| Operator | Notes |
|---|---|
| `IS NULL` | True if NULL. |
| `IS NOT NULL` | True if not NULL. |
| `IS TRUE` / `IS FALSE` | Three-valued logic: NULL is not TRUE and is not FALSE. |
| `IS NOT TRUE` / `IS NOT FALSE` | Inverse, including NULL. |
| `IS UNKNOWN` / `IS NOT UNKNOWN` | Same as `IS NULL` / `IS NOT NULL` for boolean expressions. |

## Logical

| Operator | Notes |
|---|---|
| `AND` | Three-valued: NULL AND TRUE = NULL; NULL AND FALSE = FALSE. |
| `OR` | Three-valued: NULL OR TRUE = TRUE; NULL OR FALSE = NULL. |
| `NOT` | NULL stays NULL. |

## Set membership

| Operator | Notes |
|---|---|
| `IN (a, b, c)` | List membership. NULL in list is ignored. |
| `IN (subquery)` | Subquery membership. |
| `NOT IN (...)` | Negated. CARE: `NOT IN` with NULL in list returns NULL, not TRUE. |
| `EXISTS (subquery)` | True if subquery returns any row. |
| `NOT EXISTS (subquery)` | Negated. Safer than `NOT IN` for NULL handling. |
| `ANY (subquery)` / `SOME (subquery)` | Compares to any row. `x = ANY (subquery)` = `x IN (subquery)`. |
| `ALL (subquery)` | Compares to every row. |

```sql
-- IN (NOT recommended when subquery may produce NULLs)
SELECT * FROM users WHERE id IN (SELECT user_id FROM blocked);

-- NOT EXISTS (safer)
SELECT * FROM users u WHERE NOT EXISTS (
    SELECT 1 FROM blocked b WHERE b.user_id = u.id
);
```

## Type cast

| Operator | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|
| `CAST(expr AS type)` | SQL standard cast. Errors on overflow / parse failure. | yes | yes | yes | yes |
| `TRY_CAST(expr AS type)` | Returns NULL on failure. | yes | yes | partial | yes |
| `expr::type` | Postgres-style shorthand for CAST. | yes | yes | - | yes |
| `try(CAST(...))` | `try()` wraps any expression; same effect for casts. | yes | - | - | - |

```sql
SELECT CAST('42' AS BIGINT);          -- 42
SELECT TRY_CAST('not a number' AS BIGINT);  -- NULL (no error)
SELECT '42'::BIGINT;                  -- 42 (Postgres style)
SELECT try(CAST(payload AS BIGINT)) FROM events;
```

## Field access

| Operator | Notes |
|---|---|
| `expr.field` | Struct field access. |
| `expr['key']` | Map subscript. |
| `expr[index]` | Array subscript. 1-based, NULL on out-of-bounds. |

```sql
SELECT
    address.city,                          -- struct field
    settings['theme'],                     -- map lookup
    tags[1]                                -- first array element
FROM users;
```

## Quantifier shortcut

| Operator | Notes |
|---|---|
| `expr IN (subquery)` | Equivalent to `expr = ANY (subquery)`. |
| `expr NOT IN (subquery)` | Equivalent to `expr <> ALL (subquery)`. |

## Operator precedence

Higher binds tighter:

1. `::` (postfix cast)
2. `[]` (subscript), `.` (field access)
3. `unary +`, `unary -`, `NOT`
4. `*`, `/`, `%`
5. `+`, `-`
6. `||`
7. `LIKE`, `ILIKE`, `SIMILAR TO`, `~`, `BETWEEN`, `IN`, `IS NULL`, `IS NOT NULL`
8. `=`, `<>`, `!=`, `<`, `<=`, `>`, `>=`, `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`
9. `AND`
10. `OR`

Use parentheses when the order is not obvious; the planner does not warn on ambiguity.

## What is NOT supported

- **`@>`, `<@`** (Postgres array containment). Use `array_has_all` / `array_contains`.
- **`->`, `->>`** (Postgres JSON arrow). Use `json_get` / `json_get_str` (DataFusion JSON layer) or `json_extract` / `json_extract_scalar` (Trino layer). See [JSON](./json.md).
- **`<<`, `>>`** (bit shift). Use `power(2, n) * x` for left shift; `floor(x / power(2, n))` for right shift.
- **`<=>`** (MySQL null-safe equals). Use `IS NOT DISTINCT FROM`.
- **Regex named captures** (`(?P<name>...)`). DataFusion's regex backend (Rust `regex` crate) does not support PCRE-style named captures; use numbered captures via `regexp_extract(s, p, n)`.
