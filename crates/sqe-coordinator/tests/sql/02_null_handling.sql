--- null_is_null
SELECT NULL IS NULL AS result;
--- expect
result
true

--- null_is_not_null
SELECT NULL IS NOT NULL AS result;
--- expect
result
false

--- coalesce_single_null
SELECT COALESCE(NULL, 'fallback') AS val;
--- expect
val
fallback

--- coalesce_all_nulls
SELECT COALESCE(NULL, NULL, NULL) AS val;
--- expect
val
NULL

--- coalesce_already_set
SELECT COALESCE('first', 'second') AS val;
--- expect
val
first

--- nullif_equal_values
SELECT NULLIF(42, 42) AS val;
--- expect
val
NULL

--- nullif_different_values
SELECT NULLIF(42, 0) AS val;
--- expect
val
42

--- null_in_arithmetic_is_null
SELECT 1 + NULL AS val;
--- expect
val
NULL

--- null_comparison_is_unknown
SELECT (NULL = NULL) IS NULL AS val;
--- expect
val
true

--- case_when_null_else
SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END AS val;
--- expect
val
no
