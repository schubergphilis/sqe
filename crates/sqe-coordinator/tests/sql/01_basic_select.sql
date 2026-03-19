--- select_integer_literal
SELECT 1 AS n;
--- expect
n
1

--- select_multiple_literals
SELECT 1 AS a, 2 AS b, 3 AS c;
--- expect
a | b | c
1 | 2 | 3

--- arithmetic_add_sub_mul
SELECT 1 + 2 AS sum, 10 - 3 AS diff, 4 * 5 AS prod;
--- expect
sum | diff | prod
3 | 7 | 20

--- arithmetic_division
SELECT 10 / 2 AS quot;
--- expect
quot
5

--- cast_integer_to_double
SELECT CAST(7 AS DOUBLE) AS d;
--- expect
d
7.00

--- boolean_comparisons
SELECT 1 < 2 AS lt, 2 > 1 AS gt, 1 = 1 AS eq;
--- expect
lt | gt | eq
true | true | true

--- case_expression_simple
SELECT CASE WHEN 1 > 0 THEN 'positive' ELSE 'non-positive' END AS sign;
--- expect
sign
positive

--- case_expression_multi_branch
SELECT CASE WHEN 10 >= 90 THEN 'high' WHEN 10 >= 50 THEN 'mid' ELSE 'low' END AS band;
--- expect
band
low

--- select_null_literal
SELECT NULL AS n;
--- expect
n
NULL

--- coalesce_first_non_null
SELECT COALESCE(NULL, NULL, 42) AS val;
--- expect
val
42
