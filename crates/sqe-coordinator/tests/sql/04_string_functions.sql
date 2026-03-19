--- upper
SELECT UPPER('hello') AS val;
--- expect
val
HELLO

--- lower
SELECT LOWER('WORLD') AS val;
--- expect
val
world

--- length
SELECT LENGTH('hello') AS val;
--- expect
val
5

--- concat
SELECT CONCAT('foo', 'bar') AS val;
--- expect
val
foobar

--- concat_with_cast
SELECT CONCAT('id=', CAST(42 AS VARCHAR)) AS val;
--- expect
val
id=42

--- trim_leading
SELECT LTRIM('  hello') AS val;
--- expect
val
hello

--- trim_trailing
SELECT RTRIM('hello  ') AS val;
--- expect
val
hello

--- trim_both
SELECT TRIM('  hello  ') AS val;
--- expect
val
hello

--- substring
SELECT SUBSTRING('hello world' FROM 1 FOR 5) AS val;
--- expect
val
hello

--- like_pattern_match
SELECT 'hello' LIKE '%ell%' AS val;
--- expect
val
true

--- like_pattern_no_match
SELECT 'hello' LIKE 'world%' AS val;
--- expect
val
false

--- string_case_in_cte
WITH words AS (
  SELECT 1 AS id, 'apple'  AS w UNION ALL
  SELECT 2,       'Banana'      UNION ALL
  SELECT 3,       'CHERRY'
)
SELECT UPPER(w) AS up, LOWER(w) AS lo FROM words ORDER BY id;
--- expect
up | lo
APPLE | apple
BANANA | banana
CHERRY | cherry
