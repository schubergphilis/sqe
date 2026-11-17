# Trino function coverage (Trino 484)

Signature scopes: iceberg 676, trino 230, untested 5.

- **http**: 121 of 296 functions covered (2 on typed NULLs only: to_char, to_date); signatures {'MATCH': 289, 'TYPE': 46, 'DIFF': 66, 'MISSING': 216, 'SQE-ERR': 59, 'UNTESTED': 5}
- **flight**: 115 of 296 functions covered (2 on typed NULLs only: to_char, to_date); signatures {'MATCH': 277, 'TYPE': 57, 'MISSING': 215, 'SQE-ERR': 61, 'DIFF': 66, 'UNTESTED': 5}

| Function | Kind | http | flight | First failing call |
|---|---|---|---|---|
| `abs` | scalar | TYPE | TYPE | `SELECT abs(DECIMAL '12.34')` -> TYPE: _col0: decimal(38,10) vs decimal(4,2) |
| `acos` | scalar | DIFF | MATCH | `SELECT acos(DOUBLE '2.5')` -> DIFF: SQE=1r Trino=1r \| SQE [[None]] vs Trino [['NaN']] |
| `approx_distinct` | aggregate | MISSING | MISSING | `SELECT approx_distinct(BIGINT '7')` -> TYPE: _col0: arrow:uint64 vs bigint |
| `approx_most_frequent` | aggregate | MISSING | MISSING | `SELECT approx_most_frequent(BIGINT '7', BIGINT '7', BIGINT '7')` -> MISSING: Invalid function 'approx_most_frequent'. |
| `approx_percentile` | aggregate | SQE-ERR | SQE-ERR | `SELECT approx_percentile(CAST(NULL AS bigint), CAST(NULL AS array(double)))` -> SQE-ERR: Parse error: sql parser error: Expected: <, found: ( at Line: 1, Column: 66 |
| `array_first` | scalar | SQE-ERR | SQE-ERR | `SELECT array_first(ARRAY[BIGINT '7', BIGINT '7'])` -> SQE-ERR: The function 'array_first' expected 2 argument(s) but received 1 |
| `array_histogram` | scalar | MISSING | MISSING | `SELECT array_histogram(ARRAY[BIGINT '7', BIGINT '7'])` -> MISSING: Invalid function 'array_histogram'. |
| `array_last` | scalar | MISSING | MISSING | `SELECT array_last(ARRAY[BIGINT '7', BIGINT '7'])` -> MISSING: Invalid function 'array_last'. |
| `array_position` | scalar | MATCH | TYPE | `SELECT array_position(ARRAY[BIGINT '7', BIGINT '7'], BIGINT '7')` -> TYPE: _col0: arrow:uint64 vs bigint |
| `array_remove` | scalar | DIFF | DIFF | `SELECT array_remove(ARRAY[BIGINT '7', BIGINT '7'], BIGINT '7')` -> DIFF: SQE=1r Trino=1r \| SQE [[[7]]] vs Trino [[[]]] |
| `array_sort` | scalar | SQE-ERR | SQE-ERR | `SELECT array_sort(CAST(NULL AS array(bigint)), (x, y) -> 7)` -> SQE-ERR: Parse error: sql parser error: Expected: <, found: ( at Line: 1, Column: 37 |
| `asin` | scalar | DIFF | MATCH | `SELECT asin(DOUBLE '2.5')` -> DIFF: SQE=1r Trino=1r \| SQE [[None]] vs Trino [['NaN']] |
| `at_timezone` | scalar | TYPE | TYPE | `SELECT at_timezone(TIMESTAMP '2024-03-15 12:34:56.789 UTC', 'UTC')` -> TYPE: _col0: timestamp(6) with time zone vs timestamp with time zone |
| `avg` | aggregate | SQE-ERR | SQE-ERR | `SELECT avg(DECIMAL '12.34')` -> TYPE: _col0: decimal(38,10) vs decimal(4,2) |
| `bar` | scalar | MISSING | MISSING | `SELECT bar(DOUBLE '2.5', BIGINT '7')` -> MISSING: Invalid function 'bar'. |
| `beta_cdf` | scalar | MISSING | MISSING | `SELECT beta_cdf(CAST(NULL AS double), CAST(NULL AS double), CAST(NULL AS double))` -> MISSING: Invalid function 'beta_cdf'. |
| `bit_count` | scalar | MISSING | MISSING | `SELECT bit_count(BIGINT '7', BIGINT '7')` -> MISSING: Invalid function 'bit_count'. |
| `bitwise_left_shift` | scalar | DIFF | DIFF | `SELECT bitwise_left_shift(7, 7)` -> TYPE: _col0: bigint vs integer |
| `bitwise_right_shift` | scalar | TYPE | TYPE | `SELECT bitwise_right_shift(7, 7)` -> TYPE: _col0: bigint vs integer |
| `bitwise_right_shift_arithmetic` | scalar | MISSING | MISSING | `SELECT bitwise_right_shift_arithmetic(BIGINT '7', 7)` -> MISSING: Invalid function 'bitwise_right_shift_arithmetic'. |
| `cardinality` | scalar | MATCH | TYPE | `SELECT cardinality(ARRAY[BIGINT '7', BIGINT '7'])` -> TYPE: _col0: arrow:uint64 vs bigint |
| `ceil` | scalar | DIFF | DIFF | `SELECT ceil(BIGINT '7')` -> DIFF: SQE=1r Trino=1r \| SQE [[7.0]] vs Trino [[7]] |
| `ceiling` | scalar | MISSING | MISSING | `SELECT ceiling(BIGINT '7')` -> MISSING: Invalid function 'ceiling'. |
| `char2hexint` | scalar | MISSING | MISSING | `SELECT char2hexint('abc')` -> MISSING: Invalid function 'char2hexint'. |
| `checksum` | aggregate | DIFF | DIFF | `SELECT checksum(BIGINT '7')` -> DIFF: SQE=1r Trino=1r \| SQE [['63deaab9756e2bb3']] vs Trino [['03oVhdo7DZU=']] |
| `combinations` | scalar | SQE-ERR | SQE-ERR | `SELECT combinations(CAST(NULL AS array(bigint)), CAST(NULL AS integer))` -> SQE-ERR: Parse error: sql parser error: Expected: <, found: ( at Line: 1, Column: 39 |
| `concat` | scalar | SQE-ERR | SQE-ERR | `SELECT concat(BIGINT '7', ARRAY[BIGINT '7', BIGINT '7'])` -> DIFF: SQE=1r Trino=1r \| SQE [['7[7, 7]']] vs Trino [[[7, 7, 7]]] |
| `concat_ws` | scalar | DIFF | DIFF | `SELECT concat_ws('abc', ARRAY['abc', 'abc'])` -> DIFF: SQE=1r Trino=1r \| SQE [['[abc, abc]']] vs Trino [['abcabcabc']] |
| `contains_sequence` | scalar | MISSING | MISSING | `SELECT contains_sequence(ARRAY[BIGINT '7', BIGINT '7'], ARRAY[BIGINT '7', BIGINT '7'])` -> MISSING: Invalid function 'contains_sequence'. |
| `corr` | aggregate | TYPE | TYPE | `SELECT corr(REAL '2.5', REAL '2.5')` -> TYPE: _col0: double vs real |
| `cosine_distance` | scalar | SQE-ERR | SQE-ERR | `SELECT cosine_distance(ARRAY[DOUBLE '2.5', DOUBLE '2.5'], ARRAY[DOUBLE '2.5', DOUBLE '2.5'])` -> DIFF: SQE=1r Trino=1r \| SQE [[1.1102230246251563e-16]] vs Trino [[0.0]] |
| `cosine_similarity` | scalar | SQE-ERR | SQE-ERR | `SELECT cosine_similarity(MAP(ARRAY['abc'], ARRAY[DOUBLE '2.5']), MAP(ARRAY['abc'], ARRAY[DOUBLE '2.5']))` -> SQE-ERR: Execution error: Function 'cosine_similarity' user-defined coercion failed with: Error during planning: cosine_distance  |
| `covar_pop` | aggregate | TYPE | TYPE | `SELECT covar_pop(REAL '2.5', REAL '2.5')` -> TYPE: _col0: double vs real |
| `covar_samp` | aggregate | TYPE | TYPE | `SELECT covar_samp(REAL '2.5', REAL '2.5')` -> TYPE: _col0: double vs real |
| `crc32` | scalar | MISSING | MISSING | `SELECT crc32(X'616263')` -> MISSING: Invalid function 'crc32'. |
| `current_date` | scalar | UNTESTED | UNTESTED | UNTESTED: no generated call Trino accepts () |
| `current_groups` | scalar | MISSING | MISSING | `SELECT current_groups()` -> MISSING: Invalid function 'current_groups'. |
| `date` | scalar | DIFF | DIFF | `SELECT date('2024-03-15')` -> DIFF: SQE=1r Trino=1r \| SQE [[None]] vs Trino [['2024-03-15']] |
| `date_add` | scalar | SQE-ERR | SQE-ERR | `SELECT date_add(CAST(NULL AS varchar), CAST(NULL AS bigint), CAST(NULL AS time(3)))` -> SQE-ERR: This feature is not implemented: Unsupported SQL type TIME(3) |
| `date_diff` | scalar | SQE-ERR | SQE-ERR | `SELECT date_diff(CAST(NULL AS varchar), CAST(NULL AS time(3) with time zone), CAST(NULL AS time(3) with time zone))` -> SQE-ERR: This feature is not implemented: Unsupported SQL type TIME(3) WITH TIME ZONE |
| `date_parse` | scalar | SQE-ERR | SQE-ERR | `SELECT date_parse('abc', 'abc')` -> SQE-ERR: date_parse: failed to parse 'abc' with format 'abc': input is not enough for unique date and time |
| `date_trunc` | scalar | SQE-ERR | SQE-ERR | `SELECT date_trunc('day', DATE '2024-03-15')` -> DIFF: SQE=1r Trino=1r \| SQE [['2024-03-15 00:00:00.000000']] vs Trino [['2024-03-15']] |
| `day` | scalar | SQE-ERR | SQE-ERR | `SELECT day(INTERVAL '2' DAY)` -> SQE-ERR: Expected date, timestamp, or time scalar, got IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 2, nanosecon |
| `day_of_month` | scalar | MISSING | MISSING | `SELECT day_of_month(DATE '2024-03-15')` -> MISSING: Invalid function 'day_of_month'. |
| `dense_rank` | window | MATCH | TYPE | `SELECT dense_rank() OVER (ORDER BY 1)` -> TYPE: _col0: arrow:uint64 vs bigint |
| `dow` | scalar | MISSING | MISSING | `SELECT dow(DATE '2024-03-15')` -> MISSING: Invalid function 'dow'. |
| `doy` | scalar | MISSING | MISSING | `SELECT doy(DATE '2024-03-15')` -> MISSING: Invalid function 'doy'. |
| `euclidean_distance` | scalar | MISSING | MISSING | `SELECT euclidean_distance(ARRAY[DOUBLE '2.5', DOUBLE '2.5'], ARRAY[DOUBLE '2.5', DOUBLE '2.5'])` -> MISSING: Invalid function 'euclidean_distance'. |
| `evaluate_classifier_predictions` | aggregate | MISSING | MISSING | `SELECT evaluate_classifier_predictions(BIGINT '7', BIGINT '7')` -> MISSING: Invalid function 'evaluate_classifier_predictions'. |
| `features` | scalar | MISSING | MISSING | `SELECT features(DOUBLE '2.5')` -> MISSING: Invalid function 'features'. |
| `floor` | scalar | DIFF | DIFF | `SELECT floor(BIGINT '7')` -> DIFF: SQE=1r Trino=1r \| SQE [[7.0]] vs Trino [[7]] |
| `format_datetime` | scalar | DIFF | DIFF | `SELECT format_datetime(TIMESTAMP '2024-03-15 12:34:56.789 UTC', 'day')` -> DIFF: SQE=1r Trino=1r \| SQE [['15PMy']] vs Trino [['15PM2024']] |
| `format_number` | scalar | MISSING | MISSING | `SELECT format_number(BIGINT '7')` -> MISSING: Invalid function 'format_number'. |
| `from_base32` | scalar | MISSING | MISSING | `SELECT from_base32(CAST(NULL AS varbinary))` -> MISSING: Invalid function 'from_base32'. |
| `from_base64` | scalar | DIFF | DIFF | `SELECT from_base64(X'616263')` -> DIFF: SQE=1r Trino=1r \| SQE [['']] vs Trino [['abc=']] |
| `from_base64url` | scalar | MISSING | MISSING | `SELECT from_base64url(X'616263')` -> MISSING: Invalid function 'from_base64url'. |
| `from_big_endian_32` | scalar | MISSING | MISSING | `SELECT from_big_endian_32(CAST(NULL AS varbinary))` -> MISSING: Invalid function 'from_big_endian_32'. |
| `from_big_endian_64` | scalar | MISSING | MISSING | `SELECT from_big_endian_64(CAST(NULL AS varbinary))` -> MISSING: Invalid function 'from_big_endian_64'. |
| `from_hex` | scalar | DIFF | DIFF | `SELECT from_hex(CAST(NULL AS varbinary))` -> TYPE: _col0: varchar vs varbinary |
| `from_ieee754_32` | scalar | MISSING | MISSING | `SELECT from_ieee754_32(CAST(NULL AS varbinary))` -> MISSING: Invalid function 'from_ieee754_32'. |
| `from_ieee754_64` | scalar | MISSING | MISSING | `SELECT from_ieee754_64(CAST(NULL AS varbinary))` -> MISSING: Invalid function 'from_ieee754_64'. |
| `from_iso8601_timestamp` | scalar | DIFF | DIFF | `SELECT from_iso8601_timestamp('2024-03-15')` -> DIFF: SQE=1r Trino=1r \| SQE [['2024-03-15 00:00:00.000000']] vs Trino [['2024-03-15 00:00:00.000 UTC']] |
| `from_iso8601_timestamp_nanos` | scalar | MISSING | MISSING | `SELECT from_iso8601_timestamp_nanos('2024-03-15T12:34:56Z')` -> MISSING: Invalid function 'from_iso8601_timestamp_nanos'. |
| `from_unixtime` | scalar | MISSING | MISSING | `SELECT from_unixtime(DOUBLE '2.5')` -> DIFF: SQE=1r Trino=1r \| SQE [['1970-01-01 00:00:02.500000']] vs Trino [['1970-01-01 00:00:02.500 UTC']] |
| `from_unixtime_nanos` | scalar | MISSING | MISSING | `SELECT from_unixtime_nanos(BIGINT '7')` -> MISSING: Invalid function 'from_unixtime_nanos'. |
| `from_utf8` | scalar | MISSING | MISSING | `SELECT from_utf8(X'616263')` -> DIFF: SQE=1r Trino=1r \| SQE [['�']] vs Trino [['abc']] |
| `geometric_mean` | aggregate | MISSING | MISSING | `SELECT geometric_mean(BIGINT '7')` -> MISSING: Invalid function 'geometric_mean'. |
| `great_circle_distance` | scalar | MISSING | MISSING | `SELECT great_circle_distance(DOUBLE '2.5', DOUBLE '2.5', DOUBLE '2.5', DOUBLE '2.5')` -> MISSING: Invalid function 'great_circle_distance'. |
| `histogram` | aggregate | MATCH | DIFF | `SELECT histogram(BIGINT '7')` -> DIFF: SQE=1r Trino=1r \| SQE [[{7: 1}]] vs Trino [[{'7': 1}]] |
| `hmac_md5` | scalar | MISSING | MISSING | `SELECT hmac_md5(X'616263', X'616263')` -> MISSING: Invalid function 'hmac_md5'. |
| `hmac_sha1` | scalar | MISSING | MISSING | `SELECT hmac_sha1(X'616263', X'616263')` -> MISSING: Invalid function 'hmac_sha1'. |
| `hmac_sha256` | scalar | MISSING | MISSING | `SELECT hmac_sha256(X'616263', X'616263')` -> MISSING: Invalid function 'hmac_sha256'. |
| `hmac_sha512` | scalar | MISSING | MISSING | `SELECT hmac_sha512(X'616263', X'616263')` -> MISSING: Invalid function 'hmac_sha512'. |
| `hour` | scalar | SQE-ERR | SQE-ERR | `SELECT hour(INTERVAL '2' DAY)` -> SQE-ERR: Expected date, timestamp, or time scalar, got IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 2, nanosecon |
| `human_readable_seconds` | scalar | DIFF | DIFF | `SELECT human_readable_seconds(DOUBLE '2.5')` -> DIFF: SQE=1r Trino=1r \| SQE [['2.50 seconds']] vs Trino [['3 seconds']] |
| `index` | scalar | MISSING | MISSING | `SELECT index('abc', 'abc')` -> MISSING: Invalid function 'index'. |
| `infinity` | scalar | DIFF | DIFF | `SELECT infinity()` -> DIFF: SQE=1r Trino=1r \| SQE [[None]] vs Trino [['Infinity']] |
| `inverse_beta_cdf` | scalar | MISSING | MISSING | `SELECT inverse_beta_cdf(CAST(NULL AS double), CAST(NULL AS double), CAST(NULL AS double))` -> MISSING: Invalid function 'inverse_beta_cdf'. |
| `inverse_normal_cdf` | scalar | MISSING | MISSING | `SELECT inverse_normal_cdf(CAST(NULL AS double), CAST(NULL AS double), CAST(NULL AS double))` -> MISSING: Invalid function 'inverse_normal_cdf'. |
| `is_finite` | scalar | MISSING | MISSING | `SELECT is_finite(DOUBLE '2.5')` -> MISSING: Invalid function 'is_finite'. |
| `is_infinite` | scalar | MISSING | MISSING | `SELECT is_infinite(DOUBLE '2.5')` -> MISSING: Invalid function 'is_infinite'. |
| `is_nan` | scalar | MISSING | MISSING | `SELECT is_nan(DOUBLE '2.5')` -> MISSING: Invalid function 'is_nan'. |
| `json_array_contains` | scalar | DIFF | DIFF | `SELECT json_array_contains(JSON '{"a":[1,2]}', BIGINT '7')` -> DIFF: SQE=1r Trino=1r \| SQE [[False]] vs Trino [[None]] |
| `json_array_get` | scalar | MATCH | TYPE | `SELECT json_array_get(JSON '{"a":[1,2]}', BIGINT '7')` -> TYPE: _col0: varchar vs json |
| `json_parse` | scalar | MATCH | TYPE | `SELECT json_parse('12')` -> TYPE: _col0: varchar vs json |
| `json_scalar` | scalar | MISSING | MISSING | `SELECT json_scalar(BIGINT '7')` -> MISSING: Invalid function 'json_scalar'. |
| `length` | scalar | TYPE | TYPE | `SELECT length(CHAR 'abc')` -> TYPE: _col0: integer vs bigint |
| `levenshtein_distance` | scalar | MISSING | MISSING | `SELECT levenshtein_distance('abc', 'abc')` -> MISSING: Invalid function 'levenshtein_distance'. |
| `listagg` | aggregate | UNTESTED | UNTESTED | UNTESTED: no generated call Trino accepts (varchar, varchar, boolean, varchar, boolean) |
| `lower` | scalar | TYPE | TYPE | `SELECT lower(CHAR 'abc')` -> TYPE: _col0: varchar vs char(3) |
| `lpad` | scalar | DIFF | DIFF | `SELECT lpad(X'616263', BIGINT '7', X'616263')` -> DIFF: SQE=1r Trino=1r \| SQE [['abcaabc']] vs Trino [['YWJjYWFiYw==']] |
| `luhn_check` | scalar | MISSING | MISSING | `SELECT luhn_check('12')` -> MISSING: Invalid function 'luhn_check'. |
| `map` | scalar | SQE-ERR | SQE-ERR | `SELECT map(CAST(NULL AS array(varchar)), CAST(NULL AS array(bigint)))` -> SQE-ERR: Parse error: sql parser error: Expected: <, found: ( at Line: 1, Column: 30 |
| `map_concat` | scalar | MISSING | MISSING | `SELECT map_concat(CAST(NULL AS map(varchar,bigint)))` -> MISSING: Invalid function 'map_concat'. |
| `map_entries` | scalar | TYPE | TYPE | `SELECT map_entries(MAP(ARRAY['abc'], ARRAY[BIGINT '7']))` -> TYPE: _col0: array(row(key varchar,value bigint)) vs array(row(varchar,bigint)) |
| `map_filter` | scalar | MISSING | MISSING | `SELECT map_filter(MAP(ARRAY['abc'], ARRAY[BIGINT '7']), (x, y) -> true)` -> MISSING: Invalid function 'map_filter'. |
| `map_from_entries` | scalar | UNTESTED | UNTESTED | UNTESTED: no generated call Trino accepts (array(row(K,V))) |
| `max` | aggregate | MISSING | MISSING | `SELECT max(BIGINT '7', BIGINT '7')` -> MISSING: Execution error: Function 'max' user-defined coercion failed with: Execution error: min/max was called with 2 arguments. |
| `max_by` | aggregate | MISSING | MISSING | `SELECT max_by(BIGINT '7', 'abc', BIGINT '7')` -> MISSING: The function 'max_by' expected 2 arguments but received 3. No function matches the given name and argument types 'max_by |
| `md5` | scalar | DIFF | DIFF | `SELECT md5(X'616263')` -> DIFF: SQE=1r Trino=1r \| SQE [['900150983cd24fb0d6963f7d28e17f72']] vs Trino [['kAFQmDzST7DWlj99KOF/cg==']] |
| `millisecond` | scalar | SQE-ERR | SQE-ERR | `SELECT millisecond(INTERVAL '2' DAY)` -> SQE-ERR: Unexpected parameters (interval(monthdaynano)) for function millisecond. Expected: millisecond(timestamp) |
| `min` | aggregate | MISSING | MISSING | `SELECT min(BIGINT '7', BIGINT '7')` -> MISSING: Execution error: Function 'min' user-defined coercion failed with: Execution error: min/max was called with 2 arguments. |
| `min_by` | aggregate | MISSING | MISSING | `SELECT min_by(BIGINT '7', 'abc', BIGINT '7')` -> MISSING: The function 'min_by' expected 2 arguments but received 3. No function matches the given name and argument types 'min_by |
| `minute` | scalar | SQE-ERR | SQE-ERR | `SELECT minute(INTERVAL '2' DAY)` -> SQE-ERR: Expected date, timestamp, or time scalar, got IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 2, nanosecon |
| `mod` | scalar | SQE-ERR | SQE-ERR | `SELECT mod(BIGINT '7', BIGINT '7')` -> DIFF: SQE=1r Trino=1r \| SQE [[0.0]] vs Trino [[0]] |
| `month` | scalar | SQE-ERR | SQE-ERR | `SELECT month(INTERVAL '3' MONTH)` -> SQE-ERR: Expected date, timestamp, or time scalar, got IntervalMonthDayNano("IntervalMonthDayNano { months: 3, days: 0, nanosecon |
| `multimap_from_entries` | scalar | UNTESTED | UNTESTED | UNTESTED: no generated call Trino accepts (array(row(K,V))) |
| `murmur3` | scalar | MISSING | MISSING | `SELECT murmur3(X'616263')` -> MISSING: Invalid function 'murmur3'. |
| `nan` | scalar | DIFF | MATCH | `SELECT nan()` -> DIFF: SQE=1r Trino=1r \| SQE [[None]] vs Trino [['NaN']] |
| `ngrams` | scalar | MISSING | MISSING | `SELECT ngrams(ARRAY[BIGINT '7', BIGINT '7'], 7)` -> MISSING: Invalid function 'ngrams'. |
| `normal_cdf` | scalar | MISSING | MISSING | `SELECT normal_cdf(DOUBLE '2.5', DOUBLE '2.5', DOUBLE '2.5')` -> MISSING: Invalid function 'normal_cdf'. |
| `normalize` | scalar | UNTESTED | UNTESTED | UNTESTED: no generated call Trino accepts (varchar(x), varchar(y)) |
| `now` | scalar | DIFF | DIFF | `SELECT now()` -> DIFF: SQE=1r Trino=1r \| SQE [['2026-11-17 20:35:10.555320']] vs Trino [['2026-11-17 20:35:10.548 UTC']] |
| `ntile` | window | MATCH | TYPE | `SELECT ntile(BIGINT '7') OVER (ORDER BY 1)` -> TYPE: _col0: arrow:uint64 vs bigint |
| `numeric_histogram` | aggregate | MISSING | MISSING | `SELECT numeric_histogram(BIGINT '7', DOUBLE '2.5')` -> MISSING: Invalid function 'numeric_histogram'. |
| `parse_data_size` | scalar | MISSING | MISSING | `SELECT parse_data_size(CAST(NULL AS varchar))` -> MISSING: Invalid function 'parse_data_size'. |
| `parse_datetime` | scalar | DIFF | DIFF | `SELECT parse_datetime('2024-03-15', 'yyyy-MM-dd')` -> DIFF: SQE=1r Trino=1r \| SQE [['2024-03-15 00:00:00.000000']] vs Trino [['2024-03-15 00:00:00.000 UTC']] |
| `parse_duration` | scalar | MISSING | MISSING | `SELECT parse_duration(CAST(NULL AS varchar))` -> MISSING: Invalid function 'parse_duration'. |
| `parse_presto_data_size` | scalar | MISSING | MISSING | `SELECT parse_presto_data_size(CAST(NULL AS varchar))` -> MISSING: Invalid function 'parse_presto_data_size'. |
| `rand` | scalar | MISSING | MISSING | `SELECT rand(BIGINT '7')` -> MISSING: The function 'random' expected zero argument but received 1. No function matches the given name and argument types 'rand |
| `random` | scalar | MISSING | MISSING | `SELECT random(BIGINT '7')` -> MISSING: The function 'random' expected zero argument but received 1. No function matches the given name and argument types 'rand |
| `rank` | window | MATCH | TYPE | `SELECT rank() OVER (ORDER BY 1)` -> TYPE: _col0: arrow:uint64 vs bigint |
| `reduce_agg` | aggregate | MISSING | MISSING | `SELECT reduce_agg(BIGINT '7', BIGINT '7', (x, y) -> x, (x, y) -> x)` -> MISSING: Invalid function 'reduce_agg'. |
| `regr_intercept` | aggregate | TYPE | TYPE | `SELECT regr_intercept(REAL '2.5', REAL '2.5')` -> TYPE: _col0: double vs real |
| `regr_slope` | aggregate | TYPE | TYPE | `SELECT regr_slope(REAL '2.5', REAL '2.5')` -> TYPE: _col0: double vs real |
| `render` | scalar | MISSING | MISSING | `SELECT render(true)` -> MISSING: Invalid function 'render'. |
| `repeat` | scalar | MISSING | MISSING | `SELECT repeat(BIGINT '7', BIGINT '7')` -> MISSING: Function 'repeat' requires String, but received Int64 (DataType: Int64).. No function matches the given name and argumen |
| `replace` | scalar | MISSING | MISSING | `SELECT replace('abc', 'abc')` -> MISSING: Function 'replace' expects 3 arguments but received 2. No function matches the given name and argument types 'replace(Ut |
| `reverse` | scalar | DIFF | DIFF | `SELECT reverse(ARRAY[BIGINT '7', BIGINT '7'])` -> DIFF: SQE=1r Trino=1r \| SQE [[']7 ,7[']] vs Trino [[[7, 7]]] |
| `round` | scalar | TYPE | SQE-ERR | `SELECT round(DECIMAL '12.34')` -> TYPE: _col0: decimal(38,0) vs decimal(3,0) |
| `row_number` | window | MATCH | TYPE | `SELECT row_number() OVER (ORDER BY 1)` -> TYPE: _col0: arrow:uint64 vs bigint |
| `rpad` | scalar | DIFF | DIFF | `SELECT rpad(X'616263', BIGINT '7', X'616263')` -> DIFF: SQE=1r Trino=1r \| SQE [['abcabca']] vs Trino [['YWJjYWJjYQ==']] |
| `second` | scalar | SQE-ERR | SQE-ERR | `SELECT second(INTERVAL '2' DAY)` -> SQE-ERR: Expected date, timestamp, or time scalar, got IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 2, nanosecon |
| `sequence` | scalar | SQE-ERR | SQE-ERR | `SELECT sequence(DATE '2024-03-15', DATE '2024-03-15')` -> SQE-ERR: Internal error: Function 'generate_series' failed to match any signature, errors: Error during planning: Function 'gener |
| `sha1` | scalar | MISSING | MISSING | `SELECT sha1(X'616263')` -> MISSING: Invalid function 'sha1'. |
| `sha256` | scalar | DIFF | DIFF | `SELECT sha256(X'616263')` -> DIFF: SQE=1r Trino=1r \| SQE [['ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad']] vs Trino [['ungWv48Bz+pBQUD |
| `shuffle` | scalar | MISSING | MISSING | `SELECT shuffle(ARRAY[BIGINT '7', BIGINT '7'])` -> MISSING: Invalid function 'shuffle'. |
| `sign` | scalar | SQE-ERR | SQE-ERR | `SELECT sign(BIGINT '7')` -> DIFF: SQE=1r Trino=1r \| SQE [[1.0]] vs Trino [[1]] |
| `split_part` | scalar | DIFF | DIFF | `SELECT split_part('abc', 'abc', BIGINT '7')` -> DIFF: SQE=1r Trino=1r \| SQE [['']] vs Trino [[None]] |
| `split_to_map` | scalar | MISSING | MISSING | `SELECT split_to_map(CAST(NULL AS varchar), CAST(NULL AS varchar), CAST(NULL AS varchar))` -> MISSING: Invalid function 'split_to_map'. |
| `split_to_multimap` | scalar | MISSING | MISSING | `SELECT split_to_multimap(CAST(NULL AS varchar), CAST(NULL AS varchar), CAST(NULL AS varchar))` -> MISSING: Invalid function 'split_to_multimap'. |
| `spooky_hash_v2_32` | scalar | MISSING | MISSING | `SELECT spooky_hash_v2_32(X'616263')` -> MISSING: Invalid function 'spooky_hash_v2_32'. |
| `spooky_hash_v2_64` | scalar | MISSING | MISSING | `SELECT spooky_hash_v2_64(X'616263')` -> MISSING: Invalid function 'spooky_hash_v2_64'. |
| `strpos` | scalar | MISSING | MISSING | `SELECT strpos('abc', 'abc')` -> TYPE: _col0: integer vs bigint |
| `substr` | scalar | SQE-ERR | SQE-ERR | `SELECT substr(X'616263', BIGINT '7')` -> SQE-ERR: Internal error: Function 'substr' failed to match any signature, errors: Error during planning: Function 'substr' requir |
| `sum` | aggregate | TYPE | DIFF | `SELECT sum(DECIMAL '12.34')` -> TYPE: _col0: decimal(38,10) vs decimal(38,2) |
| `t_cdf` | scalar | MISSING | MISSING | `SELECT t_cdf(DOUBLE '2.5', DOUBLE '2.5')` -> MISSING: Invalid function 't_cdf'. |
| `t_pdf` | scalar | MISSING | MISSING | `SELECT t_pdf(DOUBLE '2.5', DOUBLE '2.5')` -> MISSING: Invalid function 't_pdf'. |
| `timezone` | scalar | MISSING | MISSING | `SELECT timezone(TIME '12:34:56.789 +01:00')` -> MISSING: Invalid function 'timezone'. |
| `timezone_hour` | scalar | SQE-ERR | SQE-ERR | `SELECT timezone_hour(TIME '12:34:56.789 +01:00')` -> SQE-ERR: Physical plan creation failed: Optimizer rule 'simplify_expressions' failed |
| `timezone_minute` | scalar | SQE-ERR | SQE-ERR | `SELECT timezone_minute(TIME '12:34:56.789 +01:00')` -> SQE-ERR: Physical plan creation failed: Optimizer rule 'simplify_expressions' failed |
| `title_case` | scalar | MISSING | MISSING | `SELECT title_case(CHAR 'abc')` -> MISSING: Invalid function 'title_case'. |
| `to_base32` | scalar | MISSING | MISSING | `SELECT to_base32(X'616263')` -> MISSING: Invalid function 'to_base32'. |
| `to_base64url` | scalar | MISSING | MISSING | `SELECT to_base64url(X'616263')` -> MISSING: Invalid function 'to_base64url'. |
| `to_big_endian_32` | scalar | MISSING | MISSING | `SELECT to_big_endian_32(7)` -> MISSING: Invalid function 'to_big_endian_32'. |
| `to_big_endian_64` | scalar | MISSING | MISSING | `SELECT to_big_endian_64(BIGINT '7')` -> MISSING: Invalid function 'to_big_endian_64'. |
| `to_hex` | scalar | SQE-ERR | SQE-ERR | `SELECT to_hex(X'616263')` -> SQE-ERR: Function 'to_hex' requires Integer, but received Binary (DataType: Binary). |
| `to_ieee754_32` | scalar | MISSING | MISSING | `SELECT to_ieee754_32(REAL '2.5')` -> MISSING: Invalid function 'to_ieee754_32'. |
| `to_ieee754_64` | scalar | MISSING | MISSING | `SELECT to_ieee754_64(DOUBLE '2.5')` -> MISSING: Invalid function 'to_ieee754_64'. |
| `to_iso8601` | scalar | DIFF | DIFF | `SELECT to_iso8601(TIMESTAMP '2024-03-15 12:34:56.789 UTC')` -> DIFF: SQE=1r Trino=1r \| SQE [['2024-03-15T12:34:56.789']] vs Trino [['2024-03-15T12:34:56.789Z']] |
| `to_milliseconds` | scalar | MISSING | MISSING | `SELECT to_milliseconds(INTERVAL '2' DAY)` -> MISSING: Invalid function 'to_milliseconds'. |
| `to_timestamp` | scalar | TYPE | TYPE | `SELECT to_timestamp(CAST(NULL AS varchar), CAST(NULL AS varchar))` -> TYPE: _col0: timestamp(6) with time zone vs timestamp |
| `to_utf8` | scalar | DIFF | DIFF | `SELECT to_utf8(CHAR 'abc')` -> DIFF: SQE=1r Trino=1r \| SQE [['616263']] vs Trino [['YWJj']] |
| `trim_array` | scalar | SQE-ERR | SQE-ERR | `SELECT trim_array(CAST(NULL AS array(bigint)), CAST(NULL AS bigint))` -> SQE-ERR: Parse error: sql parser error: Expected: <, found: ( at Line: 1, Column: 37 |
| `truncate` | scalar | SQE-ERR | SQE-ERR | `SELECT truncate(DECIMAL '12.34', 7)` -> SQE-ERR: Execution error: expected numeric scalar, got Scalar(Decimal128(12.3400000000,38,10)) |
| `upper` | scalar | TYPE | TYPE | `SELECT upper(CHAR 'abc')` -> TYPE: _col0: varchar vs char(3) |
| `url_extract_fragment` | scalar | MISSING | MISSING | `SELECT url_extract_fragment('abc')` -> MISSING: Invalid function 'url_extract_fragment'. |
| `url_extract_host` | scalar | DIFF | DIFF | `SELECT url_extract_host('abc')` -> DIFF: SQE=1r Trino=1r \| SQE [[None]] vs Trino [['']] |
| `url_extract_path` | scalar | DIFF | DIFF | `SELECT url_extract_path('abc')` -> DIFF: SQE=1r Trino=1r \| SQE [[None]] vs Trino [['abc']] |
| `url_extract_port` | scalar | TYPE | TYPE | `SELECT url_extract_port('abc')` -> TYPE: _col0: varchar vs bigint |
| `url_extract_protocol` | scalar | DIFF | DIFF | `SELECT url_extract_protocol('abc')` -> DIFF: SQE=1r Trino=1r \| SQE [[None]] vs Trino [['']] |
| `url_extract_query` | scalar | DIFF | DIFF | `SELECT url_extract_query('abc')` -> DIFF: SQE=1r Trino=1r \| SQE [[None]] vs Trino [['']] |
| `uuid` | scalar | TYPE | TYPE | `SELECT uuid()` -> TYPE: _col0: varchar vs uuid |
| `week_of_year` | scalar | MISSING | MISSING | `SELECT week_of_year(DATE '2024-03-15')` -> MISSING: Invalid function 'week_of_year'. |
| `width_bucket` | scalar | MISSING | MISSING | `SELECT width_bucket(DOUBLE '2.5', ARRAY[DOUBLE '2.5', DOUBLE '2.5'])` -> MISSING: Invalid function 'width_bucket'. |
| `wilson_interval_lower` | scalar | MISSING | MISSING | `SELECT wilson_interval_lower(BIGINT '7', BIGINT '7', DOUBLE '2.5')` -> MISSING: Invalid function 'wilson_interval_lower'. |
| `wilson_interval_upper` | scalar | MISSING | MISSING | `SELECT wilson_interval_upper(BIGINT '7', BIGINT '7', DOUBLE '2.5')` -> MISSING: Invalid function 'wilson_interval_upper'. |
| `with_timezone` | scalar | TYPE | TYPE | `SELECT with_timezone(TIMESTAMP '2024-03-15 12:34:56.789', 'UTC')` -> TYPE: _col0: timestamp(6) with time zone vs timestamp with time zone |
| `xxhash64` | scalar | MISSING | MISSING | `SELECT xxhash64(X'616263')` -> MISSING: Invalid function 'xxhash64'. |
| `year` | scalar | SQE-ERR | SQE-ERR | `SELECT year(INTERVAL '3' MONTH)` -> SQE-ERR: Expected date, timestamp, or time scalar, got IntervalMonthDayNano("IntervalMonthDayNano { months: 3, days: 0, nanosecon |
| `year_of_week` | scalar | MISSING | MISSING | `SELECT year_of_week(DATE '2024-03-15')` -> MISSING: Invalid function 'year_of_week'. |
| `yow` | scalar | MISSING | MISSING | `SELECT yow(DATE '2024-03-15')` -> MISSING: Invalid function 'yow'. |
| `zip` | scalar | MISSING | MISSING | `SELECT zip(ARRAY[BIGINT '7', BIGINT '7'], ARRAY['abc', 'abc'])` -> MISSING: Invalid function 'zip'. |
| `zip_with` | scalar | MISSING | MISSING | `SELECT zip_with(ARRAY[BIGINT '7', BIGINT '7'], ARRAY['abc', 'abc'], (x, y) -> x)` -> MISSING: Invalid function 'zip_with'. |
