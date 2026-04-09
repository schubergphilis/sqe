#!/usr/bin/env bash
set -euo pipefail

# Verify Trino compatibility by comparing SQE's output with Trino's output
# This script requires:
# 1. Trino CLI installed and available in PATH
# 2. SQE server running on localhost:18080 (Trino HTTP port)
# 3. Trino server running on localhost:8080 (default)

# Configuration
SQE_URL="http://localhost:18080"
TRINO_URL="http://localhost:8080"
TRINO_CLI="trino"

# Test queries - these must match Trino's exact output format
TEST_QUERIES=(
    "SELECT 1"
    "SELECT 1 as id, 'hello' as name"
    "SELECT count(*) from (values (1), (2), (3)) as t(x)"
    "SELECT * from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(id, name)"
    "SELECT 2 + 2"
    "SELECT 2.5 * 2"
    "SELECT 'hello' || ' world'"
    "SELECT upper('hello')"
    "SELECT lower('WORLD')"
    "SELECT length('hello')"
    "SELECT substring('hello world', 1, 5)"
    "SELECT coalesce(null, 'fallback')"
    "SELECT nullif(42, 42)"
    "SELECT nullif(42, 0)"
    "SELECT 1 + null"
    "SELECT cast('2023-01-01' as date)"
    "SELECT cast('2023-01-01 12:34:56' as timestamp)"
    "SELECT cast('123.45' as decimal(10,2))"
    "SELECT 10/3"
    "SELECT 10 % 3"
    "SELECT case when 1 = 1 then 'yes' else 'no' end"
    "SELECT dept_id, count(*) from (values (10, 'alice'), (10, 'bob'), (20, 'charlie')) as t(dept_id, name) group by dept_id"
    "SELECT sum(salary) from (values (90000, 'alice'), (85000, 'bob')) as t(salary, name)"
    "SELECT avg(salary) from (values (90000, 'alice'), (85000, 'bob')) as t(salary, name)"
    "SELECT name from (values ('alice'), ('bob'), ('charlie')) as t(name) order by name"
    "SELECT e.name, d.dept_name from (values (10, 'alice'), (20, 'bob')) as e(dept_id, name) join (values (10, 'engineering'), (20, 'marketing')) as d(dept_id, dept_name) on e.dept_id = d.dept_id"
    "SELECT name, salary, row_number() over (order by salary desc) as rn from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)"
    "with dept_stats as (select dept_id, count(*) as headcount from (values (10, 'alice'), (10, 'bob'), (20, 'charlie')) as t(dept_id, name) group by dept_id) select * from dept_stats"
    "select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where name in (select name from (values ('alice'), ('bob')) as t2(name))"
    "select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where name > any (select 'bob')"
    "select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where exists (select 1 from (values ('alice'), ('bob')) as t2(name) where t.name = t2.name)"
    "select array[1, 2, 3] as arr"
    "select map(array['a', 'b'], array[1, 2]) as m"
    "select row(1, 'hello') as r"
    "select null is null"
    "select null is not null"
    "select current_date"
    "select current_time"
    "select current_timestamp"
    "select name, department, salary, rank() over (partition by department order by salary desc) as rank from (values ('alice', 'engineering', 90000), ('bob', 'engineering', 85000), ('charlie', 'marketing', 70000)) as t(name, department, salary)"
    "select name from (values ('alice'), ('bob'), ('charlie'), ('dave')) as t(name) order by name limit 2 offset 1"
    "select name from (values ('alice'), ('bob'), ('charlie'), ('dave')) as t(name) where name like 'a%' or name like 'b%'"
    "select dept_id, avg(salary) as avg_salary from (values (10, 90000), (10, 85000), (20, 70000), (20, 75000)) as t(dept_id, salary) group by dept_id having avg(salary) > 75000"
    "select name from (values ('alice'), ('bob')) as t1(name) union all select name from (values ('charlie'), ('dave')) as t2(name)"
    "select name from (values ('alice'), ('bob'), ('charlie')) as t1(name) except select name from (values ('bob'), ('charlie')) as t2(name)"
    "select name from (values ('alice'), ('bob'), ('charlie')) as t1(name) intersect select name from (values ('bob'), ('charlie'), ('dave')) as t2(name)"
    "select name, case when salary > 80000 then 'high' else 'low' end as level from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)"
    "select cast('123' as integer)"
    "select cast('123.45' as decimal(10,2))"
    "select json_parse('{\"a\": 1, \"b\": 2}') as j"
    "select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where name like '%a%'"
    "select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where name similar to '%a%'"
    "select name from (values ('Alice'), ('Bob'), ('Charlie')) as t(name) where name ilike '%a%'"
    "select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where salary > all (select salary from (values (80000), (85000)) as t2(salary))"
    "select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where exists (select 1 from (values ('alice'), ('bob')) as t2(name) where t.name = t2.name)"
    "select name, salary, row_number() over (partition by department order by salary desc) as rn from (values ('alice', 'engineering', 90000), ('bob', 'engineering', 85000), ('charlie', 'marketing', 70000)) as t(name, department, salary)"
    "with dept_avg as (select dept_id, avg(salary) as avg_sal from (values (10, 90000), (10, 85000), (20, 70000), (20, 75000)) as t(dept_id, salary) group by dept_id) select e.name, d.avg_sal from (values (10, 'alice'), (10, 'bob'), (20, 'charlie'), (20, 'dave')) as e(dept_id, name) join dept_avg d on e.dept_id = d.dept_id where d.avg_sal > 75000"
    "select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where name in (select name from (values ('alice'), ('bob')) as t2(name)) and name not in (select name from (values ('bob'), ('dave')) as t3(name))"
    "select name, case when salary > 90000 then 'super' when salary > 80000 then 'high' when salary > 70000 then 'medium' else 'low' end as level from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000), ('dave', 60000)) as t(name, salary)"
    "select array[1, 2, 3] as arr, array['a', 'b', 'c'] as arr2"
    "select map(array['a', 'b'], array[1, 2]) as m"
    "select row(1, 'hello') as r"
    "select json_extract_scalar('{\"a\": 1, \"b\": 2}', '$.a') as a"
    "select date_add('day', 1, date '2023-01-01') as d"
    "select timestamp_add('hour', 1, timestamp '2023-01-01 12:00:00') as t"
    "select abs(-123) as a, ceil(123.45) as c, floor(123.45) as f, round(123.456, 2) as r"
    "select trim(' hello ') as t, ltrim(' hello ') as lt, rtrim(' hello ') as rt"
    "select power(2, 3) as p, sqrt(16) as s, log(10) as l, exp(1) as e"
    "select coalesce(null, 'default', 'fallback') as c, nullif(42, 42) as n, case when 1 = 1 then 'yes' else 'no' end as b"
    "select min(salary) as min, max(salary) as max, sum(salary) as sum, avg(salary) as avg, count(*) as cnt from (values (90000, 'alice'), (85000, 'bob'), (70000, 'charlie')) as t(salary, name)"
    "select name, salary, sum(salary) over (order by salary) as running_total from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)"
    "select name, salary, rank() over (order by salary desc) as r, dense_rank() over (order by salary desc) as dr, row_number() over (order by salary desc) as rn from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)"
    "select name, salary, lag(salary, 1) over (order by salary) as prev_salary, lead(salary, 1) over (order by salary) as next_salary from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)"
    "select name, salary, first_value(name) over (order by salary) as first_name, last_value(name) over (order by salary) as last_name from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)"
    "select name, salary from (select name, salary from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary) where salary > 75000) as t2"
    "select avg(high_salary) from (select case when salary > 80000 then salary else null end as high_salary from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)) as t2"
    "select name, salary * 1.1 as bonus, salary + 1000 as adjusted from (values ('alice', 90000), ('bob', 85000)) as t(name, salary)"
    "select e.name, d.dept_name, p.project_name from (values (10, 'alice'), (20, 'bob')) as e(dept_id, name) join (values (10, 'engineering'), (20, 'marketing')) as d(dept_id, dept_name) on e.dept_id = d.dept_id join (values (101, 10), (102, 20)) as p(project_id, owner_dept) on e.dept_id = p.owner_dept"
    "select name, (select count(*) from (values ('alice'), ('bob')) as t2(name) where t.name = t2.name) as cnt from (values ('alice'), ('bob'), ('charlie')) as t(name)"
    "select name, salary from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary) where salary > (select avg(salary) from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t2(name, salary))"
    "select dept_id, count(*) as cnt, sum(salary) as total, avg(salary) as avg, min(salary) as min, max(salary) as max from (values (10, 90000), (10, 85000), (20, 70000), (20, 75000)) as t(dept_id, salary) group by dept_id"
    "select dept_id, gender, count(*) as cnt from (values (10, 'm', 'alice'), (10, 'f', 'bob'), (20, 'm', 'charlie'), (20, 'f', 'dave')) as t(dept_id, gender, name) group by dept_id, gender"
    "select dept_id, gender, count(*) as cnt from (values (10, 'm', 'alice'), (10, 'f', 'bob'), (20, 'm', 'charlie'), (20, 'f', 'dave')) as t(dept_id, gender, name) group by rollup(dept_id, gender)"
    "select dept_id, gender, count(*) as cnt from (values (10, 'm', 'alice'), (10, 'f', 'bob'), (20, 'm', 'charlie'), (20, 'f', 'dave')) as t(dept_id, gender, name) group by cube(dept_id, gender)"
    "select dept_id, gender, count(*) as cnt from (values (10, 'm', 'alice'), (10, 'f', 'bob'), (20, 'm', 'charlie'), (20, 'f', 'dave')) as t(dept_id, gender, name) group by grouping sets ((dept_id), (gender), ())"
    "select * from (values ('alice', 'engineering', 90000), ('bob', 'engineering', 85000), ('charlie', 'marketing', 70000)) as t(name, dept, salary) pivot (sum(salary) for dept in ('engineering', 'marketing')) as p"
    "select * from (values ('alice', 90000, 85000), ('bob', 85000, 70000)) as t(name, engineering, marketing) unpivot (salary for dept in (engineering, marketing)) as u"
    "select name, salary, t2.value from (values ('alice', 90000), ('bob', 85000)) as t(name, salary), lateral (select salary * 1.1 as value) t2"
    "select name, unnest(array[1, 2, 3]) as n from (values ('alice'), ('bob')) as t(name)"
    "select array[array[1, 2], array[3, 4]] as arr"
    "select map('a', map('b', 1)) as m"
    "select row(1, row(2, 'hello')) as r"
    "select json_parse('{\"a\": [1, 2], \"b\": {\"c\": 3}}') as j"
    "select array_append(array[1, 2], 3) as a, array_prepend(3, array[1, 2]) as ap, array_cat(array[1, 2], array[3, 4]) as ac"
    "select map_concat(map('a', 1), map('b', 2)) as m, map_entries(map('a', 1, 'b', 2)) as me"
    "select row_field(row(1, 'hello'), 1) as f1, row_field(row(1, 'hello'), 2) as f2"
    "select date_diff('day', date '2023-01-01', date '2023-01-10') as d, date_add('day', 5, date '2023-01-01') as da, date_trunc('month', date '2023-01-15') as dt"
    "select timestamp_diff('hour', timestamp '2023-01-01 10:00:00', timestamp '2023-01-01 12:00:00') as t, timestamp_add('hour', 2, timestamp '2023-01-01 10:00:00') as ta, timestamp_trunc('hour', timestamp '2023-01-01 12:34:56') as tt"
    "select time_diff('second', time '10:00:00', time '10:05:30') as t, time_add('minute', 5, time '10:00:00') as ta, time_trunc('minute', time '10:34:56') as tt"
    "select current_user, current_catalog, current_schema"
    "select * from information_schema.tables where table_schema = 'public'"
    "select * from information_schema.columns where table_name = 'test_table'"
    "select * from information_schema.schemata where schema_name = 'public'"
    "select * from information_schema.catalogs where catalog_name = 'iceberg'"
    "select * from information_schema.views where table_schema = 'public'"
    "select * from information_schema.routines where routine_schema = 'public'"
    "select * from information_schema.triggers where trigger_schema = 'public'"
    "select * from information_schema.udfs where udf_schema = 'public'"
    "select * from information_schema.sequences where sequence_schema = 'public'"
)

# Test functions

# Run a query and capture output
run_query() {
    local query="$1"
    local url="$2"
    
    # Use curl to get JSON response from SQE's Trino-compatible HTTP API
    curl -s -X POST "$url/v1/statement" \
        -H "Content-Type: application/json" \
        -H "Authorization: Basic cm9vdDpyb290" \
        -d "\"$query\"" \
        --connect-timeout 10 \
        --max-time 30
}

# Normalize output for comparison
normalize_output() {
    local json="$1"
    
    # Extract data and columns from Trino response
    echo "$json" | jq -c '.data // [] | map(.[0])' \
        | tr -d '\n' \
        | sed 's/"//g' \
        | sed 's/,/\n/g' \
        | grep -v '^\s*$'
}

# Run a single test
run_test() {
    local query="$1"
    local test_num="$2"
    
    echo -n "Test $test_num: $query... "
    
    # Run against both SQE and Trino
    local sqe_result=$(run_query "$query" "$SQE_URL" 2>/dev/null)
    local trino_result=$(trino --server "$TRINO_URL" --catalog iceberg --schema public --execute "$query" 2>/dev/null)
    
    # If Trino failed, skip comparison
    if [ $? -ne 0 ]; then
        echo "[TRINO FAILED]"
        return 1
    fi
    
    # Check if SQE response was successful
    if echo "$sqe_result" | jq -e '.error // empty' >/dev/null; then
        echo "[SQE ERROR]"
        echo "  SQE error: "
        echo "$sqe_result" | jq -r '.error.message // "Unknown error"'
        return 1
    fi
    
    # Get the actual data from both responses
    local sqe_data=$(echo "$sqe_result" | jq -r '.data // [] | map(.[0]) | join("\n")' | sed 's/"//g')
    local trino_data=$(echo "$trino_result" | grep -v '^$' | tr -d '\r')
    
    # Compare
    if [ "$sqe_data" = "$trino_data" ]; then
        echo "[PASS]"
        return 0
    else
        echo "[FAIL]"
        echo "  SQE: $sqe_data"
        echo "  Trino: $trino_data"
        return 1
    fi
}

# Main execution
main() {
    echo "Verifying Trino compatibility..."
    echo "SQE server: $SQE_URL"
    echo "Trino server: $TRINO_URL"
    echo "Using Trino CLI: $TRINO_CLI"
    echo "Testing $(echo ${#TEST_QUERIES[@]}) queries..."
    echo ""
    
    # Check if Trino CLI is available
    if ! command -v $TRINO_CLI &>/dev/null; then
        echo "Error: Trino CLI not found. Please install Trino CLI and ensure it's in PATH."
        echo "Download from: https://trino.io/download.html"
        exit 1
    fi
    
    # Check if SQE server is running
    if ! curl -s --head "$SQE_URL/v1/info" >/dev/null; then
        echo "Error: SQE server not running at $SQE_URL. Start SQE with Trino HTTP port enabled."
        exit 1
    fi
    
    # Check if Trino server is running
    if ! curl -s --head "$TRINO_URL/v1/info" >/dev/null; then
        echo "Error: Trino server not running at $TRINO_URL. Start Trino server."
        exit 1
    fi
    
    # Run tests
    local passed=0
    local total=${#TEST_QUERIES[@]}
    local i=1
    
    for query in "${TEST_QUERIES[@]}"; do
        if run_test "$query" "$i"; then
            ((passed++))
        fi
        ((i++))
    done
    
    echo ""
    echo "Summary: $passed/$total tests passed"
    
    if [ $passed -eq $total ]; then
        echo "🎉 All tests passed! SQE is Trino-compatible."
        exit 0
    else
        echo "❌ Some tests failed. SQE is not fully Trino-compatible."
        exit 1
    fi
}

# Run main
main
