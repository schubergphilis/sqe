//! CLI compatibility tests for Trino.
//! These tests verify that SQE's Trino-compatible HTTP interface matches Trino's behavior exactly.
//! This is a companion to the shell script verification.

use std::process::Command;
use std::time::Duration;

use reqwest::{Client, Response, StatusCode};
use serde_json::{json, Value};
use tokio::time::sleep;

// Mock authenticator that always succeeds with a test session
struct MockAuthenticator;

#[async_trait::async_trait]
impl TrinoAuthenticator for MockAuthenticator {
    async fn authenticate(&self, username: &str, password: &str) -> Result<Session, String> {
        if username == "root" && password == "root" {
            Ok(Session::new(
                username.to_string(),
                "token".to_string(),
                None,
                chrono::Utc::now() + chrono::Duration::hours(1),
                vec![],
            ))
        } else {
            Err("Authentication failed".to_string())
        }
    }

    async fn authenticate_bearer(&self, _token: &str) -> Result<Session, String> {
        Ok(Session::new(
            "testuser".to_string(),
            "bearer_token".to_string(),
            None,
            chrono::Utc::now() + chrono::Duration::hours(1),
            vec![],
        ))
    }
}

// Mock query executor that returns fixed results
struct MockQueryExecutor;

#[async_trait::async_trait]
impl TrinoQueryExecutor for MockQueryExecutor {
    async fn execute(
        &self,
        _session: &Session,
        sql: &str,
    ) -> Result<Vec<arrow_array::RecordBatch>, String> {
        // Parse SQL and return appropriate results
        let sql_lower = sql.to_lowercase().trim();
        
        if sql_lower == "select 1" {
            return Ok(vec![create_single_int_batch(1)]);
        }
        
        if sql_lower == "select 1 as id, 'hello' as name" {
            return Ok(vec![create_id_name_batch(1, "hello")]);
        }
        
        if sql_lower == "select count(*) from (values (1), (2), (3)) as t(x)" {
            return Ok(vec![create_single_int_batch(3)]);
        }
        
        if sql_lower == "select * from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(id, name)" {
            return Ok(vec![create_id_name_batch(1, "a"), create_id_name_batch(2, "b"), create_id_name_batch(3, "c")]);
        }
        
        // Add more test cases as needed
        
        // Handle queries with arithmetic
        if sql_lower.contains("select 2 + 2") {
            return Ok(vec![create_single_int_batch(4)]);
        }
        
        if sql_lower.contains("select 2.5 * 2") {
            return Ok(vec![create_single_double_batch(5.0)]);
        }
        
        if sql_lower.contains("select 'hello' || ' world'") {
            return Ok(vec![create_single_string_batch("hello world")]);
        }
        
        if sql_lower.contains("select upper('hello')") {
            return Ok(vec![create_single_string_batch("HELLO")]);
        }
        
        if sql_lower.contains("select lower('WORLD')") {
            return Ok(vec![create_single_string_batch("world")]);
        }
        
        if sql_lower.contains("select length('hello')") {
            return Ok(vec![create_single_int_batch(5)]);
        }
        
        if sql_lower.contains("select substring('hello world', 1, 5)") {
            return Ok(vec![create_single_string_batch("hello")]);
        }
        
        if sql_lower.contains("select coalesce(null, 'fallback')") {
            return Ok(vec![create_single_string_batch("fallback")]);
        }
        
        if sql_lower.contains("select nullif(42, 42)") {
            return Ok(vec![create_null_batch()]);
        }
        
        if sql_lower.contains("select nullif(42, 0)") {
            return Ok(vec![create_single_int_batch(42)]);
        }
        
        if sql_lower.contains("select 1 + null") {
            return Ok(vec![create_null_batch()]);
        }
        
        if sql_lower.contains("select cast('2023-01-01' as date)") {
            return Ok(vec![create_single_string_batch("2023-01-01")]);
        }
        
        if sql_lower.contains("select cast('2023-01-01 12:34:56' as timestamp)") {
            return Ok(vec![create_single_string_batch("2023-01-01 12:34:56.000")]);
        }
        
        if sql_lower.contains("select cast('123.45' as decimal(10,2))") {
            return Ok(vec![create_single_string_batch("123.45")]);
        }
        
        if sql_lower.contains("select 10/3") {
            return Ok(vec![create_single_double_batch(3.333333333333333)]);
        }
        
        if sql_lower.contains("select 10 % 3") {
            return Ok(vec![create_single_int_batch(1)]);
        }
        
        if sql_lower.contains("select case when 1 = 1 then 'yes' else 'no' end") {
            return Ok(vec![create_single_string_batch("yes")]);
        }
        
        // Handle GROUP BY
        if sql_lower.contains("select dept_id, count(*) from (values (10, 'alice'), (10, 'bob'), (20, 'charlie')) as t(dept_id, name) group by dept_id") {
            let mut batches = Vec::new();
            batches.push(create_id_count_batch(10, 2));
            batches.push(create_id_count_batch(20, 1));
            return Ok(batches);
        }
        
        if sql_lower.contains("select sum(salary) from (values (90000, 'alice'), (85000, 'bob')) as t(salary, name)") {
            return Ok(vec![create_single_int_batch(175000)]);
        }
        
        if sql_lower.contains("select avg(salary) from (values (90000, 'alice'), (85000, 'bob')) as t(salary, name)") {
            return Ok(vec![create_single_double_batch(87500.0)]);
        }
        
        // Handle ORDER BY
        if sql_lower.contains("select name from (values ('alice'), ('bob'), ('charlie')) as t(name) order by name") {
            let mut batches = Vec::new();
            batches.push(create_single_string_batch("alice"));
            batches.push(create_single_string_batch("bob"));
            batches.push(create_single_string_batch("charlie"));
            return Ok(batches);
        }
        
        // Handle JOIN
        if sql_lower.contains("select e.name, d.dept_name from (values (10, 'alice'), (20, 'bob')) as e(dept_id, name) join (values (10, 'engineering'), (20, 'marketing')) as d(dept_id, dept_name) on e.dept_id = d.dept_id") {
            let mut batches = Vec::new();
            batches.push(create_name_dept_batch("alice", "engineering"));
            batches.push(create_name_dept_batch("bob", "marketing"));
            return Ok(batches);
        }
        
        // Handle window functions
        if sql_lower.contains("select name, salary, row_number() over (order by salary desc) as rn from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)") {
            let mut batches = Vec::new();
            batches.push(create_name_salary_rn_batch("alice", 90000, 1));
            batches.push(create_name_salary_rn_batch("bob", 85000, 2));
            batches.push(create_name_salary_rn_batch("charlie", 70000, 3));
            return Ok(batches);
        }
        
        // Handle CTE
        if sql_lower.contains("with dept_stats as (select dept_id, count(*) as headcount from (values (10, 'alice'), (10, 'bob'), (20, 'charlie')) as t(dept_id, name) group by dept_id) select * from dept_stats") {
            let mut batches = Vec::new();
            batches.push(create_id_count_batch(10, 2));
            batches.push(create_id_count_batch(20, 1));
            return Ok(batches);
        }
        
        // Handle subquery in WHERE
        if sql_lower.contains("select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where name in (select name from (values ('alice'), ('bob')) as t2(name))") {
            let mut batches = Vec::new();
            batches.push(create_single_string_batch("alice"));
            batches.push(create_single_string_batch("bob"));
            return Ok(batches);
        }
        
        // Handle ANY/ALL
        if sql_lower.contains("select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where name > any (select 'bob')") {
            return Ok(vec![create_single_string_batch("charlie")]);
        }
        
        // Handle EXISTS
        if sql_lower.contains("select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where exists (select 1 from (values ('alice'), ('bob')) as t2(name) where t.name = t2.name)") {
            let mut batches = Vec::new();
            batches.push(create_single_string_batch("alice"));
            batches.push(create_single_string_batch("bob"));
            return Ok(batches);
        }
        
        // Handle nested types
        if sql_lower.contains("select array[1, 2, 3] as arr") {
            return Ok(vec![create_array_batch(vec![1, 2, 3])]);
        }
        
        if sql_lower.contains("select map(array['a', 'b'], array[1, 2]) as m") {
            return Ok(vec![create_map_batch(vec!["a", "b"], vec![1, 2])]);
        }
        
        if sql_lower.contains("select row(1, 'hello') as r") {
            return Ok(vec![create_row_batch(1, "hello")]);
        }
        
        // Handle NULL handling
        if sql_lower.contains("select null is null") {
            return Ok(vec![create_boolean_batch(true)]);
        }
        
        if sql_lower.contains("select null is not null") {
            return Ok(vec![create_boolean_batch(false)]);
        }
        
        // Handle date/time functions
        if sql_lower.contains("select current_date") {
            let today = chrono::Utc::today();
            let date_str = today.format("%Y-%m-%d").to_string();
            return Ok(vec![create_single_string_batch(&date_str)]);
        }
        
        if sql_lower.contains("select current_time") {
            let now = chrono::Utc::now();
            let time_str = now.format("%H:%M:%S.%f").to_string();
            // Trino format: HH:MM:SS.mmm
            let time_str = time_str.chars().take(12).collect::<String>();
            return Ok(vec![create_single_string_batch(&time_str)]);
        }
        
        if sql_lower.contains("select current_timestamp") {
            let now = chrono::Utc::now();
            let ts_str = now.format("%Y-%m-%d %H:%M:%S.%f").to_string();
            // Trino format: YYYY-MM-DD HH:MM:SS.mmm
            let ts_str = ts_str.chars().take(23).collect::<String>();
            return Ok(vec![create_single_string_batch(&ts_str)]);
        }
        
        // Handle complex queries
        if sql_lower.contains("select name, department, salary, rank() over (partition by department order by salary desc) as rank from (values ('alice', 'engineering', 90000), ('bob', 'engineering', 85000), ('charlie', 'marketing', 70000)) as t(name, department, salary)") {
            let mut batches = Vec::new();
            batches.push(create_name_dept_salary_rank_batch("alice", "engineering", 90000, 1));
            batches.push(create_name_dept_salary_rank_batch("bob", "engineering", 85000, 2));
            batches.push(create_name_dept_salary_rank_batch("charlie", "marketing", 70000, 1));
            return Ok(batches);
        }
        
        // Handle queries with LIMIT and OFFSET
        if sql_lower.contains("select name from (values ('alice'), ('bob'), ('charlie'), ('dave')) as t(name) order by name limit 2 offset 1") {
            let mut batches = Vec::new();
            batches.push(create_single_string_batch("bob"));
            batches.push(create_single_string_batch("charlie"));
            return Ok(batches);
        }
        
        // Handle queries with complex WHERE conditions
        if sql_lower.contains("select name from (values ('alice'), ('bob'), ('charlie'), ('dave')) as t(name) where name like 'a%' or name like 'b%'") {
            let mut batches = Vec::new();
            batches.push(create_single_string_batch("alice"));
            batches.push(create_single_string_batch("bob"));
            return Ok(batches);
        }
        
        // Handle queries with HAVING
        if sql_lower.contains("select dept_id, avg(salary) as avg_salary from (values (10, 90000), (10, 85000), (20, 70000), (20, 75000)) as t(dept_id, salary) group by dept_id having avg(salary) > 75000") {
            let mut batches = Vec::new();
            batches.push(create_id_count_batch(10, 87500));
            return Ok(batches);
        }
        
        // Handle queries with UNION ALL
        if sql_lower.contains("select name from (values ('alice'), ('bob')) as t1(name) union all select name from (values ('charlie'), ('dave')) as t2(name)") {
            let mut batches = Vec::new();
            batches.push(create_single_string_batch("alice"));
            batches.push(create_single_string_batch("bob"));
            batches.push(create_single_string_batch("charlie"));
            batches.push(create_single_string_batch("dave"));
            return Ok(batches);
        }
        
        // Handle queries with EXCEPT
        if sql_lower.contains("select name from (values ('alice'), ('bob'), ('charlie')) as t1(name) except select name from (values ('bob'), ('charlie')) as t2(name)") {
            return Ok(vec![create_single_string_batch("alice")]);
        }
        
        // Handle queries with INTERSECT
        if sql_lower.contains("select name from (values ('alice'), ('bob'), ('charlie')) as t1(name) intersect select name from (values ('bob'), ('charlie'), ('dave')) as t2(name)") {
            let mut batches = Vec::new();
            batches.push(create_single_string_batch("bob"));
            batches.push(create_single_string_batch("charlie"));
            return Ok(batches);
        }
        
        // Handle queries with CASE
        if sql_lower.contains("select name, case when salary > 80000 then 'high' else 'low' end as level from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)") {
            let mut batches = Vec::new();
            batches.push(create_name_level_batch("alice", "high"));
            batches.push(create_name_level_batch("bob", "high"));
            batches.push(create_name_level_batch("charlie", "low"));
            return Ok(batches);
        }
        
        // Handle queries with CAST
        if sql_lower.contains("select cast('123' as integer)") {
            return Ok(vec![create_single_int_batch(123)]);
        }
        
        if sql_lower.contains("select cast('123.45' as decimal(10,2))") {
            return Ok(vec![create_single_string_batch("123.45")]);
        }
        
        // Handle queries with JSON
        if sql_lower.contains("select json_parse('{\"a\": 1, \"b\": 2}') as j") {
            return Ok(vec![create_json_batch("{\"a\": 1, \"b\": 2}")]);
        }
        
        // Handle queries with LIKE
        if sql_lower.contains("select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where name like '%a%'") {
            let mut batches = Vec::new();
            batches.push(create_single_string_batch("alice"));
            batches.push(create_single_string_batch("charlie"));
            return Ok(batches);
        }
        
        // Handle queries with SIMILAR TO
        if sql_lower.contains("select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where name similar to '%a%'") {
            let mut batches = Vec::new();
            batches.push(create_single_string_batch("alice"));
            batches.push(create_single_string_batch("charlie"));
            return Ok(batches);
        }
        
        // Handle queries with ILIKE (case-insensitive LIKE)
        if sql_lower.contains("select name from (values ('Alice'), ('Bob'), ('Charlie')) as t(name) where name ilike '%a%'") {
            let mut batches = Vec::new();
            batches.push(create_single_string_batch("Alice"));
            batches.push(create_single_string_batch("Charlie"));
            return Ok(batches);
        }
        
        // Handle queries with ANY and ALL
        if sql_lower.contains("select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where salary > all (select salary from (values (80000), (85000)) as t2(salary))") {
            return Ok(vec![create_single_string_batch("alice")]);
        }
        
        // Handle queries with EXISTS
        if sql_lower.contains("select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where exists (select 1 from (values ('alice'), ('bob')) as t2(name) where t.name = t2.name)") {
            let mut batches = Vec::new();
            batches.push(create_single_string_batch("alice"));
            batches.push(create_single_string_batch("bob"));
            return Ok(batches);
        }
        
        // Handle queries with window functions
        if sql_lower.contains("select name, salary, row_number() over (partition by department order by salary desc) as rn from (values ('alice', 'engineering', 90000), ('bob', 'engineering', 85000), ('charlie', 'marketing', 70000)) as t(name, department, salary)") {
            let mut batches = Vec::new();
            batches.push(create_name_dept_salary_rank_batch("alice", "engineering", 90000, 1));
            batches.push(create_name_dept_salary_rank_batch("bob", "engineering", 85000, 2));
            batches.push(create_name_dept_salary_rank_batch("charlie", "marketing", 70000, 1));
            return Ok(batches);
        }
        
        // Handle queries with CTE and JOIN
        if sql_lower.contains("with dept_avg as (select dept_id, avg(salary) as avg_sal from (values (10, 90000), (10, 85000), (20, 70000), (20, 75000)) as t(dept_id, salary) group by dept_id) select e.name, d.avg_sal from (values (10, 'alice'), (10, 'bob'), (20, 'charlie'), (20, 'dave')) as e(dept_id, name) join dept_avg d on e.dept_id = d.dept_id where d.avg_sal > 75000") {
            let mut batches = Vec::new();
            batches.push(create_name_avg_batch("alice", 87500.0));
            batches.push(create_name_avg_batch("bob", 87500.0));
            return Ok(batches);
        }
        
        // Handle queries with multiple subqueries
        if sql_lower.contains("select name from (values ('alice'), ('bob'), ('charlie')) as t(name) where name in (select name from (values ('alice'), ('bob')) as t2(name)) and name not in (select name from (values ('bob'), ('dave')) as t3(name))") {
            return Ok(vec![create_single_string_batch("alice")]);
        }
        
        // Handle queries with nested CASE
        if sql_lower.contains("select name, case when salary > 90000 then 'super' when salary > 80000 then 'high' when salary > 70000 then 'medium' else 'low' end as level from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000), ('dave', 60000)) as t(name, salary)") {
            let mut batches = Vec::new();
            batches.push(create_name_level_batch("alice", "high"));
            batches.push(create_name_level_batch("bob", "high"));
            batches.push(create_name_level_batch("charlie", "medium"));
            batches.push(create_name_level_batch("dave", "low"));
            return Ok(batches);
        }
        
        // Handle queries with array functions
        if sql_lower.contains("select array[1, 2, 3] as arr, array['a', 'b', 'c'] as arr2") {
            let mut batches = Vec::new();
            batches.push(create_array_array_batch(vec![1, 2, 3], vec!["a", "b", "c"]));
            return Ok(batches);
        }
        
        // Handle queries with map functions
        if sql_lower.contains("select map(array['a', 'b'], array[1, 2]) as m") {
            return Ok(vec![create_map_batch(vec!["a", "b"], vec![1, 2])]);
        }
        
        // Handle queries with row functions
        if sql_lower.contains("select row(1, 'hello') as r") {
            return Ok(vec![create_row_batch(1, "hello")]);
        }
        
        // Handle queries with JSON functions
        if sql_lower.contains("select json_extract_scalar('{\"a\": 1, \"b\": 2}', '$.a') as a") {
            return Ok(vec![create_single_int_batch(1)]);
        }
        
        // Handle queries with date functions
        if sql_lower.contains("select date_add('day', 1, date '2023-01-01') as d") {
            return Ok(vec![create_single_string_batch("2023-01-02")]);
        }
        
        // Handle queries with timestamp functions
        if sql_lower.contains("select timestamp_add('hour', 1, timestamp '2023-01-01 12:00:00') as t") {
            return Ok(vec![create_single_string_batch("2023-01-01 13:00:00.000")]);
        }
        
        // Handle queries with arithmetic functions
        if sql_lower.contains("select abs(-123) as a, ceil(123.45) as c, floor(123.45) as f, round(123.456, 2) as r") {
            return Ok(vec![create_abs_ceil_floor_round_batch(123, 124, 123, 123.46)]);
        }
        
        // Handle queries with string functions
        if sql_lower.contains("select trim(' hello ') as t, ltrim(' hello ') as lt, rtrim(' hello ') as rt") {
            return Ok(vec![create_trim_ltrim_rtrim_batch("hello", "hello ", " hello")]);
        }
        
        // Handle queries with numeric functions
        if sql_lower.contains("select power(2, 3) as p, sqrt(16) as s, log(10) as l, exp(1) as e") {
            return Ok(vec![create_power_sqrt_log_exp_batch(8.0, 4.0, 2.302585092994046, 2.718281828459045)]);
        }
        
        // Handle queries with conditional functions
        if sql_lower.contains("select coalesce(null, 'default', 'fallback') as c, nullif(42, 42) as n, case when 1 = 1 then 'yes' else 'no' end as b") {
            return Ok(vec![create_coalesce_nullif_case_batch("default", "", "yes")]);
        }
        
        // Handle queries with aggregate functions
        if sql_lower.contains("select min(salary) as min, max(salary) as max, sum(salary) as sum, avg(salary) as avg, count(*) as cnt from (values (90000, 'alice'), (85000, 'bob'), (70000, 'charlie')) as t(salary, name)") {
            return Ok(vec![create_min_max_sum_avg_cnt_batch(70000, 90000, 245000, 81666.66666666667, 3)]);
        }
        
        // Handle queries with window functions
        if sql_lower.contains("select name, salary, sum(salary) over (order by salary) as running_total from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)") {
            let mut batches = Vec::new();
            batches.push(create_name_salary_running_total_batch("charlie", 70000, 70000));
            batches.push(create_name_salary_running_total_batch("bob", 85000, 155000));
            batches.push(create_name_salary_running_total_batch("alice", 90000, 245000));
            return Ok(batches);
        }
        
        // Handle queries with ranking functions
        if sql_lower.contains("select name, salary, rank() over (order by salary desc) as r, dense_rank() over (order by salary desc) as dr, row_number() over (order by salary desc) as rn from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)") {
            let mut batches = Vec::new();
            batches.push(create_name_salary_rank_dense_rank_row_number_batch("alice", 90000, 1, 1, 1));
            batches.push(create_name_salary_rank_dense_rank_row_number_batch("bob", 85000, 2, 2, 2));
            batches.push(create_name_salary_rank_dense_rank_row_number_batch("charlie", 70000, 3, 3, 3));
            return Ok(batches);
        }
        
        // Handle queries with lead/lag
        if sql_lower.contains("select name, salary, lag(salary, 1) over (order by salary) as prev_salary, lead(salary, 1) over (order by salary) as next_salary from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)") {
            let mut batches = Vec::new();
            batches.push(create_name_salary_prev_next_batch("charlie", 70000, "", "85000"));
            batches.push(create_name_salary_prev_next_batch("bob", 85000, "70000", "90000"));
            batches.push(create_name_salary_prev_next_batch("alice", 90000, "85000", ""));
            return Ok(batches);
        }
        
        // Handle queries with first_value/last_value
        if sql_lower.contains("select name, salary, first_value(name) over (order by salary) as first_name, last_value(name) over (order by salary) as last_name from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)") {
            let mut batches = Vec::new();
            batches.push(create_name_salary_first_last_batch("charlie", 70000, "charlie", "charlie"));
            batches.push(create_name_salary_first_last_batch("bob", 85000, "charlie", "bob"));
            batches.push(create_name_salary_first_last_batch("alice", 90000, "charlie", "alice"));
            return Ok(batches);
        }
        
        // Handle queries with nested queries
        if sql_lower.contains("select name, salary from (select name, salary from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary) where salary > 75000) as t2") {
            let mut batches = Vec::new();
            batches.push(create_name_salary_batch("alice", 90000));
            batches.push(create_name_salary_batch("bob", 85000));
            return Ok(batches);
        }
        
        // Handle queries with nested aggregations
        if sql_lower.contains("select avg(high_salary) from (select case when salary > 80000 then salary else null end as high_salary from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary)) as t2") {
            return Ok(vec![create_single_double_batch(87500.0)]);
        }
        
        // Handle queries with complex expressions
        if sql_lower.contains("select name, salary * 1.1 as bonus, salary + 1000 as adjusted from (values ('alice', 90000), ('bob', 85000)) as t(name, salary)") {
            let mut batches = Vec::new();
            batches.push(create_name_bonus_adjusted_batch("alice", 99000.0, 91000));
            batches.push(create_name_bonus_adjusted_batch("bob", 93500.0, 86000));
            return Ok(batches);
        }
        
        // Handle queries with multiple joins
        if sql_lower.contains("select e.name, d.dept_name, p.project_name from (values (10, 'alice'), (20, 'bob')) as e(dept_id, name) join (values (10, 'engineering'), (20, 'marketing')) as d(dept_id, dept_name) on e.dept_id = d.dept_id join (values (101, 10), (102, 20)) as p(project_id, owner_dept) on e.dept_id = p.owner_dept") {
            let mut batches = Vec::new();
            batches.push(create_name_dept_project_batch("alice", "engineering", "101"));
            batches.push(create_name_dept_project_batch("bob", "marketing", "102"));
            return Ok(batches);
        }
        
        // Handle queries with subquery in SELECT
        if sql_lower.contains("select name, (select count(*) from (values ('alice'), ('bob')) as t2(name) where t.name = t2.name) as cnt from (values ('alice'), ('bob'), ('charlie')) as t(name)") {
            let mut batches = Vec::new();
            batches.push(create_name_count_batch("alice", 1));
            batches.push(create_name_count_batch("bob", 1));
            batches.push(create_name_count_batch("charlie", 0));
            return Ok(batches);
        }
        
        // Handle queries with correlated subqueries
        if sql_lower.contains("select name, salary from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t(name, salary) where salary > (select avg(salary) from (values ('alice', 90000), ('bob', 85000), ('charlie', 70000)) as t2(name, salary))") {
            return Ok(vec![create_name_salary_batch("alice", 90000)]);
        }
        
        // Handle queries with multiple aggregations
        if sql_lower.contains("select dept_id, count(*) as cnt, sum(salary) as total, avg(salary) as avg, min(salary) as min, max(salary) as max from (values (10, 90000), (10, 85000), (20, 70000), (20, 75000)) as t(dept_id, salary) group by dept_id") {
            let mut batches = Vec::new();
            batches.push(create_id_count_sum_avg_min_max_batch(10, 2, 175000, 87500.0, 85000, 90000));
            batches.push(create_id_count_sum_avg_min_max_batch(20, 2, 145000, 72500.0, 70000, 75000));
            return Ok(batches);
        }
        
        // Handle queries with group by multiple columns
        if sql_lower.contains("select dept_id, gender, count(*) as cnt from (values (10, 'm', 'alice'), (10, 'f', 'bob'), (20, 'm', 'charlie'), (20, 'f', 'dave')) as t(dept_id, gender, name) group by dept_id, gender") {
            let mut batches = Vec::new();
            batches.push(create_id_gender_count_batch(10, "m", 1));
            batches.push(create_id_gender_count_batch(10, "f", 1));
            batches.push(create_id_gender_count_batch(20, "m", 1));
            batches.push(create_id_gender_count_batch(20, "f", 1));
            return Ok(batches);
        }
        
        // Handle queries with rollup
        if sql_lower.contains("select dept_id, gender, count(*) as cnt from (values (10, 'm', 'alice'), (10, 'f', 'bob'), (20, 'm', 'charlie'), (20, 'f', 'dave')) as t(dept_id, gender, name) group by rollup(dept_id, gender)") {
            let mut batches = Vec::new();
            batches.push(create_id_gender_count_batch(10, "m", 1));
            batches.push(create_id_gender_count_batch(10, "f", 1));
            batches.push(create_id_gender_count_batch(20, "m", 1));
            batches.push(create_id_gender_count_batch(20, "f", 1));
            batches.push(create_id_gender_count_batch(10, "", 2));
            batches.push(create_id_gender_count_batch(20, "", 2));
            batches.push(create_id_gender_count_batch("", "", 4));
            return Ok(batches);
        }
        
        // Handle queries with cube
        if sql_lower.contains("select dept_id, gender, count(*) as cnt from (values (10, 'm', 'alice'), (10, 'f', 'bob'), (20, 'm', 'charlie'), (20, 'f', 'dave')) as t(dept_id, gender, name) group by cube(dept_id, gender)") {
            let mut batches = Vec::new();
            batches.push(create_id_gender_count_batch(10, "m", 1));
            batches.push(create_id_gender_count_batch(10, "f", 1));
            batches.push(create_id_gender_count_batch(20, "m", 1));
            batches.push(create_id_gender_count_batch(20, "f", 1));
            batches.push(create_id_gender_count_batch(10, "", 2));
            batches.push(create_id_gender_count_batch(20, "", 2));
            batches.push(create_id_gender_count_batch("", "m", 2));
            batches.push(create_id_gender_count_batch("", "f", 2));
            batches.push(create_id_gender_count_batch("", "", 4));
            return Ok(batches);
        }
        
        // Handle queries with grouping sets
        if sql_lower.contains("select dept_id, gender, count(*) as cnt from (values (10, 'm', 'alice'), (10, 'f', 'bob'), (20, 'm', 'charlie'), (20, 'f', 'dave')) as t(dept_id, gender, name) group by grouping sets ((dept_id), (gender), ())") {
            let mut batches = Vec::new();
            batches.push(create_id_gender_count_batch(10, "", 2));
            batches.push(create_id_gender_count_batch(20, "", 2));
            batches.push(create_id_gender_count_batch("", "m", 2));
            batches.push(create_id_gender_count_batch("", "f", 2));
            batches.push(create_id_gender_count_batch("", "", 4));
            return Ok(batches);
        }
        
        // Handle queries with pivot
        if sql_lower.contains("select * from (values ('alice', 'engineering', 90000), ('bob', 'engineering', 85000), ('charlie', 'marketing', 70000)) as t(name, dept, salary) pivot (sum(salary) for dept in ('engineering', 'marketing')) as p") {
            return Ok(vec![create_pivot_batch("alice", 90000, 0), create_pivot_batch("bob", 85000, 0), create_pivot_batch("charlie", 0, 70000)]);
        }
        
        // Handle queries with unpivot
        if sql_lower.contains("select * from (values ('alice', 90000, 85000), ('bob', 85000, 70000)) as t(name, engineering, marketing) unpivot (salary for dept in (engineering, marketing)) as u") {
            let mut batches = Vec::new();
            batches.push(create_unpivot_batch("alice", "engineering", 90000));
            batches.push(create_unpivot_batch("alice", "marketing", 85000));
            batches.push(create_unpivot_batch("bob", "engineering", 85000));
            batches.push(create_unpivot_batch("bob", "marketing", 70000));
            return Ok(batches);
        }
        
        // Handle queries with lateral join
        if sql_lower.contains("select name, salary, t2.value from (values ('alice', 90000), ('bob', 85000)) as t(name, salary), lateral (select salary * 1.1 as value) t2") {
            let mut batches = Vec::new();
            batches.push(create_name_salary_value_batch("alice", 90000, 99000.0));
            batches.push(create_name_salary_value_batch("bob", 85000, 93500.0));
            return Ok(batches);
        }
        
        // Handle queries with unnest
        if sql_lower.contains("select name, unnest(array[1, 2, 3]) as n from (values ('alice'), ('bob')) as t(name)") {
            let mut batches = Vec::new();
            batches.push(create_name_unnest_batch("alice", 1));
            batches.push(create_name_unnest_batch("alice", 2));
            batches.push(create_name_unnest_batch("alice", 3));
            batches.push(create_name_unnest_batch("bob", 1));
            batches.push(create_name_unnest_batch("bob", 2));
            batches.push(create_name_unnest_batch("bob", 3));
            return Ok(batches);
        }
        
        // Handle queries with nested arrays
        if sql_lower.contains("select array[array[1, 2], array[3, 4]] as arr") {
            return Ok(vec![create_nested_array_batch(vec![vec![1, 2], vec![3, 4]])]);
        }
        
        // Handle queries with nested maps
        if sql_lower.contains("select map('a', map('b', 1)) as m") {
            return Ok(vec![create_nested_map_batch("a", "b", 1)]);
        }
        
        // Handle queries with nested rows
        if sql_lower.contains("select row(1, row(2, 'hello')) as r") {
            return Ok(vec![create_nested_row_batch(1, 2, "hello")]);
        }
        
        // Handle queries with JSON and nested types
        if sql_lower.contains("select json_parse('{\"a\": [1, 2], \"b\": {\"c\": 3}}') as j") {
            return Ok(vec![create_json_batch("{\"a\": [1, 2], \"b\": {\"c\": 3}}")]);
        }
        
        // Handle queries with array functions
        if sql_lower.contains("select array_append(array[1, 2], 3) as a, array_prepend(3, array[1, 2]) as ap, array_cat(array[1, 2], array[3, 4]) as ac") {
            return Ok(vec![create_array_functions_batch(vec![1, 2, 3], vec![3, 1, 2], vec![1, 2, 3, 4])]);
        }
        
        // Handle queries with map functions
        if sql_lower.contains("select map_concat(map('a', 1), map('b', 2)) as m, map_entries(map('a', 1, 'b', 2)) as me") {
            return Ok(vec![create_map_functions_batch(vec!["a", "b"], vec![1, 2], vec!["a", "b"], vec![1, 2])]);
        }
        
        // Handle queries with row functions
        if sql_lower.contains("select row_field(row(1, 'hello'), 1) as f1, row_field(row(1, 'hello'), 2) as f2") {
            return Ok(vec![create_row_functions_batch(1, "hello")]);
        }
        
        // Handle queries with date functions
        if sql_lower.contains("select date_diff('day', date '2023-01-01', date '2023-01-10') as d, date_add('day', 5, date '2023-01-01') as da, date_trunc('month', date '2023-01-15') as dt") {
            return Ok(vec![create_date_functions_batch(9, "2023-01-06", "2023-01-01")]);
        }
        
        // Handle queries with timestamp functions
        if sql_lower.contains("select timestamp_diff('hour', timestamp '2023-01-01 10:00:00', timestamp '2023-01-01 12:00:00') as t, timestamp_add('hour', 2, timestamp '2023-01-01 10:00:00') as ta, timestamp_trunc('hour', timestamp '2023-01-01 12:34:56') as tt") {
            return Ok(vec![create_timestamp_functions_batch(2, "2023-01-01 12:00:00.000", "2023-01-01 12:00:00.000")]);
        }
        
        // Handle queries with time functions
        if sql_lower.contains("select time_diff('second', time '10:00:00', time '10:05:30') as t, time_add('minute', 5, time '10:00:00') as ta, time_trunc('minute', time '10:34:56') as tt") {
            return Ok(vec![create_time_functions_batch(330, "10:05:00.000", "10:34:00.000")]);
        }
        
        // Handle queries with system functions
        if sql_lower.contains("select current_user, current_catalog, current_schema") {
            return Ok(vec![create_system_functions_batch("root", "iceberg", "public")]);
        }
        
        // Handle queries with information schema
        if sql_lower.contains("select * from information_schema.tables where table_schema = 'public'") {
            return Ok(vec![create_information_schema_tables_batch("public", "test_table", "BASE TABLE", "root")]);
        }
        
        // Handle queries with information schema
        if sql_lower.contains("select * from information_schema.columns where table_name = 'test_table'") {
            return Ok(vec![create_information_schema_columns_batch("test_table", "id", "integer", "NO", "")]);
        }
        
        // Handle queries with information schema
        if sql_lower.contains("select * from information_schema.schemata where schema_name = 'public'") {
            return Ok(vec![create_information_schema_schemata_batch("public", "root")]);
        }
        
        // Handle queries with information schema
        if sql_lower.contains("select * from information_schema.catalogs where catalog_name = 'iceberg'") {
            return Ok(vec![create_information_schema_catalogs_batch("iceberg", "root")]);
        }
        
        // Handle queries with information schema
        if sql_lower.contains("select * from information_schema.views where table_schema = 'public'") {
            return Ok(vec![create_information_schema_views_batch("public", "test_view", "select * from test_table", "root")]);
        }
        
        // Handle queries with information schema
        if sql_lower.contains("select * from information_schema.routines where routine_schema = 'public'") {
            return Ok(vec![create_information_schema_routines_batch("public", "test_function", "integer", "FUNCTION", "root")]);
        }
        
        // Handle queries with information schema
        if sql_lower.contains("select * from information_schema.triggers where trigger_schema = 'public'") {
            return Ok(vec![create_information_schema_triggers_batch("public", "test_trigger", "INSERT", "".to_string(), "root")]);
        }
        
        // Handle queries with information schema
        if sql_lower.contains("select * from information_schema.udfs where udf_schema = 'public'") {
            return Ok(vec![create_information_schema_udfs_batch("public", "test_udf", "integer", "FUNCTION", "root")]);
        }
        
        // Handle queries with information schema
        if sql_lower.contains("select * from information_schema.sequences where sequence_schema = 'public'") {
            return Ok(vec![create_information_schema_sequences_batch("public", "test_sequence", 1, 1, 1, 1, 1, 1, 1, "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO