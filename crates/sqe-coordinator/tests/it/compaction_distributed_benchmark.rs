//! Informational benchmark: distributed rewrite wall-clock vs coordinator-
//! local (Phase 4c Task 6, Step 2).
//!
//! Times `CALL system.rewrite_data_files(..., distributed => 'require')`
//! against `distributed => 'local'` on two identically-sized tables and
//! prints the ratio. Target from the task brief: distributed on 4 workers
//! should beat coordinator-local by more than 2.5x on SF10-scale data. This
//! is **informational only** -- the ratio is printed and, if below target,
//! a warning is printed alongside it, but nothing here fails the test (and
//! by extension never gates CI) on the ratio itself. The only hard
//! assertions are correctness guardrails (row count preserved).
//!
//! ## Honesty about scale
//!
//! Generating real SF10-scale data (tens of millions of rows, GB-scale
//! files) inside a test binary is not something this environment can do
//! (no multi-worker stack to run it against either -- see below). The
//! default run here seeds a small synthetic fixture (`SQE_IT_BENCH_FILES`
//! small single-row files, default 200) purely to exercise the wiring: it
//! proves the timing harness and the distributed-vs-local CALL sequence
//! work, and will typically show a ratio well BELOW 2.5x (at this scale,
//! per-group dispatch/network overhead dominates over the trivial compute
//! each group does -- there is nothing to parallelize away). Treat the
//! default run's ratio as a smoke signal, not the benchmark result the task
//! brief asks for.
//!
//! For a real measurement, load an actual SF10-scale table (e.g. via
//! `scripts/benchmark-test.sh` / `sqe-bench`, following the conventions
//! `compaction_benchmark.rs` documents) into TWO identically-shaped tables
//! -- one for each timing run, since the local run mutates its table -- and
//! point this test at them:
//!
//! ```text
//! SQE_IT_BENCH_DIST_TABLE=default.sf10_rewrite_bench_distributed \
//! SQE_IT_BENCH_LOCAL_TABLE=default.sf10_rewrite_bench_local \
//! SQE_IT_WORKER_URLS=http://localhost:50052,http://localhost:50053,http://localhost:50054,http://localhost:50055 \
//!   cargo test -p sqe-coordinator --test it -- --ignored \
//!   distributed_rewrite_wall_clock_ratio --nocapture
//! ```
//!
//! ## Running this test (default synthetic wiring-smoke mode)
//!
//! Needs Polaris + RustFS plus N live `sqe-worker` processes (default 4, at
//! `:50052`-`:50055`; override count/ports via `SQE_IT_WORKER_URLS`) -- same
//! native-worker setup as `rewrite_data_files_distributed_parity.rs`
//! documents, just more of them:
//!
//! ```text
//! docker compose -f docker-compose.test.yml up -d
//! ./scripts/bootstrap-test.sh
//!
//! cargo run -p sqe-worker -- tests/sqe-test.toml &
//! SQE_WORKER__FLIGHT_PORT=50053 cargo run -p sqe-worker -- tests/sqe-test.toml &
//! SQE_WORKER__FLIGHT_PORT=50054 cargo run -p sqe-worker -- tests/sqe-test.toml &
//! SQE_WORKER__FLIGHT_PORT=50055 cargo run -p sqe-worker -- tests/sqe-test.toml &
//!
//! cargo test -p sqe-coordinator --test it -- --ignored \
//!   distributed_rewrite_wall_clock_ratio --nocapture
//! ```

use std::time::Instant;

use arrow_array::{Array, Int64Array};

fn worker_urls() -> Vec<String> {
    std::env::var("SQE_IT_WORKER_URLS")
        .unwrap_or_else(|_| {
            "http://localhost:50052,http://localhost:50053,\
             http://localhost:50054,http://localhost:50055"
                .to_string()
        })
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Split a `namespace.table` reference into its two parts, as the metadata
/// TVFs (`table_files`, `table_snapshots`) require separate arguments.
fn split_ns_table(table: &str) -> (String, String) {
    let (ns, name) = table
        .rsplit_once('.')
        .unwrap_or_else(|| panic!("expected 'namespace.table', got '{table}'"));
    (ns.to_string(), name.to_string())
}

async fn exec(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    sql: &str,
) -> Vec<arrow_array::RecordBatch> {
    handler
        .execute(session, sql, None)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}: {e}"))
}

async fn count_rows(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> i64 {
    let b = exec(handler, session, &format!("SELECT COUNT(*) FROM {table}")).await;
    b[0].column(0).as_any().downcast_ref::<Int64Array>().expect("Int64Array").value(0)
}

async fn max_file_size_bytes(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> i64 {
    let (ns, name) = split_ns_table(table);
    let b = exec(
        handler,
        session,
        &format!("SELECT MAX(file_size_in_bytes) FROM table_files('{ns}', '{name}')"),
    )
    .await;
    b[0].column(0).as_any().downcast_ref::<Int64Array>().expect("Int64Array").value(0)
}

async fn seed_synthetic_fixture(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
    files: i64,
    rows_per_file: i64,
) {
    let _ = exec(handler, session, &format!("DROP TABLE IF EXISTS {table}")).await;
    exec(
        handler,
        session,
        &format!("CREATE TABLE {table} (id BIGINT, category BIGINT, val BIGINT)"),
    )
    .await;
    for f in 0..files {
        let values: Vec<String> = (0..rows_per_file)
            .map(|r| {
                let id = f * rows_per_file + r;
                let category = (id * 7 + 3) % 20;
                format!("({id}, {category}, {})", id * 2)
            })
            .collect();
        exec(
            handler,
            session,
            &format!("INSERT INTO {table} VALUES {}", values.join(", ")),
        )
        .await;
    }
}

async fn run_rewrite_timed(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
    target_file_size_bytes: i64,
    distributed: &str,
) -> std::time::Duration {
    let start = Instant::now();
    let summary = exec(
        handler,
        session,
        &format!(
            "CALL system.rewrite_data_files(table => '{table}', min_input_files => 2, \
             target_file_size_bytes => {target_file_size_bytes}, distributed => '{distributed}')"
        ),
    )
    .await;
    let elapsed = start.elapsed();
    let status = summary[0]
        .column_by_name("status")
        .expect("status column")
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("status StringArray")
        .value(0)
        .to_string();
    assert!(
        !status.contains("skipped"),
        "distributed='{distributed}' benchmark rewrite of {table} must run, not skip; got '{status}'"
    );
    elapsed
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "informational benchmark; not a CI gate; see module doc for real-scale usage"]
async fn distributed_rewrite_wall_clock_ratio() {
    let workers = worker_urls();
    let (session, handler) = crate::common::setup_handler_with_workers(&workers).await;

    let preloaded = match (
        std::env::var("SQE_IT_BENCH_DIST_TABLE"),
        std::env::var("SQE_IT_BENCH_LOCAL_TABLE"),
    ) {
        (Ok(d), Ok(l)) => Some((d, l)),
        _ => None,
    };

    let (dist_table, local_table, synthetic) = match preloaded {
        Some((d, l)) => (d, l, false),
        None => {
            let files: i64 = std::env::var("SQE_IT_BENCH_FILES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(200);
            let rows_per_file: i64 = std::env::var("SQE_IT_BENCH_ROWS_PER_FILE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(500);

            let dist_table = "default.compaction_bench_wallclock_distributed".to_string();
            let local_table = "default.compaction_bench_wallclock_local".to_string();
            eprintln!(
                "\n[compaction benchmark] no SQE_IT_BENCH_DIST_TABLE/SQE_IT_BENCH_LOCAL_TABLE \
                 set -- seeding a synthetic {files}-file x {rows_per_file}-row fixture. This is \
                 a wiring smoke, NOT the SF10-scale measurement the task brief targets; see this \
                 file's module doc for how to point it at a real SF10 table pair.\n"
            );
            seed_synthetic_fixture(&handler, &session, &dist_table, files, rows_per_file).await;
            seed_synthetic_fixture(&handler, &session, &local_table, files, rows_per_file).await;
            (dist_table, local_table, true)
        }
    };

    let before_rows_dist = count_rows(&handler, &session, &dist_table).await;
    let before_rows_local = count_rows(&handler, &session, &local_table).await;

    let explicit_target_bytes: Option<i64> = std::env::var("SQE_IT_BENCH_TARGET_FILE_SIZE_BYTES")
        .ok()
        .and_then(|v| v.parse().ok());
    let target_bytes: i64 = match explicit_target_bytes {
        Some(v) => v,
        None => {
            // Force multiple bin-pack groups (>= 2x the worker count) rather
            // than letting everything collapse into one group that only one
            // worker ever sees. See rewrite_data_files_distributed_parity.rs
            // for why 2.5x the largest observed file achieves that.
            let max_size = max_file_size_bytes(&handler, &session, &dist_table).await;
            (max_size * 5) / 2
        }
    };

    let local_elapsed =
        run_rewrite_timed(&handler, &session, &local_table, target_bytes, "local").await;
    let dist_elapsed =
        run_rewrite_timed(&handler, &session, &dist_table, target_bytes, "require").await;

    let after_rows_dist = count_rows(&handler, &session, &dist_table).await;
    let after_rows_local = count_rows(&handler, &session, &local_table).await;
    assert_eq!(
        before_rows_dist, after_rows_dist,
        "distributed rewrite must not change row count"
    );
    assert_eq!(
        before_rows_local, after_rows_local,
        "local rewrite must not change row count"
    );

    let ratio = local_elapsed.as_secs_f64() / dist_elapsed.as_secs_f64().max(1e-6);
    const TARGET_RATIO: f64 = 2.5;

    eprintln!("\n=== distributed rewrite wall-clock benchmark (informational) ===");
    eprintln!("mode                : {}", if synthetic { "synthetic (wiring smoke)" } else { "pre-loaded tables" });
    eprintln!("workers registered  : {}", workers.len());
    eprintln!("dist table          : {dist_table}");
    eprintln!("local table         : {local_table}");
    eprintln!("target_file_size_bytes: {target_bytes}");
    eprintln!("local wall-clock    : {local_elapsed:?}");
    eprintln!("distributed wall-clock: {dist_elapsed:?}");
    eprintln!("ratio (local/distributed): {ratio:.2}x (target: >{TARGET_RATIO}x)");
    if synthetic {
        eprintln!(
            "NOTE: synthetic mode -- this ratio is not the SF10-scale measurement the task \
             brief targets; see the module doc for how to run against real data."
        );
    } else if ratio < TARGET_RATIO {
        eprintln!(
            "WARNING: ratio {ratio:.2}x is below the {TARGET_RATIO}x target. Informational \
             only -- this does not fail the test or gate CI."
        );
    }
    eprintln!("===============================================================\n");

    // Only clean up the synthetic fixture we seeded ourselves. A pre-loaded
    // real-scale table pair (SQE_IT_BENCH_DIST_TABLE/SQE_IT_BENCH_LOCAL_TABLE)
    // is the caller's, expensive to reload, and left alone -- the local run
    // already consolidated it in place, same as any other rewrite.
    if synthetic {
        let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {dist_table}")).await;
        let _ = exec(&handler, &session, &format!("DROP TABLE IF EXISTS {local_table}")).await;
    }
}
