//! End-to-end: `sqe-bench run tpch --profile local --compare-trino` against a
//! live stack with a pre-provisioned golden catalog AND a running compare
//! Trino. This is the SP1 single-pass compare coverage: it exercises the
//! `catalog=None` + Trino branch of `run::run` (see `run.rs`'s catalog
//! resolution comment) that `run_attach_gated.rs` does not reach, and asserts
//! that both the report artifact (`BENCH_SUMMARY:`) and the compare artifact
//! (`compare tpch:`) come from the same invocation -- i.e. one sweep produces
//! both outputs, per spec §6. Gated behind BENCH_STACK_UP=1 because it needs
//! Polaris + RustFS + a coordinator on localhost:60051, a golden
//! `tpch_sf<scale>` namespace, and a compare Trino reachable via
//! BENCH_TRINO_ENDPOINT.

use std::process::Command;

#[test]
fn run_tpch_compare_via_golden_attach() {
    if std::env::var("BENCH_STACK_UP").as_deref() != Ok("1") {
        eprintln!("skipping: set BENCH_STACK_UP=1 with a live golden stack + compare Trino to run");
        return;
    }
    let token = match std::env::var("BENCH_GOLDEN_TOKEN") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("skipping: BENCH_GOLDEN_TOKEN not set");
            return;
        }
    };
    let trino_endpoint = match std::env::var("BENCH_TRINO_ENDPOINT") {
        Ok(t) => t,
        Err(_) => {
            eprintln!("skipping: BENCH_TRINO_ENDPOINT not set (no compare Trino to run against)");
            return;
        }
    };

    let bin = env!("CARGO_BIN_EXE_sqe-bench");
    let out = Command::new(bin)
        .args([
            "run",
            "tpch",
            "--profile",
            "local",
            "--scale",
            "0.01",
            "--compare-trino",
        ])
        .env("BENCH_GOLDEN_TOKEN", token)
        .env("BENCH_TRINO_ENDPOINT", trino_endpoint)
        .output()
        .expect("run sqe-bench");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "run failed: {stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("BENCH_SUMMARY:tpch:"), "no summary line: {stdout}");
    assert!(stdout.contains("compare tpch:"), "no compare line: {stdout}");
}
