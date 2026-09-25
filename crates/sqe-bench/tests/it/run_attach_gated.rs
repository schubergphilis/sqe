//! End-to-end: `sqe-bench run tpch --profile local` against a live stack with a
//! pre-provisioned golden catalog. Gated behind BENCH_STACK_UP=1 because it
//! needs Polaris + RustFS + a coordinator on localhost:60051 and a golden
//! `tpch_sf<scale>` namespace. Mirrors the direct-sink gated integration test.

use std::process::Command;

#[test]
fn run_tpch_via_golden_attach() {
    if std::env::var("BENCH_STACK_UP").as_deref() != Ok("1") {
        eprintln!("skipping: set BENCH_STACK_UP=1 with a live golden stack to run");
        return;
    }
    let token = std::env::var("BENCH_GOLDEN_TOKEN").expect("BENCH_GOLDEN_TOKEN");
    let bin = env!("CARGO_BIN_EXE_sqe-bench");
    let out = Command::new(bin)
        .args(["run", "tpch", "--profile", "local", "--scale", "0.01"])
        .env("BENCH_GOLDEN_TOKEN", token)
        .output()
        .expect("run sqe-bench");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "run failed: {stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("BENCH_SUMMARY:tpch:"),
        "no summary line: {stdout}"
    );
}
