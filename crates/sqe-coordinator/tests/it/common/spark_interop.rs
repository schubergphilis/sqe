//! The Spark 4.x interop container (`docker-compose.spark.yml`): Spark reads
//! and writes the same Polaris catalog the integration stack serves, as
//! catalog `rest`. Bring it up with `./scripts/start-spark-interop.sh`.

use std::process::Command;

/// Return `Some(reason)` when the Spark interop stack is not running and
/// the test should skip. Returns `None` when it is ready for a live run.
///
/// The probe runs `docker ps` first because the scripts rely on docker
/// being present and the sqe-spark-iceberg container being up. Without
/// docker we exit quietly; with docker but no container we print the
/// command operators should run.
pub fn spark_stack_skip_reason() -> Option<String> {
    // Env opt-out for CI systems that cannot start docker. Setting
    // SQE_SKIP_SPARK_INTEROP=1 bypasses the live probe entirely.
    if std::env::var("SQE_SKIP_SPARK_INTEROP").ok().as_deref() == Some("1") {
        return Some("SQE_SKIP_SPARK_INTEROP=1".to_string());
    }

    let docker_ok = Command::new("docker")
        .args(["version", "--format", "{{.Server.Version}}"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !docker_ok {
        return Some("docker daemon is not reachable".to_string());
    }

    let ps = Command::new("docker")
        .args(["ps", "--format", "{{.Names}}"])
        .output();
    match ps {
        Ok(o) if o.status.success() => {
            let names = String::from_utf8_lossy(&o.stdout);
            if !names.lines().any(|n| n.trim() == "sqe-spark-iceberg") {
                Some(
                    "sqe-spark-iceberg container is not running. \
                     Run ./scripts/start-spark-interop.sh first."
                        .to_string(),
                )
            } else {
                None
            }
        }
        Ok(o) => Some(format!(
            "docker ps failed: {}",
            String::from_utf8_lossy(&o.stderr)
        )),
        Err(e) => Some(format!("could not spawn docker: {e}")),
    }
}

/// Workspace root for the test binary. `CARGO_MANIFEST_DIR` points at
/// `crates/sqe-coordinator`; the scripts live two levels up.
fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

/// Execute a SQL query inside the Spark container via scripts/spark-interop.sh
/// and return stdout as a String. The script returns TSV with one row per
/// line; callers parse the expected shape.
pub fn spark_sql(sql: &str) -> std::io::Result<String> {
    let script = workspace_root().join("scripts").join("spark-interop.sh");
    let out = Command::new(script).arg(sql).output()?;
    if !out.status.success() {
        return Err(std::io::Error::other(format!(
            "spark-interop.sh failed ({:?}): stderr={}",
            out.status,
            String::from_utf8_lossy(&out.stderr),
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}
