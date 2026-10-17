//! Child processes with a deadline.
//!
//! `Command::output()` waits forever, so a wedged `docker exec` or a JVM
//! that never starts hangs the whole suite. Every external command the
//! tests run goes through [`output_with_timeout`] instead.

use std::io::Read;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

/// Budget for one `spark-sql` run: a JVM start plus a Ranger policy download.
pub const SPARK_SQL_TIMEOUT: Duration = Duration::from_secs(300);
/// Budget for a short `docker` call (`version`, `ps`, an `exec` of `sh`).
pub const DOCKER_TIMEOUT: Duration = Duration::from_secs(60);
/// Budget for a `docker compose` lifecycle call (`restart`, `up`).
pub const COMPOSE_TIMEOUT: Duration = Duration::from_secs(600);
/// Budget for one client CLI statement (`trino-cli --execute`).
pub const CLI_TIMEOUT: Duration = Duration::from_secs(120);

/// [`Command::output`] with a deadline. On expiry the child is killed and
/// the result is an `ErrorKind::TimedOut` error naming the program.
///
/// Killing `docker` ends the client only; a process it started inside a
/// container may run on, but the test no longer waits for it.
pub fn output_with_timeout(cmd: &mut Command, timeout: Duration) -> std::io::Result<Output> {
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    // Drain both pipes on their own threads: a child that fills a pipe
    // nobody reads blocks, and would then always look timed out.
    let drain = |pipe: Option<Box<dyn Read + Send>>| {
        std::thread::spawn(move || {
            let mut buf = Vec::new();
            if let Some(mut p) = pipe {
                let _ = p.read_to_end(&mut buf);
            }
            buf
        })
    };
    let stdout = drain(
        child
            .stdout
            .take()
            .map(|p| Box::new(p) as Box<dyn Read + Send>),
    );
    let stderr = drain(
        child
            .stderr
            .take()
            .map(|p| Box::new(p) as Box<dyn Read + Send>),
    );

    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("{:?} did not finish within {timeout:?}", cmd.get_program()),
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    Ok(Output {
        status,
        stdout: stdout.join().unwrap_or_default(),
        stderr: stderr.join().unwrap_or_default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_the_output_of_a_quick_command() {
        let out = output_with_timeout(
            Command::new("sh").args(["-c", "echo hi; echo err >&2"]),
            DOCKER_TIMEOUT,
        )
        .expect("sh runs");
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout), "hi\n");
        assert_eq!(String::from_utf8_lossy(&out.stderr), "err\n");
    }

    #[test]
    fn kills_a_command_that_outlives_its_deadline() {
        let start = Instant::now();
        let err = output_with_timeout(Command::new("sleep").arg("30"), Duration::from_millis(300))
            .expect_err("sleep 30 must time out");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "returned at the deadline"
        );
    }
}
