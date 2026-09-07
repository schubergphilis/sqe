//! `sqe-bench` binary: a shim over the library's CLI so the binary carries no
//! private copy of any module (review 706).

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sqe_bench::cli::run().await
}
