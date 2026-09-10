//! `sqe-bench` binary: a shim over the library's CLI so the binary carries no
//! private copy of any module (review 706).

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // reqwest is `rustls-no-provider` workspace-wide (no aws-lc C build) and
    // panics at client construction without a process default provider.
    // sqe-bench does not link sqe-core's pre-main install, so do it here.
    let _ = rustls::crypto::ring::default_provider().install_default();
    sqe_bench::cli::run().await
}
