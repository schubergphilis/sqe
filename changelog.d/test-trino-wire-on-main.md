### Changed

- The main branch pipeline runs the Trino wire golden tests (`sqe-trino-compat-wire`)

    Main ran no test jobs, so a merge that broke the Trino HTTP protocol goldens (`crates/sqe-trino-compat/tests/wire.rs`) stayed green on main and surfaced only in later, unrelated MRs. The new job runs `cargo test -p sqe-trino-compat --test wire` on every main push that changes Rust sources, manifests, `Cargo.lock` or the toolchain, on the warm deps image. MR pipelines run it when `.gitlab-ci.yml` changes; otherwise the MR Rust test job already covers the test. Scheduled pipelines skip it.
