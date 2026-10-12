**STATUS 2026-09-10 (d): ring is the only rustls crypto provider; aws-lc-sys (the BoringSSL C build) is out of every graph (`build/rustls-ring-provider`).**

Both providers were compiled and aws-lc was the one in use. Three roots:
the workspace `reqwest` 0.13 `rustls` feature (= `__rustls-aws-lc-rs`),
`rustls` declared with default features in sqe-core and sqe-coordinator
(`aws_lc_rs` + `prefer-post-quantum`), and `aws-config` with its default
`default-https-client`, which aws-smithy-runtime hardcodes to
`aws-smithy-http-client/rustls-aws-lc`; that came in through the vendored
REST crate's `aws-sigv4` feature and sqe-catalog's glue/s3tables. Fixes:
reqwest `rustls-no-provider`; rustls `default-features = false` +
`["ring","std","tls12","logging"]`; aws-config `default-features = false`
everywhere with an explicit ring `aws_smithy_http_client` HTTPS client
(rest `sigv4.rs`, sqe-catalog `aws_config.rs`, same shape the vendored
Glue/S3 Tables crates already used, #133). hakari regenerated; the
workspace-hack pins on aws-config, aws-smithy-http-client and
rustls-webpki `aws-lc-rs` fell away. `cargo tree -i aws-lc-sys` matches
nothing, default and `--all-features`. **The trap:** reqwest 0.13 under
`rustls-no-provider` panics at `Client` construction unless a process
default provider is installed (it does not auto-select from crate
features), and clients are built in 25 places across a dozen crates plus
tests. So sqe-core installs ring pre-main (`#[ctor::ctor]` in `tls.rs`,
new dep `ctor 0.5`), the five binaries call `ensure_crypto_provider()`
first thing, and the three client crates that do not link sqe-core
(sqe-trino-protocol, sqe-quack-client, sqe-bench) install it themselves.
Verified with the real binary: `sqe-cli --protocol http --tls` to a valid
host reaches HTTP 422 (TLS fine), to self-signed.badssl.com fails on the
certificate (verification live); sqe-auth 282, trino-protocol 19,
quack-client/server, sqe-core tls 30, coordinator tls 5 all pass; clippy
`-D warnings` clean on the eight touched crates. Trade-off accepted: no
FIPS module and no post-quantum hybrid key exchange until ring or a
pure-Rust provider (graviola) offers them; same BoringSSL lineage
otherwise. **NEXT:** measure the cold-build win in CI (aws-lc-sys was the
single slowest C build); evaluate `rustls-graviola` as a drop-in once
ring is the sole provider.
