//! One integration-test binary for sqe-policy (was one per file, each linking
//! DataFusion). Add new tests as a module here, not as `tests/*.rs`.

mod policy_fingerprint;
mod ranger_grants_wiremock;
mod ranger_store_wiremock;
mod rewriter_integration;
mod rewriter_shapes;
mod view_bypass_policy;
