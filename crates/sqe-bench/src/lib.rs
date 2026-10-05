//! sqe-bench library surface.
//!
//! Every module is declared here exactly once (review 706). `src/main.rs` is a
//! one-line shim over [`cli::run`], so the binary and the integration tests
//! under `tests/` share a single compiled copy of the generators, the
//! comparison verdict logic, the loader and the clients.

pub mod cli;
pub mod client;
pub mod comparison;
pub mod execute;
pub mod generate;
pub mod layout;
pub mod load;
pub mod profile;
pub mod query;
pub mod report;
pub mod run;
pub mod sink;
pub mod status;
pub mod suite;

/// Format a scale factor as an identifier-safe string (no dots).
/// 0.01 → "0_01", 1.0 → "1", 10.0 → "10"
pub fn format_scale(scale: f64) -> String {
    if scale == scale.floor() {
        format!("{}", scale as u64)
    } else {
        format!("{scale}").replace('.', "_")
    }
}

/// Build the namespace name for a benchmark at a given scale factor.
pub fn bench_namespace(benchmark: &str, scale: f64) -> String {
    format!("{}_sf{}", benchmark, format_scale(scale))
}

/// The namespace a suite's tables live in: an explicit override wins; TPC-BB
/// reads the TPC-DS namespace (its two extra tables are loaded there).
pub fn suite_namespace(benchmark: &str, scale: f64, namespace_override: Option<&str>) -> String {
    match namespace_override {
        Some(ns) => ns.to_string(),
        None if benchmark == "tpcbb" => bench_namespace("tpcds", scale),
        None => bench_namespace(benchmark, scale),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_formatting_is_identifier_safe() {
        assert_eq!(format_scale(1.0), "1");
        assert_eq!(format_scale(10.0), "10");
        assert_eq!(format_scale(0.01), "0_01");
        assert_eq!(bench_namespace("tpch", 1.0), "tpch_sf1");
    }

    #[test]
    fn suite_namespace_override_and_tpcbb_alias() {
        assert_eq!(suite_namespace("tpch", 1.0, None), "tpch_sf1");
        assert_eq!(suite_namespace("tpcbb", 1.0, None), "tpcds_sf1");
        assert_eq!(suite_namespace("tpch", 1.0, Some("golden")), "golden");
    }
}
