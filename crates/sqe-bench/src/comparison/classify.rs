//! Comparison classification helpers: canonical-row manifest lookup and
//! status classification. Pure logic, split out of `comparison/mod.rs` so it
//! is easy to unit-test in isolation.

use crate::report::CompareStatusReport;
use std::collections::HashMap;
use tracing::warn;

/// Default location (relative to repo root) of the canonical-row manifest.
const DEFAULT_EXPECTED_ROWS_PATH: &str = "benchmarks/expected/canonical_rows_duckdb.json";

/// Canonical-row manifest: { benchmark -> { query_name -> { "sf{N}_official_rows": count } } }.
pub(crate) type ExpectedRows = HashMap<String, HashMap<String, HashMap<String, i64>>>;

/// Load the canonical-row manifest. Path comes from `BENCH_EXPECTED_ROWS`, or
/// defaults to `benchmarks/expected/canonical_rows_duckdb.json`. Returns `None`
/// if the file is absent (existing runs keep working) or unparseable (with a
/// warning), so the assertion gracefully no-ops.
pub(crate) fn load_expected_rows() -> Option<ExpectedRows> {
    let path = std::env::var("BENCH_EXPECTED_ROWS")
        .unwrap_or_else(|_| DEFAULT_EXPECTED_ROWS_PATH.to_string());
    if !std::path::Path::new(&path).exists() {
        return None;
    }
    match std::fs::read_to_string(&path) {
        Ok(s) => match serde_json::from_str::<ExpectedRows>(&s) {
            Ok(m) => Some(m),
            Err(e) => {
                warn!("Could not parse expected-rows manifest {}: {}", path, e);
                None
            }
        },
        Err(e) => {
            warn!("Could not read expected-rows manifest {}: {}", path, e);
            None
        }
    }
}

/// Look up the canonical row count for `(benchmark, query_name)` at `scale`.
/// Returns `None` when there is no manifest, no entry, or no count for this
/// scale (e.g. SF10, which is not in the manifest yet). The scale key is built
/// from `format_scale` so it is forward-compatible: scale 1.0 -> `sf1_official_rows`.
pub(crate) fn canonical_rows(
    manifest: Option<&ExpectedRows>,
    benchmark: &str,
    query_name: &str,
    scale: f64,
) -> Option<i64> {
    let key = format!("sf{}_official_rows", crate::format_scale(scale));
    manifest?
        .get(benchmark)?
        .get(query_name)?
        .get(&key)
        .copied()
}

/// Classify a single query comparison. Pure: no I/O, so it is unit-testable.
/// `canonical` is the manifest-declared row count for this query/scale, or
/// `None` when unknown. The vacuous (0-rows-on-both) arm splits on `canonical`:
/// unknown -> `Vacuous`, `Some(0)` -> `ExpectedEmpty` (pass), `Some(n>0)` ->
/// `VacuousBug` (fail). All other arms are unchanged.
pub(crate) fn classify_status(
    sqe_error: &Option<String>,
    trino_error: &Option<String>,
    sqe_rows: usize,
    trino_rows: usize,
    canonical: Option<i64>,
) -> CompareStatusReport {
    let rows_match = sqe_error.is_none() && trino_error.is_none() && sqe_rows == trino_rows;
    match (sqe_error, trino_error) {
        (None, None) if rows_match && sqe_rows == 0 => match canonical {
            Some(0) => CompareStatusReport::ExpectedEmpty,
            Some(_) => CompareStatusReport::VacuousBug,
            None => CompareStatusReport::Vacuous,
        },
        (None, None) if rows_match => CompareStatusReport::Match,
        // Row counts differ but both engines succeeded. When the manifest
        // declares a canonical count and SQE matches it while Trino does not,
        // Trino is the outlier on a SQL dialect difference (e.g. regexp_replace
        // backreference syntax). SQE is correct -> pass.
        (None, None) if matches!(canonical, Some(c) if sqe_rows as i64 == c && trino_rows as i64 != c) => {
            CompareStatusReport::DialectDiff
        }
        (None, None) => CompareStatusReport::RowDiff,
        (Some(_), None) => CompareStatusReport::SqeFailed,
        (None, Some(_)) => CompareStatusReport::TrinoFailed,
        (Some(_), Some(_)) => CompareStatusReport::BothFailed,
    }
}

/// Detect a DML statement (UPDATE / DELETE / INSERT / MERGE). Comparison mode
/// skips these: running the same DML against both engines would mutate the one
/// shared table and corrupt the data. DML correctness is verified by the
/// regular sqe-bench test, not by compare. Moved verbatim from
/// `run_comparison`'s inline block so the single-pass `suite::run_suite` driver
/// and `run_comparison` share one detector.
pub(crate) fn is_dml(sql: &str) -> bool {
    let sql_upper = sql.trim().to_uppercase();
    let is_dml = sql_upper.starts_with("UPDATE ")
        || sql_upper.starts_with("DELETE ")
        || sql_upper.starts_with("INSERT ")
        || sql_upper.starts_with("MERGE ");
    // Also check after stripping comments (-- name: ...)
    let first_stmt = sql
        .lines()
        .find(|l| !l.trim().starts_with("--") && !l.trim().is_empty())
        .unwrap_or("")
        .trim()
        .to_uppercase();
    is_dml
        || first_stmt.starts_with("UPDATE ")
        || first_stmt.starts_with("DELETE ")
        || first_stmt.starts_with("INSERT ")
        || first_stmt.starts_with("MERGE ")
}

/// Connection/transport-level failure, as opposed to a query-level error.
/// These are safe to retry once: the query never reached execution, or the
/// stream died for reasons unrelated to the SQL.
pub(crate) fn is_transport_error(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    m.contains("h2 protocol error")
        || m.contains("transport error")
        || m.contains("connection reset")
        || m.contains("connection refused")
        || m.contains("broken pipe")
        || m.contains("goaway")
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn none() -> Option<String> {
        None
    }
    fn err() -> Option<String> {
        Some("boom".to_string())
    }

    #[test]
    fn vacuous_with_canonical_zero_is_expected_empty() {
        let s = classify_status(&none(), &none(), 0, 0, Some(0));
        assert!(matches!(s, CompareStatusReport::ExpectedEmpty));
    }

    #[test]
    fn vacuous_with_canonical_nonzero_is_vacuous_bug() {
        let s = classify_status(&none(), &none(), 0, 0, Some(5));
        assert!(matches!(s, CompareStatusReport::VacuousBug));
    }

    #[test]
    fn vacuous_without_manifest_entry_stays_vacuous() {
        let s = classify_status(&none(), &none(), 0, 0, None);
        assert!(matches!(s, CompareStatusReport::Vacuous));
    }

    #[test]
    fn non_vacuous_unaffected_by_canonical() {
        // Rows on both, matching => Match regardless of canonical.
        assert!(matches!(
            classify_status(&none(), &none(), 5, 5, Some(0)),
            CompareStatusReport::Match
        ));
        // Row count differs => RowDiff.
        assert!(matches!(
            classify_status(&none(), &none(), 5, 7, Some(0)),
            CompareStatusReport::RowDiff
        ));
        // Engine failures unchanged.
        assert!(matches!(
            classify_status(&err(), &none(), 0, 0, Some(0)),
            CompareStatusReport::SqeFailed
        ));
        assert!(matches!(
            classify_status(&none(), &err(), 0, 0, Some(7)),
            CompareStatusReport::TrinoFailed
        ));
        assert!(matches!(
            classify_status(&err(), &err(), 0, 0, None),
            CompareStatusReport::BothFailed
        ));
    }

    #[test]
    fn rowdiff_with_sqe_matching_canonical_is_dialect_diff() {
        // SQE 6 rows == canonical 6, Trino 1 row diverges => SQE is correct on a
        // dialect difference (clickbench q28 regexp_replace backreference).
        let s = classify_status(&none(), &none(), 6, 1, Some(6));
        assert!(matches!(s, CompareStatusReport::DialectDiff));
    }

    #[test]
    fn rowdiff_when_neither_engine_matches_canonical_stays_rowdiff() {
        // Canonical present but SQE also wrong => genuine RowDiff, not a pass.
        let s = classify_status(&none(), &none(), 5, 1, Some(6));
        assert!(matches!(s, CompareStatusReport::RowDiff));
    }

    #[test]
    fn rowdiff_without_canonical_stays_rowdiff() {
        let s = classify_status(&none(), &none(), 6, 1, None);
        assert!(matches!(s, CompareStatusReport::RowDiff));
    }

    #[test]
    fn canonical_rows_builds_scale_key_and_gates() {
        let mut manifest: ExpectedRows = HashMap::new();
        let mut tpcds = HashMap::new();
        let mut q17 = HashMap::new();
        q17.insert("sf1_official_rows".to_string(), 0i64);
        tpcds.insert("q17".to_string(), q17);
        manifest.insert("tpcds".to_string(), tpcds);

        // sf1 entry exists -> Some(0).
        assert_eq!(
            canonical_rows(Some(&manifest), "tpcds", "q17", 1.0),
            Some(0)
        );
        // sf10 has no key in the manifest -> None (treated as today).
        assert_eq!(canonical_rows(Some(&manifest), "tpcds", "q17", 10.0), None);
        // Unknown query -> None.
        assert_eq!(canonical_rows(Some(&manifest), "tpcds", "q99", 1.0), None);
        // No manifest at all -> None.
        assert_eq!(canonical_rows(None, "tpcds", "q17", 1.0), None);
    }

    #[test]
    fn clickbench_q28_sf10_resolves_to_dialect_diff_end_to_end() {
        // Locks the full seam: scale 10.0 -> key "sf10_official_rows" -> Some(6),
        // then classify a 6-vs-1 row diff as DialectDiff. Mirrors the shipped
        // manifest entry; guards against format_scale drift making the fix inert.
        let mut manifest: ExpectedRows = HashMap::new();
        let mut clickbench = HashMap::new();
        let mut q28 = HashMap::new();
        q28.insert("sf10_official_rows".to_string(), 6i64);
        clickbench.insert("q28".to_string(), q28);
        manifest.insert("clickbench".to_string(), clickbench);

        let canonical = canonical_rows(Some(&manifest), "clickbench", "q28", 10.0);
        assert_eq!(canonical, Some(6), "scale 10.0 must build the sf10 key");

        let status = classify_status(&none(), &none(), 6, 1, canonical);
        assert!(matches!(status, CompareStatusReport::DialectDiff));
    }
}
