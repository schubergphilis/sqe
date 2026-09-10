// Copyright 2026 Schuberg Philis
// SPDX-License-Identifier: Apache-2.0
//! Cost model (#461, spec 011/Req 3): [`Stats`] decodes `sqlite_stat1`
//! rows for one table into an in-memory shape, and [`estimate_scan_cost`]/
//! [`estimate_index_cost`] turn those stats into a [`PlanCost`].
//! [`load_stats`] reads every table's `sqlite_stat1` rows in one pass —
//! the CLI (`query`/`repl`) calls it once per statement, alongside its
//! existing `read_schema` call, and threads the result into
//! `codegen::select::join_access::choose_join_access` (spec 011/Req 4,
//! #461 Phase 3) so a table with no `ANALYZE` history behaves exactly
//! as it did before this module existed.
//!
//! Missing stats (no `ANALYZE` has ever run) deliberately produce a
//! conservative worst-case estimate rather than panicking or dividing by
//! zero — that's what keeps every existing stats-free optimization
//! (`009-vdbe-codegen` Requirement 16) behaviorally unaffected by this
//! module's mere existence.

use std::collections::HashMap;

use crate::btree::TableCursor;
use crate::header::DatabaseHeader;
use crate::record::{decode_record, Value};
use crate::schema::TableSchema;
use crate::vfs::PageSource;

/// Row-count and per-index `avg_eq` statistics for one table, decoded
/// from its `sqlite_stat1` rows (spec 011/Req 2's `"<rows>"` table-row
/// format and `"<rows> <avg_eq>"` index-row format). Empty (the
/// `Default`) when `ANALYZE` has never populated stats for this table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    table_rows: Option<u64>,
    /// Index name -> `(index row count, avg_eq)`.
    index_stats: HashMap<String, (u64, u64)>,
}

impl Stats {
    /// Decodes `Stats` from a table's `sqlite_stat1` rows: `(idx, stat)`
    /// pairs, `idx = None` for the table's own row-count row and
    /// `idx = Some(name)` for one of its indexes — exactly the shape
    /// `SELECT idx, stat FROM sqlite_stat1 WHERE tbl = ?` returns. A row
    /// whose `stat` text doesn't parse as the expected integer(s) is
    /// skipped rather than treated as a hard error — a hand-edited or
    /// corrupt `sqlite_stat1` degrades to "no stats for that entry",
    /// which [`estimate_scan_cost`]/[`estimate_index_cost`] already
    /// handle safely.
    pub fn from_stat1_rows(rows: impl IntoIterator<Item = (Option<String>, String)>) -> Self {
        let mut table_rows = None;
        let mut index_stats = HashMap::new();
        for (idx, stat) in rows {
            let mut parts = stat.split_whitespace();
            match idx {
                None => table_rows = parts.next().and_then(|s| s.parse().ok()),
                Some(name) => {
                    let rows = parts.next().and_then(|s| s.parse().ok());
                    let avg_eq = parts.next().and_then(|s| s.parse().ok());
                    if let (Some(rows), Some(avg_eq)) = (rows, avg_eq) {
                        index_stats.insert(name, (rows, avg_eq));
                    }
                }
            }
        }
        Stats {
            table_rows,
            index_stats,
        }
    }

    /// The table's total row count, or `None` if `ANALYZE` has never
    /// recorded one.
    pub fn table_rows(&self) -> Option<u64> {
        self.table_rows
    }

    /// `(index row count, avg_eq)` for the named index, or `None` if
    /// `ANALYZE` has never recorded stats for it.
    pub fn index_stats(&self, index_name: &str) -> Option<(u64, u64)> {
        self.index_stats.get(index_name).copied()
    }
}

/// A plan's estimated cost: rows it would touch, and (in this MVP cost
/// model) I/O treated as one page-worth of work per row — spec 011/Req 3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanCost {
    /// Estimated number of rows the plan would touch.
    pub estimated_rows: u64,
    /// Estimated I/O cost, in page-worth-of-work units (one per row).
    pub estimated_io: u64,
}

impl PlanCost {
    /// The conservative "no stats available" estimate: `estimated_rows`
    /// pinned to `u64::MAX` so a full scan under this estimate always
    /// loses to any index probe that *does* have stats, and never loses
    /// to another stats-free scan (every stats-free estimate is equally
    /// maximal, so callers comparing two of these must not treat the
    /// comparison as meaningful — see spec 011/Req 4's "no ANALYZE"
    /// scenario, which never reaches a cost comparison at all).
    const UNKNOWN: PlanCost = PlanCost {
        estimated_rows: u64::MAX,
        estimated_io: u64::MAX,
    };
}

/// Estimates the cost of a full table scan: `stats`' recorded row count,
/// or [`PlanCost::UNKNOWN`] if `ANALYZE` has never run for this table.
pub fn estimate_scan_cost(stats: &Stats) -> PlanCost {
    match stats.table_rows() {
        Some(rows) => PlanCost {
            estimated_rows: rows,
            estimated_io: rows,
        },
        None => PlanCost::UNKNOWN,
    }
}

/// The minimum leading-column `avg_eq` (average rows sharing one
/// leading-column value) for skip-scan to be considered worthwhile —
/// #485. Mirrors oracle sqlite3's documented "about 18 or more
/// duplicates" threshold (sqlite.org/optoverview.html's Skip-Scan
/// section), confirmed empirically against sqlite3 3.51.0: `avg_eq =
/// 19` on a composite index's leading column picks a skip-scan plan
/// (`SEARCH t USING INDEX idx (ANY(a) AND b=?)`), `avg_eq = 17` falls
/// back to a full scan.
const SKIP_SCAN_MIN_AVG_EQ: u64 = 18;

/// Whether a skip-scan over `index_name` (probing a non-leading column
/// while the leading column is unconstrained) is worth attempting —
/// #485. `false` whenever `ANALYZE` has never recorded stats for this
/// index, matching oracle sqlite3's behavior of never choosing
/// skip-scan without `ANALYZE` history (its own no-stats default guess
/// of 10 duplicates sits below [`SKIP_SCAN_MIN_AVG_EQ`]).
pub fn is_skip_scan_worthwhile(index_name: &str, stats: &Stats) -> bool {
    stats
        .index_stats(index_name)
        .is_some_and(|(_rows, avg_eq)| avg_eq >= SKIP_SCAN_MIN_AVG_EQ)
}

/// Estimates the cost of an equality probe against `index_name`: the
/// index's recorded `avg_eq` (average rows sharing one key value,
/// floored at 1 since even a matching probe touches at least one row),
/// or [`PlanCost::UNKNOWN`] if `ANALYZE` has never recorded stats for
/// this index.
pub fn estimate_index_cost(index_name: &str, stats: &Stats) -> PlanCost {
    match stats.index_stats(index_name) {
        Some((_rows, avg_eq)) => {
            let estimated_rows = avg_eq.max(1);
            PlanCost {
                estimated_rows,
                estimated_io: estimated_rows,
            }
        }
        None => PlanCost::UNKNOWN,
    }
}

/// #545: the smallest `ANALYZE`-recorded row count a join level's inner
/// table must clear before building a transient automatic index for an
/// otherwise-unindexed equality join column is estimated cheaper than
/// the plain nested-loop scan it replaces (sqlite.org/optoverview.html
/// #autoindex) — building the index costs one full scan of the table up
/// front, so it only pays for itself once the table is big enough that
/// repeatedly nested-loop-scanning it (once per outer row) would cost
/// more overall: a small table's full scan is already cheap, so skip
/// the extra machinery.
const MIN_ROWS_TO_AUTO_INDEX: u64 = 25;

/// Whether building a transient automatic index (#545) for `stats`'
/// table is estimated worthwhile in place of a plain nested-loop scan:
/// `true` only when `ANALYZE` has recorded at least
/// [`MIN_ROWS_TO_AUTO_INDEX`] rows — a stats-free database (no
/// `ANALYZE` has ever run) never triggers this optimization, matching
/// [`is_skip_scan_worthwhile`]'s own "no stats, no optimization"
/// default.
pub fn is_automatic_index_worthwhile(stats: &Stats) -> bool {
    stats
        .table_rows()
        .is_some_and(|rows| rows >= MIN_ROWS_TO_AUTO_INDEX)
}

/// Reads every table's `sqlite_stat1` rows in one pass and returns a
/// `table name -> Stats` map — empty if `sqlite_stat1` isn't in
/// `schemas` at all (no `ANALYZE` has ever run against this database),
/// which is exactly the "behave as before this module existed" case
/// [`estimate_scan_cost`]/[`estimate_index_cost`] already handle safely.
/// Malformed rows are skipped the same way [`Stats::from_stat1_rows`]
/// skips malformed `stat` text — a corrupt `sqlite_stat1` degrades to
/// "no stats for that entry", never a hard error.
pub fn load_stats<P: PageSource>(
    source: P,
    header: &DatabaseHeader,
    schemas: &[TableSchema],
) -> HashMap<String, Stats> {
    let Some(stat1) = schemas
        .iter()
        .find(|s| s.name.eq_ignore_ascii_case("sqlite_stat1"))
    else {
        return HashMap::new();
    };

    let mut rows_by_table: HashMap<String, Vec<(Option<String>, String)>> = HashMap::new();
    let mut cursor = TableCursor::new(source, header, stat1.root_page);
    let Ok(mut row) = cursor.first_row() else {
        return HashMap::new();
    };
    while let Some(r) = row {
        if let Ok(values) = decode_record(&r.payload, header.text_encoding) {
            let tbl = match values.first() {
                Some(Value::Text(s)) => Some(s.to_string()),
                _ => None,
            };
            if let Some(tbl) = tbl {
                let idx = match values.get(1) {
                    Some(Value::Text(s)) => Some(s.to_string()),
                    _ => None,
                };
                let stat = match values.get(2) {
                    Some(Value::Text(s)) => s.to_string(),
                    _ => String::new(),
                };
                rows_by_table.entry(tbl).or_default().push((idx, stat));
            }
        }
        row = match cursor.next_row() {
            Ok(r) => r,
            Err(_) => break,
        };
    }

    rows_by_table
        .into_iter()
        .map(|(tbl, rows)| (tbl, Stats::from_stat1_rows(rows)))
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn load_stats_is_empty_when_sqlite_stat1_does_not_exist() {
        let (vfs, header) = crate::btree::test_minimal_db(512);
        let pager = crate::pager::Pager::open(&vfs, std::path::Path::new("/test.db"), 512).unwrap();
        let schemas: Vec<TableSchema> = Vec::new();
        let stats = load_stats(&pager, &header, &schemas);
        assert!(stats.is_empty());
    }

    #[test]
    fn load_stats_decodes_rows_for_every_table() {
        let (vfs, header) = crate::btree::test_minimal_db(512);
        let mut pager =
            crate::pager::Pager::open(&vfs, std::path::Path::new("/test.db"), 512).unwrap();
        let stat1_root = crate::btree::ensure_sqlite_stat1_table(&mut pager, &header).unwrap();
        crate::btree::insert_stat1_row(&mut pager, &header, stat1_root, "t", None, "10000")
            .unwrap();
        crate::btree::insert_stat1_row(
            &mut pager,
            &header,
            stat1_root,
            "t",
            Some("idx_a"),
            "10000 10",
        )
        .unwrap();

        let schemas = vec![TableSchema {
            unresolved_autoindex: false,
            name: "sqlite_stat1".to_string(),
            root_page: stat1_root,
            columns: vec!["tbl".to_string(), "idx".to_string(), "stat".to_string()],
            column_types: vec![String::new(), String::new(), String::new()],
            column_collations: vec![],
            without_rowid: false,
            strict: false,
            is_virtual: false,
            sql: "CREATE TABLE sqlite_stat1(tbl,idx,stat)".to_string(),
            indexes: vec![],
            rowid_alias: None,
        }
        .with_computed_rowid_alias()];

        let all_stats = load_stats(&pager, &header, &schemas);
        let stats = all_stats.get("t").unwrap();
        assert_eq!(stats.table_rows(), Some(10000));
        assert_eq!(stats.index_stats("idx_a"), Some((10000, 10)));
    }

    /// spec 011/Req 3 scenario "Missing stats fall back to a conservative
    /// default".
    #[test]
    fn missing_stats_fall_back_to_max_cost() {
        let stats = Stats::default();
        let cost = estimate_scan_cost(&stats);
        assert_eq!(cost.estimated_rows, u64::MAX);
        assert_eq!(cost.estimated_io, u64::MAX);

        let idx_cost = estimate_index_cost("idx_a", &stats);
        assert_eq!(idx_cost.estimated_rows, u64::MAX);
    }

    /// spec 011/Req 3 scenario "An indexed equality is cheaper than a
    /// scan once stats exist".
    #[test]
    fn indexed_equality_cheaper_than_scan_with_stats() {
        let stats = Stats::from_stat1_rows(vec![
            (None, "10000".to_string()),
            (Some("idx_a".to_string()), "10000 10".to_string()),
        ]);

        let scan = estimate_scan_cost(&stats);
        let indexed = estimate_index_cost("idx_a", &stats);

        assert_eq!(scan.estimated_rows, 10000);
        assert_eq!(indexed.estimated_rows, 10);
        assert!(indexed.estimated_rows < scan.estimated_rows);
    }

    #[test]
    fn unknown_index_name_falls_back_to_unknown() {
        let stats = Stats::from_stat1_rows(vec![(None, "5".to_string())]);
        let cost = estimate_index_cost("no_such_index", &stats);
        assert_eq!(cost.estimated_rows, u64::MAX);
    }

    #[test]
    fn malformed_stat_text_is_skipped_not_a_hard_error() {
        let stats = Stats::from_stat1_rows(vec![(None, "not-a-number".to_string())]);
        assert_eq!(stats.table_rows(), None);
    }

    /// #485: mirrors oracle sqlite3 3.51.0's empirically-confirmed
    /// skip-scan threshold — a leading-column `avg_eq` of 19 picks
    /// skip-scan, 17 does not (`SKIP_SCAN_MIN_AVG_EQ = 18`).
    #[test]
    fn skip_scan_worthwhile_matches_oracle_threshold() {
        let above = Stats::from_stat1_rows(vec![(Some("idx".to_string()), "20001 19".to_string())]);
        assert!(is_skip_scan_worthwhile("idx", &above));

        let below = Stats::from_stat1_rows(vec![(Some("idx".to_string()), "20001 17".to_string())]);
        assert!(!is_skip_scan_worthwhile("idx", &below));

        let at_threshold =
            Stats::from_stat1_rows(vec![(Some("idx".to_string()), "20001 18".to_string())]);
        assert!(is_skip_scan_worthwhile("idx", &at_threshold));
    }

    /// #485: without `ANALYZE` having ever recorded stats for the
    /// index, skip-scan is never chosen — matches oracle's behavior of
    /// never picking skip-scan absent `ANALYZE` history.
    #[test]
    fn skip_scan_never_worthwhile_without_analyze_stats() {
        let stats = Stats::default();
        assert!(!is_skip_scan_worthwhile("idx", &stats));
    }

    /// #545: a table below the row-count threshold isn't worth building
    /// a transient automatic index for.
    #[test]
    fn automatic_index_not_worthwhile_below_threshold() {
        let stats = Stats::from_stat1_rows(vec![(None, "24".to_string())]);
        assert!(!is_automatic_index_worthwhile(&stats));
    }

    /// #545: at/above the row-count threshold, a transient automatic
    /// index is judged worthwhile.
    #[test]
    fn automatic_index_worthwhile_at_and_above_threshold() {
        let stats = Stats::from_stat1_rows(vec![(None, "25".to_string())]);
        assert!(is_automatic_index_worthwhile(&stats));

        let stats = Stats::from_stat1_rows(vec![(None, "10000".to_string())]);
        assert!(is_automatic_index_worthwhile(&stats));
    }

    /// #545: without `ANALYZE` having ever recorded a row count, the
    /// automatic index is never chosen — same "no stats, no
    /// optimization" default as [`is_skip_scan_worthwhile`].
    #[test]
    fn automatic_index_never_worthwhile_without_analyze_stats() {
        let stats = Stats::default();
        assert!(!is_automatic_index_worthwhile(&stats));
    }
}
