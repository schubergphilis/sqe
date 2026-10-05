//! Physical layout of each benchmark table: partitioning, sort-on-write
//! clustering and bloom-filter columns.
//!
//! One definition for both ways data reaches Iceberg. `sqe-bench load` turns
//! it into the CTAS clauses; the direct sink (`generate --sink iceberg`)
//! checks it before writing and refuses a table whose layout it cannot yet
//! reproduce, so the two paths cannot drift into tables that benchmark
//! differently.

/// The layout `load` gives one benchmark table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableLayout {
    /// Iceberg partition transform expression for `PARTITIONED BY`.
    pub partition: Option<&'static str>,
    /// Sort-on-write key, see [`clustering_key`]. Ignored for a partitioned
    /// table; use [`TableLayout::sort_key`].
    pub clustering: Option<&'static str>,
    /// Explicit bloom-filter columns, written only under `--bloom-filter`.
    pub bloom_columns: &'static [&'static str],
}

impl TableLayout {
    /// The key the table is actually sorted on. A partitioned table skips
    /// the sort: the partition transform already clusters it, and a sort
    /// fanned out per partition cannot spill (see `load_benchmark`).
    pub fn sort_key(&self) -> Option<&'static str> {
        self.clustering.filter(|_| self.partition.is_none())
    }
}

pub fn table_layout(benchmark: &str, table: &str) -> TableLayout {
    TableLayout {
        partition: partition_spec(benchmark, table),
        clustering: clustering_key(benchmark, table),
        bloom_columns: bloom_columns(benchmark, table),
    }
}

/// Sort-on-write clustering key for a fact table.
///
/// Loads sort fact tables by their dominant date/range key so each Parquet
/// row group gets a tight min/max on that column. Most analytical queries
/// filter or range on the date key (directly or via a dim join whose dynamic
/// filter pushes a contiguous date-key set to the fact scan), so tight zone
/// maps let the reader prune whole row groups instead of decoding them
/// (`files_pruned_minmax` was 0 on unsorted loads). Dimensions are small and
/// left unsorted. Cross-engine fair: Trino reads the same sorted files.
fn clustering_key(benchmark: &str, table: &str) -> Option<&'static str> {
    match (benchmark, table) {
        ("tpch", "lineitem") => Some("l_shipdate"),
        ("tpch", "orders") => Some("o_orderdate"),
        ("ssb", "lineorder") => Some("lo_orderdate"),
        ("tpcds" | "tpcbb", "store_sales") => Some("ss_sold_date_sk"),
        ("tpcds" | "tpcbb", "catalog_sales") => Some("cs_sold_date_sk"),
        ("tpcds" | "tpcbb", "web_sales") => Some("ws_sold_date_sk"),
        ("tpcds" | "tpcbb", "inventory") => Some("inv_date_sk"),
        ("tpcds" | "tpcbb", "store_returns") => Some("sr_returned_date_sk"),
        ("tpcds" | "tpcbb", "catalog_returns") => Some("cr_returned_date_sk"),
        ("tpcds" | "tpcbb", "web_returns") => Some("wr_returned_date_sk"),
        _ => None,
    }
}

/// Iceberg partition spec (transform expression) for a fact table's `PARTITIONED
/// BY` clause, or `None` to leave it unpartitioned. Partitioning enables
/// manifest-level file pruning (stronger than row-group min/max). Only
/// date-typed columns get `month()`; integer surrogate date keys (TPC-DS/SSB)
/// need bucket/truncate and are left for a later pass.
fn partition_spec(benchmark: &str, table: &str) -> Option<&'static str> {
    match (benchmark, table) {
        ("tpch", "lineitem") => Some("month(l_shipdate)"),
        ("tpch", "orders") => Some("month(o_orderdate)"),
        // TPC-C: partition by the warehouse[/district] key that the OLTP-style
        // UPDATE/DELETE statements filter on (`w_id`, `d_id`). With partition-
        // aware Copy-on-Write (issue #263), a `WHERE w_id=1 AND d_id=1` write
        // rewrites only that one partition instead of the whole table -- correct
        // and bounded, without Merge-on-Read. `item` is warehouse-independent
        // (global) so it stays unpartitioned.
        ("tpcc", "warehouse") => Some("w_id"),
        ("tpcc", "stock") => Some("s_w_id"),
        ("tpcc", "district") => Some("d_w_id, d_id"),
        ("tpcc", "customer") => Some("c_w_id, c_d_id"),
        ("tpcc", "orders") => Some("o_w_id, o_d_id"),
        ("tpcc", "new_order") => Some("no_w_id, no_d_id"),
        ("tpcc", "order_line") => Some("ol_w_id, ol_d_id"),
        ("tpcc", "hist") => Some("h_w_id, h_d_id"),
        _ => None,
    }
}

/// Join-key columns to bloom-filter for a benchmark table, or `&[]` for none.
///
/// Set only when the caller passes `--bloom-filter`. The names become the
/// Iceberg table property `write.parquet.bloom-filter-columns` on the load
/// CTAS, so SQE's Iceberg writer emits per-row-group bloom footers on the
/// *queried* data files (not the staging Parquet, which the CTAS reads whole
/// and discards). DataFusion's `parquet.bloom_filter_on_read` (default true)
/// then consults those footers to skip row groups on literal-equality probes.
///
/// TPC-H/SSB join keys use the `_key` suffix, which the engine's default
/// `write.parquet.bloom-filter-auto` (it targets `_sk` surrogate keys) does
/// not catch, so without this list those suites get no blooms. TPC-DS `_sk`
/// keys already auto-bloom and are intentionally omitted here. Keyed by
/// `(benchmark, table)` because table names collide across suites
/// (`orders`/`customer` exist in both TPC-H and TPC-C with different keys).
fn bloom_columns(benchmark: &str, table: &str) -> &'static [&'static str] {
    match (benchmark, table) {
        // TPC-H
        ("tpch", "lineitem") => &["l_orderkey", "l_partkey", "l_suppkey"],
        ("tpch", "orders") => &["o_orderkey", "o_custkey"],
        ("tpch", "customer") => &["c_custkey", "c_nationkey"],
        ("tpch", "supplier") => &["s_suppkey", "s_nationkey"],
        ("tpch", "part") => &["p_partkey"],
        ("tpch", "partsupp") => &["ps_partkey", "ps_suppkey"],
        ("tpch", "nation") => &["n_nationkey", "n_regionkey"],
        ("tpch", "region") => &["r_regionkey"],
        // SSB
        ("ssb", "lineorder") => &["lo_custkey", "lo_partkey", "lo_suppkey", "lo_orderdate"],
        ("ssb", "dim_date") => &["d_datekey"],
        // Other suites: no explicit list. TPC-DS `_sk` keys auto-bloom via
        // the engine default; other suites are out of scope for now.
        _ => &[],
    }
}

/// Columns the engine's Iceberg writer blooms with no table property set:
/// integer columns named `<prefix>_sk` (`auto_detect_fk_columns` in
/// `sqe-compaction`'s `parquet_writer_config`, at its 1% default FPP). The
/// direct sink applies the same rule so both paths write the same footers.
pub fn engine_auto_bloom_columns(schema: &arrow_schema::Schema) -> Vec<String> {
    use arrow_schema::DataType;
    schema
        .fields()
        .iter()
        .filter(|f| f.name().len() > 3 && f.name().ends_with("_sk"))
        .filter(|f| matches!(f.data_type(), DataType::Int32 | DataType::Int64))
        .map(|f| f.name().clone())
        .collect()
}

/// False-positive rate the engine writes auto blooms at.
pub const ENGINE_BLOOM_FPP: f64 = 0.01;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::get_generator;

    #[test]
    fn bloom_columns_cover_tpch_and_ssb_join_keys() {
        assert_eq!(
            bloom_columns("tpch", "lineitem"),
            &["l_orderkey", "l_partkey", "l_suppkey"]
        );
        assert_eq!(
            bloom_columns("ssb", "lineorder"),
            &["lo_custkey", "lo_partkey", "lo_suppkey", "lo_orderdate"]
        );
        assert_eq!(bloom_columns("tpch", "region"), &["r_regionkey"]);
    }

    #[test]
    fn bloom_columns_are_suite_scoped_not_table_name_scoped() {
        // `orders` and `customer` exist in both TPC-H and TPC-C with
        // different key columns. A table-name-only lookup would wrongly
        // bloom tpcc.orders with TPC-H's o_orderkey/o_custkey.
        assert_eq!(
            bloom_columns("tpch", "orders"),
            &["o_orderkey", "o_custkey"]
        );
        assert!(bloom_columns("tpcc", "orders").is_empty());
        assert!(bloom_columns("tpcc", "customer").is_empty());
    }

    #[test]
    fn bloom_columns_empty_for_auto_bloomed_and_unknown_suites() {
        // TPC-DS `_sk` keys auto-bloom via the engine default, so no
        // explicit list here; unknown suites get nothing.
        assert!(bloom_columns("tpcds", "store_sales").is_empty());
        assert!(bloom_columns("clickbench", "hits").is_empty());
    }

    /// Every table and column a layout names exists in the generator's
    /// schema. The SSB date table was keyed `ddate` while the generator
    /// calls it `dim_date`, so `--bloom-filter` silently skipped it.
    #[test]
    fn every_layout_names_a_real_table_and_column() {
        let suites = [
            "tpch",
            "ssb",
            "tpcds",
            "tpcbb",
            "tpcc",
            "tpce",
            "clickbench",
        ];
        let mut named = 0;
        for suite in suites {
            for t in get_generator(suite).unwrap().tables() {
                let layout = table_layout(suite, &t.name);
                let mut columns: Vec<&str> = layout.bloom_columns.to_vec();
                columns.extend(layout.clustering);
                if let Some(spec) = layout.partition {
                    for part in spec.split(',') {
                        let part = part.trim();
                        let col = part
                            .strip_prefix("month(")
                            .and_then(|p| p.strip_suffix(')'))
                            .unwrap_or(part);
                        columns.push(col);
                    }
                }
                for col in columns {
                    assert!(
                        t.schema.field_with_name(col).is_ok(),
                        "{suite}.{}: layout names unknown column {col}",
                        t.name
                    );
                    named += 1;
                }
            }
        }
        // Guards against a table rename making every lookup miss at once.
        assert!(named >= 35, "only {named} layout columns resolved");
    }

    #[test]
    fn a_partitioned_table_is_not_also_sorted() {
        let lineitem = table_layout("tpch", "lineitem");
        assert_eq!(lineitem.clustering, Some("l_shipdate"));
        assert_eq!(lineitem.sort_key(), None);
        assert_eq!(
            table_layout("ssb", "lineorder").sort_key(),
            Some("lo_orderdate")
        );
    }

    #[test]
    fn auto_bloom_matches_the_engine_rule() {
        use arrow_schema::{DataType, Field, Schema};
        let schema = Schema::new(vec![
            Field::new("ss_item_sk", DataType::Int64, true),
            Field::new("ss_store_sk", DataType::Int32, true),
            Field::new("ss_ticket_sk", DataType::Utf8, true),
            Field::new("_sk", DataType::Int64, true),
            Field::new("c_customer_id", DataType::Int64, true),
        ]);
        assert_eq!(
            engine_auto_bloom_columns(&schema),
            vec!["ss_item_sk".to_string(), "ss_store_sk".to_string()]
        );
    }
}
