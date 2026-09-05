//! Shared DataFusion session flags used by the coordinator and sqe-cli
//! embedded mode so the two paths cannot drift.

use datafusion::prelude::SessionConfig;

/// Engine-wide DataFusion session flags (decimal literals, DuckDB dialect,
/// broadcast threshold, dynamic filter pushdown, Parquet predicate pushdown).
pub fn engine_session_config() -> SessionConfig {
    SessionConfig::new()
        // Parse numeric literals like 0.06 as DECIMAL instead of DOUBLE.
        .set_bool("datafusion.sql_parser.parse_float_as_decimal", true)
        // DuckDB dialect adds `x -> expr` lambda syntax without changing
        // identifier/quoting behaviour. See #354.
        .set_str("datafusion.sql_parser.dialect", "DuckDB")
        .set_usize(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            64 * 1024 * 1024,
        )
        .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.reorder_filters", true)
}
