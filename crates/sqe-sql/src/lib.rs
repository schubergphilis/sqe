pub mod alter_execute;
pub mod attach;
pub mod bare_table;
pub mod catalog_qualifiers;
pub mod classifier;
pub mod ctas_compat;
pub mod ddl;
pub mod nested_row_cast;
pub mod paren_less_values;
pub mod partition;
pub mod partition_evolution;
pub mod pipeline_types;
pub mod policy_ddl;
pub mod procedures;
pub mod tags;
pub mod time_travel;
pub mod trino_compat;
pub mod tvf_named_args;
pub mod v3_types;
pub mod view_compat;

pub use alter_execute::rewrite_alter_execute;
pub use attach::{
    build_secret_from_stmt, AttachStatement, CatalogKind, CreateSecretStatement, DetachStatement,
    DropSecretStatement, OptionValue, SecretKind,
};
pub use bare_table::rewrite_bare_table;
pub use catalog_qualifiers::{extract_catalog_qualifiers, extract_catalog_qualifiers_from_sql};
pub use classifier::{
    parse_and_classify, parse_and_classify_typed, CheckAccessParams, ShowEffectivePolicyParams,
    ShowGrantsTarget, StatementKind,
};
pub use ctas_compat::rewrite_ctas_compat;
pub use ddl::{try_parse_ref_ddl, BranchRetention, RefDdl};
pub use nested_row_cast::rewrite_nested_row_cast;
pub use paren_less_values::rewrite_paren_less_values;
pub use partition::normalize_partitioned_by;
pub use partition_evolution::{try_parse_partition_evolution, PartitionEvolution};
pub use pipeline_types::{pre_parse_pipeline, ClassifiableSql, UserSql};
pub use procedures::{try_parse_call, NamespaceRef, ProcedureCall, TableRef};
pub use time_travel::{
    extract_incremental_spec, extract_time_travel_spec, IncrementalSpec, TimeTravelSpec, VersionRef,
};
pub use trino_compat::{
    alias_anonymous_select_columns, check_expression_depth, rewrite_trino_compat,
};
pub use tvf_named_args::rewrite_named_tvf_args;
pub use v3_types::{
    detect_ns_timestamp, extract_default_literal, is_tz_variant, is_v3_only_type, DefaultError,
    DefaultLiteral, NsTimestamp,
};
pub use view_compat::{rewrite_view_compat, ViewCompat, ViewSecurity, VIEW_SECURITY_OPTION};

#[cfg(test)]
mod insert_overwrite_parse_tests {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse_one(sql: &str) -> Statement {
        Parser::parse_sql(&GenericDialect {}, sql)
            .expect("parse")
            .pop()
            .expect("one statement")
    }

    #[test]
    fn insert_overwrite_sets_overwrite_flag() {
        for sql in [
            "INSERT OVERWRITE t SELECT 1 AS id",
            "INSERT OVERWRITE INTO t SELECT 1 AS id",
            "INSERT OVERWRITE TABLE t SELECT 1 AS id",
        ] {
            match parse_one(sql) {
                Statement::Insert(ins) => {
                    assert!(ins.overwrite, "overwrite flag not set for: {sql}");
                    assert!(ins.partitioned.is_none(), "unexpected PARTITION for: {sql}");
                }
                other => panic!("expected Insert, got {other:?} for {sql}"),
            }
        }
    }

    #[test]
    fn plain_insert_does_not_set_overwrite() {
        match parse_one("INSERT INTO t SELECT 1 AS id") {
            Statement::Insert(ins) => assert!(!ins.overwrite),
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn insert_overwrite_static_partition_is_captured() {
        // Static Hive PARTITION clause must be visible so the handler can
        // reject it loudly rather than mishandle it.
        match parse_one("INSERT OVERWRITE t PARTITION (region='eu') SELECT 1 AS id") {
            Statement::Insert(ins) => {
                assert!(ins.overwrite);
                assert!(ins.partitioned.is_some(), "static PARTITION not captured");
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }
}
