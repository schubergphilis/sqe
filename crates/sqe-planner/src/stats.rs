//! Query-aware requests for Iceberg/Puffin distinct-count statistics.
//!
//! DataFusion 55 threads [`StatisticsRequest`] values from logical table scans
//! into [`datafusion::catalog::ScanArgs`], but deliberately does not decide
//! which statistics an external provider should load. This opt-in optimizer
//! rule asks for NDV on projected scan columns. `SqeTableProvider` answers the
//! requests from the snapshot's Iceberg `StatisticsFile` metadata and leaves
//! unsupported or absent values unknown.
//!
//! Register this rule together with
//! `datafusion.optimizer.use_statistics_registry = true`. Both are disabled by
//! default through SQE's `query.puffin_statistics` experiment flag.

use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DataFusionError, Result};
use datafusion::logical_expr::logical_plan::{LogicalPlan, TableScanBuilder};
use datafusion::logical_expr::statistics::StatisticsRequest;
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

/// Add distinct-count requests to each projected column of a table scan.
#[derive(Debug, Default)]
pub struct PuffinStatisticsRequestRule;

impl PuffinStatisticsRequestRule {
    /// Create the request-producing rule.
    pub fn new() -> Self {
        Self
    }
}

impl OptimizerRule for PuffinStatisticsRequestRule {
    fn name(&self) -> &str {
        "puffin_statistics_requests"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let LogicalPlan::TableScan(mut scan) = plan else {
            return Ok(Transformed::no(plan));
        };

        let before = scan.statistics_requests.len();
        for field in scan.projected_schema.fields() {
            scan.statistics_requests
                .insert(StatisticsRequest::DistinctCount(Arc::new(Column::new(
                    Some(scan.table_name.clone()),
                    field.name(),
                ))));
        }

        if scan.statistics_requests.len() == before {
            Ok(Transformed::no(LogicalPlan::TableScan(scan)))
        } else {
            let rebuilt = TableScanBuilder::from(scan).build()?;
            Ok(Transformed::yes(LogicalPlan::TableScan(rebuilt)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::{provider_as_source, MemTable};
    use datafusion::optimizer::optimizer::OptimizerContext;

    fn scan(projection: Option<Vec<usize>>) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("unused", DataType::Int32, true),
        ]));
        let table = Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap());
        LogicalPlan::TableScan(
            TableScanBuilder::new("t", provider_as_source(table))
                .with_projection(projection)
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn requests_ndv_only_for_projected_columns() {
        let transformed = PuffinStatisticsRequestRule::new()
            .rewrite(scan(Some(vec![1, 0])), &OptimizerContext::new())
            .unwrap();
        let LogicalPlan::TableScan(scan) = transformed.data else {
            panic!("expected table scan");
        };

        let names: Vec<_> = scan
            .statistics_requests
            .iter()
            .filter_map(|request| match request {
                StatisticsRequest::DistinctCount(column) => Some(column.name.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(names, vec!["id", "name"]);
    }

    #[test]
    fn preserves_existing_requests_and_is_idempotent() {
        let LogicalPlan::TableScan(mut scan) = scan(Some(vec![0])) else {
            unreachable!();
        };
        scan.statistics_requests.insert(StatisticsRequest::RowCount);
        let rule = PuffinStatisticsRequestRule::new();
        let first = rule
            .rewrite(LogicalPlan::TableScan(scan), &OptimizerContext::new())
            .unwrap();
        let second = rule.rewrite(first.data, &OptimizerContext::new()).unwrap();
        assert!(!second.transformed);
        let LogicalPlan::TableScan(scan) = second.data else {
            panic!("expected table scan");
        };
        assert!(scan
            .statistics_requests
            .contains(&StatisticsRequest::RowCount));
        assert_eq!(scan.statistics_requests.len(), 2);
    }
}
