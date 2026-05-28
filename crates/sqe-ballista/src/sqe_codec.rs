//! Production ballista extension codecs that target **SQE's own** iceberg
//! nodes: `SqeTableProvider` (logical) and `IcebergScanExec` (physical),
//! both from `sqe-catalog`.
//!
//! ## Why these, not the [`crate::codec`] pair
//!
//! [`crate::codec`] targets iceberg-datafusion's `IcebergTableProvider` /
//! `IcebergTableScan`. That pair proved the mechanism in the PoC and is kept
//! as the upstream-PR reference (cutover design D1). But the real SQE
//! coordinator registers `SqeCatalogProvider`, whose tables are
//! `SqeTableProvider` and whose `scan()` returns SQE's `IcebergScanExec` —
//! a node that carries dynamic filters, late materialization, small-file /
//! manifest / direct-read concurrency, cached stats, and policy integration
//! the upstream node lacks. So the hot path needs codecs for SQE's nodes.
//!
//! ## Shape (identical strategy to the PoC pair)
//!
//! - **Logical:** encode the table *reference*; on decode look it up in the
//!   `SqeCatalogProvider` the codec holds (`schema(ns).table(name)`).
//! - **Physical:** encode `(namespace, table, snapshot_id, projection,
//!   predicate, config knobs, output schema)`; on decode reload the `Table`
//!   from the `SessionCatalog` (executor-side credentials) and rebuild via
//!   [`IcebergScanExec::from_codec_parts`]. A 1-byte discriminator delegates
//!   every non-iceberg node to ballista's default codec — replacing the
//!   default without delegation breaks ballista's own `ShuffleWriterExec`.
//!
//! Dynamic runtime filters are NOT serialized (cutover design D6); ballista
//! runs joins itself for v1.

use std::fmt::Debug;
use std::sync::Arc;

use ballista_core::serde::{BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use iceberg::expr::Predicate;
use iceberg::{NamespaceIdent, TableIdent};
use prost::Message;
use serde::{Deserialize, Serialize};
use sqe_catalog::{IcebergScanExec, SessionCatalog};

use crate::block_on_in_runtime;

/// Logical codec that resolves `SqeTableProvider`s against a
/// `SqeCatalogProvider` on decode, delegating every non-table call to
/// ballista's default logical codec.
pub struct SqeLogicalCodec {
    catalog: Arc<dyn CatalogProvider>,
    default: BallistaLogicalExtensionCodec,
}

impl Debug for SqeLogicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SqeLogicalCodec")
    }
}

impl SqeLogicalCodec {
    /// `catalog` is the per-session `SqeCatalogProvider` registered on the
    /// client context (cheap to clone; `Arc` of schema providers).
    pub fn new(catalog: Arc<dyn CatalogProvider>) -> Self {
        Self {
            catalog,
            default: BallistaLogicalExtensionCodec::default(),
        }
    }
}

impl LogicalExtensionCodec for SqeLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &TaskContext,
    ) -> DFResult<Extension> {
        self.default.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> DFResult<()> {
        self.default.try_encode(node, buf)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        _node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> DFResult<()> {
        // SQE only registers iceberg-backed tables, so encoding the
        // reference and rehydrating on decode is sufficient. `Display`
        // gives `schema.table` (or `catalog.schema.table`).
        buf.extend_from_slice(table_ref.to_string().as_bytes());
        Ok(())
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _table_ref: &TableReference,
        _schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> DFResult<Arc<dyn TableProvider>> {
        let encoded = std::str::from_utf8(buf)
            .map_err(|e| DataFusionError::Internal(format!("table ref not UTF-8: {e}")))?;
        let parsed = TableReference::parse_str(encoded);

        let (schema_name, table_name) = match &parsed {
            TableReference::Bare { table } => {
                return Err(DataFusionError::Internal(format!(
                    "iceberg table ref missing schema: {table}"
                )));
            }
            TableReference::Partial { schema, table } => (schema.to_string(), table.to_string()),
            TableReference::Full { schema, table, .. } => (schema.to_string(), table.to_string()),
        };

        let schema_provider = self.catalog.schema(&schema_name).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "sqe codec: namespace '{schema_name}' not found on executor catalog"
            ))
        })?;

        // Sync trait method, async lookup, on a tokio worker: block_in_place
        // + current Handle so the reactor keeps driving the catalog call.
        let table_name_for_lookup = table_name.clone();
        let provider =
            block_on_in_runtime(async move { schema_provider.table(&table_name_for_lookup).await })?
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "sqe codec: table '{schema_name}.{table_name}' not found"
                    ))
                })?;
        Ok(provider)
    }
}

/// Wire format for an [`IcebergScanExec`] physical node.
#[derive(Serialize, Deserialize)]
struct EncodedSqeScan {
    /// Namespace parts, e.g. `["tpch_sf0_1"]`.
    namespace: Vec<String>,
    table: String,
    snapshot_id: Option<i64>,
    /// Projected column names; `None` = all columns.
    projection: Option<Vec<String>>,
    /// Static iceberg `Predicate` pushed in at plan time (serde-friendly).
    /// Runtime/dynamic filters are NOT carried (cutover design D6).
    predicate: Option<Predicate>,
    // Config knobs that affect scan behaviour and must survive the wire.
    small_file_threshold_bytes: u64,
    manifest_concurrency: usize,
    direct_read_concurrency: usize,
    target_partitions: usize,
    trust_sort_order: bool,
    /// Output (already-projected) Arrow schema, datafusion-proto bytes.
    schema_proto: Vec<u8>,
}

/// Physical codec that rehydrates [`IcebergScanExec`] on the executor by
/// reloading the table from the `SessionCatalog` and rebuilding the scan
/// from the wire-encoded parts.
pub struct SqePhysicalCodec {
    catalog: Arc<SessionCatalog>,
    default: BallistaPhysicalExtensionCodec,
}

impl Debug for SqePhysicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SqePhysicalCodec")
    }
}

impl SqePhysicalCodec {
    pub fn new(catalog: Arc<SessionCatalog>) -> Self {
        Self {
            catalog,
            default: BallistaPhysicalExtensionCodec::default(),
        }
    }
}

impl PhysicalExtensionCodec for SqePhysicalCodec {
    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        // Discriminator byte: 1 = SQE iceberg scan, 0 = delegate to ballista.
        // Non-iceberg nodes (ShuffleWriterExec, etc.) MUST delegate or
        // ballista's distributed plan breaks.
        let Some(scan) = node.as_any().downcast_ref::<IcebergScanExec>() else {
            buf.push(0u8);
            return self.default.try_encode(node, buf);
        };
        buf.push(1u8);

        let ident = scan.table().identifier();
        let schema_proto: datafusion_proto::protobuf::Schema = scan
            .projected_schema()
            .as_ref()
            .try_into()
            .map_err(|e| DataFusionError::Internal(format!("schema encode: {e}")))?;

        let encoded = EncodedSqeScan {
            namespace: ident.namespace().clone().inner(),
            table: ident.name().to_string(),
            snapshot_id: scan.snapshot_id(),
            projection: scan.projection().map(|p| p.to_vec()),
            predicate: scan.predicates().cloned(),
            small_file_threshold_bytes: scan.small_file_threshold_bytes(),
            manifest_concurrency: scan.manifest_concurrency(),
            direct_read_concurrency: scan.direct_read_concurrency(),
            target_partitions: scan.target_partitions(),
            trust_sort_order: scan.trust_sort_order(),
            schema_proto: schema_proto.encode_to_vec(),
        };

        let bytes = serde_json::to_vec(&encoded)
            .map_err(|e| DataFusionError::Internal(format!("scan encode: {e}")))?;
        buf.extend_from_slice(&bytes);
        Ok(())
    }

    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let (tag, rest) = buf
            .split_first()
            .ok_or_else(|| DataFusionError::Internal("empty physical codec buffer".into()))?;
        if *tag == 0 {
            return self.default.try_decode(rest, inputs, ctx);
        }

        let encoded: EncodedSqeScan = serde_json::from_slice(rest)
            .map_err(|e| DataFusionError::Internal(format!("scan decode: {e}")))?;

        let schema: SchemaRef = {
            let proto = datafusion_proto::protobuf::Schema::decode(&encoded.schema_proto[..])
                .map_err(|e| DataFusionError::Internal(format!("schema proto decode: {e}")))?;
            Arc::new(
                Schema::try_from(&proto)
                    .map_err(|e| DataFusionError::Internal(format!("schema from proto: {e}")))?,
            )
        };

        let namespace = NamespaceIdent::from_vec(encoded.namespace)
            .map_err(|e| DataFusionError::Internal(format!("bad namespace: {e}")))?;
        let ident = TableIdent::new(namespace, encoded.table);

        // Reload the table from the catalog on the executor — this is where
        // the executor's own vended credentials apply. block_in_place +
        // current Handle so the tokio reactor keeps driving the REST call.
        let catalog = self.catalog.clone();
        let table = block_on_in_runtime(async move { catalog.load_table(&ident).await })
            .map_err(|e| DataFusionError::Internal(format!("load_table on executor: {e}")))?;

        let scan = IcebergScanExec::from_codec_parts(
            table,
            schema,
            encoded.projection,
            encoded.predicate,
            encoded.snapshot_id,
            encoded.small_file_threshold_bytes,
            encoded.manifest_concurrency,
            encoded.direct_read_concurrency,
            encoded.target_partitions,
            encoded.trust_sort_order,
        );
        Ok(Arc::new(scan))
    }
}

#[cfg(test)]
mod tests {
    use iceberg::expr::Reference;
    use iceberg::spec::Datum;

    use super::*;

    /// The `EncodedSqeScan` wire format is what crosses the scheduler ->
    /// executor boundary. Pin the round-trip, including the predicate and
    /// every config knob (a silently-dropped knob changes scan behaviour on
    /// the executor).
    #[test]
    fn encoded_sqe_scan_round_trips() {
        let predicate = Reference::new("l_quantity")
            .greater_than(Datum::double(30.0))
            .and(Reference::new("l_orderkey").equal_to(Datum::long(42)));

        let original = EncodedSqeScan {
            namespace: vec!["tpch_sf0_1".to_string()],
            table: "lineitem".to_string(),
            snapshot_id: Some(987654321),
            projection: Some(vec!["l_orderkey".to_string(), "l_quantity".to_string()]),
            predicate: Some(predicate.clone()),
            small_file_threshold_bytes: 33_554_432,
            manifest_concurrency: 8,
            direct_read_concurrency: 16,
            target_partitions: 4,
            trust_sort_order: true,
            schema_proto: vec![9, 8, 7],
        };

        let bytes = serde_json::to_vec(&original).expect("encode");
        let decoded: EncodedSqeScan = serde_json::from_slice(&bytes).expect("decode");

        assert_eq!(decoded.namespace, original.namespace);
        assert_eq!(decoded.table, original.table);
        assert_eq!(decoded.snapshot_id, original.snapshot_id);
        assert_eq!(decoded.projection, original.projection);
        assert_eq!(decoded.predicate, Some(predicate));
        assert_eq!(
            decoded.small_file_threshold_bytes,
            original.small_file_threshold_bytes
        );
        assert_eq!(decoded.manifest_concurrency, original.manifest_concurrency);
        assert_eq!(
            decoded.direct_read_concurrency,
            original.direct_read_concurrency
        );
        assert_eq!(decoded.target_partitions, original.target_partitions);
        assert_eq!(decoded.trust_sort_order, original.trust_sort_order);
        assert_eq!(decoded.schema_proto, original.schema_proto);
    }
}
