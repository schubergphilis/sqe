//! `LogicalExtensionCodec` that rehydrates iceberg table providers on the
//! ballista executor side.
//!
//! ## Why this exists (PoC finding)
//!
//! `iceberg-datafusion` ships no `LogicalExtensionCodec`, so ballista's
//! default codec cannot serialize an `IcebergTableProvider` across the
//! scheduler -> executor boundary.  The query fails at plan submission
//! with:
//!
//! ```text
//! Internal error: failed to serialize logical plan:
//!   Error serializing custom table ...
//!   NotImplemented("LogicalExtensionCodec is not provided")
//! ```
//!
//! Ballista always serializes the plan to ship it to executors, even in
//! standalone mode (the executor builds its own server-side
//! `SessionContext`).  So *some* codec must know how to round-trip an
//! iceberg table.
//!
//! ## The approach
//!
//! Rather than serialize the table's full state (catalog config, metadata
//! location, schema), we encode only the table *reference*
//! (`schema.table`) and rehydrate by looking it up in an
//! `IcebergCatalogProvider` the codec holds a handle to.  The catalog
//! provider is cheap to clone (`Arc` of schema providers) and is the same
//! one registered on the client context.
//!
//! This is the pattern we'd want to upstream into `iceberg-datafusion`
//! as a reusable `IcebergLogicalCodec`, parameterized over the catalog.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;
use ballista_core::serde::{BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergCatalogProvider;
use iceberg_datafusion::physical_plan::IcebergTableScan;
use prost::Message;
use serde::{Deserialize, Serialize};

/// Drive an async future to completion from a sync context that is itself
/// running on a tokio worker thread (codec decode happens inside the
/// ballista executor's runtime).
///
/// `futures::executor::block_on` deadlocks here: it parks the worker
/// thread without pumping the tokio reactor, so the iceberg REST client's
/// HTTP future never makes progress.  `block_in_place` hands the thread
/// back to the runtime for other tasks while we block, and the current
/// `Handle` drives our future on the same multi-threaded runtime.
fn block_on_in_runtime<F: std::future::Future>(fut: F) -> F::Output {
    let handle = tokio::runtime::Handle::current();
    tokio::task::block_in_place(move || handle.block_on(fut))
}

/// Codec that resolves iceberg tables against a catalog provider on decode,
/// delegating every non-table call to ballista's default logical codec.
pub struct IcebergLogicalCodec {
    catalog: Arc<IcebergCatalogProvider>,
    default: BallistaLogicalExtensionCodec,
}

impl Debug for IcebergLogicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IcebergLogicalCodec")
    }
}

impl IcebergLogicalCodec {
    pub fn new(catalog: Arc<IcebergCatalogProvider>) -> Self {
        Self {
            catalog,
            default: BallistaLogicalExtensionCodec::default(),
        }
    }
}

impl LogicalExtensionCodec for IcebergLogicalCodec {
    // Custom LogicalPlan extension nodes (ballista's cache node etc.) are
    // ballista's; delegate so we don't break its machinery.
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
        // We can't name `IcebergTableProvider` (it's `pub(crate)` in
        // iceberg-datafusion), so we don't type-check `_node`.  The SQE
        // PoC context only registers iceberg tables, so encoding just the
        // reference and rehydrating on decode is sufficient.  A production
        // codec upstreamed into iceberg-datafusion would downcast and
        // serialize the table_ident + a catalog discriminator.
        //
        // TableReference's Display gives `schema.table` (or
        // `catalog.schema.table`).
        let s = table_ref.to_string();
        buf.extend_from_slice(s.as_bytes());
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
            TableReference::Partial { schema, table } => {
                (schema.to_string(), table.to_string())
            }
            TableReference::Full { schema, table, .. } => {
                (schema.to_string(), table.to_string())
            }
        };

        let schema_provider = self.catalog.schema(&schema_name).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "iceberg codec: namespace '{schema_name}' not found on executor catalog"
            ))
        })?;

        // try_decode_table_provider is sync but the lookup is async, and we
        // run inside a tokio worker.  Use block_in_place + the current
        // Handle so the runtime keeps driving the HTTP catalog call —
        // futures::executor::block_on would deadlock (it doesn't pump the
        // tokio reactor the iceberg REST client needs).
        let table_name_owned = table_name.clone();
        let provider = block_on_in_runtime(async move {
            schema_provider.table(&table_name_owned).await
        })?
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "iceberg codec: table '{schema_name}.{table_name}' not found"
            ))
        })?;

        Ok(provider)
    }
}

/// Wire format for an `IcebergTableScan` physical node.
#[derive(Serialize, Deserialize)]
struct EncodedScan {
    /// Namespace parts, e.g. `["tpch_sf0_1"]`.
    namespace: Vec<String>,
    table: String,
    snapshot_id: Option<i64>,
    /// Projected column names; `None` = all columns.
    projection: Option<Vec<String>>,
    limit: Option<usize>,
    /// `true` if the original scan carried pushed-down predicates.  The
    /// PoC bails in that case rather than silently dropping them (the
    /// COUNT(*) test query has none).  A production codec would serialize
    /// the iceberg `Predicate`.
    had_predicates: bool,
    /// Output (already-projected) Arrow schema, datafusion-proto bytes.
    schema_proto: Vec<u8>,
}

/// `PhysicalExtensionCodec` that rehydrates `IcebergTableScan` on the
/// executor by reloading the table from the catalog and rebuilding the
/// scan from the wire-encoded projection / limit / schema.
///
/// ## PoC finding
///
/// Like the logical side, `iceberg-datafusion` ships no physical codec,
/// and `IcebergTableScan` holds a live `Table` (FileIO handle, S3 config,
/// bearer token) that isn't serializable by ballista's default codec.
/// The error without this codec is:
///
/// ```text
/// Internal error: Unsupported plan and extension codec failed with
///   [unsupported plan type: IcebergTableScan { table: Table { ... } }]
/// ```
///
/// Note the bearer token is visible in that error — confirming
/// verification point #2: per-query credentials DO reach the executor,
/// embedded in the scan node.  This codec replaces that embedded-state
/// transport with a reload-from-catalog approach (cleaner, and the
/// executor's catalog handle carries its own fresh credentials).
pub struct IcebergPhysicalCodec {
    catalog: Arc<dyn Catalog>,
    default: BallistaPhysicalExtensionCodec,
}

impl Debug for IcebergPhysicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IcebergPhysicalCodec")
    }
}

impl IcebergPhysicalCodec {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self {
            catalog,
            default: BallistaPhysicalExtensionCodec::default(),
        }
    }
}

impl PhysicalExtensionCodec for IcebergPhysicalCodec {
    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        // Discriminator byte: 1 = iceberg scan (our format), 0 = delegate.
        // Non-iceberg nodes (ShuffleWriterExec, etc.) belong to ballista's
        // default codec — we MUST delegate or its distributed plan breaks.
        let Some(scan) = node.as_any().downcast_ref::<IcebergTableScan>() else {
            buf.push(0u8);
            return self.default.try_encode(node, buf);
        };
        buf.push(1u8);

        let ident = scan.table().identifier();
        let schema_proto: datafusion_proto::protobuf::Schema = scan
            .schema()
            .as_ref()
            .try_into()
            .map_err(|e| DataFusionError::Internal(format!("schema encode: {e}")))?;

        let encoded = EncodedScan {
            namespace: ident.namespace().clone().inner(),
            table: ident.name().to_string(),
            snapshot_id: scan.snapshot_id(),
            projection: scan.projection().map(|p| p.to_vec()),
            limit: scan.limit(),
            had_predicates: scan.predicates().is_some(),
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
        // Read the discriminator byte written by try_encode.
        let (tag, rest) = buf
            .split_first()
            .ok_or_else(|| DataFusionError::Internal("empty physical codec buffer".into()))?;
        if *tag == 0 {
            return self.default.try_decode(rest, inputs, ctx);
        }

        let encoded: EncodedScan = serde_json::from_slice(rest)
            .map_err(|e| DataFusionError::Internal(format!("scan decode: {e}")))?;

        if encoded.had_predicates {
            return Err(DataFusionError::NotImplemented(
                "IcebergPhysicalCodec PoC does not serialize pushed-down predicates yet"
                    .into(),
            ));
        }

        // Decode output schema.
        let schema: SchemaRef = {
            let proto = datafusion_proto::protobuf::Schema::decode(&encoded.schema_proto[..])
                .map_err(|e| DataFusionError::Internal(format!("schema proto decode: {e}")))?;
            Arc::new(Schema::try_from(&proto).map_err(|e| {
                DataFusionError::Internal(format!("schema from proto: {e}"))
            })?)
        };

        let namespace = NamespaceIdent::from_vec(encoded.namespace)
            .map_err(|e| DataFusionError::Internal(format!("bad namespace: {e}")))?;
        let ident = TableIdent::new(namespace, encoded.table);

        // Reload the table from the catalog on the executor side — this
        // is where the executor's own credentials apply.  block_in_place +
        // Handle so the tokio reactor keeps driving the REST call.
        let catalog = self.catalog.clone();
        let table = block_on_in_runtime(async move { catalog.load_table(&ident).await })
            .map_err(|e| DataFusionError::Internal(format!("load_table on executor: {e}")))?;

        let scan = IcebergTableScan::from_codec_parts(
            table,
            encoded.snapshot_id,
            schema,
            encoded.projection,
            None, // had_predicates == false guaranteed above
            encoded.limit,
        );
        Ok(Arc::new(scan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `EncodedScan` wire format is what crosses the scheduler ->
    /// executor boundary; a silent field drop would corrupt scans. Pin the
    /// round-trip.
    #[test]
    fn encoded_scan_round_trips() {
        let original = EncodedScan {
            namespace: vec!["tpch_sf0_1".to_string()],
            table: "lineitem".to_string(),
            snapshot_id: Some(123456789),
            projection: Some(vec!["l_orderkey".to_string(), "l_quantity".to_string()]),
            limit: Some(100),
            had_predicates: false,
            schema_proto: vec![1, 2, 3, 4],
        };

        let bytes = serde_json::to_vec(&original).expect("encode");
        let decoded: EncodedScan = serde_json::from_slice(&bytes).expect("decode");

        assert_eq!(decoded.namespace, original.namespace);
        assert_eq!(decoded.table, original.table);
        assert_eq!(decoded.snapshot_id, original.snapshot_id);
        assert_eq!(decoded.projection, original.projection);
        assert_eq!(decoded.limit, original.limit);
        assert_eq!(decoded.had_predicates, original.had_predicates);
        assert_eq!(decoded.schema_proto, original.schema_proto);
    }

    /// `None` projection means "all columns" — distinct from an empty Vec.
    /// Confirm the distinction survives the wire.
    #[test]
    fn encoded_scan_preserves_none_projection() {
        let original = EncodedScan {
            namespace: vec!["ns".to_string()],
            table: "t".to_string(),
            snapshot_id: None,
            projection: None,
            limit: None,
            had_predicates: false,
            schema_proto: vec![],
        };

        let bytes = serde_json::to_vec(&original).expect("encode");
        let decoded: EncodedScan = serde_json::from_slice(&bytes).expect("decode");

        assert!(decoded.projection.is_none());
        assert!(decoded.snapshot_id.is_none());
        assert!(decoded.limit.is_none());
    }
}
