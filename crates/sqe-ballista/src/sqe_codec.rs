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

use std::collections::HashMap;
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
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::spec::TableMetadata;
use iceberg::table::Table;
use iceberg::{NamespaceIdent, TableIdent};
use prost::Message;
use serde::{Deserialize, Serialize};
use sqe_catalog::{IcebergScanExec, SessionCatalog, TableMetadataCache};
use sqe_core::config::{CatalogConfig, StorageConfig};

use crate::auth_ext::SqeAuthOptions;
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
    /// Serialized `iceberg::spec::TableMetadata` (JSON). Carrying it lets the
    /// executor rebuild the `Table` synchronously at decode instead of an
    /// async catalog round-trip per scan task (cutover design D4 — the per-task
    /// `block_on` in decode starved the executor runtime under concurrent
    /// multi-stage plans and hung TPC-DS). Empty = legacy/compat: fall back to
    /// reloading from the catalog (the old blocking path).
    #[serde(default)]
    metadata_json: Vec<u8>,
    /// Table metadata file location (e.g. `s3://.../metadata/v3.json`). Used to
    /// infer the FileIO scheme on decode, and passed to the rebuilt `Table`.
    #[serde(default)]
    metadata_location: Option<String>,
    /// The authenticated user's OIDC bearer, threaded through the plan so the
    /// executor can mint per-user vended S3 creds (parity #1 / D8). `None` =
    /// single-tenant fallback (Phase 3 behaviour). Only the bearer travels,
    /// never S3 secrets (auth_ext.rs trust model).
    #[serde(default)]
    bearer: Option<String>,
}

/// Physical codec that rehydrates [`IcebergScanExec`] on the executor by
/// reloading the table from a `SessionCatalog` and rebuilding the scan from
/// the wire-encoded parts.
///
/// Per-query auth (Phase 4): if the task carries a `sqe_auth.bearer`
/// (propagated via [`crate::auth_ext::SqeAuthOptions`]), the codec mints a
/// per-user `SessionCatalog` from `cat_cfg` + `storage` with that bearer and
/// reloads the table through it, so Polaris vends per-user S3 creds and
/// enforces per-user access. Per-token catalogs are cached. With no bearer it
/// falls back to the config-built `fallback` catalog (Phase 3 single-tenant).
pub struct SqePhysicalCodec {
    /// Config service-token catalog; used when no per-query bearer is present.
    fallback: Arc<SessionCatalog>,
    /// Catalog + storage config used to mint per-user `SessionCatalog`s.
    cat_cfg: CatalogConfig,
    storage: StorageConfig,
    /// Shared table-metadata cache passed to every minted `SessionCatalog`
    /// so `load_table` doesn't refetch metadata per scan task.
    table_cache: TableMetadataCache,
    /// Per-user catalog cache keyed by a non-crypto hash of the bearer.
    per_user: Arc<tokio::sync::Mutex<HashMap<u64, Arc<SessionCatalog>>>>,
    default: BallistaPhysicalExtensionCodec,
}

impl Debug for SqePhysicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SqePhysicalCodec")
    }
}

impl SqePhysicalCodec {
    pub fn new(
        fallback: Arc<SessionCatalog>,
        cat_cfg: CatalogConfig,
        storage: StorageConfig,
        table_cache: TableMetadataCache,
    ) -> Self {
        Self {
            fallback,
            cat_cfg,
            storage,
            table_cache,
            per_user: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            default: BallistaPhysicalExtensionCodec::default(),
        }
    }

    /// Resolve the `SessionCatalog` to use for this task: a per-user one keyed
    /// by the task's `sqe_auth.bearer`, or the fallback when none is present.
    async fn resolve_catalog(&self, ctx: &TaskContext) -> DFResult<Arc<SessionCatalog>> {
        let bearer = ctx
            .session_config()
            .options()
            .extensions
            .get::<SqeAuthOptions>()
            .map(|o| o.bearer.clone())
            .unwrap_or_default();

        if bearer.is_empty() {
            return Ok(self.fallback.clone());
        }

        let key = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            bearer.hash(&mut h);
            h.finish()
        };

        if let Some(existing) = self.per_user.lock().await.get(&key) {
            return Ok(existing.clone());
        }

        let session_catalog = SessionCatalog::for_session_with(
            &self.cat_cfg,
            &self.storage,
            Some(self.table_cache.clone()),
            &bearer,
        )
        .await
        .map_err(|e| DataFusionError::Internal(format!("per-user catalog on executor: {e}")))?;
        let arc = Arc::new(session_catalog);
        self.per_user.lock().await.insert(key, arc.clone());
        Ok(arc)
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

        // Serialize the table metadata so the executor can rebuild the `Table`
        // synchronously at decode (D4), avoiding a per-task catalog round-trip.
        let metadata_json = serde_json::to_vec(scan.table().metadata())
            .map_err(|e| DataFusionError::Internal(format!("table metadata encode: {e}")))?;
        let metadata_location = scan.table().metadata_location().map(|s| s.to_string());

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
            metadata_json,
            metadata_location,
            // Populated from the node's bearer in a later task (parity #1 / D8).
            bearer: None,
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

        // D4: rebuild the `Table` synchronously from the serialized metadata +
        // a FileIO built from static storage config. No catalog round-trip, no
        // `block_on` -> decode is pure CPU and cannot starve the executor's
        // tokio runtime under concurrent multi-stage task decodes.
        //
        // Empty `metadata_json` is the legacy/compat path (plans from an older
        // encoder, or the per-user seam Phase 4b extends): fall back to the
        // catalog reload, which blocks. New encoders always populate it, so the
        // blocking branch is not hit on the hot path.
        let table = if encoded.metadata_json.is_empty() {
            block_on_in_runtime(async move {
                let catalog = self.resolve_catalog(ctx).await?;
                catalog
                    .load_table(&ident)
                    .await
                    .map_err(|e| DataFusionError::Internal(format!("load_table on executor: {e}")))
            })?
        } else {
            let metadata: TableMetadata = serde_json::from_slice(&encoded.metadata_json)
                .map_err(|e| DataFusionError::Internal(format!("table metadata decode: {e}")))?;
            let file_io = build_file_io(&self.storage, encoded.metadata_location.as_deref())?;
            let mut builder = Table::builder()
                .identifier(ident)
                .metadata(Arc::new(metadata))
                .file_io(file_io);
            if let Some(loc) = &encoded.metadata_location {
                builder = builder.metadata_location(loc.clone());
            }
            builder
                .build()
                .map_err(|e| DataFusionError::Internal(format!("rebuild table on executor: {e}")))?
        };

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

/// Build a synchronous `FileIO` from static S3 storage config, mirroring the
/// `s3.*` props the per-session REST catalog injects (see `rest_catalog.rs`).
/// Used on the executor to rebuild a `Table` without a catalog round-trip (D4).
/// Single-tenant / service-principal creds only; per-user vended creds are
/// Phase 4b (threaded through the plan bytes, fed into these props).
fn build_file_io(storage: &StorageConfig, metadata_location: Option<&str>) -> DFResult<FileIO> {
    let mut props: HashMap<String, String> = HashMap::new();
    if !storage.s3_endpoint.is_empty() {
        props.insert("s3.endpoint".to_string(), storage.s3_endpoint.clone());
    }
    if !storage.s3_region.is_empty() {
        props.insert("s3.region".to_string(), storage.s3_region.clone());
    }
    if !storage.s3_access_key.is_empty() {
        props.insert("s3.access-key-id".to_string(), storage.s3_access_key.clone());
    }
    if !storage.s3_secret_key.is_empty() {
        props.insert(
            "s3.secret-access-key".to_string(),
            storage.s3_secret_key.expose().to_string(),
        );
    }
    if storage.s3_path_style {
        props.insert("s3.path-style-access".to_string(), "true".to_string());
    }

    // Prefer inferring the scheme from the metadata location (matches the way
    // iceberg-rust's RestCatalog builds FileIO); fall back to plain "s3".
    let builder = match metadata_location {
        Some(loc) => FileIO::from_path(loc)
            .map_err(|e| DataFusionError::Internal(format!("file_io from_path: {e}")))?,
        None => FileIOBuilder::new("s3"),
    };
    builder
        .with_props(props)
        .build()
        .map_err(|e| DataFusionError::Internal(format!("build executor file_io: {e}")))
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
            metadata_json: vec![123, 34, 102, 111, 114, 109, 97, 116, 34, 125],
            metadata_location: Some(
                "s3://warehouse/tpch_sf0_1/lineitem/metadata/00003.metadata.json".to_string(),
            ),
            bearer: Some("eyJhbGciOiJ.user-bearer.sig".to_string()),
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
        assert_eq!(decoded.metadata_json, original.metadata_json);
        assert_eq!(decoded.metadata_location, original.metadata_location);
        assert_eq!(decoded.bearer, original.bearer);
    }
}
