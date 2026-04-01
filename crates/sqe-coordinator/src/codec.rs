use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf;
use prost::Message;
use serde::{Deserialize, Serialize};
use sqe_planner::ScanTask;

use crate::distributed_scan::DistributedScanExec;

/// DataFusion physical extension codec for SQE's custom plan nodes.
///
/// Implements `PhysicalExtensionCodec` for `DistributedScanExec`, enabling
/// DataFusion's proto serialization machinery to handle our distributed scan
/// plan nodes. This replaces the ad-hoc JSON ScanTask approach with a
/// proper DataFusion codec integration point (DataFusion PR #19437).
#[derive(Debug, Default)]
pub struct SqePhysicalCodec;

/// Wire format for a serialized `DistributedScanExec`.
#[derive(Serialize, Deserialize)]
struct EncodedDistributedScan {
    scan_tasks: Vec<ScanTask>,
    worker_urls: Vec<String>,
    /// Arrow schema serialized as protobuf bytes (base64-encoded).
    schema_proto_b64: String,
}

impl SqePhysicalCodec {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalExtensionCodec for SqePhysicalCodec {
    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DFResult<()> {
        if let Some(scan) = node.as_any().downcast_ref::<DistributedScanExec>() {
            let proto_schema: protobuf::Schema =
                scan.schema().as_ref().try_into().map_err(|e| {
                    DataFusionError::External(Box::new(std::io::Error::other(format!(
                        "Schema encoding failed: {e}"
                    ))))
                })?;

            let schema_bytes = proto_schema.encode_to_vec();
            let schema_proto_b64 = base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &schema_bytes,
            );

            let encoded = EncodedDistributedScan {
                scan_tasks: scan.scan_tasks().to_vec(),
                worker_urls: scan.worker_urls().to_vec(),
                schema_proto_b64,
            };

            let json =
                serde_json::to_vec(&encoded).map_err(|e| DataFusionError::External(Box::new(e)))?;
            buf.extend_from_slice(&json);
            Ok(())
        } else {
            Err(DataFusionError::NotImplemented(format!(
                "SqePhysicalCodec: cannot encode plan node '{}'",
                node.name()
            )))
        }
    }

    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let encoded: EncodedDistributedScan = serde_json::from_slice(buf)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let schema_bytes = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            &encoded.schema_proto_b64,
        )
        .map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Schema base64 decode failed: {e}"
            ))))
        })?;

        let proto_schema = protobuf::Schema::decode(schema_bytes.as_slice()).map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Schema proto decode failed: {e}"
            ))))
        })?;

        let schema = datafusion::arrow::datatypes::Schema::try_from(&proto_schema).map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::other(format!(
                "Schema conversion failed: {e}"
            ))))
        })?;

        Ok(Arc::new(DistributedScanExec::new(
            encoded.scan_tasks,
            encoded.worker_urls,
            Arc::new(schema),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::TaskContext;

    fn make_task(id: &str) -> ScanTask {
        ScanTask {
            fragment_id: id.to_string(),
            data_file_paths: vec!["s3://bucket/file.parquet".to_string()],
            file_sizes_bytes: vec![],
            projected_columns: vec![],
            s3_endpoint: "http://localhost:9000".to_string(),
            s3_region: "us-east-1".to_string(),
            s3_access_key: "key".to_string(),
            s3_secret_key: "secret".to_string(),
            s3_session_token: String::new(),
            s3_path_style: true,
            s3_allow_http: true,
        }
    }

    #[test]
    fn test_roundtrip_distributed_scan_exec() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let original = Arc::new(DistributedScanExec::new(
            vec![make_task("f1"), make_task("f2")],
            vec![
                "http://w1:50052".to_string(),
                "http://w2:50052".to_string(),
            ],
            schema.clone(),
        ));

        let codec = SqePhysicalCodec::new();
        let mut buf = Vec::new();
        codec
            .try_encode(original.clone() as Arc<dyn ExecutionPlan>, &mut buf)
            .expect("encode failed");

        let ctx = Arc::new(TaskContext::default());
        let decoded = codec
            .try_decode(&buf, &[], &ctx)
            .expect("decode failed");

        let decoded_scan = decoded
            .as_any()
            .downcast_ref::<DistributedScanExec>()
            .expect("Expected DistributedScanExec");

        assert_eq!(decoded_scan.scan_tasks().len(), 2);
        assert_eq!(decoded_scan.worker_urls(), &["http://w1:50052", "http://w2:50052"]);
        assert_eq!(*decoded_scan.schema(), *schema);
    }

    #[test]
    fn test_encode_unknown_node_returns_error() {
        use datafusion::physical_plan::empty::EmptyExec;
        let schema = Arc::new(Schema::empty());
        let empty = Arc::new(EmptyExec::new(schema));
        let codec = SqePhysicalCodec::new();
        let mut buf = Vec::new();
        let result = codec.try_encode(empty as Arc<dyn ExecutionPlan>, &mut buf);
        assert!(result.is_err());
    }
}
