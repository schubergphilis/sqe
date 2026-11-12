//! The Flight SQL side of bulk ingest (`DoPut` + `CommandStatementIngest`):
//! mapping the wire `TableDefinitionOptions` onto the write path's
//! [`IngestMode`], and reading the schema the upload announced.
//!
//! The mapping lives here so `sqe-write` needs no `arrow-flight` dependency.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::sql::{TableDefinitionOptions, TableExistsOption, TableNotExistOption};
use arrow_schema::SchemaRef;
use futures::stream::BoxStream;
use futures::StreamExt;
use sqe_core::snapshot_properties::SnapshotProperties;
use sqe_write::write_handler::{IngestMode, OnExisting, OnMissing};
use tonic::Status;

/// Map the request's `TableDefinitionOptions` onto an [`IngestMode`].
///
/// ADBC's four ingest modes arrive as: `append` = (FAIL, APPEND),
/// `create` = (CREATE, FAIL), `create_append` = (CREATE, APPEND),
/// `replace` = (CREATE, REPLACE). Absent options, and an `UNSPECIFIED` value
/// in either field, mean what the server did before the options were read:
/// fail on a missing table, append to an existing one. A value outside the
/// enum is the client's error.
pub(crate) fn ingest_mode(options: Option<&TableDefinitionOptions>) -> Result<IngestMode, Status> {
    let Some(options) = options else {
        return Ok(IngestMode::default());
    };
    let on_missing = match TableNotExistOption::try_from(options.if_not_exist) {
        Ok(TableNotExistOption::Unspecified) | Ok(TableNotExistOption::Fail) => OnMissing::Fail,
        Ok(TableNotExistOption::Create) => OnMissing::Create,
        Err(_) => {
            return Err(Status::invalid_argument(format!(
                "Unknown table_definition_options.if_not_exist value {}",
                options.if_not_exist
            )))
        }
    };
    let on_existing = match TableExistsOption::try_from(options.if_exists) {
        Ok(TableExistsOption::Unspecified) | Ok(TableExistsOption::Append) => OnExisting::Append,
        Ok(TableExistsOption::Fail) => OnExisting::Fail,
        Ok(TableExistsOption::Replace) => OnExisting::Replace,
        Err(_) => {
            return Err(Status::invalid_argument(format!(
                "Unknown table_definition_options.if_exists value {}",
                options.if_exists
            )))
        }
    };
    Ok(IngestMode {
        on_missing,
        on_existing,
    })
}

/// The upload as the write path takes it: the schema the client announced
/// (read only when `need_schema`) and the batches, with none lost to the peek.
pub(crate) struct PeekedUpload {
    pub(crate) schema: Option<SchemaRef>,
    pub(crate) batches: BoxStream<'static, Result<RecordBatch, FlightError>>,
}

/// Read the schema an upload announced, without losing a batch.
///
/// The Flight decoder only learns the schema when it has read up to the first
/// batch (or the end of an empty stream), so this pulls one item and puts it
/// back in front of the rest. When `need_schema` is false the stream is passed
/// through untouched: an ingest that cannot create a table never needs it.
///
/// A stream that fails before it announces a schema is the client's error.
pub(crate) async fn peek_upload_schema(
    mut stream: FlightRecordBatchStream,
    need_schema: bool,
) -> Result<PeekedUpload, Status> {
    if !need_schema {
        return Ok(PeekedUpload {
            schema: None,
            batches: stream.boxed(),
        });
    }
    let first = stream.next().await;
    let schema = stream.schema().cloned();
    if let (None, Some(Err(e))) = (&schema, &first) {
        return Err(Status::invalid_argument(format!(
            "The ingest stream failed before it announced a schema: {e}"
        )));
    }
    let batches = futures::stream::iter(first).chain(stream).boxed();
    Ok(PeekedUpload { schema, batches })
}

/// What an ingest commits under: the client's transaction, if any, and the
/// snapshot properties resolved for this request.
pub(crate) struct IngestScope {
    pub(crate) txn: Option<Arc<sqe_catalog::txn::TxnScope>>,
    pub(crate) snapshot_properties: Option<Arc<SnapshotProperties>>,
}

/// The option prefix Spark uses for per-write snapshot properties.
const SNAPSHOT_PROPERTY_PREFIX: &str = "snapshot-property.";

/// The snapshot properties of one ingest request: `base` (the transaction's or
/// the session's) with the request's `snapshot-property.<key>` options laid
/// over it, the request winning per key.
///
/// The prefix matches without regard to ASCII case, so a mistyped
/// `Snapshot-Property.x` is applied or refused, never dropped. Every other
/// option stays ignored, as before. A refused key or value is an
/// `InvalidArgument` raised before the upload is read, so no data file is
/// written for it.
pub(crate) fn ingest_snapshot_properties(
    options: &HashMap<String, String>,
    base: Option<Arc<SnapshotProperties>>,
) -> Result<Option<Arc<SnapshotProperties>>, Status> {
    let prefix_len = SNAPSHOT_PROPERTY_PREFIX.len();
    let pairs: Vec<(String, String)> = options
        .iter()
        .filter(|(name, _)| {
            name.get(..prefix_len)
                .is_some_and(|p| p.eq_ignore_ascii_case(SNAPSHOT_PROPERTY_PREFIX))
        })
        .map(|(name, value)| (name[prefix_len..].to_string(), value.clone()))
        .collect();
    if pairs.is_empty() {
        return Ok(base);
    }
    let invalid = |e: sqe_core::SqeError| Status::invalid_argument(e.to_string());
    let overrides = SnapshotProperties::try_new(pairs).map_err(invalid)?;
    let merged = match base {
        Some(base) => base.merged_with(&overrides).map_err(invalid)?,
        None => overrides,
    };
    Ok(Some(Arc::new(merged)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::Int32Array;
    use arrow_flight::encode::FlightDataEncoderBuilder;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn options(
        if_not_exist: TableNotExistOption,
        if_exists: TableExistsOption,
    ) -> TableDefinitionOptions {
        TableDefinitionOptions {
            if_not_exist: if_not_exist as i32,
            if_exists: if_exists as i32,
        }
    }

    fn mode(on_missing: OnMissing, on_existing: OnExisting) -> IngestMode {
        IngestMode {
            on_missing,
            on_existing,
        }
    }

    #[test]
    fn every_adbc_mode_maps_to_its_ingest_mode() {
        use TableExistsOption as E;
        use TableNotExistOption as N;
        // ADBC: append, create, create_append, replace.
        for (name, opts, want) in [
            (
                "append",
                options(N::Fail, E::Append),
                mode(OnMissing::Fail, OnExisting::Append),
            ),
            (
                "create",
                options(N::Create, E::Fail),
                mode(OnMissing::Create, OnExisting::Fail),
            ),
            (
                "create_append",
                options(N::Create, E::Append),
                mode(OnMissing::Create, OnExisting::Append),
            ),
            (
                "replace",
                options(N::Create, E::Replace),
                mode(OnMissing::Create, OnExisting::Replace),
            ),
        ] {
            assert_eq!(ingest_mode(Some(&opts)).unwrap(), want, "{name}");
        }
    }

    #[test]
    fn absent_and_unspecified_options_keep_todays_behaviour() {
        use TableExistsOption as E;
        use TableNotExistOption as N;
        let today = mode(OnMissing::Fail, OnExisting::Append);
        assert_eq!(ingest_mode(None).unwrap(), today);
        assert_eq!(
            ingest_mode(Some(&options(N::Unspecified, E::Unspecified))).unwrap(),
            today
        );
        // Each field is read on its own: one UNSPECIFIED value does not
        // discard the other.
        assert_eq!(
            ingest_mode(Some(&options(N::Create, E::Unspecified))).unwrap(),
            mode(OnMissing::Create, OnExisting::Append)
        );
        assert_eq!(
            ingest_mode(Some(&options(N::Unspecified, E::Replace))).unwrap(),
            mode(OnMissing::Fail, OnExisting::Replace)
        );
    }

    #[test]
    fn a_value_outside_the_enums_is_an_invalid_argument() {
        let bad_missing = TableDefinitionOptions {
            if_not_exist: 99,
            if_exists: TableExistsOption::Append as i32,
        };
        let err = ingest_mode(Some(&bad_missing)).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        let bad_existing = TableDefinitionOptions {
            if_not_exist: TableNotExistOption::Create as i32,
            if_exists: 99,
        };
        let err = ingest_mode(Some(&bad_existing)).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    fn props(pairs: &[(&str, &str)]) -> Arc<SnapshotProperties> {
        Arc::new(
            SnapshotProperties::try_new(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())))
                .unwrap(),
        )
    }

    fn opts(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn no_snapshot_property_option_keeps_the_base() {
        let base = props(&[("copy.run", "1")]);
        let got = ingest_snapshot_properties(&opts(&[("other", "x")]), Some(base.clone())).unwrap();
        assert!(Arc::ptr_eq(&got.unwrap(), &base));
        assert!(ingest_snapshot_properties(&opts(&[]), None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn options_lay_over_the_base_and_win_per_key() {
        let base = props(&[("copy.run", "1"), ("copy.table", "t")]);
        let got = ingest_snapshot_properties(
            &opts(&[
                ("snapshot-property.copy.run", "42"),
                ("Snapshot-Property.loader", "adbc"),
                ("unrelated", "ignored"),
            ]),
            Some(base),
        )
        .unwrap()
        .unwrap();
        let map = got.as_map();
        assert_eq!(map.len(), 3);
        assert_eq!(map["copy.run"], "42");
        assert_eq!(map["copy.table"], "t");
        assert_eq!(map["loader"], "adbc");
    }

    #[test]
    fn a_refused_option_is_an_invalid_argument() {
        for (name, value) in [
            ("snapshot-property.operation", "append"),
            ("snapshot-property.", "empty key"),
            ("snapshot-property.wap.id", "x"),
            ("snapshot-property.ok", "bad\u{7}value"),
        ] {
            let err = ingest_snapshot_properties(&opts(&[(name, value)]), None).unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument, "{name}");
        }
    }

    #[test]
    fn the_merged_set_is_held_to_the_caps() {
        let base = props(
            &(0..32)
                .map(|i| (format!("b{i}"), "v".to_string()))
                .collect::<Vec<_>>()
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect::<Vec<_>>(),
        );
        let err =
            ingest_snapshot_properties(&opts(&[("snapshot-property.extra", "v")]), Some(base))
                .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap()
    }

    /// The upload a client sends: batches through the Flight encoder, read
    /// back through the decoder the DoPut handler uses.
    fn upload(batches: Vec<RecordBatch>, announce: Option<SchemaRef>) -> FlightRecordBatchStream {
        let mut builder = FlightDataEncoderBuilder::new();
        if let Some(schema) = announce {
            builder = builder.with_schema(schema);
        }
        let data = builder.build(futures::stream::iter(batches.into_iter().map(Ok)));
        FlightRecordBatchStream::new_from_flight_data(data)
    }

    #[tokio::test]
    async fn peeking_keeps_every_batch_and_reports_the_schema() {
        let b = batch();
        let schema = b.schema();
        let peeked = peek_upload_schema(upload(vec![b.clone(), b.clone()], None), true)
            .await
            .unwrap();
        assert_eq!(peeked.schema, Some(schema));
        let rows: usize = peeked
            .batches
            .map(|r| r.unwrap().num_rows())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .sum();
        assert_eq!(rows, 4, "no batch is lost to the peek");
    }

    #[tokio::test]
    async fn an_empty_upload_reports_its_schema_when_the_client_announces_one() {
        let schema = batch().schema();
        let peeked = peek_upload_schema(upload(vec![], Some(schema.clone())), true)
            .await
            .unwrap();
        assert_eq!(peeked.schema, Some(schema));
        assert_eq!(peeked.batches.count().await, 0);
    }

    #[tokio::test]
    async fn an_empty_upload_without_a_schema_reports_none() {
        // The Flight encoder sends no schema for a stream with no batches
        // unless the client set one: the write path then refuses a create.
        let peeked = peek_upload_schema(upload(vec![], None), true)
            .await
            .unwrap();
        assert_eq!(peeked.schema, None);
    }

    #[tokio::test]
    async fn an_upload_that_fails_before_its_schema_is_an_invalid_argument() {
        let failing = futures::stream::iter(vec![Err(FlightError::protocol("boom"))]);
        let stream = FlightRecordBatchStream::new_from_flight_data(failing);
        let err = peek_upload_schema(stream, true)
            .await
            .err()
            .expect("an error");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn without_need_the_stream_is_passed_through_unread() {
        let b = batch();
        let peeked = peek_upload_schema(upload(vec![b], None), false)
            .await
            .unwrap();
        assert!(peeked.schema.is_none());
        assert_eq!(peeked.batches.count().await, 1);
    }
}
