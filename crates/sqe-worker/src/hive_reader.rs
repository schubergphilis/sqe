//! Worker-side CSV and line-delimited JSON readers for Hive external tables.
//!
//! Cycle 5, task 5. Until this existed the worker built every reader through
//! `ParquetRecordBatchStreamBuilder`, so `servability` refused any non-parquet
//! Hive table and every CSV table fell back to coordinator-local execution.
//!
//! ## The schema is declared, never inferred
//!
//! Both readers are handed the file schema from the descriptor
//! ([`crate::hive_partition::file_schema_of`]) and neither is allowed to infer
//! one. Inference is the mechanism behind two of the failures design.md names:
//!
//! - two files in one table disagreeing on a column's type, because each was
//!   inferred from its own contents;
//! - a header row counted as data, because inference consumed it as the type
//!   sample and the reader then read it again.
//!
//! With a declared schema, an unhonored `skip.header.line.count` turns the
//! header into either a parse error or a row of NULLs. The NULL case is silent,
//! which is why the differential tests assert row COUNTS and not only schemas.
//!
//! This also matches how the coordinator-local path binds: `file_schema_of` in
//! `hive/provider.rs` builds the schema from the manifest's declared column
//! list, all fields nullable, and the CSV reader binds by ordinal against it.
//! Two readers, one binding rule.
//!
//! ## One task per file, read whole
//!
//! Neither reader splits a file, and both read the object into memory before
//! decoding. See the cycle-5 design note "Why CSV and JSON are one task per
//! file" for the reasoning; the short version is that neither format has row
//! groups, so a byte-range split needs a record-boundary scan the coordinator
//! cannot do from a listing, and a compressed file needs the whole stream
//! regardless.
//!
//! The consequence is real and is not hidden: a Hive CSV task's peak residency
//! includes the whole encoded object, where a parquet task's includes only the
//! ranges it fetched. The caller charges the object's size against the scan
//! budget for exactly that reason.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::stream::{self, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};

/// How many rows a reader decodes per batch.
///
/// Matches the parquet path's effective batch size closely enough that the
/// scan budget sees comparable per-batch charges across formats. A much larger
/// value would make the first `acquire` on a tight budget fail for a reason
/// that has nothing to do with the data.
const READER_BATCH_SIZE: usize = 8192;

/// Fetch one object whole.
///
/// Returns the bytes and their length, so the caller can charge the same
/// number it is actually holding.
async fn fetch_whole(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
) -> anyhow::Result<(bytes::Bytes, u64)> {
    let got = store
        .get(path)
        .await
        .map_err(|e| anyhow::anyhow!("hive reader: fetching `{path}`: {e}"))?;
    let bytes = got
        .bytes()
        .await
        .map_err(|e| anyhow::anyhow!("hive reader: reading `{path}`: {e}"))?;
    let len = bytes.len() as u64;
    Ok((bytes, len))
}

/// Open a CSV data file as a batch stream.
///
/// `has_header` comes from the manifest's `skip.header.line.count`, resolved by
/// `serde_map::resolve_format`, so it is the table's declaration rather than a
/// guess about the file.
///
/// `quote` and `escape` are optional in the descriptor because Hive's SerDe
/// parameters are optional. Absent means the reader's own default is used
/// rather than a substitute character being invented: `arrow`'s CSV reader
/// defaults to `"` for quoting and no escape, which is what a Hive table with
/// no `quoteChar`/`escapeChar` in its SerDe properties means.
pub async fn open_csv_stream(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_schema: SchemaRef,
    delimiter: u8,
    quote: Option<u8>,
    escape: Option<u8>,
    has_header: bool,
) -> anyhow::Result<(
    std::pin::Pin<Box<dyn futures::Stream<Item = anyhow::Result<RecordBatch>> + Send>>,
    SchemaRef,
    u64,
)> {
    let (bytes, len) = fetch_whole(store, path).await?;

    let mut builder = arrow::csv::ReaderBuilder::new(Arc::clone(&file_schema))
        .with_header(has_header)
        .with_delimiter(delimiter)
        .with_batch_size(READER_BATCH_SIZE);
    if let Some(q) = quote {
        builder = builder.with_quote(q);
    }
    if let Some(e) = escape {
        builder = builder.with_escape(e);
    }

    let reader = builder
        .build(Cursor::new(bytes))
        .map_err(|e| anyhow::anyhow!("hive csv reader for `{path}`: {e}"))?;

    // The reader is a blocking `Iterator`. Collected eagerly rather than
    // wrapped in `spawn_blocking` per batch: the whole object is already in
    // memory, so decoding does no I/O and cannot block on the network, and the
    // per-batch spawn would cost more than the decode.
    //
    // The batches are still yielded one at a time downstream, which is what the
    // scan budget admits against.
    let batches: Vec<anyhow::Result<RecordBatch>> = reader
        .map(|r| r.map_err(|e| anyhow::anyhow!("hive csv `{path}`: {e}")))
        .collect();

    Ok((
        Box::pin(stream::iter(batches).boxed()),
        file_schema,
        len,
    ))
}

/// Open a line-delimited JSON data file as a batch stream.
///
/// Line-delimited specifically, which is what Hive's `JsonSerDe` and Athena's
/// `org.openx.data.jsonserde.JsonSerDe` write: one object per line, no
/// enclosing array. `arrow`'s JSON reader in this configuration expects exactly
/// that, so a JSON array file fails to parse rather than being read wrongly.
///
/// A field the declared schema does not name is ignored, and a declared field
/// absent from a line becomes NULL. That is the same tolerance the
/// coordinator-local path's reader has, and it is what makes a Hive JSON table
/// readable at all -- the format carries no column list.
pub async fn open_json_stream(
    store: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    file_schema: SchemaRef,
) -> anyhow::Result<(
    std::pin::Pin<Box<dyn futures::Stream<Item = anyhow::Result<RecordBatch>> + Send>>,
    SchemaRef,
    u64,
)> {
    let (bytes, len) = fetch_whole(store, path).await?;

    let reader = arrow::json::ReaderBuilder::new(Arc::clone(&file_schema))
        .with_batch_size(READER_BATCH_SIZE)
        .build(std::io::BufReader::new(Cursor::new(bytes)))
        .map_err(|e| anyhow::anyhow!("hive json reader for `{path}`: {e}"))?;

    let batches: Vec<anyhow::Result<RecordBatch>> = reader
        .map(|r| r.map_err(|e| anyhow::anyhow!("hive json `{path}`: {e}")))
        .collect();

    Ok((
        Box::pin(stream::iter(batches).boxed()),
        file_schema,
        len,
    ))
}
