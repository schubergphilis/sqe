//! Writes into a Hive external table.
//!
//! Cycle 4. The Iceberg write path (`write_handler`) is not reusable here and
//! is not reused: it ends in `commit_written_files`, an atomic
//! `rewrite_files`/`fast_append` against table metadata. A Hive external table
//! has no metadata pointer to swap -- its content is whatever is in the
//! directory -- so there is nothing for that commit to do.
//!
//! What replaces it is DataFusion's own listing-table sink. `HiveTableProvider`
//! delegates `insert_into` to its inner `ListingTable`, and DataFusion 55's
//! `ListingTable::insert_into` carries `table_partition_cols` into
//! `FileSinkConfig`, so the `k=v/` fan-out already exists upstream. This module
//! therefore routes rather than writes: it plans the statement against the
//! session context, which is what makes `insert_into` get called at all.
//!
//! ## The visibility contract, up front
//!
//! Files land in their final partition directories. A single object's
//! appearance is atomic on an object store, so no reader ever sees a torn file,
//! but a MULTI-file write is not atomic and a concurrent reader may see a
//! subset of it. That is the format's contract, not a gap in this module: every
//! other engine reading the directory decides the table's content the same way.
//! A caller who needs snapshot isolation on write needs an Iceberg table.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use sqlparser::ast::Statement;

use sqe_core::SqeError;

/// One-row `count` result, matching what the Iceberg insert path returns so a
/// client cannot tell which engine path served the statement.
fn rows_written(n: u64) -> Result<Vec<RecordBatch>, SqeError> {
    let schema = Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]));
    let col: ArrayRef = Arc::new(UInt64Array::from(vec![n]));
    RecordBatch::try_new(schema, vec![col])
        .map(|b| vec![b])
        .map_err(|e| SqeError::Execution(format!("hive insert result: {e}")))
}

/// Reject `INSERT OVERWRITE` against a Hive external table, with the reason.
///
/// Matched on the parsed statement's `overwrite` flag rather than left to the
/// sink. **The failure to prevent is not the refusal, it is an overwrite
/// reaching the sink unnoticed and being executed as an append**, which doubles
/// the data and returns success. That is the same class of silent wrong answer
/// the whole Hive program has been guarding against.
///
/// Deferred rather than unimplementable: without an atomic commit an overwrite
/// is delete-then-write, and an interruption between the two loses the
/// partition. That is real data loss from a statement that reads as routine, so
/// it does not ship alongside the first working append.
pub fn reject_overwrite(stmt: &Statement) -> Result<(), SqeError> {
    let overwrite = match stmt {
        Statement::Insert(ins) => ins.overwrite || ins.replace_into,
        _ => false,
    };
    if overwrite {
        return Err(SqeError::NotImplemented(
            "hive_external: INSERT OVERWRITE is not supported for Hive external tables. \
             An overwrite would have to delete the partition's objects and then write new \
             ones, and no object store can make those two steps atomic, so an interruption \
             between them loses the partition. Use an Iceberg table for overwrite \
             semantics, or DROP and recreate the partition directory deliberately."
                .to_string(),
        ));
    }
    Ok(())
}

/// Plan and execute an `INSERT INTO` against a Hive external table.
///
/// `sql` is the original statement text. It is planned through the session
/// context on purpose: DataFusion's own INSERT planning is what calls
/// `TableProvider::insert_into`, and reimplementing the fan-out here would mean
/// a second writer that has to agree with the read path's file schema.
pub async fn handle_insert(
    ctx: &SessionContext,
    stmt: &Statement,
    sql: &str,
) -> Result<Vec<RecordBatch>, SqeError> {
    reject_overwrite(stmt)?;

    // Pin `keep_partition_by_columns = false` rather than inheriting the
    // default. Partition columns are directory-encoded, and the read path
    // derives its file schema from the manifest's declared column list with the
    // partition columns EXCLUDED. A data file that also contained them would
    // have more fields than the reader expects -- and the failure would surface
    // on the next SELECT, not on this write. Relying on a default for something
    // that breaks reads is relying on the wrong thing.
    ctx.state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .keep_partition_by_columns = false;

    let df = ctx
        .sql(sql)
        .await
        .map_err(|e| SqeError::Execution(format!("hive insert: planning failed: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| SqeError::Execution(format!("hive insert: {e}")))?;

    // DataFusion's insert returns a one-row count. Read it rather than
    // re-deriving one, so the number reported is the number the sink wrote.
    let written = batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<UInt64Array>())
        .filter(|a| a.len() == 1)
        .map(|a| a.value(0))
        .unwrap_or(0);

    rows_written(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&GenericDialect {}, sql)
            .expect("parse")
            .remove(0)
    }

    #[test]
    fn a_plain_insert_is_allowed() {
        assert!(reject_overwrite(&parse("INSERT INTO t SELECT 1")).is_ok());
    }

    #[test]
    fn insert_overwrite_is_refused_and_says_why_it_cannot_be_atomic() {
        let err = reject_overwrite(&parse("INSERT OVERWRITE t SELECT 1"))
            .expect_err("overwrite must be refused");
        let msg = err.to_string();
        // The reason has to be in the message. "Not supported" alone sends the
        // user looking for a flag to turn on.
        assert!(msg.contains("atomic"), "the reason must be stated: {msg}");
        assert!(
            msg.contains("Iceberg"),
            "the message must name the alternative: {msg}"
        );
    }

    #[test]
    fn replace_into_is_refused_too() {
        // `REPLACE INTO` parses to the same statement with `replace_into` set
        // rather than `overwrite`. Checking only `overwrite` would let this
        // reach the sink and append.
        let err = reject_overwrite(&parse("REPLACE INTO t SELECT 1"));
        assert!(err.is_err(), "REPLACE INTO must not fall through to an append");
    }
}
