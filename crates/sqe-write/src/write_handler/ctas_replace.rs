//! The planning half of a `CREATE OR REPLACE TABLE ... AS SELECT` whose
//! columns or partitioning differ from the live table's (#892): one commit on
//! the existing table that makes the new schema and spec current and swaps
//! the data, instead of a staged table swapped in with DROP + RENAME.

use iceberg::spec::{FormatVersion, Schema as IcebergSchema, UnboundPartitionSpec};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::ReplaceTableShapeAction;
use iceberg::TableIdent;
use sqe_core::{SqeError, SqeErrorCode};

/// The format version a replace leaves the table at.
///
/// A replace keeps the live table's version unless the statement asks for a
/// higher one or the SELECT needs V3 (a variant column). Asking for a lower
/// one is refused before any data is written: a table cannot be downgraded.
pub(super) fn replace_format_version(
    table_ident: &TableIdent,
    live: FormatVersion,
    requested: Option<FormatVersion>,
    needs_v3: bool,
) -> sqe_core::Result<FormatVersion> {
    if let Some(requested) = requested.filter(|r| *r < live) {
        return Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!(
                "CREATE OR REPLACE cannot lower the format version of '{table_ident}' from \
                 {live} to {requested}; DROP the table first to create it at {requested}"
            ),
        ));
    }
    let needed = if needs_v3 {
        FormatVersion::V3
    } else {
        FormatVersion::V1
    };
    Ok(live.max(requested.unwrap_or(live)).max(needed))
}

/// Refuse a schema- or partition-changing replace that would commit on a
/// branch: the schema and spec are table-wide, so they cannot stay on a
/// branch while `main` keeps the old ones.
pub(super) fn refuse_shape_change_off_main(table_ident: &TableIdent) -> sqe_core::Result<()> {
    let refused = |why: String| {
        Err(SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!(
                "CREATE OR REPLACE of '{table_ident}' changes its columns or partitioning, \
                 which applies to every branch; {why}"
            ),
        ))
    };
    if sqe_catalog::txn::current().is_some() {
        return refused("run it outside the transaction".to_string());
    }
    if let Some(branch) = sqe_catalog::txn::write_branch() {
        return refused(format!(
            "WRITE_BRANCH is '{branch}', so RESET WRITE_BRANCH first"
        ));
    }
    Ok(())
}

/// The shape action that makes `schema` and `spec` (none: unpartitioned)
/// current on `live` at `format_version`, and the table as it reads after
/// it: the view the replace writes its data files through. Fails, before any
/// data is written, when the table metadata cannot take the new shape.
pub(super) fn plan_replace_shape(
    live: &IcebergTable,
    schema: IcebergSchema,
    spec: Option<UnboundPartitionSpec>,
    format_version: FormatVersion,
) -> sqe_core::Result<(ReplaceTableShapeAction, IcebergTable)> {
    let failed = |e: iceberg::Error| {
        SqeError::with_code(
            SqeErrorCode::ExecutionFailed,
            format!("CREATE OR REPLACE cannot apply the new columns or partitioning: {e}"),
        )
    };
    let spec = spec.unwrap_or_else(|| UnboundPartitionSpec::builder().build());
    let action = ReplaceTableShapeAction::new(live, schema, spec)
        .map_err(failed)?
        .with_format_version(format_version);
    let evolved = action.evolved_table(live).map_err(failed)?;
    Ok((action, evolved))
}

#[cfg(test)]
mod tests {
    use super::*;
    use FormatVersion::{V1, V2, V3};

    fn ident() -> TableIdent {
        TableIdent::from_strs(["ns", "t"]).unwrap()
    }

    #[test]
    fn a_replace_keeps_the_version_unless_asked_or_needed() {
        let v =
            |live, requested, needs_v3| replace_format_version(&ident(), live, requested, needs_v3);
        assert_eq!(v(V2, None, false).unwrap(), V2);
        assert_eq!(v(V3, None, false).unwrap(), V3);
        assert_eq!(v(V2, Some(V3), false).unwrap(), V3);
        assert_eq!(v(V2, None, true).unwrap(), V3);
        assert_eq!(v(V1, Some(V2), false).unwrap(), V2);
    }

    #[test]
    fn a_lower_requested_version_is_refused_naming_both() {
        let err = replace_format_version(&ident(), V3, Some(V2), false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("from v3 to v2"), "{err}");
    }
}
