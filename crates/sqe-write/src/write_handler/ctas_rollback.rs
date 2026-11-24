//! Rollback of a CTAS that failed after `create_table` registered the table.
//! Without it, the next CTAS on the same name hits Polaris's location-conflict
//! check and fails for good. The rollback runs as the caller; one who may
//! create but not drop leaves the entry, and the error then names the table.

use iceberg::table::Table as IcebergTable;
use iceberg::{Catalog, TableIdent};
use sqe_core::{SqeError, SqeErrorCode};
use std::sync::Arc;
use tracing::{info, warn};

/// Best-effort: delete the S3 prefix, then drop the catalog entry without
/// purge. Returns the CTAS error, naming the entry when it stays registered.
pub(super) async fn roll_back(
    catalog: &Arc<dyn Catalog>,
    table: &IcebergTable,
    table_ident: &TableIdent,
    err: SqeError,
) -> SqeError {
    warn!(table = %table_ident, error = %err,
        "CTAS (streaming) failed after create_table; rolling back to prevent orphan");
    let location = table.metadata().location();
    match table.file_io().delete_prefix(location).await {
        Ok(_) => info!(table = %table_ident, location, "CTAS rollback: deleted S3 prefix"),
        Err(e) => warn!(table = %table_ident, location, error = %e,
            "CTAS rollback: failed to delete S3 prefix — orphan data may remain"),
    }
    match catalog.drop_table(table_ident).await {
        Ok(_) => info!(table = %table_ident, "CTAS rollback: dropped catalog entry"),
        Err(e) => {
            warn!(table = %table_ident, error = %e,
                "CTAS rollback: failed to drop catalog entry — orphan registration remains");
            return with_orphan_entry(err, table_ident);
        }
    }
    err
}

/// The CTAS error, naming an entry the rollback could not remove. The code is
/// kept (an internal error, whose text the client never sees, becomes an
/// execution failure with the generic text), so only the message changes.
pub(super) fn with_orphan_entry(err: SqeError, table_ident: &TableIdent) -> SqeError {
    let code = match err.error_code() {
        SqeErrorCode::InternalError => SqeErrorCode::ExecutionFailed,
        code => code,
    };
    let message = format!(
        "{}; the catalog entry for table '{table_ident}' could not be removed \
         and stays registered with no data",
        err.client_message()
    );
    SqeError::sourced(code, message, err)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn orphan(code: SqeErrorCode, msg: &str) -> SqeError {
        let ns = iceberg::NamespaceIdent::new("scratch".to_string());
        with_orphan_entry(
            SqeError::with_code(code, msg),
            &TableIdent::new(ns, "copy".into()),
        )
    }

    #[test]
    fn an_orphan_entry_is_named_and_the_code_kept() {
        let err = orphan(
            SqeErrorCode::AccessDenied,
            "Access denied: nothing was written",
        );
        assert_eq!(err.error_code(), SqeErrorCode::AccessDenied);
        let msg = err.client_message();
        assert!(msg.starts_with("Access denied: nothing was written; the catalog entry for table 'scratch.copy' could not be removed"), "{msg}");
        let source = std::error::Error::source(&err).expect("source kept");
        assert!(source.to_string().contains("nothing was written"));
    }

    #[test]
    fn an_internal_error_names_the_orphan_without_its_detail() {
        let err = orphan(SqeErrorCode::InternalError, "secret path /x/y");
        assert_eq!(err.error_code(), SqeErrorCode::ExecutionFailed);
        let msg = err.client_message();
        assert!(
            msg.contains("'scratch.copy'") && !msg.contains("secret path"),
            "{msg}"
        );
    }
}
