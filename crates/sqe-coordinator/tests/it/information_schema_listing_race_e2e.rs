//! `information_schema.columns` while other sessions run DDL (#890).
//!
//! Every commit invalidated the namespace's table-name snapshot, which
//! `SchemaProvider::table_names()` serves. A commit between the pre-plan
//! refresh and execution left the reader with no names, so DataFusion
//! answered zero rows for a table that was never touched, and dbt's
//! `get_columns_in_relation` rendered an empty MERGE column list.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

const NS: &str = "isr_890";
/// Commits that must land while the reads run, or the test proves nothing.
const MIN_DDL: usize = 100;

async fn column_count(
    handler: &sqe_coordinator::QueryHandler,
    session: &sqe_core::Session,
    table: &str,
) -> Result<usize, String> {
    let sql = format!(
        "SELECT column_name FROM information_schema.columns \
         WHERE table_schema = '{NS}' AND table_name = '{table}'"
    );
    let batches = handler.execute(session, &sql, None).await;
    let batches = batches.map_err(|e| e.to_string())?;
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

/// Four sessions replace tables and views of the namespace in a loop (one of
/// them with schema-changing replaces) while a fifth reads the columns of a
/// table nobody touches and of the table being replaced: every read must
/// return the full column list, never a short one and never an error.
///
/// A schema-changing replace is one commit on the existing table (#892), so
/// the replaced table never leaves the catalog and always has two columns.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs:polaris"]
async fn information_schema_columns_survive_concurrent_replace_890() {
    let (session, handler) = crate::common::setup_handler().await;
    let ns_sql = format!("CREATE SCHEMA IF NOT EXISTS {NS}");
    handler
        .execute(&session, &ns_sql, None)
        .await
        .expect("schema");
    let stable = format!("CREATE OR REPLACE TABLE {NS}.stable AS SELECT 1 AS a, 'x' AS b");
    handler
        .execute(&session, &stable, None)
        .await
        .expect("stable");
    let moving = format!("CREATE OR REPLACE TABLE {NS}.moving AS SELECT 1 AS a, 2 AS b");
    handler
        .execute(&session, &moving, None)
        .await
        .expect("moving");

    let handler = Arc::new(handler);
    let session = Arc::new(session);
    let stop = Arc::new(AtomicBool::new(false));
    let committed = Arc::new(AtomicUsize::new(0));
    let mut churn = Vec::new();
    for i in 0..4 {
        let (h, s) = (Arc::clone(&handler), Arc::clone(&session));
        let (stop, done) = (Arc::clone(&stop), Arc::clone(&committed));
        churn.push(tokio::spawn(async move {
            let mut n = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let sql = match (i, n % 2) {
                    // Same two columns, other types: the replace changes the
                    // schema, so it is not the in-place path.
                    (0, 0) => {
                        format!("CREATE OR REPLACE TABLE {NS}.moving AS SELECT 1 AS a, 2 AS b")
                    }
                    (0, _) => {
                        format!("CREATE OR REPLACE TABLE {NS}.moving AS SELECT 'a' AS a, 'b' AS b")
                    }
                    (_, 0) => format!("CREATE OR REPLACE TABLE {NS}.churn_{i} AS SELECT {n} AS a"),
                    _ => format!("CREATE OR REPLACE VIEW {NS}.churn_v{i} AS SELECT {n} AS a"),
                };
                if h.execute(&s, &sql, None).await.is_ok() {
                    n += 1;
                    done.fetch_add(1, Ordering::Relaxed);
                }
            }
            n
        }));
    }

    // Read until enough commits have landed between reads (a read of an
    // untouched table is far faster than a commit), at least 300 reads and at
    // most 20 000.
    let mut bad = Vec::new();
    let mut reads = 0;
    while reads < 300 || (committed.load(Ordering::Relaxed) < MIN_DDL && reads < 20_000) {
        for table in ["stable", "moving"] {
            match column_count(&handler, &session, table).await {
                Ok(2) => {}
                other => bad.push(format!("read {reads} of {table}: {other:?}")),
            }
        }
        reads += 1;
    }
    stop.store(true, Ordering::Relaxed);
    let mut ddl = 0;
    for c in churn {
        ddl += c.await.expect("churn task");
    }

    for t in ["stable", "moving", "churn_1", "churn_2", "churn_3"] {
        let _ = handler
            .execute(&session, &format!("DROP TABLE IF EXISTS {NS}.{t}"), None)
            .await;
    }
    for v in ["churn_v1", "churn_v2", "churn_v3"] {
        let _ = handler
            .execute(&session, &format!("DROP VIEW IF EXISTS {NS}.{v}"), None)
            .await;
    }

    // Without concurrent commits the test proves nothing.
    assert!(
        ddl >= MIN_DDL,
        "only {ddl} DDL statements committed during {reads} reads"
    );
    assert!(
        bad.is_empty(),
        "{} of {reads} reads were short or failed: {bad:#?}",
        bad.len()
    );
}
