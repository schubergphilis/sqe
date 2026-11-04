//! Issue #788: the coordinator's spill settings reach DataFusion.
//! `coordinator.spill_compression` was logged but never applied (spills were
//! written uncompressed), and the spill directory had no SQE cap: DataFusion's
//! 100 GB default applied, and its refusal named a DataFusion setting no
//! operator can set.
//!
//! Uses the Hive CSV tables of `join_fallback_e2e` (1.5M rows, 16 MB cap),
//! so a full sort of one of them spills.

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::RecordBatch;
use sqe_core::config::FlightCompression;

use crate::join_fallback_e2e::fixture_with;

/// Sorts all of `lake.t.a` (more than the 16 MB cap, so it spills) and
/// returns one row, which the buffered path can hold under the cap.
const SORT: &str = "SELECT COUNT(*) FROM (SELECT row_number() OVER (ORDER BY x DESC, k) AS rn \
                    FROM lake.t.a) s WHERE rn % 2 = 0";

/// Room for the sort's merge phase, which fails at the fixture's 16 MB, while
/// the sorted rows still do not fit, so the sort spills.
const SORT_CAP: &str = "32MB";

const SPILL_CODEC: &str = "SELECT value FROM information_schema.df_settings \
                           WHERE name = 'datafusion.execution.spill_compression'";

fn single_string(batches: &[RecordBatch]) -> String {
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("one result row");
    batch.column(0).as_string::<i32>().value(0).to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn spill_compression_reaches_the_session() {
    for (setting, codec) in [
        (None, "lz4_frame"),
        (Some(FlightCompression::Zstd), "zstd"),
        (Some(FlightCompression::None), "uncompressed"),
    ] {
        let f = fixture_with(|config| {
            if let Some(setting) = setting {
                config.coordinator.spill_compression = setting;
            }
        })
        .await;
        let batches = f
            .handler
            .execute(&f.session, SPILL_CODEC, None)
            .await
            .expect("read df_settings");
        assert_eq!(single_string(&batches), codec, "{setting:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spill_past_spill_max_bytes_fails_naming_the_setting() {
    let f = fixture_with(|config| {
        config.query.max_query_memory = SORT_CAP.to_string();
        config.coordinator.spill_max_bytes = "1MB".to_string();
    })
    .await;
    let err = f
        .handler
        .execute(&f.session, SORT, None)
        .await
        .expect_err("a sort spilling past 1 MB must fail");
    assert_eq!(
        err.error_code(),
        sqe_core::SqeErrorCode::ResourceExhausted,
        "{err}"
    );
    let msg = err.to_string();
    assert!(msg.contains("coordinator.spill_max_bytes"), "{msg}");
    assert!(!msg.contains("max_temp_directory_size"), "{msg}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_same_sort_completes_under_the_default_cap() {
    let f = fixture_with(|config| config.query.max_query_memory = SORT_CAP.to_string()).await;
    let batches = f
        .handler
        .execute(&f.session, SORT, None)
        .await
        .expect("the sort spills and completes under the 100 GB default");
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("one result row");
    let even_rows = batch.column(0).as_primitive::<Int64Type>().value(0);
    assert_eq!(even_rows, 750_000);
}
