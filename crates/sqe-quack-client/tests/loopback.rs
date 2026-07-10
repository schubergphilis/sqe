//! Loopback integration test: drive a real `sqe-server` instance (must be
//! running on the host) using `QuackClient`, and confirm the round-trip
//! ConnectionRequest -> PrepareRequest -> RecordBatch path works end-to-end.
//!
//! The test is gated on an env var because it requires:
//!   - sqe-server bound on 9494 (`tests/sqe-quack-test.toml`)
//!   - a Polaris bearer token reachable at `$QUACK_TOKEN`
//!
//! Skip-by-default keeps `cargo test` green on CI runners that don't have
//! the live stack up; run locally with:
//!
//!     QUACK_RUN_LOOPBACK=1 QUACK_TOKEN=$(curl ...) \
//!         cargo test -p sqe-quack-client --test loopback
//!
//! When the env var isn't set the test prints a skip reason and exits 0.

use sqe_quack_client::QuackClient;

const QUACK_URI: &str = "quack:localhost:9494";

fn should_run() -> Option<String> {
    if std::env::var("QUACK_RUN_LOOPBACK").is_err() {
        eprintln!("skipping: set QUACK_RUN_LOOPBACK=1 to drive a live sqe-server");
        return None;
    }
    std::env::var("QUACK_TOKEN")
        .ok()
        .or_else(|| Some(String::new()))
}

#[test]
fn select_int_and_varchar() {
    let Some(token) = should_run() else {
        return;
    };
    let mut client = QuackClient::connect(QUACK_URI, Some(&token)).expect("connect");
    let result = client
        .execute("SELECT 42 AS id, 'alice' AS name")
        .expect("execute");
    assert_eq!(result.names, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(result.batches.len(), 1);
    let batch = &result.batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 2);
    // Column 0: id should be an i64 with value 42 (DataFusion widens int
    // literals to bigint).
    let id = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("id is Int64");
    assert_eq!(id.value(0), 42);
    // Column 1: name should be a varchar.
    let name = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .expect("name is String");
    assert_eq!(name.value(0), "alice");
}

#[test]
fn select_decimal_round_trips_via_arrow() {
    let Some(token) = should_run() else {
        return;
    };
    let mut client = QuackClient::connect(QUACK_URI, Some(&token)).expect("connect");
    let result = client
        .execute("SELECT CAST(1.23 AS DECIMAL(10, 2)) AS price")
        .expect("execute");
    assert_eq!(result.batches.len(), 1);
    let batch = &result.batches[0];
    let price = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Decimal128Array>()
        .expect("price is Decimal128");
    assert_eq!(price.precision(), 10);
    assert_eq!(price.scale(), 2);
    assert_eq!(price.value(0), 123);
}
