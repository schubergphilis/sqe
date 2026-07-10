//! Fixture-based regression tests that pin our codec against the byte form
//! emitted by a real `duckdb 1.5.3` running `CALL quack_serve(...)`.
//!
//! The fixtures in `tests/fixtures/` were captured by
//! `examples/capture_upstream.rs`. They are checked in so CI can assert
//! interop without having a DuckDB binary in the test image.

use std::fs;
use std::path::PathBuf;

use sqe_quack_wire::data_chunk::{LogicalTypeId, VectorData};
use sqe_quack_wire::message::{decode_message, MessageType, QuackMessage};

fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    fs::read(&path).unwrap_or_else(|e| panic!("read fixture {}: {}", path.display(), e))
}

#[test]
fn decodes_real_duckdb_connection_response() {
    let bytes = fixture("connection_response.bin");
    let (header, body) = decode_message(&bytes).expect("decode upstream ConnectionResponse");
    assert_eq!(header.r#type, MessageType::ConnectionResponse);
    assert!(
        !header.connection_id.is_empty(),
        "server assigned a connection_id"
    );
    assert!(
        header.client_query_id.is_none(),
        "no client_query_id was sent"
    );
    match body {
        QuackMessage::ConnectionResponse(c) => {
            assert_eq!(c.quack_version, 1);
            assert!(c.server_duckdb_version.starts_with('v'));
            assert!(!c.server_platform.is_empty());
        }
        other => panic!("expected ConnectionResponse, got {other:?}"),
    }
}

#[test]
fn decodes_real_duckdb_prepare_response_select_two_columns() {
    // PrepareRequest was: `SELECT 1 AS a, 'hello' AS b`
    let bytes = fixture("prepare_response_select_1.bin");
    let (header, body) = decode_message(&bytes).expect("decode upstream PrepareResponse");
    assert_eq!(header.r#type, MessageType::PrepareResponse);

    let resp = match body {
        QuackMessage::PrepareResponse(r) => r,
        other => panic!("expected PrepareResponse, got {other:?}"),
    };

    // Schema
    assert_eq!(resp.result_names, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(resp.result_types.len(), 2);
    assert_eq!(resp.result_types[0].id, LogicalTypeId::Integer);
    assert_eq!(resp.result_types[1].id, LogicalTypeId::Varchar);

    // DuckDB omits needs_more_fetch when false (WritePropertyWithDefault) —
    // verify the decoder reconstructs the default.
    assert!(
        !resp.needs_more_fetch,
        "default false reconstructed from absent field"
    );

    // One DataChunk with one row, two columns
    assert_eq!(resp.results.len(), 1);
    let chunk = &resp.results[0];
    assert_eq!(chunk.row_count, 1);
    assert_eq!(chunk.columns.len(), 2);

    // Column 0: INT32 column "a" with value 1
    assert_eq!(chunk.columns[0].logical_type.id, LogicalTypeId::Integer);
    match &chunk.columns[0].data {
        VectorData::Fixed(bytes) => {
            assert_eq!(
                bytes,
                &[0x01, 0x00, 0x00, 0x00],
                "INT32 value 1 in little-endian"
            );
        }
        other => panic!("expected Fixed VectorData for INT column, got {other:?}"),
    }

    // Column 1: VARCHAR column "b" with value "hello"
    assert_eq!(chunk.columns[1].logical_type.id, LogicalTypeId::Varchar);
    match &chunk.columns[1].data {
        VectorData::Strings(values) => {
            assert_eq!(values, &vec![Some("hello".to_string())]);
        }
        other => panic!("expected Strings VectorData for VARCHAR column, got {other:?}"),
    }
}
