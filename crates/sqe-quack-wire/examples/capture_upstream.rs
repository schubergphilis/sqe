//! Capture wire bytes from a live `quack_serve()` DuckDB instance and save them
//! as test fixtures. Used to discover and lock down byte-level compatibility
//! between our codec and the real upstream encoder.
//!
//! Prereq: a `duckdb 1.5.3` running quack_serve() on the address below.
//!     INSTALL quack FROM core_nightly;
//!     LOAD quack;
//!     CALL quack_serve('quack:127.0.0.1:9495', token => 'demo-token', disable_ssl => true);
//!
//! Run with:
//!     cargo run --example capture_upstream -p sqe-quack-wire

use sqe_quack_wire::message::{
    encode_message, ConnectionRequest, MessageHeader, MessageType, PrepareRequest, QuackMessage,
};
use std::error::Error;
use std::fs;
use std::path::Path;

const UPSTREAM: &str = "http://127.0.0.1:9495/quack";
const TOKEN: &str = "demo-token";
const FIXTURES: &str = "crates/sqe-quack-wire/tests/fixtures";

fn hexdump(label: &str, bytes: &[u8]) {
    println!("\n== {} ({} bytes) ==", label, bytes.len());
    for chunk in bytes.chunks(16) {
        let hex: Vec<String> = chunk.iter().map(|b| format!("{b:02x}")).collect();
        let ascii: String = chunk
            .iter()
            .map(|&b| {
                if (0x20..0x7F).contains(&b) {
                    b as char
                } else {
                    '.'
                }
            })
            .collect();
        println!("  {:<48}  {}", hex.join(" "), ascii);
    }
}

fn save(name: &str, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    let path = Path::new(FIXTURES).join(name);
    fs::write(&path, bytes)?;
    println!("  -> wrote {}", path.display());
    Ok(())
}

fn post_quack(
    client: &reqwest::blocking::Client,
    body: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let resp = client
        .post(UPSTREAM)
        .header("content-type", "application/vnd.duckdb")
        .body(body)
        .send()?;
    let status = resp.status();
    let bytes = resp.bytes()?.to_vec();
    if !status.is_success() {
        return Err(format!("HTTP {status}: {} bytes returned", bytes.len()).into());
    }
    Ok(bytes)
}

fn main() -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(FIXTURES)?;
    let client = reqwest::blocking::Client::new();

    // ── 1. ConnectionRequest ────────────────────────────────────────────
    let connect_header = MessageHeader {
        r#type: MessageType::ConnectionRequest,
        connection_id: String::new(),
        client_query_id: None,
    };
    let connect_body = QuackMessage::ConnectionRequest(ConnectionRequest {
        auth_string: TOKEN.to_string(),
        client_duckdb_version: "v1.5.3".to_string(),
        client_platform: "test".to_string(),
        min_supported_quack_version: 1,
        max_supported_quack_version: 1,
    });
    let request_bytes = encode_message(&connect_header, &connect_body);
    hexdump("OUR encoded ConnectionRequest", &request_bytes);
    save("connection_request.bin", &request_bytes)?;

    let response_bytes = post_quack(&client, request_bytes.clone())?;
    hexdump("DuckDB response to ConnectionRequest", &response_bytes);
    save("connection_response.bin", &response_bytes)?;

    // Pull out the connection_id from DuckDB's response so we can reuse it
    // in the PrepareRequest. We do a manual scan rather than going through
    // decode_message in case the response shape itself differs.
    let connection_id = extract_connection_id(&response_bytes)?;
    println!("\nextracted connection_id from DuckDB response: {connection_id:?}");

    // ── 2. PrepareRequest("SELECT 1 AS a") ──────────────────────────────
    let prepare_header = MessageHeader {
        r#type: MessageType::PrepareRequest,
        connection_id: connection_id.clone(),
        client_query_id: Some(1),
    };
    let prepare_body = QuackMessage::PrepareRequest(PrepareRequest {
        sql_query: "SELECT 1 AS a, 'hello' AS b".to_string(),
    });
    let request_bytes = encode_message(&prepare_header, &prepare_body);
    hexdump("OUR encoded PrepareRequest", &request_bytes);
    save("prepare_request.bin", &request_bytes)?;

    let response_bytes = post_quack(&client, request_bytes)?;
    hexdump("DuckDB response to PrepareRequest", &response_bytes);
    save("prepare_response_select_1.bin", &response_bytes)?;

    // ── 3. PrepareRequest("SELECT CAST(... AS DECIMAL(10,2))") ─────────────
    // Exercises the DECIMAL_TYPE_INFO path: LogicalType field 101 should
    // emit a non-null ExtraTypeInfo carrying width=10 and scale=2. The
    // physical width per row is 8 bytes (i64) per DuckDB's tier table.
    let decimal_header = MessageHeader {
        r#type: MessageType::PrepareRequest,
        connection_id: connection_id.clone(),
        client_query_id: Some(2),
    };
    let decimal_body = QuackMessage::PrepareRequest(PrepareRequest {
        sql_query: "SELECT CAST(1.23 AS DECIMAL(10, 2)) AS price".to_string(),
    });
    let request_bytes = encode_message(&decimal_header, &decimal_body);
    save("prepare_request_decimal_10_2.bin", &request_bytes)?;

    let response_bytes = post_quack(&client, request_bytes)?;
    hexdump("DuckDB response to DECIMAL(10,2) prepare", &response_bytes);
    save("prepare_response_decimal_10_2.bin", &response_bytes)?;

    // ── 4. PrepareRequest("SELECT [1, 2, 3]") — LIST<INTEGER> ──────────────
    // Exercises ListTypeInfo: LogicalType field 101 emits ExtraTypeInfo with
    // discriminant=LIST_TYPE_INFO and subclass field 200 carrying a nested
    // LogicalType object. Vector payload uses fields 104 (list_size),
    // 105 (entries: u64 offset + u64 length per row), 106 (child vector).
    let list_header = MessageHeader {
        r#type: MessageType::PrepareRequest,
        connection_id: connection_id.clone(),
        client_query_id: Some(3),
    };
    let list_body = QuackMessage::PrepareRequest(PrepareRequest {
        sql_query: "SELECT [10, 20, 30] AS xs".to_string(),
    });
    let request_bytes = encode_message(&list_header, &list_body);
    save("prepare_request_list_int.bin", &request_bytes)?;
    let response_bytes = post_quack(&client, request_bytes)?;
    hexdump("DuckDB response to LIST<INTEGER>", &response_bytes);
    save("prepare_response_list_int.bin", &response_bytes)?;

    // ── 5. PrepareRequest("SELECT {a: 1, b: 'hi'}") — STRUCT(a INT, b VARCHAR)
    // Exercises StructTypeInfo: ExtraTypeInfo subclass field 200 is a
    // child_list_t<LogicalType> — each entry is a pair object with field 0
    // (name) and field 1 (LogicalType). Vector payload uses field 103
    // ("children": parallel child vectors).
    let struct_header = MessageHeader {
        r#type: MessageType::PrepareRequest,
        connection_id: connection_id.clone(),
        client_query_id: Some(4),
    };
    let struct_body = QuackMessage::PrepareRequest(PrepareRequest {
        sql_query: "SELECT {a: 1, b: 'hi'} AS s".to_string(),
    });
    let request_bytes = encode_message(&struct_header, &struct_body);
    save("prepare_request_struct.bin", &request_bytes)?;
    let response_bytes = post_quack(&client, request_bytes)?;
    hexdump("DuckDB response to STRUCT(a INT, b VARCHAR)", &response_bytes);
    save("prepare_response_struct.bin", &response_bytes)?;

    // ── 6. MAP<VARCHAR, INTEGER> ──────────────────────────────────────────
    // DuckDB stores MAP as LIST<STRUCT<key,value>>; the LogicalType id is
    // MAP (102) but the ExtraTypeInfo carries a ListTypeInfo with a STRUCT
    // child. Same Vector wire as LIST.
    let map_header = MessageHeader {
        r#type: MessageType::PrepareRequest,
        connection_id: connection_id.clone(),
        client_query_id: Some(5),
    };
    let map_body = QuackMessage::PrepareRequest(PrepareRequest {
        sql_query: "SELECT MAP {'a': 1, 'b': 2} AS m".to_string(),
    });
    let request_bytes = encode_message(&map_header, &map_body);
    save("prepare_request_map.bin", &request_bytes)?;
    let response_bytes = post_quack(&client, request_bytes)?;
    hexdump("DuckDB response to MAP<VARCHAR,INTEGER>", &response_bytes);
    save("prepare_response_map.bin", &response_bytes)?;

    // ── 7. ARRAY<INTEGER, 3> ──────────────────────────────────────────────
    // Fixed-size list. ArrayTypeInfo subclass fields are 200 (child_type)
    // and 201 (size). Vector payload uses fields 103 (array_size, u64)
    // and 104 (child vector with array_size * count elements).
    let array_header = MessageHeader {
        r#type: MessageType::PrepareRequest,
        connection_id: connection_id.clone(),
        client_query_id: Some(6),
    };
    let array_body = QuackMessage::PrepareRequest(PrepareRequest {
        sql_query: "SELECT CAST([10, 20, 30] AS INTEGER[3]) AS a".to_string(),
    });
    let request_bytes = encode_message(&array_header, &array_body);
    save("prepare_request_array.bin", &request_bytes)?;
    let response_bytes = post_quack(&client, request_bytes)?;
    hexdump("DuckDB response to ARRAY<INTEGER,3>", &response_bytes);
    save("prepare_response_array.bin", &response_bytes)?;

    // ── 8. ENUM ────────────────────────────────────────────────────────────
    // EnumTypeInfo is hand-written: field 200 = values_count (u64,
    // WriteProperty), field 201 = WriteList<string>. Vector payload is
    // a Fixed buffer of u8/u16/u32 indices per the dict-size tier
    // (here three entries -> u8 / 1 byte per row).
    let enum_header = MessageHeader {
        r#type: MessageType::PrepareRequest,
        connection_id,
        client_query_id: Some(7),
    };
    let enum_body = QuackMessage::PrepareRequest(PrepareRequest {
        sql_query: "SELECT 'green'::ENUM('red', 'green', 'blue') AS color".to_string(),
    });
    let request_bytes = encode_message(&enum_header, &enum_body);
    save("prepare_request_enum.bin", &request_bytes)?;
    let response_bytes = post_quack(&client, request_bytes)?;
    hexdump("DuckDB response to ENUM('red','green','blue')", &response_bytes);
    save("prepare_response_enum.bin", &response_bytes)?;

    Ok(())
}

/// Scan a binary header object for field 2 (connection_id) and return its
/// string value. Lives outside our normal decoder because the goal of this
/// example is to compare wire forms, not to assume they match.
fn extract_connection_id(bytes: &[u8]) -> Result<String, Box<dyn Error>> {
    // Header starts at byte 0. Field IDs are u16 LE. We're looking for field 2.
    let mut i = 0;
    loop {
        if i + 2 > bytes.len() {
            return Err("ran out of bytes scanning header".into());
        }
        let field_id = u16::from_le_bytes([bytes[i], bytes[i + 1]]);
        i += 2;
        if field_id == 0xFFFF {
            // Object terminator before we found field 2.
            return Ok(String::new());
        }
        if field_id == 2 {
            // Next is varint length, then UTF-8 bytes.
            let (len, consumed) = decode_varint(&bytes[i..])?;
            i += consumed;
            let end = i + len as usize;
            if end > bytes.len() {
                return Err("connection_id string truncated".into());
            }
            return Ok(String::from_utf8(bytes[i..end].to_vec())?);
        }
        // Skip this field's value. Without knowing its type we can't, so
        // bail. In practice DuckDB writes field 1 (type, varint) before
        // field 2, so we have one varint to skip.
        if field_id == 1 {
            let (_, consumed) = decode_varint(&bytes[i..])?;
            i += consumed;
            continue;
        }
        return Err(format!("unexpected field {field_id:#06x} while scanning for 2").into());
    }
}

fn decode_varint(input: &[u8]) -> Result<(u64, usize), Box<dyn Error>> {
    let mut value = 0u64;
    let mut shift = 0u32;
    for (i, &byte) in input.iter().enumerate() {
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err("varint overflow".into());
        }
    }
    Err("varint truncated".into())
}
