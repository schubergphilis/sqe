//! Capture wire bytes from a remote DuckDB `quack_serve()` for a single
//! `SELECT NULL::VARCHAR` query. Used to debug NULL-handling decode bugs
//! in `Vector::decode` for VARCHAR columns.
//!
//! Prereq: a `duckdb 1.5.3` running `CALL quack_serve('quack:127.0.0.1:9495',
//! token => 'remote-secret', disable_ssl => true);`
//!
//! Run:
//!     cargo run --example capture_null -p sqe-quack-wire

use sqe_quack_wire::message::{
    encode_message, ConnectionRequest, MessageHeader, MessageType, PrepareRequest, QuackMessage,
};
use std::error::Error;

const URL: &str = "http://127.0.0.1:9495/quack";

fn main() -> Result<(), Box<dyn Error>> {
    let client = reqwest::blocking::Client::new();

    let connect_h = MessageHeader {
        r#type: MessageType::ConnectionRequest,
        connection_id: String::new(),
        client_query_id: None,
    };
    let connect_b = QuackMessage::ConnectionRequest(ConnectionRequest {
        auth_string: "remote-secret".to_string(),
        client_duckdb_version: "v1.5.3".to_string(),
        client_platform: "debug".to_string(),
        min_supported_quack_version: 1,
        max_supported_quack_version: 1,
    });
    let req = encode_message(&connect_h, &connect_b);
    let resp = client
        .post(URL)
        .header("content-type", "application/vnd.duckdb")
        .body(req)
        .send()?;
    let bytes = resp.bytes()?;
    let conn_id = extract_connection_id(&bytes)?;
    eprintln!("conn_id = {conn_id:?}");

    // Now prepare the offending query.
    let h = MessageHeader {
        r#type: MessageType::PrepareRequest,
        connection_id: conn_id,
        client_query_id: Some(1),
    };
    let b = QuackMessage::PrepareRequest(PrepareRequest {
        sql_query: "SELECT NULL::VARCHAR AS x".to_string(),
    });
    let req = encode_message(&h, &b);
    let resp = client
        .post(URL)
        .header("content-type", "application/vnd.duckdb")
        .body(req)
        .send()?;
    let bytes = resp.bytes()?;
    eprintln!("\n== PrepareResponse ({} bytes) ==", bytes.len());
    for (off, chunk) in bytes.chunks(16).enumerate() {
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
        eprintln!("  {:04x}  {:<48}  {}", off * 16, hex.join(" "), ascii);
    }
    Ok(())
}

fn extract_connection_id(bytes: &[u8]) -> Result<String, Box<dyn Error>> {
    let mut i = 0;
    loop {
        if i + 2 > bytes.len() {
            return Err("ran out".into());
        }
        let fid = u16::from_le_bytes([bytes[i], bytes[i + 1]]);
        i += 2;
        if fid == 0xFFFF {
            return Ok(String::new());
        }
        if fid == 1 {
            // type varint
            loop {
                let byte = bytes[i];
                i += 1;
                if byte & 0x80 == 0 {
                    break;
                }
            }
            continue;
        }
        if fid == 2 {
            let mut len = 0u64;
            let mut shift = 0u32;
            loop {
                let byte = bytes[i];
                i += 1;
                len |= ((byte & 0x7F) as u64) << shift;
                if byte & 0x80 == 0 {
                    break;
                }
                shift += 7;
            }
            return Ok(String::from_utf8(bytes[i..i + len as usize].to_vec())?);
        }
        return Err(format!("unexpected fid {fid:#06x}").into());
    }
}
