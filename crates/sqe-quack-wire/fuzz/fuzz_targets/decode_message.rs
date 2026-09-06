//! Fuzz the full message decoder: header + body, including the DataChunk
//! payloads of PrepareResponse / FetchResponse / AppendRequest, and then the
//! Arrow bridge for every chunk a well-formed message carries. Seeds live in
//! `corpus/decode_message/` (captured from a real DuckDB `quack_serve`).
#![no_main]

use libfuzzer_sys::fuzz_target;
use sqe_quack_wire::arrow_bridge::data_chunk_to_record_batch;
use sqe_quack_wire::message::{decode_message, QuackMessage};

fuzz_target!(|data: &[u8]| {
    let Ok((_header, body)) = decode_message(data) else {
        return;
    };
    let chunks = match &body {
        QuackMessage::PrepareResponse(r) => r.results.iter().collect::<Vec<_>>(),
        QuackMessage::FetchResponse(r) => r.results.iter().collect(),
        QuackMessage::AppendRequest(r) => vec![&r.append_chunk],
        _ => Vec::new(),
    };
    for chunk in chunks {
        let names: Vec<String> = (0..chunk.columns.len()).map(|i| format!("c{i}")).collect();
        let _ = data_chunk_to_record_batch(&names, chunk);
    }
});
