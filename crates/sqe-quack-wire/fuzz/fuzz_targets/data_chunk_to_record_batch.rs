//! Fuzz the DataChunk decoder followed by the Arrow bridge. This is the exact
//! path a Quack *client* (the coordinator's `quack_query()`) runs on bytes a
//! remote server controls, so it is the target that matters most. Seeds live
//! in `corpus/data_chunk_to_record_batch/`, written by
//! `examples/emit_fuzz_seeds.rs`.
#![no_main]

use libfuzzer_sys::fuzz_target;
use sqe_quack_wire::arrow_bridge::data_chunk_to_record_batch;
use sqe_quack_wire::codec::BinaryDeserializer;
use sqe_quack_wire::data_chunk::DataChunk;

fuzz_target!(|data: &[u8]| {
    let mut d = BinaryDeserializer::new(data);
    let Ok(chunk) = DataChunk::decode(&mut d) else {
        return;
    };
    let names: Vec<String> = (0..chunk.columns.len()).map(|i| format!("c{i}")).collect();
    let _ = data_chunk_to_record_batch(&names, &chunk);
});
