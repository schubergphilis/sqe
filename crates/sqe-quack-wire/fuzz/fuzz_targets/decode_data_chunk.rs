//! Fuzz the bare DataChunk decoder (row_count, types list, column vectors)
//! without the message framing, so the fuzzer spends its budget on the vector
//! codecs rather than on finding a valid header. Seeds live in
//! `corpus/decode_data_chunk/`, written by `examples/emit_fuzz_seeds.rs`.
#![no_main]

use libfuzzer_sys::fuzz_target;
use sqe_quack_wire::codec::BinaryDeserializer;
use sqe_quack_wire::data_chunk::DataChunk;

fuzz_target!(|data: &[u8]| {
    let mut d = BinaryDeserializer::new(data);
    if let Ok(_chunk) = DataChunk::decode(&mut d) {
        let _ = d.expect_object_end();
    }
});
