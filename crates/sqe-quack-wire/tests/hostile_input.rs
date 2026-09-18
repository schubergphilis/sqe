//! Adversarial-input tests for the codec and the Arrow bridge (issue #612).
//!
//! The invariant under test: for any byte string a peer can send,
//! `decode_message` / `DataChunk::decode` followed by
//! `data_chunk_to_record_batch` returns `Ok` or `Err` and never panics or
//! allocates from a wire header that the payload does not back.
//!
//! Two property tests drive it (10k cases each): raw random bytes, and valid
//! encodings of every supported vector shape with random byte mutations
//! applied. The explicit regressions below pin the shapes called out in the
//! review: `row_count = u32::MAX` over an empty column, LIST entries that
//! point past their child, and offsets that sum past `i32::MAX`.

use arrow_array::Array;
use proptest::prelude::*;
use sqe_quack_wire::arrow_bridge::data_chunk_to_record_batch;
use sqe_quack_wire::codec::{BinaryDeserializer, BinarySerializer};
use sqe_quack_wire::data_chunk::{
    DataChunk, ExtraTypeInfo, LogicalType, LogicalTypeId, Vector, VectorData,
};
use sqe_quack_wire::message::{decode_message, QuackMessage};
use sqe_quack_wire::WireError;

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn column_names(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("c{i}")).collect()
}

/// Exercise the full untrusted path on `bytes`: message decode, then the
/// Arrow bridge for every chunk the message carries; and independently the
/// bare `DataChunk` decoder, which random bytes reach far more often than a
/// well-formed message header does.
fn exercise(bytes: &[u8]) {
    if let Ok((_header, body)) = decode_message(bytes) {
        let chunks: Vec<&DataChunk> = match &body {
            QuackMessage::PrepareResponse(r) => r.results.iter().collect(),
            QuackMessage::FetchResponse(r) => r.results.iter().collect(),
            _ => Vec::new(),
        };
        for chunk in chunks {
            let _ = data_chunk_to_record_batch(&column_names(chunk.columns.len()), chunk);
        }
    }
    let mut d = BinaryDeserializer::new(bytes);
    if let Ok(chunk) = DataChunk::decode(&mut d) {
        let _ = data_chunk_to_record_batch(&column_names(chunk.columns.len()), &chunk);
    }
}

/// Deterministic byte stream from a seed (SplitMix64), used to expand a
/// proptest-chosen seed into a large body cheaply.
fn splitmix64_bytes(seed: u64) -> impl Iterator<Item = u8> {
    let mut state = seed;
    std::iter::from_fn(move || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        Some((z ^ (z >> 31)).to_le_bytes())
    })
    .flatten()
}

fn encode_chunk(chunk: &DataChunk) -> Vec<u8> {
    let mut s = BinarySerializer::new();
    s.begin_object();
    chunk.encode(&mut s);
    s.end_object();
    s.into_bytes()
}

fn decode_chunk(bytes: &[u8]) -> sqe_quack_wire::Result<DataChunk> {
    let mut d = BinaryDeserializer::new(bytes);
    let chunk = DataChunk::decode(&mut d)?;
    d.expect_object_end()?;
    Ok(chunk)
}

fn le_bytes<const N: usize>(values: impl IntoIterator<Item = [u8; N]>) -> Vec<u8> {
    values.into_iter().flatten().collect()
}

fn int_vector(values: &[i32]) -> Vector {
    Vector::new_fixed(
        LogicalTypeId::Integer,
        le_bytes(values.iter().map(|v| v.to_le_bytes())),
    )
}

fn list_type(child: LogicalType) -> LogicalType {
    LogicalType::with_extra(
        LogicalTypeId::List,
        ExtraTypeInfo::List {
            child: Box::new(child),
        },
    )
}

fn list_of_ints(entries: Vec<(u64, u64)>, child: &[i32]) -> Vector {
    Vector {
        logical_type: list_type(LogicalType::new(LogicalTypeId::Integer)),
        validity: None,
        data: VectorData::List {
            entries,
            child_count: child.len() as u64,
            child: Box::new(int_vector(child)),
        },
    }
}

fn struct_type(fields: Vec<(&str, LogicalType)>) -> LogicalType {
    LogicalType::with_extra(
        LogicalTypeId::Struct,
        ExtraTypeInfo::Struct {
            fields: fields
                .into_iter()
                .map(|(n, t)| (n.to_string(), t))
                .collect(),
        },
    )
}

/// One valid chunk per supported vector shape. Every entry round-trips
/// through encode -> decode -> Arrow, which the `well_formed_samples_round_trip`
/// test asserts so the mutation property starts from known-good seeds.
fn sample_chunks() -> Vec<DataChunk> {
    let ints = int_vector(&[1, 2, 3]);
    let ints_with_nulls = Vector {
        validity: Some(vec![true, false, true]),
        ..int_vector(&[1, 0, 3])
    };
    let strings = Vector::new_strings(vec![Some("a".into()), None, Some("ccc".into())]);
    let blobs = Vector {
        logical_type: LogicalType::new(LogicalTypeId::Blob),
        validity: Some(vec![true, true, false]),
        data: VectorData::Blobs(vec![Some(vec![1, 2]), Some(vec![]), None]),
    };
    let bools = Vector::new_fixed(LogicalTypeId::Boolean, vec![1, 0, 1]);
    let doubles = Vector::new_fixed(
        LogicalTypeId::Double,
        le_bytes([1.5f64, -2.0, 0.0].iter().map(|v| v.to_le_bytes())),
    );
    let timestamps = Vector::new_fixed(
        LogicalTypeId::Timestamp,
        le_bytes([1i64, 2, 3].iter().map(|v| v.to_le_bytes())),
    );
    let uuids = Vector::new_fixed(LogicalTypeId::Uuid, vec![7u8; 48]);
    let intervals = Vector::new_fixed(LogicalTypeId::Interval, vec![0u8; 48]);
    let decimals = Vector {
        logical_type: LogicalType::with_extra(
            LogicalTypeId::Decimal,
            ExtraTypeInfo::Decimal {
                precision: 9,
                scale: 2,
            },
        ),
        validity: None,
        data: VectorData::Fixed(le_bytes([100i32, 250, -75].iter().map(|v| v.to_le_bytes()))),
    };
    let enums = Vector {
        logical_type: LogicalType::with_extra(
            LogicalTypeId::Enum,
            ExtraTypeInfo::Enum {
                values: vec!["red".into(), "green".into(), "blue".into()],
            },
        ),
        validity: None,
        data: VectorData::Fixed(vec![0, 2, 1]),
    };
    let lists = list_of_ints(vec![(0, 2), (2, 0), (2, 3)], &[10, 20, 30, 40, 50]);
    let gapped_lists = list_of_ints(vec![(0, 1), (3, 1), (1, 2)], &[10, 20, 30, 40]);
    let structs = Vector {
        logical_type: struct_type(vec![
            ("i", LogicalType::new(LogicalTypeId::Integer)),
            ("s", LogicalType::new(LogicalTypeId::Varchar)),
        ]),
        validity: None,
        data: VectorData::Struct {
            children: vec![ints.clone(), strings.clone()],
        },
    };
    let entry_type = struct_type(vec![
        ("key", LogicalType::new(LogicalTypeId::Varchar)),
        ("value", LogicalType::new(LogicalTypeId::Integer)),
    ]);
    let maps = Vector {
        logical_type: LogicalType::with_extra(
            LogicalTypeId::Map,
            ExtraTypeInfo::List {
                child: Box::new(entry_type.clone()),
            },
        ),
        validity: None,
        data: VectorData::List {
            entries: vec![(0, 2), (2, 1), (3, 0)],
            child_count: 3,
            child: Box::new(Vector {
                logical_type: entry_type,
                validity: None,
                data: VectorData::Struct {
                    children: vec![
                        Vector::new_strings(vec![
                            Some("k1".into()),
                            Some("k2".into()),
                            Some("k3".into()),
                        ]),
                        int_vector(&[1, 2, 3]),
                    ],
                },
            }),
        },
    };
    let arrays = Vector {
        logical_type: LogicalType::with_extra(
            LogicalTypeId::Array,
            ExtraTypeInfo::Array {
                child: Box::new(LogicalType::new(LogicalTypeId::Integer)),
                size: 2,
            },
        ),
        validity: None,
        data: VectorData::Array {
            array_size: 2,
            child: Box::new(int_vector(&[1, 2, 3, 4, 5, 6])),
        },
    };
    let nested = Vector {
        logical_type: list_type(list_type(LogicalType::new(LogicalTypeId::Integer))),
        validity: Some(vec![true, false, true]),
        data: VectorData::List {
            entries: vec![(0, 1), (1, 0), (1, 1)],
            child_count: 2,
            child: Box::new(list_of_ints(vec![(0, 2), (2, 1)], &[7, 8, 9])),
        },
    };

    let single = |v: Vector| DataChunk {
        row_count: 3,
        columns: vec![v],
    };
    vec![
        single(ints),
        single(ints_with_nulls),
        single(strings.clone()),
        single(blobs),
        single(bools),
        single(doubles),
        single(timestamps),
        single(uuids),
        single(intervals),
        single(decimals),
        single(enums),
        single(lists),
        single(gapped_lists),
        single(structs),
        single(maps),
        single(arrays),
        single(nested),
        DataChunk {
            row_count: 3,
            columns: vec![int_vector(&[1, 2, 3]), strings],
        },
        DataChunk {
            row_count: 0,
            columns: vec![int_vector(&[])],
        },
    ]
}

// -----------------------------------------------------------------------------
// Property tests
// -----------------------------------------------------------------------------

#[test]
fn well_formed_samples_round_trip() {
    for (i, chunk) in sample_chunks().iter().enumerate() {
        let bytes = encode_chunk(chunk);
        let decoded = decode_chunk(&bytes).unwrap_or_else(|e| panic!("sample {i} decode: {e}"));
        assert_eq!(&decoded, chunk, "sample {i} did not round-trip");
        let batch = data_chunk_to_record_batch(&column_names(chunk.columns.len()), &decoded)
            .unwrap_or_else(|e| panic!("sample {i} arrow bridge: {e}"));
        assert_eq!(batch.num_rows(), chunk.row_count as usize, "sample {i}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 10_000,
        // Random 64 KiB inputs rarely shrink to anything more readable.
        max_shrink_iters: 200,
        .. ProptestConfig::default()
    })]

    /// Acceptance criterion from the review: any byte string up to 64 KiB
    /// decodes to `Ok` or `Err`, never a panic. The body is expanded from a
    /// proptest-chosen seed with SplitMix64 rather than drawn byte by byte:
    /// 10k cases of up to 64 KiB through proptest's per-element generator
    /// took about 90 s in a debug build, the expansion takes under a second.
    /// The first 16 bytes are drawn directly so the header (field id, type
    /// tag, counts) still shrinks to something readable on failure.
    #[test]
    fn arbitrary_bytes_never_panic(
        prefix in prop::collection::vec(any::<u8>(), 0..=16),
        seed in any::<u64>(),
        len in 0usize..=65_536,
    ) {
        let mut bytes = prefix;
        bytes.extend(splitmix64_bytes(seed).take(len.saturating_sub(bytes.len())));
        exercise(&bytes);
    }

    /// Valid encodings with a handful of byte mutations. This reaches deep
    /// into the vector decoders and the Arrow bridge with counts, offsets and
    /// type discriminants that random bytes almost never produce.
    #[test]
    fn mutated_valid_chunks_never_panic(
        sample in 0usize..19,
        mutations in prop::collection::vec((any::<prop::sample::Index>(), any::<u8>()), 1..=8),
        truncate_to in any::<Option<prop::sample::Index>>(),
    ) {
        let samples = sample_chunks();
        let mut bytes = encode_chunk(&samples[sample % samples.len()]);
        for (idx, value) in mutations {
            if !bytes.is_empty() {
                let i = idx.index(bytes.len());
                bytes[i] = value;
            }
        }
        if let Some(cut) = truncate_to {
            if !bytes.is_empty() {
                bytes.truncate(cut.index(bytes.len()));
            }
        }
        exercise(&bytes);
    }
}

// -----------------------------------------------------------------------------
// Regressions
// -----------------------------------------------------------------------------

fn assert_count_mismatch(err: WireError) {
    assert!(
        matches!(err, WireError::CountExceedsRemaining { .. }),
        "expected CountExceedsRemaining, got {err:?}"
    );
}

#[test]
fn row_count_u32_max_over_empty_fixed_column_is_rejected_at_decode() {
    // A 4-byte chunk claiming 4 billion rows: the fixed-width decoder must
    // refuse it before anything is sized from the header.
    let chunk = DataChunk {
        row_count: u32::MAX,
        columns: vec![int_vector(&[])],
    };
    let bytes = encode_chunk(&chunk);
    assert_count_mismatch(decode_chunk(&bytes).unwrap_err());
}

#[test]
fn row_count_u32_max_over_empty_varchar_column_is_rejected_at_decode() {
    let chunk = DataChunk {
        row_count: u32::MAX,
        columns: vec![Vector::new_strings(vec![])],
    };
    let bytes = encode_chunk(&chunk);
    assert_count_mismatch(decode_chunk(&bytes).unwrap_err());
}

#[test]
fn row_count_u32_max_over_empty_column_does_not_allocate_in_arrow_bridge() {
    // Bypass the decoder: a `DataChunk` value that lies about its row count
    // must fail in the bridge without reserving `row_count` slots
    // (StringBuilder::with_capacity(4e9) used to abort the process).
    for column in [
        int_vector(&[]),
        Vector::new_strings(vec![]),
        Vector {
            logical_type: LogicalType::new(LogicalTypeId::Blob),
            validity: None,
            data: VectorData::Blobs(vec![]),
        },
        Vector::new_fixed(LogicalTypeId::Boolean, vec![]),
        Vector::new_fixed(LogicalTypeId::Uuid, vec![]),
        Vector::new_fixed(LogicalTypeId::Interval, vec![]),
        list_of_ints(vec![], &[]),
    ] {
        let chunk = DataChunk {
            row_count: u32::MAX,
            columns: vec![column],
        };
        let err = data_chunk_to_record_batch(&column_names(1), &chunk).unwrap_err();
        assert_count_mismatch(err);
    }
}

#[test]
fn row_count_u32_max_with_short_validity_mask_is_rejected() {
    // has_validity = true with an 8-byte mask cannot back 4e9 rows; the mask
    // check has to fire before the `bits` vector is grown row by row. Hand
    // rolled, because `Vector::encode` would dutifully write a 512 MiB mask.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&100u16.to_le_bytes()); // field 100: row_count
    sqe_quack_wire::varint::encode_unsigned(u32::MAX as u64, &mut bytes);
    bytes.extend_from_slice(&101u16.to_le_bytes()); // field 101: types
    sqe_quack_wire::varint::encode_unsigned(1, &mut bytes);
    bytes.extend_from_slice(&100u16.to_le_bytes()); //   LogicalType.id
    sqe_quack_wire::varint::encode_unsigned(LogicalTypeId::Integer as u64, &mut bytes);
    bytes.extend_from_slice(&0xFFFFu16.to_le_bytes()); //   end LogicalType
    bytes.extend_from_slice(&102u16.to_le_bytes()); // field 102: columns
    sqe_quack_wire::varint::encode_unsigned(1, &mut bytes);
    bytes.extend_from_slice(&100u16.to_le_bytes()); //   Vector.has_validity
    bytes.push(1);
    bytes.extend_from_slice(&101u16.to_le_bytes()); //   Vector.validity
    sqe_quack_wire::varint::encode_unsigned(8, &mut bytes);
    bytes.extend_from_slice(&[0xFF; 8]);
    bytes.extend_from_slice(&102u16.to_le_bytes()); //   Vector.data
    sqe_quack_wire::varint::encode_unsigned(4, &mut bytes);
    bytes.extend_from_slice(&1i32.to_le_bytes());
    bytes.extend_from_slice(&0xFFFFu16.to_le_bytes()); //   end Vector
    bytes.extend_from_slice(&0xFFFFu16.to_le_bytes()); // end DataChunk

    let started = std::time::Instant::now();
    assert_count_mismatch(decode_chunk(&bytes).unwrap_err());
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "rejection must not first materialise 4e9 validity bits"
    );
}

#[test]
fn list_entry_past_child_is_rejected_at_decode() {
    // entries say row 0 spans child[0..10] but the child only has 2 rows.
    let chunk = DataChunk {
        row_count: 1,
        columns: vec![list_of_ints(vec![(0, 10)], &[1, 2])],
    };
    let bytes = encode_chunk(&chunk);
    assert_count_mismatch(decode_chunk(&bytes).unwrap_err());
}

#[test]
fn list_entry_past_child_is_rejected_in_arrow_bridge() {
    // Same shape handed straight to the bridge: `Array::slice` would assert.
    for entries in [
        vec![(0, 10)],
        vec![(5, 1)],
        vec![(u64::MAX, 1)],
        vec![(1, u64::MAX)],
    ] {
        let chunk = DataChunk {
            row_count: 1,
            columns: vec![list_of_ints(entries, &[1, 2])],
        };
        let err = data_chunk_to_record_batch(&column_names(1), &chunk).unwrap_err();
        assert!(
            matches!(
                err,
                WireError::CountExceedsRemaining { .. } | WireError::UnsupportedArrowType(_)
            ),
            "got {err:?}"
        );
    }
}

#[test]
fn list_entries_fewer_than_row_count_are_rejected() {
    let chunk = DataChunk {
        row_count: 3,
        columns: vec![list_of_ints(vec![(0, 1)], &[1, 2])],
    };
    let err = data_chunk_to_record_batch(&column_names(1), &chunk).unwrap_err();
    assert_count_mismatch(err);
}

#[test]
fn list_offsets_summing_past_i32_max_are_rejected() {
    // Each entry individually fits i32, but the compacted total does not.
    // Non-contiguous so the compaction (accumulating) path is the one hit.
    let big = 0x7FFF_FFF0u64;
    let chunk = DataChunk {
        row_count: 2,
        columns: vec![list_of_ints(vec![(1, big), (0, big)], &[1, 2])],
    };
    let err = data_chunk_to_record_batch(&column_names(1), &chunk).unwrap_err();
    match err {
        WireError::UnsupportedArrowType(msg) => {
            assert!(
                msg.contains("i32"),
                "message should name the i32 limit: {msg}"
            )
        }
        other => panic!("expected UnsupportedArrowType, got {other:?}"),
    }
}

#[test]
fn list_single_entry_end_past_i32_max_is_rejected() {
    // offset + length overflows i32 (and, for the last case, u64).
    for entries in [
        vec![(0u64, i32::MAX as u64 + 1)],
        vec![(i32::MAX as u64, 1)],
        vec![(u64::MAX, 1)],
    ] {
        let chunk = DataChunk {
            row_count: 1,
            columns: vec![list_of_ints(entries, &[1, 2])],
        };
        let err = data_chunk_to_record_batch(&column_names(1), &chunk).unwrap_err();
        assert!(
            matches!(err, WireError::UnsupportedArrowType(_)),
            "got {err:?}"
        );
    }
}

#[test]
fn array_size_past_i32_max_is_rejected() {
    let chunk = DataChunk {
        row_count: 1,
        columns: vec![Vector {
            logical_type: LogicalType::with_extra(
                LogicalTypeId::Array,
                ExtraTypeInfo::Array {
                    child: Box::new(LogicalType::new(LogicalTypeId::Integer)),
                    size: u32::MAX,
                },
            ),
            validity: None,
            data: VectorData::Array {
                array_size: u64::MAX,
                child: Box::new(int_vector(&[1])),
            },
        }],
    };
    let bytes = encode_chunk(&chunk);
    assert!(decode_chunk(&bytes).is_err());
    assert!(data_chunk_to_record_batch(&column_names(1), &chunk).is_err());
}

#[test]
fn enum_index_past_dictionary_is_an_error_not_a_panic() {
    let chunk = DataChunk {
        row_count: 1,
        columns: vec![Vector {
            logical_type: LogicalType::with_extra(
                LogicalTypeId::Enum,
                ExtraTypeInfo::Enum {
                    values: vec!["only".into()],
                },
            ),
            validity: None,
            data: VectorData::Fixed(vec![200]),
        }],
    };
    let err = data_chunk_to_record_batch(&column_names(1), &chunk).unwrap_err();
    assert!(
        matches!(err, WireError::UnsupportedArrowType(_)),
        "got {err:?}"
    );
}

#[test]
fn validity_shorter_than_row_count_is_an_error_not_a_panic() {
    // PrimitiveArray::new asserts on a nulls/values length mismatch.
    let chunk = DataChunk {
        row_count: 3,
        columns: vec![Vector {
            validity: Some(vec![true]),
            ..int_vector(&[1, 2, 3])
        }],
    };
    assert_count_mismatch(data_chunk_to_record_batch(&column_names(1), &chunk).unwrap_err());
}

#[test]
fn gapped_list_still_compacts_correctly() {
    // Guard against the offset validation breaking the legitimate gap path.
    let chunk = DataChunk {
        row_count: 3,
        columns: vec![list_of_ints(
            vec![(0, 1), (3, 1), (1, 2)],
            &[10, 20, 30, 40],
        )],
    };
    let batch = data_chunk_to_record_batch(&column_names(1), &chunk).unwrap();
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .unwrap();
    assert_eq!(list.len(), 3);
    assert_eq!(list.value_offsets(), &[0, 1, 2, 4]);
    let values = list
        .values()
        .as_any()
        .downcast_ref::<arrow_array::Int32Array>()
        .unwrap();
    assert_eq!(values.values(), &[10, 40, 20, 30]);
}
