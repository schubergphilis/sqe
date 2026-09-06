//! Write the seed corpus for `fuzz/fuzz_targets/*` (issue #612).
//!
//! - `fuzz/corpus/decode_message/`: every upstream fixture in `tests/fixtures/`
//!   (captured from a real DuckDB `quack_serve` by `capture_upstream.rs`).
//! - `fuzz/corpus/decode_data_chunk/` and
//!   `fuzz/corpus/data_chunk_to_record_batch/`: one valid `DataChunk`
//!   encoding per supported vector shape (scalars, VARCHAR/BLOB with nulls,
//!   DECIMAL, ENUM, LIST with gaps, STRUCT, MAP, ARRAY, nested LIST), plus
//!   the DataChunks extracted from the upstream PrepareResponse fixtures.
//!
//! Run from the repository root:
//!     cargo run -p sqe-quack-wire --example emit_fuzz_seeds
//!
//! Re-run whenever a new vector shape gains codec support so the fuzzer
//! starts from an input that reaches it.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use sqe_quack_wire::codec::BinarySerializer;
use sqe_quack_wire::data_chunk::{
    DataChunk, ExtraTypeInfo, LogicalType, LogicalTypeId, Vector, VectorData,
};
use sqe_quack_wire::message::{decode_message, QuackMessage};

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn encode_chunk(chunk: &DataChunk) -> Vec<u8> {
    let mut s = BinarySerializer::new();
    s.begin_object();
    chunk.encode(&mut s);
    s.end_object();
    s.into_bytes()
}

fn le<const N: usize>(values: impl IntoIterator<Item = [u8; N]>) -> Vec<u8> {
    values.into_iter().flatten().collect()
}

fn ints(values: &[i32]) -> Vector {
    Vector::new_fixed(
        LogicalTypeId::Integer,
        le(values.iter().map(|v| v.to_le_bytes())),
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

fn struct_type(fields: &[(&str, LogicalType)]) -> LogicalType {
    LogicalType::with_extra(
        LogicalTypeId::Struct,
        ExtraTypeInfo::Struct {
            fields: fields
                .iter()
                .map(|(n, t)| (n.to_string(), t.clone()))
                .collect(),
        },
    )
}

fn single(name: &str, v: Vector) -> (String, DataChunk) {
    (
        name.to_string(),
        DataChunk {
            row_count: 3,
            columns: vec![v],
        },
    )
}

fn sample_chunks() -> Vec<(String, DataChunk)> {
    let strings = Vector::new_strings(vec![Some("a".into()), None, Some("ccc".into())]);
    let entry_type = struct_type(&[
        ("key", LogicalType::new(LogicalTypeId::Varchar)),
        ("value", LogicalType::new(LogicalTypeId::Integer)),
    ]);
    vec![
        single("integer", ints(&[1, 2, 3])),
        single(
            "integer_nulls",
            Vector {
                validity: Some(vec![true, false, true]),
                ..ints(&[1, 0, 3])
            },
        ),
        single("varchar_nulls", strings.clone()),
        single(
            "blob_nulls",
            Vector {
                logical_type: LogicalType::new(LogicalTypeId::Blob),
                validity: Some(vec![true, true, false]),
                data: VectorData::Blobs(vec![Some(vec![1, 2]), Some(vec![]), None]),
            },
        ),
        single(
            "boolean",
            Vector::new_fixed(LogicalTypeId::Boolean, vec![1, 0, 1]),
        ),
        single(
            "double",
            Vector::new_fixed(
                LogicalTypeId::Double,
                le([1.5f64, -2.0, 0.0].iter().map(|v| v.to_le_bytes())),
            ),
        ),
        single(
            "timestamp",
            Vector::new_fixed(
                LogicalTypeId::Timestamp,
                le([1i64, 2, 3].iter().map(|v| v.to_le_bytes())),
            ),
        ),
        single(
            "uuid",
            Vector::new_fixed(LogicalTypeId::Uuid, vec![7u8; 48]),
        ),
        single(
            "interval",
            Vector::new_fixed(LogicalTypeId::Interval, vec![0u8; 48]),
        ),
        single(
            "decimal",
            Vector {
                logical_type: LogicalType::with_extra(
                    LogicalTypeId::Decimal,
                    ExtraTypeInfo::Decimal {
                        precision: 9,
                        scale: 2,
                    },
                ),
                validity: None,
                data: VectorData::Fixed(le([100i32, 250, -75].iter().map(|v| v.to_le_bytes()))),
            },
        ),
        single(
            "enum",
            Vector {
                logical_type: LogicalType::with_extra(
                    LogicalTypeId::Enum,
                    ExtraTypeInfo::Enum {
                        values: vec!["red".into(), "green".into(), "blue".into()],
                    },
                ),
                validity: None,
                data: VectorData::Fixed(vec![0, 2, 1]),
            },
        ),
        single(
            "list_gapped",
            Vector {
                logical_type: list_type(LogicalType::new(LogicalTypeId::Integer)),
                validity: None,
                data: VectorData::List {
                    entries: vec![(0, 1), (3, 1), (1, 2)],
                    child_count: 4,
                    child: Box::new(ints(&[10, 20, 30, 40])),
                },
            },
        ),
        single(
            "struct",
            Vector {
                logical_type: struct_type(&[
                    ("i", LogicalType::new(LogicalTypeId::Integer)),
                    ("s", LogicalType::new(LogicalTypeId::Varchar)),
                ]),
                validity: None,
                data: VectorData::Struct {
                    children: vec![ints(&[1, 2, 3]), strings],
                },
            },
        ),
        single(
            "map",
            Vector {
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
                                ints(&[1, 2, 3]),
                            ],
                        },
                    }),
                },
            },
        ),
        single(
            "array",
            Vector {
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
                    child: Box::new(ints(&[1, 2, 3, 4, 5, 6])),
                },
            },
        ),
        single(
            "list_of_list_nulls",
            Vector {
                logical_type: list_type(list_type(LogicalType::new(LogicalTypeId::Integer))),
                validity: Some(vec![true, false, true]),
                data: VectorData::List {
                    entries: vec![(0, 1), (1, 0), (1, 1)],
                    child_count: 2,
                    child: Box::new(Vector {
                        logical_type: list_type(LogicalType::new(LogicalTypeId::Integer)),
                        validity: None,
                        data: VectorData::List {
                            entries: vec![(0, 2), (2, 1)],
                            child_count: 3,
                            child: Box::new(ints(&[7, 8, 9])),
                        },
                    }),
                },
            },
        ),
    ]
}

fn write(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(dir)?;
    let path = dir.join(name);
    fs::write(&path, bytes)?;
    println!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let root = crate_dir();
    let fixtures = root.join("tests/fixtures");
    let corpus = root.join("fuzz/corpus");
    let message_dir = corpus.join("decode_message");
    let chunk_dirs = [
        corpus.join("decode_data_chunk"),
        corpus.join("data_chunk_to_record_batch"),
    ];

    // 1. Upstream message fixtures seed the message decoder; the DataChunks
    //    inside any response also seed the chunk targets.
    let mut fixture_paths: Vec<PathBuf> = fs::read_dir(&fixtures)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "bin"))
        .collect();
    fixture_paths.sort();
    for path in &fixture_paths {
        let bytes = fs::read(path)?;
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("fixture.bin");
        write(&message_dir, name, &bytes)?;
        if let Ok((_, QuackMessage::PrepareResponse(r))) = decode_message(&bytes) {
            for (i, chunk) in r.results.iter().enumerate() {
                let stem = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("fixture");
                for dir in &chunk_dirs {
                    write(
                        dir,
                        &format!("upstream_{stem}_{i}.bin"),
                        &encode_chunk(chunk),
                    )?;
                }
            }
        }
    }

    // 2. One synthetic chunk per vector shape.
    for (name, chunk) in sample_chunks() {
        let bytes = encode_chunk(&chunk);
        for dir in &chunk_dirs {
            write(dir, &format!("{name}.bin"), &bytes)?;
        }
    }
    Ok(())
}
