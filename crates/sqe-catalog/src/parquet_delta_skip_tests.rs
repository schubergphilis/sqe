//! Guards the parquet dependency against a decoder limit that broke reads of
//! Snowflake-written Iceberg tables.
//!
//! Snowflake writes `DELTA_BINARY_PACKED` pages with 512-value miniblocks,
//! which the parquet spec allows. parquet 59.3.1 refused to *skip* inside
//! such a miniblock ("cannot skip miniblock of size 512"), and a skip is what
//! a row selection does: applying a position delete or a deletion vector, or
//! reading the rows a page-index predicate kept. A Snowflake table with
//! merge-on-read deletes therefore failed on any read of a delta-encoded
//! column that crossed a deleted row. Fixed upstream in arrow-rs #11021 and
//! released in 59.3.2.
//!
//! The page here is hand-encoded because no Rust or Python writer emits
//! miniblocks wider than 32 values.

use std::sync::Arc;

use bytes::Bytes;
use parquet::basic::{Encoding, Repetition, Type as PhysicalType};
use parquet::column::page::{Page, PageMetadata, PageReader};
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::Int64Type;
use parquet::errors::Result;
use parquet::schema::types::{ColumnDescriptor, ColumnPath, Type};

const BLOCK_SIZE: usize = 2048;
const MINIBLOCKS: usize = 4;
const MINIBLOCK_VALUES: usize = BLOCK_SIZE / MINIBLOCKS; // 512, as Snowflake writes

fn uleb(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn zigzag(out: &mut Vec<u8>, v: i64) {
    uleb(out, ((v << 1) ^ (v >> 63)) as u64);
}

/// Encode `values` (exactly one value plus whole blocks of deltas) as
/// `DELTA_BINARY_PACKED` with 512-value miniblocks.
fn encode_delta(values: &[i64]) -> Vec<u8> {
    assert_eq!((values.len() - 1) % BLOCK_SIZE, 0, "whole blocks only");
    let mut out = Vec::new();
    uleb(&mut out, BLOCK_SIZE as u64);
    uleb(&mut out, MINIBLOCKS as u64);
    uleb(&mut out, values.len() as u64);
    zigzag(&mut out, values[0]);
    let deltas: Vec<i64> = values.windows(2).map(|w| w[1] - w[0]).collect();
    for block in deltas.chunks(BLOCK_SIZE) {
        let min = *block.iter().min().expect("non-empty block");
        zigzag(&mut out, min);
        let minis: Vec<&[i64]> = block.chunks(MINIBLOCK_VALUES).collect();
        let widths: Vec<u32> = minis
            .iter()
            .map(|m| {
                let max = m.iter().map(|d| (d - min) as u64).max().unwrap_or(0);
                64 - max.leading_zeros()
            })
            .collect();
        out.extend(widths.iter().map(|w| *w as u8));
        for (mini, width) in minis.iter().zip(&widths) {
            // LSB-first bit packing of the residuals.
            let mut acc: u128 = 0;
            let mut bits = 0u32;
            for d in mini.iter() {
                acc |= ((d - min) as u128) << bits;
                bits += width;
                while bits >= 8 {
                    out.push(acc as u8);
                    acc >>= 8;
                    bits -= 8;
                }
            }
            if bits > 0 {
                out.push(acc as u8);
            }
        }
    }
    out
}

/// One in-memory data page, served once.
struct OnePage(Option<(Page, usize)>);

impl Iterator for OnePage {
    type Item = Result<Page>;
    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

impl PageReader for OnePage {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        Ok(self.0.take().map(|(page, _)| page))
    }
    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        Ok(self.0.as_ref().map(|(_, rows)| PageMetadata {
            num_rows: Some(*rows),
            num_levels: Some(*rows),
            is_dict: false,
        }))
    }
    fn skip_next_page(&mut self) -> Result<()> {
        self.0 = None;
        Ok(())
    }
}

fn reader(values: &[i64]) -> ColumnReaderImpl<Int64Type> {
    let leaf = Type::primitive_type_builder("id", PhysicalType::INT64)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .expect("leaf type");
    let descr = Arc::new(ColumnDescriptor::new(
        Arc::new(leaf),
        0,
        0,
        ColumnPath::new(vec!["id".into()]),
    ));
    let page = Page::DataPage {
        buf: Bytes::from(encode_delta(values)),
        num_values: values.len() as u32,
        encoding: Encoding::DELTA_BINARY_PACKED,
        def_level_encoding: Encoding::RLE,
        rep_level_encoding: Encoding::RLE,
        statistics: None,
    };
    ColumnReaderImpl::new(descr, Box::new(OnePage(Some((page, values.len())))))
}

fn read(r: &mut ColumnReaderImpl<Int64Type>, n: usize) -> Vec<i64> {
    let mut out = Vec::new();
    let (records, _, _) = r.read_records(n, None, None, &mut out).expect("read");
    assert_eq!(records, n);
    out
}

#[test]
fn skips_inside_512_value_miniblocks() {
    // Deltas 0, 1, 2, ... wrap at 7, so every miniblock has a bit width of 3.
    let values: Vec<i64> = std::iter::once(10)
        .chain((0..BLOCK_SIZE as i64).map(|i| i % 7))
        .scan(0i64, |acc, d| {
            *acc += d;
            Some(*acc)
        })
        .collect();

    // A deletion at rows 3 and 4: read 3, skip 2, read on.
    let mut r = reader(&values);
    assert_eq!(read(&mut r, 3), values[0..3]);
    assert_eq!(r.skip_records(2).expect("skip inside a 512 miniblock"), 2);
    assert_eq!(read(&mut r, 5), values[5..10]);

    // A skip that crosses a miniblock boundary (rows 600..1300).
    assert_eq!(r.skip_records(590).expect("skip"), 590);
    assert_eq!(read(&mut r, 700), values[600..1300]);
}
