//! Bounding-box pruning on geometry columns (sqe#863).
//!
//! An Iceberg `geometry` column's manifest bounds are the corners of the
//! bounding box of its values in the file, and Parquet 2.11 keeps the same box
//! per column chunk in its geospatial statistics. This module reads a filter
//! such as `ST_Intersects(geom, ST_GeomFromText('POLYGON(...)'))`, takes the
//! bounding box of the constant geometry, and drops the data files and row
//! groups whose box does not meet it.
//!
//! Only predicates that are false whenever the two bounding boxes are
//! disjoint prune: `ST_Intersects`, `ST_Contains`, `ST_Within`, `ST_Equals`,
//! `ST_Touches`, `ST_Crosses`, `ST_Overlaps`, and `ST_DWithin` with the
//! constant's box grown by the distance. `ST_Disjoint`, a negated predicate
//! and `ST_Distance(..) < d` never do. `geography` columns are left alone:
//! their edges are not planar, Iceberg gives them no bounds, and the `ST_*`
//! predicates refuse them anyway.
//!
//! The filter itself stays in the plan (`Inexact`), so pruning only ever
//! skips rows the filter would have rejected. A missing or unreadable bound
//! keeps the file or row group.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::metrics::Count;
use datafusion::scalar::ScalarValue;
use iceberg::arrow::RowGroupPruner;
use iceberg::spec::{DataFile, PrimitiveLiteral, PrimitiveType, Type};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use serde::{Deserialize, Serialize};

/// The `ST_*` predicates that imply the two bounding boxes meet.
const INTERSECTING: &[&str] = &[
    "st_intersects",
    "st_contains",
    "st_within",
    "st_equals",
    "st_touches",
    "st_crosses",
    "st_overlaps",
];

/// One `ST_<predicate>(column, constant)` filter on a geometry column.
///
/// Plain data, so a distributed scan sends it to the workers in the scan
/// ticket ([`encode_prunes`]); workers have no `ST_*` functions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeoPrune {
    column: String,
    field_id: Option<i32>,
    /// `[xmin, ymin, xmax, ymax]` a matching value's box must meet.
    bbox: [f64; 4],
}

impl GeoPrune {
    /// Reads a filter in the shape this module prunes on, or `None`.
    ///
    /// The geometry argument must be a bare column of `schema` tagged
    /// `geoarrow.wkb` and not geography: a policy guard (`CASE WHEN ... THEN
    /// g END`) or a masked column never matches.
    pub fn from_expr(expr: &Expr, schema: &Schema) -> Option<Self> {
        let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
            return None;
        };
        let name = func.name();
        let distance = match (name, args.len()) {
            (n, 2) if INTERSECTING.contains(&n) => 0.0,
            ("st_dwithin", 3) => non_negative(&args[2])?,
            _ => return None,
        };
        let (column, constant) = match (&args[0], &args[1]) {
            (Expr::Column(c), other) | (other, Expr::Column(c)) => (c, other),
            _ => return None,
        };
        let field = schema.field_with_name(&column.name).ok()?;
        if !matches!(
            field.data_type(),
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        ) || field
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str)
            != Some(sqe_geo::GEOARROW_WKB)
            || sqe_geo::is_geography(field)
        {
            return None;
        }
        let [xmin, ymin, xmax, ymax] = sqe_geo::wkb_bounding_box(wkb_literal(constant)?)?;
        let bbox = [
            xmin - distance,
            ymin - distance,
            xmax + distance,
            ymax + distance,
        ];
        bbox.iter().all(|c| c.is_finite()).then(|| Self {
            column: column.name.clone(),
            field_id: None,
            bbox,
        })
    }

    /// The column's Iceberg field id in `iceberg_schema`, kept only when the
    /// column is a `geometry` there: the bounds of a `binary` or `geography`
    /// column are not a bounding box.
    pub fn with_field_id(mut self, iceberg_schema: &iceberg::spec::Schema) -> Self {
        self.field_id = iceberg_schema
            .field_by_name(&self.column)
            .filter(|f| matches!(*f.field_type, Type::Primitive(PrimitiveType::Geometry(_))))
            .map(|f| f.id);
        self
    }

    /// True when the box `[xmin, ymin, xmax, ymax]` cannot meet the filter's.
    fn excludes_box(&self, other: [f64; 4]) -> bool {
        let [xmin, ymin, xmax, ymax] = other;
        if !other.iter().all(|c| c.is_finite()) || xmin > xmax || ymin > ymax {
            return false;
        }
        xmax < self.bbox[0] || xmin > self.bbox[2] || ymax < self.bbox[1] || ymin > self.bbox[3]
    }

    /// True when the data file's manifest bounds exclude the filter.
    pub fn excludes_file(&self, file: &DataFile) -> bool {
        let Some(id) = self.field_id else {
            return false;
        };
        let corner = |bound: Option<&iceberg::spec::Datum>| match bound.map(|d| d.literal()) {
            Some(PrimitiveLiteral::Binary(bytes)) => point_xy(bytes),
            _ => None,
        };
        match (
            corner(file.lower_bounds().get(&id)),
            corner(file.upper_bounds().get(&id)),
        ) {
            (Some([xmin, ymin]), Some([xmax, ymax])) => self.excludes_box([xmin, ymin, xmax, ymax]),
            _ => false,
        }
    }

    /// True when the row group cannot hold a row that passes the filter: its
    /// chunk for the column is all NULL, or its bounding box misses ours.
    fn excludes(&self, metadata: &ParquetMetaData, row_group: &RowGroupMetaData) -> bool {
        let Some(leaf) = self.leaf(metadata) else {
            return false;
        };
        let chunk = row_group.column(leaf);
        let nulls = chunk.statistics().and_then(|s| s.null_count_opt());
        if nulls.is_some_and(|n| n == chunk.num_values() as u64) {
            return true;
        }
        match chunk.geo_statistics().and_then(|s| s.bounding_box()) {
            Some(b) => self.excludes_box([b.get_xmin(), b.get_ymin(), b.get_xmax(), b.get_ymax()]),
            None => false,
        }
    }

    /// The Parquet leaf of the column: top-level, matched by field id, by
    /// name when the file carries no ids.
    fn leaf(&self, metadata: &ParquetMetaData) -> Option<usize> {
        let schema = metadata.file_metadata().schema_descr();
        let top_level = |i: usize| schema.column(i).path().parts().len() == 1;
        let by_id = self.field_id.and_then(|id| {
            (0..schema.num_columns()).find(|&i| {
                let info = schema.column(i).self_type().get_basic_info().clone();
                top_level(i) && info.has_id() && info.id() == id
            })
        });
        by_id.or_else(|| {
            (0..schema.num_columns())
                .find(|&i| top_level(i) && schema.column(i).path().parts()[0] == self.column)
        })
    }
}

/// The x and y of a bound in the spec's point serialization: 16, 24 or 32
/// bytes of little-endian doubles, x and y first.
fn point_xy(bytes: &[u8]) -> Option<[f64; 2]> {
    if !matches!(bytes.len(), 16 | 24 | 32) {
        return None;
    }
    let read =
        |at: usize| -> Option<f64> { Some(f64::from_le_bytes(bytes[at..at + 8].try_into().ok()?)) };
    let xy = [read(0)?, read(8)?];
    xy.iter().all(|v| v.is_finite()).then_some(xy)
}

/// The WKB bytes of a constant geometry: a binary literal, possibly cast to
/// another binary width by type coercion.
fn wkb_literal(expr: &Expr) -> Option<&[u8]> {
    match expr {
        Expr::Literal(
            ScalarValue::Binary(Some(b))
            | ScalarValue::LargeBinary(Some(b))
            | ScalarValue::BinaryView(Some(b)),
            _,
        ) => Some(b),
        Expr::Cast(c)
            if c.field.data_type().is_binary() || c.field.data_type() == &DataType::BinaryView =>
        {
            wkb_literal(&c.expr)
        }
        _ => None,
    }
}

/// A finite, non-negative numeric literal (`ST_DWithin`'s distance).
fn non_negative(expr: &Expr) -> Option<f64> {
    let value = match expr {
        Expr::Literal(v, _) => v,
        Expr::Cast(c) => return non_negative(&c.expr),
        _ => return None,
    };
    let d = match value {
        ScalarValue::Float64(Some(v)) => *v,
        ScalarValue::Float32(Some(v)) => f64::from(*v),
        ScalarValue::Int64(Some(v)) => *v as f64,
        ScalarValue::Int32(Some(v)) => f64::from(*v),
        ScalarValue::Decimal128(Some(v), _, scale) => *v as f64 / 10f64.powi(i32::from(*scale)),
        _ => return None,
    };
    (d.is_finite() && d >= 0.0).then_some(d)
}

/// Whether `expr` is a filter [`GeoPrune::from_expr`] reads.
pub fn is_geo_prune_filter(expr: &Expr, schema: &Schema) -> bool {
    GeoPrune::from_expr(expr, schema).is_some()
}

/// The data files, by path, whose manifest bounds exclude any of `prunes`.
/// The prunes need their field ids (`with_field_id`).
pub fn files_excluded_by<'a>(
    files: impl IntoIterator<Item = &'a DataFile>,
    prunes: &[GeoPrune],
) -> std::collections::HashSet<String> {
    files
        .into_iter()
        .filter(|file| prunes.iter().any(|p| p.excludes_file(file)))
        .map(|file| file.file_path().to_string())
        .collect()
}

/// The prunes as scan-ticket bytes, or `None` when there are none.
pub fn encode_prunes(prunes: &[GeoPrune]) -> serde_json::Result<Option<Vec<u8>>> {
    if prunes.is_empty() {
        return Ok(None);
    }
    serde_json::to_vec(prunes).map(Some)
}

/// The prunes a scan ticket carries.
pub fn decode_prunes(bytes: &[u8]) -> serde_json::Result<Vec<GeoPrune>> {
    serde_json::from_slice(bytes)
}

/// Drops the row groups any of the filters excludes, for the reader hook.
#[derive(Debug)]
pub struct GeoRowGroupPruner {
    prunes: Arc<[GeoPrune]>,
    pruned: Count,
}

impl GeoRowGroupPruner {
    pub fn new(prunes: Arc<[GeoPrune]>, pruned: Count) -> Self {
        Self { prunes, pruned }
    }
}

impl RowGroupPruner for GeoRowGroupPruner {
    fn select(&self, metadata: &ParquetMetaData, candidates: Vec<usize>) -> Vec<usize> {
        let before = candidates.len();
        let kept: Vec<usize> = candidates
            .into_iter()
            .filter(|&i| {
                let row_group = metadata.row_group(i);
                !self.prunes.iter().any(|p| p.excludes(metadata, row_group))
            })
            .collect();
        self.pruned.add(before - kept.len());
        kept
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::Field;
    use datafusion::common::Column;
    use datafusion::logical_expr::{col, lit, Expr, ScalarUDF};

    use super::*;

    fn tagged(name: &str, edges: Option<&str>) -> Field {
        let mut meta = HashMap::from([(
            "ARROW:extension:name".to_string(),
            sqe_geo::GEOARROW_WKB.to_string(),
        )]);
        if let Some(e) = edges {
            meta.insert(
                "ARROW:extension:metadata".into(),
                format!(r#"{{"edges":"{e}"}}"#),
            );
        }
        Field::new(name, DataType::LargeBinary, true).with_metadata(meta)
    }

    fn schema() -> Schema {
        Schema::new(vec![
            tagged("g", None),
            tagged("geog", Some("spherical")),
            Field::new("b", DataType::LargeBinary, true),
        ])
    }

    fn udf(name: &str) -> Arc<ScalarUDF> {
        Arc::new(
            sqe_geo::geo_udfs()
                .into_iter()
                .find(|f| f.name() == name)
                .unwrap_or_else(|| panic!("no {name}")),
        )
    }

    fn call(name: &str, args: Vec<Expr>) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(udf(name), args))
    }

    /// WKB of a planar polygon over the box `[x0, y0, x1, y1]`.
    fn wkb_box(x0: f64, y0: f64, x1: f64, y1: f64) -> Expr {
        let ring = [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)];
        let mut b = vec![1u8, 3, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0];
        for (x, y) in ring {
            b.extend(x.to_le_bytes());
            b.extend(y.to_le_bytes());
        }
        lit(ScalarValue::LargeBinary(Some(b)))
    }

    fn point(x: f64, y: f64) -> Expr {
        let mut b = vec![1u8, 1, 0, 0, 0];
        b.extend(x.to_le_bytes());
        b.extend(y.to_le_bytes());
        lit(ScalarValue::Binary(Some(b)))
    }

    fn prune(expr: Expr) -> GeoPrune {
        let mut p = GeoPrune::from_expr(&expr, &schema()).expect("prune");
        p.field_id = Some(1);
        p
    }

    fn corner(x: f64, y: f64) -> iceberg::spec::Datum {
        let bytes = [x.to_le_bytes(), y.to_le_bytes()].concat();
        iceberg::spec::Datum::try_from_bytes(&bytes, PrimitiveType::Geometry(Default::default()))
            .expect("geometry bound")
    }

    fn file(lower: Option<(f64, f64)>, upper: Option<(f64, f64)>) -> DataFile {
        use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
        let bounds = |b: Option<(f64, f64)>| match b {
            Some((x, y)) => HashMap::from([(1, corner(x, y))]),
            None => HashMap::new(),
        };
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_format(DataFileFormat::Parquet)
            .file_path("f.parquet".into())
            .file_size_in_bytes(1)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .lower_bounds(bounds(lower))
            .upper_bounds(bounds(upper))
            .build()
            .expect("data file")
    }

    #[test]
    fn reads_the_supported_shapes() {
        let s = schema();
        for name in INTERSECTING {
            let expr = call(name, vec![col("g"), wkb_box(0.0, 0.0, 1.0, 1.0)]);
            assert!(GeoPrune::from_expr(&expr, &s).is_some(), "{name}");
            let flipped = call(name, vec![wkb_box(0.0, 0.0, 1.0, 1.0), col("g")]);
            assert!(
                GeoPrune::from_expr(&flipped, &s).is_some(),
                "{name} flipped"
            );
        }
        let near = call("st_dwithin", vec![col("g"), point(5.0, 5.0), lit(2.0)]);
        assert_eq!(
            GeoPrune::from_expr(&near, &s).expect("dwithin").bbox,
            [3.0, 3.0, 7.0, 7.0]
        );
    }

    #[test]
    fn refuses_other_shapes() {
        let s = schema();
        let refused = [
            // Disjoint boxes prove ST_Disjoint true, never false.
            call("st_disjoint", vec![col("g"), point(0.0, 0.0)]),
            Expr::Not(Box::new(call(
                "st_intersects",
                vec![col("g"), point(0.0, 0.0)],
            ))),
            // Geography edges are not planar; a plain binary has no box.
            call("st_intersects", vec![col("geog"), point(0.0, 0.0)]),
            call("st_intersects", vec![col("b"), point(0.0, 0.0)]),
            // Two columns, or no constant.
            call(
                "st_intersects",
                vec![col("g"), Expr::Column(Column::from_name("g"))],
            ),
            call("st_dwithin", vec![col("g"), point(0.0, 0.0), lit(-1.0)]),
            call(
                "st_dwithin",
                vec![col("g"), point(0.0, 0.0), lit(ScalarValue::Float64(None))],
            ),
            // Not WKB.
            call(
                "st_intersects",
                vec![col("g"), lit(ScalarValue::Binary(Some(vec![1, 2])))],
            ),
            call(
                "st_intersects",
                vec![col("g"), lit(ScalarValue::Binary(None))],
            ),
        ];
        for expr in refused {
            assert!(GeoPrune::from_expr(&expr, &s).is_none(), "{expr}");
        }
    }

    #[test]
    fn manifest_bounds_exclude_a_file() {
        let p = prune(call(
            "st_intersects",
            vec![col("g"), wkb_box(10.0, 10.0, 20.0, 20.0)],
        ));
        assert!(p.excludes_file(&file(Some((0.0, 0.0)), Some((5.0, 5.0)))));
        assert!(p.excludes_file(&file(Some((25.0, 0.0)), Some((30.0, 30.0)))));
        assert!(p.excludes_file(&file(Some((0.0, 21.0)), Some((30.0, 30.0)))));
        // Overlap, containment and a shared edge all keep the file.
        assert!(!p.excludes_file(&file(Some((0.0, 0.0)), Some((10.0, 10.0)))));
        assert!(!p.excludes_file(&file(Some((12.0, 12.0)), Some((13.0, 13.0)))));
        assert!(!p.excludes_file(&file(Some((-50.0, -50.0)), Some((50.0, 50.0)))));
    }

    #[test]
    fn manifest_bounds_keep_a_file_they_cannot_judge() {
        let p = prune(call(
            "st_within",
            vec![col("g"), wkb_box(10.0, 10.0, 20.0, 20.0)],
        ));
        assert!(!p.excludes_file(&file(None, None)));
        assert!(!p.excludes_file(&file(Some((0.0, 0.0)), None)));
        // Inverted (wrapped) bounds are geography-style; never prune on them.
        assert!(!p.excludes_file(&file(Some((5.0, 0.0)), Some((0.0, 5.0)))));
        assert!(!p.excludes_file(&file(Some((f64::NAN, 0.0)), Some((1.0, 1.0)))));
        let unresolved = GeoPrune {
            field_id: None,
            ..p
        };
        assert!(!unresolved.excludes_file(&file(Some((0.0, 0.0)), Some((1.0, 1.0)))));
    }

    #[test]
    fn dwithin_grows_the_box_by_the_distance() {
        let p = prune(call("st_dwithin", vec![col("g"), point(0.0, 0.0), lit(3)]));
        assert!(!p.excludes_file(&file(Some((2.0, 2.0)), Some((4.0, 4.0)))));
        assert!(p.excludes_file(&file(Some((3.5, 3.5)), Some((4.0, 4.0)))));
    }

    #[test]
    fn reads_bounds_with_z_and_m() {
        let xyzm = [1.0f64, 2.0, f64::NAN, 4.0]
            .iter()
            .flat_map(|c| c.to_le_bytes())
            .collect::<Vec<_>>();
        assert_eq!(point_xy(&xyzm), Some([1.0, 2.0]));
        assert_eq!(point_xy(&xyzm[..24]), Some([1.0, 2.0]));
        assert_eq!(point_xy(&xyzm[..20]), None);
    }

    #[test]
    fn prunes_round_trip_through_the_ticket_encoding() {
        let p = prune(call("st_contains", vec![col("g"), point(1.0, 2.0)]));
        let bytes = encode_prunes(std::slice::from_ref(&p))
            .unwrap()
            .expect("bytes");
        assert_eq!(decode_prunes(&bytes).unwrap(), vec![p]);
        assert_eq!(encode_prunes(&[]).unwrap(), None);
    }

    /// A file of four row groups of geometry points, one group per corner of
    /// a 100 x 100 square, plus a group of NULLs.
    fn parquet_file() -> ParquetMetaData {
        use datafusion::arrow::array::{ArrayRef, LargeBinaryArray, RecordBatch};
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use parquet::file::reader::FileReader;
        use parquet::file::serialized_reader::SerializedFileReader;

        let schema = Arc::new(Schema::new(vec![tagged("g", None)]));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        let wkb = |x: f64, y: f64| {
            let Expr::Literal(ScalarValue::Binary(Some(b)), _) = point(x, y) else {
                unreachable!()
            };
            Some(b)
        };
        for value in [
            wkb(0.0, 0.0),
            wkb(100.0, 0.0),
            wkb(0.0, 100.0),
            wkb(100.0, 100.0),
            None,
        ] {
            let array: ArrayRef = Arc::new(LargeBinaryArray::from_iter([value]));
            writer
                .write(&RecordBatch::try_new(schema.clone(), vec![array]).unwrap())
                .unwrap();
        }
        writer.close().unwrap();
        let reader = SerializedFileReader::new(bytes::Bytes::from(buf)).unwrap();
        reader.metadata().clone()
    }

    #[test]
    fn prunes_row_groups_outside_the_box() {
        let meta = parquet_file();
        assert_eq!(meta.num_row_groups(), 5);
        let p = prune(call(
            "st_intersects",
            vec![col("g"), wkb_box(90.0, -5.0, 110.0, 5.0)],
        ));
        let pruned = Count::new();
        let pruner = GeoRowGroupPruner::new(Arc::from(vec![p]), pruned.clone());
        assert_eq!(pruner.select(&meta, (0..5).collect()), vec![1]);
        assert_eq!(pruned.value(), 4);
    }

    #[test]
    fn a_column_without_statistics_keeps_every_row_group() {
        let meta = parquet_file();
        let mut p = prune(call("st_intersects", vec![col("g"), point(-50.0, -50.0)]));
        p.column = "missing".into();
        p.field_id = None;
        let pruner = GeoRowGroupPruner::new(Arc::from(vec![p]), Count::new());
        assert_eq!(pruner.select(&meta, (0..5).collect()), vec![0, 1, 2, 3, 4]);
    }
}
