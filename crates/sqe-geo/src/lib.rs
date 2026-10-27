//! `ST_*` scalar functions over WKB geometry values (#863).
//!
//! An Iceberg V3 `geometry` / `geography` column scans as WKB bytes in a
//! `LargeBinary` column tagged `ARROW:extension:name = geoarrow.wkb`. These
//! functions decode the WKB with the `wkb` crate, compute with `geo`, and
//! return a geometry result as little-endian WKB in a `LargeBinary`, which
//! writes into a `geometry` column. The result carries no tag and no CRS
//! (see [`geometry_field`]).
//!
//! Trino's names come first (`ST_GeometryFromText`, `ST_GeomFromBinary`),
//! with the PostGIS / Spark spellings as aliases. Every function is
//! strict: a NULL argument gives NULL.
//!
//! All the math is planar: lengths, areas and distances are in the units of
//! the coordinates, as in PostGIS `geometry`. A function whose answer depends
//! on the edge model refuses a `geography` argument (the field carries an
//! `edges` key) at planning time, rather than answer a spherical question
//! with planar math. Formatting and accessors (`ST_AsText`, `ST_X`,
//! `ST_GeometryType`, ...) accept both. To force planar math over geography
//! values, rebuild them: `ST_GeomFromBinary(ST_AsBinary(g))`.

use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBuilder, Float64Array, Float64Builder, Int64Builder,
    LargeBinaryArray, LargeBinaryBuilder, StringArray, StringBuilder,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{exec_err, plan_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use geo::{
    Area, BoundingRect, Centroid, CoordsIter, Distance, Euclidean, HasDimensions, Length, Relate,
    Validation,
};
use geo_traits::to_geo::ToGeoGeometry;
use geo_types::{Geometry, LineString, Point, Rect};
use wkt::ToWkt;

/// The Arrow extension name a WKB column carries.
pub const GEOARROW_WKB: &str = "geoarrow.wkb";
const EXTENSION_NAME: &str = "ARROW:extension:name";
const EXTENSION_METADATA: &str = "ARROW:extension:metadata";

/// Register every `ST_*` function on `ctx`.
pub fn register_geo_functions(ctx: &SessionContext) {
    for udf in geo_udfs() {
        ctx.register_udf(udf);
    }
}

/// Every `ST_*` function, for a registry other than a `SessionContext`.
pub fn geo_udfs() -> Vec<ScalarUDF> {
    Op::ALL
        .iter()
        .map(|op| ScalarUDF::new_from_impl(StFunction::new(*op)))
        .chain(std::iter::once(ScalarUDF::new_from_impl(StSrid {
            signature: Signature::user_defined(Volatility::Immutable),
        })))
        .collect()
}

/// The field a geometry result carries: WKB in a `LargeBinary`, the type a
/// scanned geometry column has. Untagged: DataFusion refuses a `VALUES` list
/// whose rows differ in field metadata, so a tagged `ST_Point(1, 2)` next to
/// a `NULL` or an `X'..'` literal would not plan.
pub fn geometry_field(name: &str) -> Field {
    Field::new(name, DataType::LargeBinary, true)
}

/// Whether `field` holds `geography` values: a `geoarrow.wkb` field whose
/// extension metadata names an edge model other than planar.
pub fn is_geography(field: &Field) -> bool {
    if field.metadata().get(EXTENSION_NAME).map(String::as_str) != Some(GEOARROW_WKB) {
        return false;
    }
    let Some(raw) = field.metadata().get(EXTENSION_METADATA) else {
        return false;
    };
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|m| {
            m.get("edges")
                .and_then(|e| e.as_str())
                .map(|e| e != "planar")
        })
        .unwrap_or(false)
}

/// The CRS and edge model a `geoarrow.wkb` field carries; `None` for any
/// other field. An absent CRS is `OGC:CRS84`, the Iceberg default; a PROJJSON
/// object CRS is kept as its JSON text.
fn wkb_crs_and_edges(field: &Field) -> Option<(String, Option<String>)> {
    if field.metadata().get(EXTENSION_NAME).map(String::as_str) != Some(GEOARROW_WKB) {
        return None;
    }
    let meta = field
        .metadata()
        .get(EXTENSION_METADATA)
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());
    let crs = match meta.as_ref().and_then(|m| m.get("crs")) {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(other) if !other.is_null() => other.to_string(),
        _ => "OGC:CRS84".to_string(),
    };
    let edges = meta
        .as_ref()
        .and_then(|m| m.get("edges"))
        .and_then(|e| e.as_str())
        .filter(|e| *e != "planar")
        .map(str::to_string);
    Some((crs, edges))
}

/// The SRID a CRS string names: `OGC:CRS84` is 4326, `EPSG:n` and
/// `SRID:n` are n. Anything else (another authority, PROJJSON) is 0, as
/// PostGIS reports an unknown SRID.
fn srid_of_crs(crs: &str) -> i64 {
    if crs.eq_ignore_ascii_case("OGC:CRS84") {
        return 4326;
    }
    let Some((authority, code)) = crs.split_once(':') else {
        return 0;
    };
    if authority.eq_ignore_ascii_case("EPSG") || authority.eq_ignore_ascii_case("SRID") {
        code.parse().unwrap_or(0)
    } else {
        0
    }
}

/// The SRID of a `geoarrow.wkb` field's values, from its CRS; 0 for an
/// untagged field (a computed geometry carries no CRS).
pub fn field_srid(field: &Field) -> i64 {
    wkb_crs_and_edges(field).map_or(0, |(crs, _)| srid_of_crs(&crs))
}

/// The type a `geoarrow.wkb` field has in CREATE TABLE, in the SRID form
/// Spark's DESCRIBE prints and SQE's DDL reads back: `geometry(4326)`,
/// `geometry(3857)`, `geography(4326, karney)`. `None` for any other field.
/// A CRS the numeric form cannot say round-trips quoted:
/// `geometry('ESRI:102100')`. A geography's default `spherical` edges are
/// left out, as Spark leaves them out.
pub fn ddl_type(field: &Field) -> Option<String> {
    let (crs, edges) = wkb_crs_and_edges(field)?;
    let srid = srid_of_crs(&crs);
    // `GEOMETRY(0)` reads back as `SRID:0` and `GEOMETRY(n)` as `EPSG:n`
    // (n = 4326 as OGC:CRS84), so the number is exact for those three.
    let exact = crs.eq_ignore_ascii_case("OGC:CRS84")
        || crs.eq_ignore_ascii_case("SRID:0")
        || (srid != 0 && crs.eq_ignore_ascii_case(&format!("EPSG:{srid}")));
    let crs_arg = if exact {
        srid.to_string()
    } else {
        format!("'{}'", crs.replace('\'', "''"))
    };
    Some(match edges {
        None => format!("geometry({crs_arg})"),
        Some(e) if e == "spherical" => format!("geography({crs_arg})"),
        Some(e) => format!("geography({crs_arg}, {e})"),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Arg {
    Geom,
    Num,
    Text,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Ret {
    Geom,
    Bytes,
    Text,
    Num,
    Int,
    Bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Op {
    GeomFromText,
    GeomFromBinary,
    Point,
    MakeEnvelope,
    AsText,
    AsBinary,
    X,
    Y,
    GeometryType,
    NumPoints,
    IsEmpty,
    IsValid,
    XMin,
    XMax,
    YMin,
    YMax,
    Envelope,
    Area,
    Length,
    Distance,
    Centroid,
    Intersects,
    Contains,
    Within,
    Disjoint,
    Equals,
    Touches,
    Crosses,
    Overlaps,
    DWithin,
}

impl Op {
    const ALL: [Op; 30] = [
        Op::GeomFromText,
        Op::GeomFromBinary,
        Op::Point,
        Op::MakeEnvelope,
        Op::AsText,
        Op::AsBinary,
        Op::X,
        Op::Y,
        Op::GeometryType,
        Op::NumPoints,
        Op::IsEmpty,
        Op::IsValid,
        Op::XMin,
        Op::XMax,
        Op::YMin,
        Op::YMax,
        Op::Envelope,
        Op::Area,
        Op::Length,
        Op::Distance,
        Op::Centroid,
        Op::Intersects,
        Op::Contains,
        Op::Within,
        Op::Disjoint,
        Op::Equals,
        Op::Touches,
        Op::Crosses,
        Op::Overlaps,
        Op::DWithin,
    ];

    /// The registered name (lower case: DataFusion folds unquoted calls).
    fn name(self) -> &'static str {
        match self {
            Op::GeomFromText => "st_geometryfromtext",
            Op::GeomFromBinary => "st_geomfrombinary",
            Op::Point => "st_point",
            Op::MakeEnvelope => "st_makeenvelope",
            Op::AsText => "st_astext",
            Op::AsBinary => "st_asbinary",
            Op::X => "st_x",
            Op::Y => "st_y",
            Op::GeometryType => "st_geometrytype",
            Op::NumPoints => "st_numpoints",
            Op::IsEmpty => "st_isempty",
            Op::IsValid => "st_isvalid",
            Op::XMin => "st_xmin",
            Op::XMax => "st_xmax",
            Op::YMin => "st_ymin",
            Op::YMax => "st_ymax",
            Op::Envelope => "st_envelope",
            Op::Area => "st_area",
            Op::Length => "st_length",
            Op::Distance => "st_distance",
            Op::Centroid => "st_centroid",
            Op::Intersects => "st_intersects",
            Op::Contains => "st_contains",
            Op::Within => "st_within",
            Op::Disjoint => "st_disjoint",
            Op::Equals => "st_equals",
            Op::Touches => "st_touches",
            Op::Crosses => "st_crosses",
            Op::Overlaps => "st_overlaps",
            Op::DWithin => "st_dwithin",
        }
    }

    /// PostGIS / Spark spellings of the same function.
    fn aliases(self) -> &'static [&'static str] {
        match self {
            Op::GeomFromText => &["st_geomfromtext"],
            Op::GeomFromBinary => &["st_geomfromwkb"],
            Op::AsText => &["st_aswkt"],
            Op::AsBinary => &["st_aswkb"],
            _ => &[],
        }
    }

    fn args(self) -> &'static [Arg] {
        use Arg::*;
        match self {
            Op::GeomFromText => &[Text],
            Op::Point => &[Num, Num],
            Op::MakeEnvelope => &[Num, Num, Num, Num],
            Op::Distance
            | Op::Intersects
            | Op::Contains
            | Op::Within
            | Op::Disjoint
            | Op::Equals
            | Op::Touches
            | Op::Crosses
            | Op::Overlaps => &[Geom, Geom],
            Op::DWithin => &[Geom, Geom, Num],
            _ => &[Geom],
        }
    }

    fn ret(self) -> Ret {
        match self {
            Op::GeomFromText
            | Op::GeomFromBinary
            | Op::Point
            | Op::MakeEnvelope
            | Op::Envelope
            | Op::Centroid => Ret::Geom,
            Op::AsBinary => Ret::Bytes,
            Op::AsText | Op::GeometryType => Ret::Text,
            Op::X
            | Op::Y
            | Op::XMin
            | Op::XMax
            | Op::YMin
            | Op::YMax
            | Op::Area
            | Op::Length
            | Op::Distance => Ret::Num,
            Op::NumPoints => Ret::Int,
            _ => Ret::Bool,
        }
    }

    /// Whether the answer depends on the edge model, so a `geography`
    /// argument is refused.
    fn planar(self) -> bool {
        !matches!(
            self,
            Op::GeomFromBinary
                | Op::AsText
                | Op::AsBinary
                | Op::X
                | Op::Y
                | Op::GeometryType
                | Op::NumPoints
                | Op::IsEmpty
        )
    }
}

/// `ST_SRID(g)`: the SRID of the column's CRS (see [`field_srid`]), NULL
/// for a NULL value. The SRID is a property of the column, not of the WKB
/// bytes, so it comes from the argument's field metadata.
#[derive(Debug, PartialEq, Eq, Hash)]
struct StSrid {
    signature: Signature,
}

impl ScalarUDFImpl for StSrid {
    fn name(&self) -> &str {
        "st_srid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [DataType::Binary | DataType::LargeBinary | DataType::BinaryView] => {
                Ok(arg_types.to_vec())
            }
            [DataType::Null] => Ok(vec![DataType::Binary]),
            [t] => plan_err!("st_srid: argument 1 must be a geometry (WKB binary), got {t}"),
            _ => plan_err!("st_srid takes 1 argument(s), got {}", arg_types.len()),
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let srid = args.arg_fields.first().map_or(0, |f| field_srid(f)) as i32;
        match &args.args[0] {
            ColumnarValue::Scalar(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                (!v.is_null()).then_some(srid),
            ))),
            ColumnarValue::Array(a) => {
                let out: arrow::array::Int32Array = (0..a.len())
                    .map(|i| a.is_valid(i).then_some(srid))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(out)))
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct StFunction {
    op: Op,
    aliases: Vec<String>,
    signature: Signature,
}

impl StFunction {
    fn new(op: Op) -> Self {
        Self {
            op,
            aliases: op.aliases().iter().map(|a| a.to_string()).collect(),
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StFunction {
    fn name(&self) -> &str {
        self.op.name()
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let want = self.op.args();
        if arg_types.len() != want.len() {
            return plan_err!(
                "{} takes {} argument(s), got {}",
                self.name(),
                want.len(),
                arg_types.len()
            );
        }
        want.iter()
            .zip(arg_types)
            .enumerate()
            .map(|(i, (kind, t))| {
                let ok = match kind {
                    Arg::Geom => matches!(
                        t,
                        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
                    ),
                    Arg::Text => {
                        matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
                    }
                    Arg::Num => t.is_numeric(),
                };
                match (kind, ok, t) {
                    (Arg::Num, true, _) => Ok(DataType::Float64),
                    (_, true, _) => Ok(t.clone()),
                    (Arg::Geom, false, DataType::Null) => Ok(DataType::Binary),
                    (Arg::Text, false, DataType::Null) => Ok(DataType::Utf8),
                    (Arg::Num, false, DataType::Null) => Ok(DataType::Float64),
                    (kind, false, t) => plan_err!(
                        "{}: argument {} must be {}, got {t}",
                        self.name(),
                        i + 1,
                        match kind {
                            Arg::Geom => "a geometry (WKB binary)",
                            Arg::Text => "a string",
                            Arg::Num => "a number",
                        }
                    ),
                }
            })
            .collect()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(format!(
            "{}: return_field_from_args is used instead",
            self.name()
        )))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if self.op.planar() {
            if let Some(f) = args.arg_fields.iter().find(|f| is_geography(f)) {
                return plan_err!(
                    "{} uses planar math and '{}' is a GEOGRAPHY value; \
                     ST_GeomFromBinary(ST_AsBinary({})) reads it as planar geometry",
                    self.name(),
                    f.name(),
                    f.name()
                );
            }
        }
        let name = self.name();
        let field = match self.op.ret() {
            Ret::Geom => geometry_field(name),
            Ret::Bytes => Field::new(name, DataType::Binary, true),
            Ret::Text => Field::new(name, DataType::Utf8, true),
            Ret::Num => Field::new(name, DataType::Float64, true),
            Ret::Int => Field::new(name, DataType::Int64, true),
            Ret::Bool => Field::new(name, DataType::Boolean, true),
        };
        Ok(Arc::new(field))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let all_scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let rows = if all_scalar { 1 } else { args.number_rows };
        let cols = args
            .args
            .iter()
            .zip(self.op.args())
            .map(|(a, kind)| Input::new(self.name(), a, *kind))
            .collect::<Result<Vec<_>>>()?;
        let mut out = Output::new(self.op.ret(), rows);
        let mut vals = Vec::with_capacity(cols.len());
        for row in 0..rows {
            vals.clear();
            for c in &cols {
                match c.get(self.name(), row)? {
                    Some(v) => vals.push(v),
                    None => break,
                }
            }
            if vals.len() < cols.len() {
                out.push(Value::Null)?;
            } else {
                out.push(eval(self.op, &vals)?)?;
            }
        }
        let array = out.finish();
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &array, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(array))
        }
    }
}

/// One argument, decoded once when it is a constant.
enum Input {
    Geom(Option<Geometry>),
    GeomArray(LargeBinaryArray),
    Num(Option<f64>),
    NumArray(Float64Array),
    Text(Option<String>),
    TextArray(StringArray),
}

/// One argument value of one row.
enum Val<'a> {
    Geom(Cow<'a, Geometry>),
    Num(f64),
    Text(&'a str),
}

impl Input {
    fn new(name: &str, arg: &ColumnarValue, kind: Arg) -> Result<Self> {
        let target = match kind {
            Arg::Geom => DataType::LargeBinary,
            Arg::Num => DataType::Float64,
            Arg::Text => DataType::Utf8,
        };
        Ok(match arg {
            ColumnarValue::Scalar(s) => {
                let s = s.cast_to(&target)?;
                match s {
                    ScalarValue::LargeBinary(b) => {
                        Input::Geom(b.as_deref().map(|b| decode(name, b)).transpose()?)
                    }
                    ScalarValue::Float64(v) => Input::Num(v),
                    ScalarValue::Utf8(v) => Input::Text(v),
                    other => return exec_err!("{name}: unexpected argument {other:?}"),
                }
            }
            ColumnarValue::Array(a) => {
                let a = cast(a, &target)?;
                match kind {
                    Arg::Geom => Input::GeomArray(downcast(&a)?),
                    Arg::Num => Input::NumArray(downcast(&a)?),
                    Arg::Text => Input::TextArray(downcast(&a)?),
                }
            }
        })
    }

    fn get(&self, name: &str, row: usize) -> Result<Option<Val<'_>>> {
        Ok(match self {
            Input::Geom(g) => g.as_ref().map(|g| Val::Geom(Cow::Borrowed(g))),
            Input::Num(v) => v.map(Val::Num),
            Input::Text(t) => t.as_deref().map(Val::Text),
            Input::GeomArray(a) if a.is_valid(row) => {
                Some(Val::Geom(Cow::Owned(decode(name, a.value(row))?)))
            }
            Input::NumArray(a) if a.is_valid(row) => Some(Val::Num(a.value(row))),
            Input::TextArray(a) if a.is_valid(row) => Some(Val::Text(a.value(row))),
            _ => None,
        })
    }
}

fn downcast<T: Array + Clone + 'static>(a: &ArrayRef) -> Result<T> {
    a.as_any()
        .downcast_ref::<T>()
        .cloned()
        .ok_or_else(|| DataFusionError::Internal(format!("unexpected array {}", a.data_type())))
}

/// Parse WKT into a `geo` geometry. `geo` has no empty point, and the `wkt`
/// conversion turns `POINT EMPTY` into an empty MULTIPOINT; refuse it as
/// [`decode`] refuses an empty WKB point.
fn from_wkt(name: &str, text: &str) -> Result<Geometry> {
    let invalid =
        |e: &dyn std::fmt::Display| DataFusionError::Execution(format!("{name}: invalid WKT: {e}"));
    let wkt: wkt::Wkt<f64> = text.parse().map_err(|e: &str| invalid(&e))?;
    if matches!(&wkt, wkt::Wkt::Point(p) if p.coord().is_none()) {
        return exec_err!("{name}: an empty POINT is not supported");
    }
    Geometry::try_from(wkt).map_err(|e| invalid(&e))
}

/// Decode WKB into a `geo` geometry.
fn decode(name: &str, bytes: &[u8]) -> Result<Geometry> {
    let wkb = wkb::reader::read_wkb(bytes)
        .map_err(|e| DataFusionError::Execution(format!("{name}: invalid WKB: {e}")))?;
    match wkb.try_to_geometry() {
        Some(g) => Ok(g),
        None => exec_err!("{name}: an empty POINT is not supported"),
    }
}

fn encode(g: &Geometry) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    wkb::writer::write_geometry(&mut buf, g, &Default::default())
        .map_err(|e| DataFusionError::Execution(format!("WKB encoding failed: {e}")))?;
    Ok(buf)
}

/// One row's result.
enum Value {
    Null,
    Geom(Geometry),
    Bytes(Vec<u8>),
    Text(String),
    Num(f64),
    Int(i64),
    Bool(bool),
}

enum Output {
    Geom(LargeBinaryBuilder),
    Bytes(BinaryBuilder),
    Text(StringBuilder),
    Num(Float64Builder),
    Int(Int64Builder),
    Bool(BooleanBuilder),
}

impl Output {
    fn new(ret: Ret, rows: usize) -> Self {
        match ret {
            Ret::Geom => Output::Geom(LargeBinaryBuilder::with_capacity(rows, rows * 21)),
            Ret::Bytes => Output::Bytes(BinaryBuilder::with_capacity(rows, rows * 21)),
            Ret::Text => Output::Text(StringBuilder::with_capacity(rows, rows * 16)),
            Ret::Num => Output::Num(Float64Builder::with_capacity(rows)),
            Ret::Int => Output::Int(Int64Builder::with_capacity(rows)),
            Ret::Bool => Output::Bool(BooleanBuilder::with_capacity(rows)),
        }
    }

    fn push(&mut self, v: Value) -> Result<()> {
        match (self, v) {
            (Output::Geom(b), Value::Null) => b.append_null(),
            (Output::Bytes(b), Value::Null) => b.append_null(),
            (Output::Text(b), Value::Null) => b.append_null(),
            (Output::Num(b), Value::Null) => b.append_null(),
            (Output::Int(b), Value::Null) => b.append_null(),
            (Output::Bool(b), Value::Null) => b.append_null(),
            (Output::Geom(b), Value::Geom(g)) => b.append_value(encode(&g)?),
            (Output::Bytes(b), Value::Bytes(v)) => b.append_value(v),
            (Output::Text(b), Value::Text(v)) => b.append_value(v),
            (Output::Num(b), Value::Num(v)) => b.append_value(v),
            (Output::Int(b), Value::Int(v)) => b.append_value(v),
            (Output::Bool(b), Value::Bool(v)) => b.append_value(v),
            _ => return exec_err!("ST function produced a value of the wrong type"),
        }
        Ok(())
    }

    fn finish(self) -> ArrayRef {
        match self {
            Output::Geom(mut b) => Arc::new(b.finish()),
            Output::Bytes(mut b) => Arc::new(b.finish()),
            Output::Text(mut b) => Arc::new(b.finish()),
            Output::Num(mut b) => Arc::new(b.finish()),
            Output::Int(mut b) => Arc::new(b.finish()),
            Output::Bool(mut b) => Arc::new(b.finish()),
        }
    }
}

fn geom<'a>(v: &'a Val<'_>) -> &'a Geometry {
    match v {
        Val::Geom(g) => g,
        _ => unreachable!("argument kinds are checked in coerce_types"),
    }
}

fn num(v: &Val<'_>) -> f64 {
    match v {
        Val::Num(n) => *n,
        _ => unreachable!("argument kinds are checked in coerce_types"),
    }
}

fn text<'a>(v: &'a Val<'_>) -> &'a str {
    match v {
        Val::Text(t) => t,
        _ => unreachable!("argument kinds are checked in coerce_types"),
    }
}

/// Trino's `ST_GeometryType` names.
fn type_name(g: &Geometry) -> &'static str {
    match g {
        Geometry::Point(_) => "ST_Point",
        Geometry::Line(_) | Geometry::LineString(_) => "ST_LineString",
        Geometry::Polygon(_) | Geometry::Rect(_) | Geometry::Triangle(_) => "ST_Polygon",
        Geometry::MultiPoint(_) => "ST_MultiPoint",
        Geometry::MultiLineString(_) => "ST_MultiLineString",
        Geometry::MultiPolygon(_) => "ST_MultiPolygon",
        Geometry::GeometryCollection(_) => "ST_GeomCollection",
    }
}

/// The bounding box as a geometry: a point or a line when it has no area,
/// as PostGIS's `ST_Envelope` does.
fn envelope(r: Rect) -> Geometry {
    let (min, max) = (r.min(), r.max());
    if r.width() == 0.0 && r.height() == 0.0 {
        Geometry::Point(Point::from(min))
    } else if r.width() == 0.0 || r.height() == 0.0 {
        Geometry::LineString(LineString::from(vec![min, max]))
    } else {
        // PostGIS's ring order: from the lower-left corner, up first.
        let ring = vec![min, (min.x, max.y).into(), max, (max.x, min.y).into(), min];
        Geometry::Polygon(geo_types::Polygon::new(LineString::from(ring), vec![]))
    }
}

fn eval(op: Op, a: &[Val<'_>]) -> Result<Value> {
    let name = op.name();
    let bbox = |f: fn(Rect) -> f64| {
        Ok(geom(&a[0])
            .bounding_rect()
            .map_or(Value::Null, |r| Value::Num(f(r))))
    };
    let relate = |f: fn(&geo::relate::IntersectionMatrix) -> bool| {
        Ok(Value::Bool(f(&geom(&a[0]).relate(geom(&a[1])))))
    };
    match op {
        Op::GeomFromText => from_wkt(name, text(&a[0])).map(Value::Geom),
        Op::GeomFromBinary => Ok(Value::Geom(geom(&a[0]).clone())),
        Op::Point => Ok(Value::Geom(Geometry::Point(Point::new(
            num(&a[0]),
            num(&a[1]),
        )))),
        Op::MakeEnvelope => {
            let (x0, y0, x1, y1) = (num(&a[0]), num(&a[1]), num(&a[2]), num(&a[3]));
            Ok(Value::Geom(envelope(Rect::new((x0, y0), (x1, y1)))))
        }
        Op::AsText => Ok(Value::Text(geom(&a[0]).wkt_string())),
        Op::AsBinary => Ok(Value::Bytes(encode(geom(&a[0]))?)),
        Op::X | Op::Y => match geom(&a[0]) {
            Geometry::Point(p) => Ok(Value::Num(if op == Op::X { p.x() } else { p.y() })),
            g => exec_err!("{name} only applies to a POINT, got {}", type_name(g)),
        },
        Op::GeometryType => Ok(Value::Text(type_name(geom(&a[0])).to_string())),
        Op::NumPoints => Ok(Value::Int(geom(&a[0]).coords_count() as i64)),
        Op::IsEmpty => Ok(Value::Bool(geom(&a[0]).is_empty())),
        Op::IsValid => Ok(Value::Bool(geom(&a[0]).is_valid())),
        Op::XMin => bbox(|r| r.min().x),
        Op::XMax => bbox(|r| r.max().x),
        Op::YMin => bbox(|r| r.min().y),
        Op::YMax => bbox(|r| r.max().y),
        Op::Envelope => Ok(geom(&a[0])
            .bounding_rect()
            .map_or(Value::Null, |r| Value::Geom(envelope(r)))),
        Op::Area => Ok(Value::Num(geom(&a[0]).unsigned_area())),
        Op::Length => match geom(&a[0]) {
            Geometry::Line(l) => Ok(Value::Num(Euclidean.length(l))),
            Geometry::LineString(l) => Ok(Value::Num(Euclidean.length(l))),
            Geometry::MultiLineString(l) => Ok(Value::Num(Euclidean.length(l))),
            g => exec_err!(
                "{name} only applies to a LINESTRING or MULTILINESTRING, got {}",
                type_name(g)
            ),
        },
        Op::Distance => Ok(distance(geom(&a[0]), geom(&a[1])).map_or(Value::Null, Value::Num)),
        Op::Centroid => Ok(geom(&a[0])
            .centroid()
            .map_or(Value::Null, |p| Value::Geom(Geometry::Point(p)))),
        Op::Intersects => relate(|m| m.is_intersects()),
        Op::Contains => relate(|m| m.is_contains()),
        Op::Within => relate(|m| m.is_within()),
        Op::Disjoint => relate(|m| m.is_disjoint()),
        Op::Equals => relate(|m| m.is_equal_topo()),
        Op::Touches => relate(|m| m.is_touches()),
        Op::Crosses => relate(|m| m.is_crosses()),
        Op::Overlaps => relate(|m| m.is_overlaps()),
        Op::DWithin => Ok(distance(geom(&a[0]), geom(&a[1]))
            .map_or(Value::Null, |d| Value::Bool(d <= num(&a[2])))),
    }
}

/// Planar distance, NULL when either side is empty.
fn distance(a: &Geometry, b: &Geometry) -> Option<f64> {
    (!a.is_empty() && !b.is_empty()).then(|| Euclidean.distance(a, b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        register_geo_functions(&ctx);
        ctx
    }

    async fn one(sql: &str) -> String {
        let batches = ctx().sql(sql).await.unwrap().collect().await.unwrap();
        let col = batches[0].column(0);
        let fmt = arrow::util::display::ArrayFormatter::try_new(col.as_ref(), &Default::default());
        let value = fmt.unwrap().value(0).to_string();
        value
    }

    async fn err(sql: &str) -> String {
        match ctx().sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => df.collect().await.expect_err(sql).to_string(),
        }
    }

    #[tokio::test]
    async fn constructors_round_trip_through_wkt_and_wkb() {
        assert_eq!(one("SELECT ST_AsText(ST_Point(1, 2))").await, "POINT(1 2)");
        assert_eq!(
            one("SELECT ST_AsText(ST_GeomFromText('LINESTRING (0 0, 3 4)'))").await,
            "LINESTRING(0 0,3 4)"
        );
        // ST_Point(1, 2) as little-endian WKB.
        let wkb = "0101000000000000000000f03f0000000000000040";
        assert_eq!(
            one("SELECT encode(ST_AsBinary(ST_Point(1, 2)), 'hex')").await,
            wkb
        );
        assert_eq!(
            one(&format!("SELECT ST_AsText(ST_GeomFromBinary(X'{wkb}'))")).await,
            "POINT(1 2)"
        );
        assert_eq!(
            one("SELECT ST_AsText(ST_MakeEnvelope(0, 0, 2, 1))").await,
            "POLYGON((0 0,0 1,2 1,2 0,0 0))"
        );
    }

    #[tokio::test]
    async fn accessors_and_measures() {
        assert_eq!(one("SELECT ST_X(ST_Point(1.5, 2))").await, "1.5");
        assert_eq!(one("SELECT ST_Y(ST_Point(1.5, 2))").await, "2.0");
        assert_eq!(
            one("SELECT ST_GeometryType(ST_GeometryFromText('POLYGON ((0 0, 1 0, 1 1, 0 0))'))")
                .await,
            "ST_Polygon"
        );
        assert_eq!(
            one("SELECT ST_NumPoints(ST_GeomFromText('LINESTRING (0 0, 1 1, 2 0)'))").await,
            "3"
        );
        assert_eq!(
            one("SELECT ST_IsEmpty(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'))").await,
            "true"
        );
        assert_eq!(
            one("SELECT ST_IsValid(ST_MakeEnvelope(0, 0, 1, 1))").await,
            "true"
        );
        assert_eq!(
            one("SELECT ST_IsValid(ST_GeomFromText('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))'))").await,
            "false"
        );
        assert_eq!(
            one("SELECT ST_XMax(ST_GeomFromText('LINESTRING (0 0, 3 4)'))").await,
            "3.0"
        );
        assert_eq!(
            one("SELECT ST_YMin(ST_GeomFromText('LINESTRING (0 -1, 3 4)'))").await,
            "-1.0"
        );
        assert_eq!(
            one("SELECT ST_Area(ST_MakeEnvelope(0, 0, 2, 3))").await,
            "6.0"
        );
        assert_eq!(
            one("SELECT ST_Length(ST_GeomFromText('LINESTRING (0 0, 3 4)'))").await,
            "5.0"
        );
        assert_eq!(
            one("SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4))").await,
            "5.0"
        );
        assert_eq!(
            one("SELECT ST_AsText(ST_Centroid(ST_MakeEnvelope(0, 0, 2, 2)))").await,
            "POINT(1 1)"
        );
        assert_eq!(
            one("SELECT ST_AsText(ST_Envelope(ST_GeomFromText('LINESTRING (0 0, 3 4)')))").await,
            "POLYGON((0 0,0 4,3 4,3 0,0 0))"
        );
        assert_eq!(
            one("SELECT ST_AsText(ST_Envelope(ST_Point(1, 2)))").await,
            "POINT(1 2)"
        );
    }

    #[tokio::test]
    async fn predicates() {
        let square = "ST_MakeEnvelope(0, 0, 10, 10)";
        let cases = [
            (format!("ST_Contains({square}, ST_Point(5, 5))"), "true"),
            (format!("ST_Contains({square}, ST_Point(15, 5))"), "false"),
            (format!("ST_Within(ST_Point(5, 5), {square})"), "true"),
            (format!("ST_Intersects({square}, ST_Point(10, 5))"), "true"),
            (format!("ST_Contains({square}, ST_Point(10, 5))"), "false"),
            (format!("ST_Touches({square}, ST_Point(10, 5))"), "true"),
            (format!("ST_Disjoint({square}, ST_Point(11, 5))"), "true"),
            (format!("ST_Equals({square}, ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'))"), "true"),
            (format!("ST_Overlaps({square}, ST_MakeEnvelope(5, 5, 15, 15))"), "true"),
            (format!("ST_Crosses(ST_GeomFromText('LINESTRING (-1 5, 11 5)'), {square})"), "true"),
            ("ST_DWithin(ST_Point(0, 0), ST_Point(3, 4), 5)".to_string(), "true"),
            ("ST_DWithin(ST_Point(0, 0), ST_Point(3, 4), 4.9)".to_string(), "false"),
        ];
        for (expr, want) in cases {
            assert_eq!(one(&format!("SELECT {expr}")).await, want, "{expr}");
        }
    }

    #[tokio::test]
    async fn null_in_null_out_and_bad_input_errors() {
        assert_eq!(one("SELECT ST_AsText(NULL)").await, "");
        assert_eq!(one("SELECT ST_Point(1, NULL) IS NULL").await, "true");
        assert!(err("SELECT ST_GeomFromText('POINT (1')")
            .await
            .contains("invalid WKT"));
        assert!(err("SELECT ST_AsText(X'0102')")
            .await
            .contains("invalid WKB"));
        assert!(err("SELECT ST_X(ST_MakeEnvelope(0, 0, 1, 1))")
            .await
            .contains("only applies to a POINT"));
        assert!(err("SELECT ST_Area('POINT (1 2)')")
            .await
            .contains("must be a geometry"));
        assert!(err("SELECT ST_GeomFromText('POINT EMPTY')")
            .await
            .contains("an empty POINT is not supported"));
        // VALUES refuses rows whose field metadata differs.
        let values = "SELECT count(*) FROM (VALUES (ST_Point(1, 2)), (NULL), (ST_GeomFromText('POINT (3 4)')))";
        assert_eq!(one(values).await, "3");
    }

    /// A table column: one WKB array, a constant geometry on the other side,
    /// and an untagged LargeBinary for every geometry result.
    #[tokio::test]
    async fn column_input_and_untagged_result() {
        let tagged = |name: &str, edges: Option<&str>| {
            let mut meta = std::collections::HashMap::from([(
                EXTENSION_NAME.to_string(),
                GEOARROW_WKB.to_string(),
            )]);
            if let Some(e) = edges {
                meta.insert(
                    EXTENSION_METADATA.to_string(),
                    format!(r#"{{"edges":"{e}"}}"#),
                );
            }
            Field::new(name, DataType::LargeBinary, true).with_metadata(meta)
        };
        let pts = [Some((1.0, 2.0)), None, Some((20.0, 20.0))];
        let wkb: Vec<Option<Vec<u8>>> = pts
            .iter()
            .map(|p| p.map(|(x, y)| encode(&Geometry::Point(Point::new(x, y))).unwrap()))
            .collect();
        let col: ArrayRef = Arc::new(LargeBinaryArray::from_iter(
            wkb.iter().map(|b| b.as_deref()),
        ));
        let schema = Arc::new(Schema::new(vec![
            tagged("g", None),
            tagged("geo", Some("spherical")),
        ]));
        let batch = RecordBatch::try_new(schema, vec![col.clone(), col]).unwrap();
        let ctx = ctx();
        ctx.register_batch("t", batch).unwrap();

        let df = ctx
            .sql("SELECT ST_Contains(ST_MakeEnvelope(0, 0, 10, 10), g) AS c, ST_Centroid(g) AS p FROM t")
            .await
            .unwrap();
        let p = df.schema().field_with_name(None, "p").unwrap().clone();
        assert!(
            p.metadata().is_empty(),
            "a VALUES list mixes results with NULL: {p:?}"
        );
        assert_eq!(
            p.data_type(),
            &DataType::LargeBinary,
            "the type a geometry column scans as"
        );
        let out = df.collect().await.unwrap();
        let c = out[0].column(0).as_boolean();
        assert_eq!(
            c.iter().collect::<Vec<_>>(),
            vec![Some(true), None, Some(false)]
        );

        // Geography: formatting works, planar math is refused at planning time.
        let text = ctx
            .sql("SELECT ST_AsText(geo) FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(text[0].column(0).as_string::<i32>().value(0), "POINT(1 2)");
        let refused = ctx
            .sql("SELECT ST_Area(geo) FROM t")
            .await
            .expect_err("planar on geography");
        assert!(refused.to_string().contains("GEOGRAPHY"), "{refused}");
        ctx.sql("SELECT ST_Area(ST_GeomFromBinary(ST_AsBinary(geo))) FROM t")
            .await
            .unwrap();
    }

    /// A `geoarrow.wkb` field as the scan tags it; `meta` is the extension
    /// metadata JSON, `None` for no metadata at all.
    fn wkb_field(meta: Option<&str>) -> Field {
        let mut m = std::collections::HashMap::from([(
            EXTENSION_NAME.to_string(),
            GEOARROW_WKB.to_string(),
        )]);
        if let Some(meta) = meta {
            m.insert(EXTENSION_METADATA.to_string(), meta.to_string());
        }
        Field::new("g", DataType::LargeBinary, true).with_metadata(m)
    }

    /// DESCRIBE / SHOW CREATE types, in the form `geospatial_type` in
    /// sqe-sql reads back to the same Iceberg type.
    #[test]
    fn ddl_type_and_srid_from_field_metadata() {
        let cases = [
            (Some(r#"{"crs":"OGC:CRS84"}"#), "geometry(4326)", 4326),
            (None, "geometry(4326)", 4326),
            (Some(r#"{"crs":"EPSG:3857"}"#), "geometry(3857)", 3857),
            (Some(r#"{"crs":"SRID:0"}"#), "geometry(0)", 0),
            (
                Some(r#"{"crs":"SRID:4326"}"#),
                "geometry('SRID:4326')",
                4326,
            ),
            (
                Some(r#"{"crs":"ESRI:102100"}"#),
                "geometry('ESRI:102100')",
                0,
            ),
            (
                Some(r#"{"crs":"OGC:CRS84","edges":"spherical"}"#),
                "geography(4326)",
                4326,
            ),
            (
                Some(r#"{"crs":"EPSG:4258","edges":"karney"}"#),
                "geography(4258, karney)",
                4258,
            ),
            (
                Some(r#"{"crs":"OGC:CRS84","edges":"planar"}"#),
                "geometry(4326)",
                4326,
            ),
        ];
        for (meta, ddl, srid) in cases {
            let f = wkb_field(meta);
            assert_eq!(ddl_type(&f).as_deref(), Some(ddl), "{meta:?}");
            assert_eq!(field_srid(&f), srid, "{meta:?}");
        }
        let plain = Field::new("b", DataType::LargeBinary, true);
        assert_eq!(ddl_type(&plain), None);
        assert_eq!(field_srid(&plain), 0);
    }

    #[tokio::test]
    async fn st_srid_reads_the_column_crs() {
        let wkb = encode(&Geometry::Point(Point::new(1.0, 2.0))).unwrap();
        let col: ArrayRef = Arc::new(LargeBinaryArray::from_iter([Some(wkb.as_slice()), None]));
        let schema = Arc::new(Schema::new(vec![
            wkb_field(Some(r#"{"crs":"OGC:CRS84"}"#)).with_name("g"),
            wkb_field(Some(r#"{"crs":"EPSG:3857"}"#)).with_name("m"),
        ]));
        let ctx = ctx();
        ctx.register_batch(
            "t",
            RecordBatch::try_new(schema, vec![col.clone(), col]).unwrap(),
        )
        .unwrap();
        let out = ctx
            .sql("SELECT ST_SRID(g), ST_SRID(m), ST_SRID(ST_Point(1, 2)) FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let ints = |i: usize| {
            out[0]
                .column(i)
                .as_primitive::<arrow::datatypes::Int32Type>()
                .iter()
                .collect::<Vec<_>>()
        };
        assert_eq!(ints(0), vec![Some(4326), None]);
        assert_eq!(ints(1), vec![Some(3857), None]);
        assert_eq!(
            ints(2),
            vec![Some(0), Some(0)],
            "a computed geometry has no CRS"
        );
        assert_eq!(one("SELECT ST_SRID(NULL)").await, "");
    }
}
