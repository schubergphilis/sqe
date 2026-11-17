//! What the stock Flight SQL drivers need from a result: a codec they can
//! decode and the column metadata JDBC/ODBC result-set metadata reads.
//!
//! **Compression.** The Arrow Java Flight client (under the Flight SQL JDBC
//! driver) ships no LZ4/ZSTD codec unless the application adds
//! `arrow-compression`, so an LZ4 body is unreadable to it.
//! [`ClientCompressionLayer`] picks the codec per request and puts it in a
//! task-local the handlers read through
//! [`client_compression_override`]:
//!
//! 1. `x-sqe-flight-compression: none|lz4|zstd` wins (an unparsable value is
//!    ignored);
//! 2. a `user-agent` containing `grpc-java` gets no compression. grpc-java
//!    appends its own token after the application's, so the JDBC driver
//!    sends `JDBC Flight SQL Driver 19.0.0 grpc-java-netty/1.x`;
//! 3. otherwise there is no override and `[coordinator] flight_compression`
//!    applies.
//!
//! The task-local is set around the handler future only, so it must be read
//! in the handler body (where the IPC options are built), not in the
//! response stream.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow_schema::{DataType, Field, TimeUnit};
use sqe_core::FlightCompression;
use tonic::codegen::http;
use tower::{Layer, Service};

/// The request header that chooses the result codec for one request.
pub const COMPRESSION_HEADER: &str = "x-sqe-flight-compression";

tokio::task_local! {
    static CLIENT_COMPRESSION: Option<FlightCompression>;
}

/// The codec the current request asked for, if any. `None` outside a request
/// the layer saw, or when the request carries no override.
pub fn client_compression_override() -> Option<FlightCompression> {
    CLIENT_COMPRESSION.try_with(|c| *c).ok().flatten()
}

/// The codec override for a request with these headers.
pub fn compression_for_headers(headers: &http::HeaderMap) -> Option<FlightCompression> {
    if let Some(choice) = headers
        .get(COMPRESSION_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<FlightCompression>().ok())
    {
        return Some(choice);
    }
    let java = headers
        .get(http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ua| ua.contains("grpc-java"));
    java.then_some(FlightCompression::None)
}

/// Sets the per-request codec override; see the module docs.
#[derive(Clone, Copy, Debug, Default)]
pub struct ClientCompressionLayer;

impl<S> Layer<S> for ClientCompressionLayer {
    type Service = ClientCompressionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ClientCompressionService { inner }
    }
}

#[derive(Clone, Debug)]
pub struct ClientCompressionService<S> {
    inner: S,
}

type BoxFuture<R, E> = Pin<Box<dyn Future<Output = Result<R, E>> + Send>>;

impl<S, B> Service<http::Request<B>> for ClientCompressionService<S>
where
    S: Service<http::Request<B>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<S::Response, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let choice = compression_for_headers(request.headers());
        let response = self.inner.call(request);
        Box::pin(CLIENT_COMPRESSION.scope(choice, response))
    }
}

const TYPE_NAME_KEY: &str = "ARROW:FLIGHT:SQL:TYPE_NAME";
const PRECISION_KEY: &str = "ARROW:FLIGHT:SQL:PRECISION";
const SCALE_KEY: &str = "ARROW:FLIGHT:SQL:SCALE";

/// `field` with the Flight SQL column metadata JDBC/ODBC result-set metadata
/// reads: the type name, and precision and scale where the type has them.
/// Derived from the data type alone, so the FlightInfo schema and the stream
/// schema get the same keys.
pub fn with_column_metadata(field: Field) -> Field {
    let mut metadata: HashMap<String, String> = field.metadata().clone();
    let data_type = field.data_type();
    metadata.insert(TYPE_NAME_KEY.into(), type_name(data_type));
    if let Some((precision, scale)) = precision_scale(data_type) {
        metadata.insert(PRECISION_KEY.into(), precision.to_string());
        metadata.insert(SCALE_KEY.into(), scale.to_string());
    }
    field.with_metadata(metadata)
}

/// Decimal precision and scale; for the other numeric types the decimal
/// digits `GetXdbcTypeInfo` reports, with scale 0.
fn precision_scale(data_type: &DataType) -> Option<(u32, i32)> {
    let digits = match data_type {
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => return Some((u32::from(*p), i32::from(*s))),
        DataType::Int8 | DataType::UInt8 => 3,
        DataType::Int16 | DataType::UInt16 => 5,
        DataType::Int32 | DataType::UInt32 => 10,
        DataType::Int64 => 19,
        DataType::UInt64 => 20,
        DataType::Float16 => 3,
        DataType::Float32 => 7,
        DataType::Float64 => 15,
        _ => return None,
    };
    Some((digits, 0))
}

fn fraction_digits(unit: &TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

/// The SQL name of an Arrow type, spelled the way Trino spells it.
fn type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Null => "unknown".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 | DataType::UInt8 => "smallint".into(),
        DataType::Int32 | DataType::UInt16 => "integer".into(),
        DataType::Int64 | DataType::UInt32 => "bigint".into(),
        DataType::UInt64 => "decimal(20,0)".into(),
        DataType::Float16 | DataType::Float32 => "real".into(),
        DataType::Float64 => "double".into(),
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => format!("decimal({p},{s})"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "varchar".into(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "varbinary".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Time32(unit) | DataType::Time64(unit) => {
            format!("time({})", fraction_digits(unit))
        }
        DataType::Timestamp(unit, None) => format!("timestamp({})", fraction_digits(unit)),
        DataType::Timestamp(unit, Some(_)) => {
            format!("timestamp({}) with time zone", fraction_digits(unit))
        }
        DataType::Interval(_) | DataType::Duration(_) => "interval".into(),
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _) => format!("array({})", type_name(item.data_type())),
        DataType::Struct(fields) => {
            let members: Vec<String> = fields
                .iter()
                .map(|f| format!("{} {}", f.name(), type_name(f.data_type())))
                .collect();
            format!("row({})", members.join(", "))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => format!(
                "map({}, {})",
                type_name(kv[0].data_type()),
                type_name(kv[1].data_type())
            ),
            _ => "map".into(),
        },
        DataType::Dictionary(_, value) => type_name(value),
        DataType::RunEndEncoded(_, value) => type_name(value.data_type()),
        DataType::Union(_, _) => "union".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &'static str)]) -> http::HeaderMap {
        let mut h = http::HeaderMap::new();
        for (k, v) in pairs {
            h.insert(*k, http::HeaderValue::from_static(v));
        }
        h
    }

    #[test]
    fn java_clients_get_no_compression() {
        let h = headers(&[("user-agent", "grpc-java-netty/1.73.0")]);
        assert_eq!(compression_for_headers(&h), Some(FlightCompression::None));
        // The JDBC driver's own agent comes first; grpc-java appends its token.
        let jdbc = "JDBC Flight SQL Driver 19.0.0 grpc-java-netty/1.73.0";
        let h = headers(&[("user-agent", jdbc)]);
        assert_eq!(compression_for_headers(&h), Some(FlightCompression::None));
    }

    #[test]
    fn header_wins_over_user_agent() {
        let h = headers(&[
            ("user-agent", "grpc-java-netty/1.73.0"),
            (COMPRESSION_HEADER, "zstd"),
        ]);
        assert_eq!(compression_for_headers(&h), Some(FlightCompression::Zstd));
    }

    #[test]
    fn other_clients_keep_the_configured_codec() {
        let h = headers(&[("user-agent", "grpc-go/1.71.0")]);
        assert_eq!(compression_for_headers(&h), None);
        assert_eq!(compression_for_headers(&http::HeaderMap::new()), None);
    }

    #[test]
    fn unparsable_header_is_ignored() {
        let h = headers(&[(COMPRESSION_HEADER, "brotli"), ("user-agent", "grpc-go/1")]);
        assert_eq!(compression_for_headers(&h), None);
    }

    #[tokio::test]
    async fn override_is_visible_inside_the_scope_only() {
        assert_eq!(client_compression_override(), None);
        let seen = CLIENT_COMPRESSION
            .scope(Some(FlightCompression::None), async {
                client_compression_override()
            })
            .await;
        assert_eq!(seen, Some(FlightCompression::None));
    }

    #[test]
    fn decimal_metadata() {
        let f = with_column_metadata(Field::new("d", DataType::Decimal128(10, 2), true));
        assert_eq!(f.metadata()[TYPE_NAME_KEY], "decimal(10,2)");
        assert_eq!(f.metadata()[PRECISION_KEY], "10");
        assert_eq!(f.metadata()[SCALE_KEY], "2");
    }

    #[test]
    fn integer_and_string_metadata() {
        let i = with_column_metadata(Field::new("i", DataType::Int32, true));
        assert_eq!(i.metadata()[TYPE_NAME_KEY], "integer");
        assert_eq!(i.metadata()[PRECISION_KEY], "10");
        assert_eq!(i.metadata()[SCALE_KEY], "0");
        let s = with_column_metadata(Field::new("s", DataType::Utf8, true));
        assert_eq!(s.metadata()[TYPE_NAME_KEY], "varchar");
        assert!(!s.metadata().contains_key(PRECISION_KEY));
    }

    #[test]
    fn existing_metadata_is_kept() {
        let f = Field::new("x", DataType::Int64, true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "7".to_string(),
        )]));
        let f = with_column_metadata(f);
        assert_eq!(f.metadata()["PARQUET:field_id"], "7");
        assert_eq!(f.metadata()[TYPE_NAME_KEY], "bigint");
    }
}
