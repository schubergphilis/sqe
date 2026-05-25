//! Quack RPC message types.
//!
//! Mirrors `src/include/quack_message.json` from `duckdb/duckdb-quack`
//! v1.5-variegata. Each `QuackMessage` is wire-format as two BinarySerializer
//! objects back-to-back: header object, then body object.

use crate::codec::{BinaryDeserializer, BinarySerializer};
use crate::data_chunk::{DataChunk, LogicalType};

const DATA_CHUNK_WRAPPER_FIELD: u16 = 300;
const OPTIONAL_IDX_INVALID_BATCH: u64 = u64::MAX;

fn encode_data_chunk_wrapper(s: &mut BinarySerializer, chunk: &DataChunk) {
    s.begin_property(DATA_CHUNK_WRAPPER_FIELD);
    s.begin_object();
    chunk.encode(s);
    s.end_object();
    s.end_property();
}

fn decode_data_chunk_wrapper(d: &mut BinaryDeserializer<'_>) -> crate::Result<DataChunk> {
    d.expect_field(DATA_CHUNK_WRAPPER_FIELD)?;
    let chunk = DataChunk::decode(d)?;
    d.expect_object_end()?;
    Ok(chunk)
}

/// Wire enum encoded as a single varint (underlying type `uint8_t`).
/// Values are not contiguous: matches DuckDB's `enum class MessageType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Invalid = 0,
    ConnectionRequest = 1,
    ConnectionResponse = 2,
    PrepareRequest = 3,
    PrepareResponse = 4,
    FetchRequest = 7,
    FetchResponse = 8,
    AppendRequest = 9,
    SuccessResponse = 10,
    DisconnectMessage = 11,
    ErrorResponse = 100,
}

impl MessageType {
    pub fn from_u8(value: u8) -> crate::Result<Self> {
        Ok(match value {
            0 => Self::Invalid,
            1 => Self::ConnectionRequest,
            2 => Self::ConnectionResponse,
            3 => Self::PrepareRequest,
            4 => Self::PrepareResponse,
            7 => Self::FetchRequest,
            8 => Self::FetchResponse,
            9 => Self::AppendRequest,
            10 => Self::SuccessResponse,
            11 => Self::DisconnectMessage,
            100 => Self::ErrorResponse,
            _ => return Err(crate::WireError::UnknownMessageType(value)),
        })
    }
}

const HEADER_FIELD_TYPE: u16 = 1;
const HEADER_FIELD_CONNECTION_ID: u16 = 2;
const HEADER_FIELD_CLIENT_QUERY_ID: u16 = 3;

/// Sentinel for `optional_idx` "not present" — matches DuckDB's
/// `DConstants::INVALID_INDEX` for `idx_t`.
const OPTIONAL_IDX_INVALID: u64 = u64::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageHeader {
    pub r#type: MessageType,
    pub connection_id: String,
    pub client_query_id: Option<u64>,
}

impl MessageHeader {
    pub fn encode(&self, s: &mut BinarySerializer) {
        s.begin_property(HEADER_FIELD_TYPE);
        s.write_u8(self.r#type as u8);
        s.end_property();

        s.begin_property(HEADER_FIELD_CONNECTION_ID);
        s.write_string(&self.connection_id);
        s.end_property();

        s.begin_property(HEADER_FIELD_CLIENT_QUERY_ID);
        s.write_u64(self.client_query_id.unwrap_or(OPTIONAL_IDX_INVALID));
        s.end_property();
    }

    pub fn decode(d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        d.expect_field(HEADER_FIELD_TYPE)?;
        let type_tag = d.read_u8()?;
        let r#type = MessageType::from_u8(type_tag)?;

        d.expect_field(HEADER_FIELD_CONNECTION_ID)?;
        let connection_id = d.read_string()?;

        d.expect_field(HEADER_FIELD_CLIENT_QUERY_ID)?;
        let raw = d.read_u64()?;
        let client_query_id = if raw == OPTIONAL_IDX_INVALID {
            None
        } else {
            Some(raw)
        };

        Ok(MessageHeader {
            r#type,
            connection_id,
            client_query_id,
        })
    }
}

// -----------------------------------------------------------------------------
// Body types
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionRequest {
    pub auth_string: String,
    pub client_duckdb_version: String,
    pub client_platform: String,
    pub min_supported_quack_version: u64,
    pub max_supported_quack_version: u64,
}

impl ConnectionRequest {
    pub fn encode(&self, s: &mut BinarySerializer) {
        s.begin_property(1);
        s.write_string(&self.auth_string);
        s.end_property();
        s.begin_property(2);
        s.write_string(&self.client_duckdb_version);
        s.end_property();
        s.begin_property(3);
        s.write_string(&self.client_platform);
        s.end_property();
        s.begin_property(4);
        s.write_u64(self.min_supported_quack_version);
        s.end_property();
        s.begin_property(5);
        s.write_u64(self.max_supported_quack_version);
        s.end_property();
    }

    pub fn decode(d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        d.expect_field(1)?;
        let auth_string = d.read_string()?;
        d.expect_field(2)?;
        let client_duckdb_version = d.read_string()?;
        d.expect_field(3)?;
        let client_platform = d.read_string()?;
        d.expect_field(4)?;
        let min_supported_quack_version = d.read_u64()?;
        d.expect_field(5)?;
        let max_supported_quack_version = d.read_u64()?;
        Ok(Self {
            auth_string,
            client_duckdb_version,
            client_platform,
            min_supported_quack_version,
            max_supported_quack_version,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionResponse {
    pub server_duckdb_version: String,
    pub server_platform: String,
    pub quack_version: u64,
}

impl ConnectionResponse {
    pub fn encode(&self, s: &mut BinarySerializer) {
        s.begin_property(1);
        s.write_string(&self.server_duckdb_version);
        s.end_property();
        s.begin_property(2);
        s.write_string(&self.server_platform);
        s.end_property();
        s.begin_property(3);
        s.write_u64(self.quack_version);
        s.end_property();
    }

    pub fn decode(d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        d.expect_field(1)?;
        let server_duckdb_version = d.read_string()?;
        d.expect_field(2)?;
        let server_platform = d.read_string()?;
        d.expect_field(3)?;
        let quack_version = d.read_u64()?;
        Ok(Self {
            server_duckdb_version,
            server_platform,
            quack_version,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareRequest {
    pub sql_query: String,
}

impl PrepareRequest {
    pub fn encode(&self, s: &mut BinarySerializer) {
        s.begin_property(1);
        s.write_string(&self.sql_query);
        s.end_property();
    }

    pub fn decode(d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        d.expect_field(1)?;
        let sql_query = d.read_string()?;
        Ok(Self { sql_query })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequest {
    pub uuid: i128,
}

impl FetchRequest {
    pub fn encode(&self, s: &mut BinarySerializer) {
        s.begin_property(1);
        s.write_hugeint(self.uuid);
        s.end_property();
    }

    pub fn decode(d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        d.expect_field(1)?;
        let uuid = d.read_hugeint()?;
        Ok(Self { uuid })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorResponse {
    pub message: String,
}

impl ErrorResponse {
    pub fn encode(&self, s: &mut BinarySerializer) {
        s.begin_property(1);
        s.write_string(&self.message);
        s.end_property();
    }

    pub fn decode(d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        d.expect_field(1)?;
        let message = d.read_string()?;
        Ok(Self { message })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuccessResponse;

impl SuccessResponse {
    pub fn encode(&self, _s: &mut BinarySerializer) {
        // Empty body.
    }

    pub fn decode(_d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        Ok(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DisconnectMessage;

impl DisconnectMessage {
    pub fn encode(&self, _s: &mut BinarySerializer) {
        // Empty body.
    }

    pub fn decode(_d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        Ok(Self)
    }
}

// -----------------------------------------------------------------------------
// DataChunk-carrying message types
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareResponse {
    pub result_types: Vec<LogicalType>,
    pub result_names: Vec<String>,
    pub needs_more_fetch: bool,
    pub results: Vec<DataChunk>,
    pub result_uuid: i128,
}

impl PrepareResponse {
    pub fn encode(&self, s: &mut BinarySerializer) {
        s.begin_property(1);
        s.begin_list(self.result_types.len() as u64);
        for t in &self.result_types {
            s.begin_object();
            t.encode(s);
            s.end_object();
        }
        s.end_list();
        s.end_property();

        s.begin_property(2);
        s.begin_list(self.result_names.len() as u64);
        for name in &self.result_names {
            s.write_string(name);
        }
        s.end_list();
        s.end_property();

        s.begin_property(3);
        s.write_bool(self.needs_more_fetch);
        s.end_property();

        s.begin_property(4);
        s.begin_list(self.results.len() as u64);
        for chunk in &self.results {
            s.begin_object();
            encode_data_chunk_wrapper(s, chunk);
            s.end_object();
        }
        s.end_list();
        s.end_property();

        s.begin_property(5);
        s.write_hugeint(self.result_uuid);
        s.end_property();
    }

    pub fn decode(d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        d.expect_field(1)?;
        let type_count = d.read_list_count()? as usize;
        let mut result_types = Vec::with_capacity(type_count);
        for _ in 0..type_count {
            let t = LogicalType::decode(d)?;
            d.expect_object_end()?;
            result_types.push(t);
        }

        d.expect_field(2)?;
        let name_count = d.read_list_count()? as usize;
        let mut result_names = Vec::with_capacity(name_count);
        for _ in 0..name_count {
            result_names.push(d.read_string()?);
        }

        d.expect_field(3)?;
        let needs_more_fetch = d.read_bool()?;

        d.expect_field(4)?;
        let results_count = d.read_list_count()? as usize;
        let mut results = Vec::with_capacity(results_count);
        for _ in 0..results_count {
            let chunk = decode_data_chunk_wrapper(d)?;
            d.expect_object_end()?;
            results.push(chunk);
        }

        d.expect_field(5)?;
        let result_uuid = d.read_hugeint()?;

        Ok(Self {
            result_types,
            result_names,
            needs_more_fetch,
            results,
            result_uuid,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResponse {
    pub results: Vec<DataChunk>,
    pub batch_index: Option<u64>,
}

impl FetchResponse {
    pub fn encode(&self, s: &mut BinarySerializer) {
        s.begin_property(1);
        s.begin_list(self.results.len() as u64);
        for chunk in &self.results {
            s.begin_object();
            encode_data_chunk_wrapper(s, chunk);
            s.end_object();
        }
        s.end_list();
        s.end_property();

        s.begin_property(2);
        s.write_u64(self.batch_index.unwrap_or(OPTIONAL_IDX_INVALID_BATCH));
        s.end_property();
    }

    pub fn decode(d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        d.expect_field(1)?;
        let count = d.read_list_count()? as usize;
        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            let chunk = decode_data_chunk_wrapper(d)?;
            d.expect_object_end()?;
            results.push(chunk);
        }

        d.expect_field(2)?;
        let raw = d.read_u64()?;
        let batch_index = if raw == OPTIONAL_IDX_INVALID_BATCH {
            None
        } else {
            Some(raw)
        };

        Ok(Self {
            results,
            batch_index,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    pub schema_name: String,
    pub table_name: String,
    pub append_chunk: DataChunk,
}

impl AppendRequest {
    pub fn encode(&self, s: &mut BinarySerializer) {
        s.begin_property(1);
        s.write_string(&self.schema_name);
        s.end_property();
        s.begin_property(2);
        s.write_string(&self.table_name);
        s.end_property();
        encode_data_chunk_wrapper(s, &self.append_chunk);
    }

    pub fn decode(d: &mut BinaryDeserializer<'_>) -> crate::Result<Self> {
        d.expect_field(1)?;
        let schema_name = d.read_string()?;
        d.expect_field(2)?;
        let table_name = d.read_string()?;
        let append_chunk = decode_data_chunk_wrapper(d)?;
        Ok(Self {
            schema_name,
            table_name,
            append_chunk,
        })
    }
}

// -----------------------------------------------------------------------------
// Top-level message envelope
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuackMessage {
    ConnectionRequest(ConnectionRequest),
    ConnectionResponse(ConnectionResponse),
    PrepareRequest(PrepareRequest),
    PrepareResponse(PrepareResponse),
    FetchRequest(FetchRequest),
    FetchResponse(FetchResponse),
    AppendRequest(AppendRequest),
    SuccessResponse,
    DisconnectMessage,
    ErrorResponse(ErrorResponse),
}

impl QuackMessage {
    pub fn message_type(&self) -> MessageType {
        match self {
            QuackMessage::ConnectionRequest(_) => MessageType::ConnectionRequest,
            QuackMessage::ConnectionResponse(_) => MessageType::ConnectionResponse,
            QuackMessage::PrepareRequest(_) => MessageType::PrepareRequest,
            QuackMessage::PrepareResponse(_) => MessageType::PrepareResponse,
            QuackMessage::FetchRequest(_) => MessageType::FetchRequest,
            QuackMessage::FetchResponse(_) => MessageType::FetchResponse,
            QuackMessage::AppendRequest(_) => MessageType::AppendRequest,
            QuackMessage::SuccessResponse => MessageType::SuccessResponse,
            QuackMessage::DisconnectMessage => MessageType::DisconnectMessage,
            QuackMessage::ErrorResponse(_) => MessageType::ErrorResponse,
        }
    }
}

pub fn encode_message(header: &MessageHeader, body: &QuackMessage) -> Vec<u8> {
    let mut s = BinarySerializer::new();
    s.begin_object();
    header.encode(&mut s);
    s.end_object();
    s.begin_object();
    match body {
        QuackMessage::ConnectionRequest(b) => b.encode(&mut s),
        QuackMessage::ConnectionResponse(b) => b.encode(&mut s),
        QuackMessage::PrepareRequest(b) => b.encode(&mut s),
        QuackMessage::PrepareResponse(b) => b.encode(&mut s),
        QuackMessage::FetchRequest(b) => b.encode(&mut s),
        QuackMessage::FetchResponse(b) => b.encode(&mut s),
        QuackMessage::AppendRequest(b) => b.encode(&mut s),
        QuackMessage::SuccessResponse => SuccessResponse.encode(&mut s),
        QuackMessage::DisconnectMessage => DisconnectMessage.encode(&mut s),
        QuackMessage::ErrorResponse(b) => b.encode(&mut s),
    }
    s.end_object();
    s.into_bytes()
}

pub fn decode_message(bytes: &[u8]) -> crate::Result<(MessageHeader, QuackMessage)> {
    let mut d = BinaryDeserializer::new(bytes);
    let header = MessageHeader::decode(&mut d)?;
    d.expect_object_end()?;

    let body = match header.r#type {
        MessageType::ConnectionRequest => {
            QuackMessage::ConnectionRequest(ConnectionRequest::decode(&mut d)?)
        }
        MessageType::ConnectionResponse => {
            QuackMessage::ConnectionResponse(ConnectionResponse::decode(&mut d)?)
        }
        MessageType::PrepareRequest => {
            QuackMessage::PrepareRequest(PrepareRequest::decode(&mut d)?)
        }
        MessageType::PrepareResponse => {
            QuackMessage::PrepareResponse(PrepareResponse::decode(&mut d)?)
        }
        MessageType::FetchRequest => QuackMessage::FetchRequest(FetchRequest::decode(&mut d)?),
        MessageType::FetchResponse => QuackMessage::FetchResponse(FetchResponse::decode(&mut d)?),
        MessageType::AppendRequest => QuackMessage::AppendRequest(AppendRequest::decode(&mut d)?),
        MessageType::SuccessResponse => {
            SuccessResponse::decode(&mut d)?;
            QuackMessage::SuccessResponse
        }
        MessageType::DisconnectMessage => {
            DisconnectMessage::decode(&mut d)?;
            QuackMessage::DisconnectMessage
        }
        MessageType::ErrorResponse => QuackMessage::ErrorResponse(ErrorResponse::decode(&mut d)?),
        other => {
            return Err(crate::WireError::UnsupportedMessageType(other));
        }
    };
    d.expect_object_end()?;
    Ok((header, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_type_from_u8_roundtrip() {
        for variant in [
            MessageType::Invalid,
            MessageType::ConnectionRequest,
            MessageType::ConnectionResponse,
            MessageType::PrepareRequest,
            MessageType::PrepareResponse,
            MessageType::FetchRequest,
            MessageType::FetchResponse,
            MessageType::AppendRequest,
            MessageType::SuccessResponse,
            MessageType::DisconnectMessage,
            MessageType::ErrorResponse,
        ] {
            let decoded = MessageType::from_u8(variant as u8).unwrap();
            assert_eq!(decoded, variant);
        }
    }

    #[test]
    fn message_type_from_u8_rejects_unknown_values() {
        let err = MessageType::from_u8(50).unwrap_err();
        assert!(matches!(err, crate::WireError::UnknownMessageType(50)));
    }

    #[test]
    fn header_with_query_id_roundtrips() {
        let header = MessageHeader {
            r#type: MessageType::PrepareRequest,
            connection_id: "abc".to_string(),
            client_query_id: Some(42),
        };
        let mut s = BinarySerializer::new();
        s.begin_object();
        header.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = MessageHeader::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn connection_request_roundtrips() {
        let original = ConnectionRequest {
            auth_string: "super_secret".to_string(),
            client_duckdb_version: "v1.5.2".to_string(),
            client_platform: "osx_arm64".to_string(),
            min_supported_quack_version: 1,
            max_supported_quack_version: 1,
        };
        let mut s = BinarySerializer::new();
        s.begin_object();
        original.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = ConnectionRequest::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn connection_response_roundtrips() {
        let original = ConnectionResponse {
            server_duckdb_version: "v1.5.2".to_string(),
            server_platform: "linux_amd64".to_string(),
            quack_version: 1,
        };
        let mut s = BinarySerializer::new();
        s.begin_object();
        original.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();
        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = ConnectionResponse::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn prepare_request_roundtrips() {
        let original = PrepareRequest {
            sql_query: "SELECT 1".to_string(),
        };
        let mut s = BinarySerializer::new();
        s.begin_object();
        original.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();
        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = PrepareRequest::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn fetch_request_roundtrips() {
        let original = FetchRequest {
            uuid: 0x0123_4567_89AB_CDEF_FEDC_BA98_7654_3210u128 as i128,
        };
        let mut s = BinarySerializer::new();
        s.begin_object();
        original.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();
        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = FetchRequest::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn error_response_roundtrips() {
        let original = ErrorResponse {
            message: "SQE-AUTH: token expired".to_string(),
        };
        let mut s = BinarySerializer::new();
        s.begin_object();
        original.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();
        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = ErrorResponse::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn success_response_is_empty_object() {
        let mut s = BinarySerializer::new();
        s.begin_object();
        SuccessResponse.encode(&mut s);
        s.end_object();
        // Empty body: only the terminator.
        assert_eq!(s.into_bytes(), &[0xFF, 0xFF]);
    }

    #[test]
    fn disconnect_message_is_empty_object() {
        let mut s = BinarySerializer::new();
        s.begin_object();
        DisconnectMessage.encode(&mut s);
        s.end_object();
        assert_eq!(s.into_bytes(), &[0xFF, 0xFF]);
    }

    fn sample_chunk_integer_column() -> DataChunk {
        let raw = [10i32, 20, 30];
        let bytes: Vec<u8> = raw.iter().flat_map(|v| v.to_le_bytes()).collect();
        DataChunk {
            row_count: 3,
            columns: vec![crate::data_chunk::Vector::new_fixed(
                crate::data_chunk::LogicalTypeId::Integer,
                bytes,
            )],
        }
    }

    #[test]
    fn prepare_response_roundtrips() {
        let original = PrepareResponse {
            result_types: vec![LogicalType::new(crate::data_chunk::LogicalTypeId::Integer)],
            result_names: vec!["x".to_string()],
            needs_more_fetch: false,
            results: vec![sample_chunk_integer_column()],
            result_uuid: 0,
        };
        let mut s = BinarySerializer::new();
        s.begin_object();
        original.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();
        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = PrepareResponse::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn fetch_response_roundtrips_with_batch_index() {
        let original = FetchResponse {
            results: vec![sample_chunk_integer_column()],
            batch_index: Some(7),
        };
        let mut s = BinarySerializer::new();
        s.begin_object();
        original.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();
        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = FetchResponse::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn fetch_response_roundtrips_without_batch_index() {
        let original = FetchResponse {
            results: vec![],
            batch_index: None,
        };
        let mut s = BinarySerializer::new();
        s.begin_object();
        original.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();
        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = FetchResponse::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn append_request_roundtrips() {
        let original = AppendRequest {
            schema_name: "main".to_string(),
            table_name: "events".to_string(),
            append_chunk: sample_chunk_integer_column(),
        };
        let mut s = BinarySerializer::new();
        s.begin_object();
        original.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();
        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = AppendRequest::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn envelope_roundtrips_prepare_response() {
        let header = MessageHeader {
            r#type: MessageType::PrepareResponse,
            connection_id: "conn-1".to_string(),
            client_query_id: Some(7),
        };
        let body = QuackMessage::PrepareResponse(PrepareResponse {
            result_types: vec![LogicalType::new(crate::data_chunk::LogicalTypeId::Integer)],
            result_names: vec!["x".to_string()],
            needs_more_fetch: false,
            results: vec![sample_chunk_integer_column()],
            result_uuid: 0,
        });
        let bytes = encode_message(&header, &body);
        let (decoded_header, decoded_body) = decode_message(&bytes).unwrap();
        assert_eq!(decoded_header, header);
        assert_eq!(decoded_body, body);
    }

    #[test]
    fn full_message_roundtrip_via_encode_decode() {
        let header = MessageHeader {
            r#type: MessageType::PrepareRequest,
            connection_id: "conn-1".to_string(),
            client_query_id: Some(7),
        };
        let body = QuackMessage::PrepareRequest(PrepareRequest {
            sql_query: "SELECT 1".to_string(),
        });
        let bytes = encode_message(&header, &body);
        let (decoded_header, decoded_body) = decode_message(&bytes).unwrap();
        assert_eq!(decoded_header, header);
        assert_eq!(decoded_body, body);
    }

    #[test]
    fn header_without_query_id_encodes_sentinel() {
        // client_query_id absent -> DConstants::INVALID_INDEX (u64::MAX)
        let header = MessageHeader {
            r#type: MessageType::ConnectionRequest,
            connection_id: "".to_string(),
            client_query_id: None,
        };
        let mut s = BinarySerializer::new();
        s.begin_object();
        header.encode(&mut s);
        s.end_object();
        let bytes = s.into_bytes();

        let mut d = BinaryDeserializer::new(&bytes);
        let decoded = MessageHeader::decode(&mut d).unwrap();
        d.expect_object_end().unwrap();
        assert_eq!(decoded.r#type, MessageType::ConnectionRequest);
        assert_eq!(decoded.connection_id, "");
        assert!(decoded.client_query_id.is_none());
    }
}
