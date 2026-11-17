//! The Flight session-option actions (`SetSessionOptions`,
//! `GetSessionOptions`, `CloseSession`) from `Flight.proto`.
//!
//! arrow-flight 59 declares none of these messages, so they are declared
//! here. The Flight SQL JDBC driver sends `SetSessionOptions {catalog}` for
//! its `catalog=` connection property and fails the connection on an error
//! reply; it sends `CloseSession` on close and ignores the reply. Action
//! bodies are the bare messages, not `Any`-wrapped.
//!
//! Two options exist, both strings, both the session's default scope as
//! `USE` sets it: `catalog` and `schema`.

use std::collections::HashMap;

use prost::Message;
use tonic::Status;

pub const SET_SESSION_OPTIONS: &str = "SetSessionOptions";
pub const GET_SESSION_OPTIONS: &str = "GetSessionOptions";
pub const CLOSE_SESSION: &str = "CloseSession";

const CATALOG: &str = "catalog";
const SCHEMA: &str = "schema";

#[derive(Clone, PartialEq, Message)]
pub struct StringListValue {
    #[prost(string, repeated, tag = "1")]
    pub values: Vec<String>,
}

// The variant names mirror the `SessionOptionValue.option_value` oneof in
// Flight.proto, which is what a reader cross-checks them against.
#[allow(clippy::enum_variant_names)]
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum OptionValue {
    #[prost(string, tag = "1")]
    StringValue(String),
    #[prost(bool, tag = "2")]
    BoolValue(bool),
    #[prost(sfixed64, tag = "3")]
    Int64Value(i64),
    #[prost(double, tag = "4")]
    DoubleValue(f64),
    #[prost(message, tag = "5")]
    StringListValue(StringListValue),
}

#[derive(Clone, PartialEq, Message)]
pub struct SessionOptionValue {
    #[prost(oneof = "OptionValue", tags = "1, 2, 3, 4, 5")]
    pub option_value: Option<OptionValue>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SetSessionOptionsRequest {
    #[prost(map = "string, message", tag = "1")]
    pub session_options: HashMap<String, SessionOptionValue>,
}

/// `SetSessionOptionsResult.ErrorValue`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum ErrorValue {
    Unspecified = 0,
    InvalidName = 1,
    InvalidValue = 2,
    Error = 3,
}

#[derive(Clone, PartialEq, Message)]
pub struct SetSessionOptionError {
    #[prost(enumeration = "ErrorValue", tag = "1")]
    pub value: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct SetSessionOptionsResult {
    #[prost(map = "string, message", tag = "1")]
    pub errors: HashMap<String, SetSessionOptionError>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetSessionOptionsResult {
    #[prost(map = "string, message", tag = "1")]
    pub session_options: HashMap<String, SessionOptionValue>,
}

/// `CloseSessionResult.Status`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum CloseSessionStatus {
    Unspecified = 0,
    Closed = 1,
    Closing = 2,
    NotCloseable = 3,
}

#[derive(Clone, PartialEq, Message)]
pub struct CloseSessionResult {
    #[prost(enumeration = "CloseSessionStatus", tag = "1")]
    pub status: i32,
}

/// The scope a `SetSessionOptions` request asks for, after validation.
#[derive(Debug, Default, PartialEq)]
pub struct ScopeChange {
    pub catalog: Option<String>,
    pub schema: Option<String>,
}

/// Validate a `SetSessionOptions` body against the catalogs SQL knows.
/// Options with an error are not part of the returned change.
pub fn plan_set(
    body: &[u8],
    known_catalogs: &[String],
) -> Result<(ScopeChange, SetSessionOptionsResult), Status> {
    let request = SetSessionOptionsRequest::decode(body)
        .map_err(|e| Status::invalid_argument(format!("SetSessionOptions: {e}")))?;
    let mut change = ScopeChange::default();
    let mut errors = HashMap::new();
    for (name, value) in request.session_options {
        let text = match value.option_value {
            Some(OptionValue::StringValue(s)) => Some(s),
            _ => None,
        };
        let verdict = match (name.as_str(), text) {
            (CATALOG, Some(c)) if known_catalogs.contains(&c) => {
                change.catalog = Some(c);
                None
            }
            (SCHEMA, Some(s)) if !s.is_empty() => {
                change.schema = Some(s);
                None
            }
            (CATALOG | SCHEMA, _) => Some(ErrorValue::InvalidValue),
            _ => Some(ErrorValue::InvalidName),
        };
        if let Some(error) = verdict {
            let value = error as i32;
            errors.insert(name, SetSessionOptionError { value });
        }
    }
    Ok((change, SetSessionOptionsResult { errors }))
}

/// The session's options as `GetSessionOptions` reports them.
pub fn current(catalog: Option<&str>, schema: Option<&str>) -> GetSessionOptionsResult {
    let string = |s: &str| SessionOptionValue {
        option_value: Some(OptionValue::StringValue(s.to_string())),
    };
    let mut session_options = HashMap::new();
    if let Some(c) = catalog {
        session_options.insert(CATALOG.to_string(), string(c));
    }
    if let Some(s) = schema {
        session_options.insert(SCHEMA.to_string(), string(s));
    }
    GetSessionOptionsResult { session_options }
}

/// `CloseSession` leaves the session alone: a bearer-token session is shared
/// by every connection presenting the token, so closing it would end another
/// connection's transaction.
pub fn close() -> CloseSessionResult {
    CloseSessionResult {
        status: CloseSessionStatus::NotCloseable as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(pairs: &[(&str, OptionValue)]) -> Vec<u8> {
        SetSessionOptionsRequest {
            session_options: pairs
                .iter()
                .map(|(k, v)| {
                    let value = SessionOptionValue {
                        option_value: Some(v.clone()),
                    };
                    (k.to_string(), value)
                })
                .collect(),
        }
        .encode_to_vec()
    }

    fn known() -> Vec<String> {
        vec!["iceberg".into(), "lake".into()]
    }

    #[test]
    fn known_catalog_and_schema_are_applied() {
        let body = request(&[
            ("catalog", OptionValue::StringValue("lake".into())),
            ("schema", OptionValue::StringValue("sales".into())),
        ]);
        let (change, result) = plan_set(&body, &known()).expect("decode");
        assert!(result.errors.is_empty());
        assert_eq!(change.catalog.as_deref(), Some("lake"));
        assert_eq!(change.schema.as_deref(), Some("sales"));
    }

    #[test]
    fn unknown_catalog_is_an_invalid_value_and_not_applied() {
        let body = request(&[("catalog", OptionValue::StringValue("nope".into()))]);
        let (change, result) = plan_set(&body, &known()).expect("decode");
        assert_eq!(change, ScopeChange::default());
        assert_eq!(
            result.errors["catalog"].value,
            ErrorValue::InvalidValue as i32
        );
    }

    #[test]
    fn unknown_name_and_non_string_value_are_reported() {
        let body = request(&[
            ("timezone", OptionValue::StringValue("UTC".into())),
            ("schema", OptionValue::BoolValue(true)),
        ]);
        let (change, result) = plan_set(&body, &known()).expect("decode");
        assert_eq!(change, ScopeChange::default());
        assert_eq!(
            result.errors["timezone"].value,
            ErrorValue::InvalidName as i32
        );
        assert_eq!(
            result.errors["schema"].value,
            ErrorValue::InvalidValue as i32
        );
    }

    #[test]
    fn current_reports_what_is_set() {
        let got = current(Some("lake"), None);
        assert_eq!(got.session_options.len(), 1);
        assert_eq!(
            got.session_options["catalog"].option_value,
            Some(OptionValue::StringValue("lake".into()))
        );
    }

    #[test]
    fn close_is_not_closeable() {
        assert_eq!(close().status, CloseSessionStatus::NotCloseable as i32);
    }
}
