//! The Trino session properties SQE honours (review finding 708).
//!
//! `SET SESSION` used to be accept-and-echo: any `name = value` was reflected
//! back in `X-Trino-Set-Session`, so a client that set `query_max_memory`
//! believed it took effect. Trino rejects an unknown property with
//! `INVALID_SESSION_PROPERTY`, and so does SQE now. This table is the
//! allow-list: a property is accepted only when something in the engine reads
//! it. `SHOW SESSION` lists exactly these rows.

/// One session property SQE honours.
#[derive(Debug, Clone, Copy)]
pub struct SessionProperty {
    /// Fully qualified name as the client writes it (`catalog.property` for
    /// connector properties, bare for system properties).
    pub name: &'static str,
    /// Trino type name shown in `SHOW SESSION`.
    pub type_name: &'static str,
    /// Value in effect when the client has not set the property. `None`
    /// renders as SQL NULL: the effective default is a coordinator setting
    /// the protocol layer cannot see.
    pub default: Option<&'static str>,
    pub description: &'static str,
    /// Accepted values, compared case-insensitively. Empty means any value.
    pub allowed_values: &'static [&'static str],
}

impl SessionProperty {
    /// Validate a value for this property. The error text is the message of
    /// the `INVALID_SESSION_PROPERTY` error the client receives.
    pub fn validate(&self, value: &str) -> Result<(), String> {
        if self.allowed_values.is_empty()
            || self
                .allowed_values
                .iter()
                .any(|v| v.eq_ignore_ascii_case(value))
        {
            Ok(())
        } else {
            Err(format!(
                "Invalid value '{value}' for session property '{}'; expected one of {}",
                self.name,
                self.allowed_values.join(", ")
            ))
        }
    }
}

/// Every session property the engine reads. Add a row here when a new
/// property gains a consumer; nothing else makes `SET SESSION` accept it.
pub const SESSION_PROPERTIES: &[SessionProperty] = &[SessionProperty {
    // Read by the write path (`Session::compression_codec`, #353): the
    // Parquet codec for data files written in this session.
    name: "iceberg.compression_codec",
    type_name: "varchar",
    default: None,
    description: "Parquet compression codec for files written by this session \
                  (ZSTD, SNAPPY, LZ4, NONE); the coordinator's configured codec \
                  applies when unset",
    allowed_values: &["ZSTD", "SNAPPY", "LZ4", "NONE"],
}];

/// Look a property up by exact name (Trino property names are
/// case-sensitive).
pub fn lookup(name: &str) -> Option<&'static SessionProperty> {
    SESSION_PROPERTIES.iter().find(|p| p.name == name)
}

/// The `INVALID_SESSION_PROPERTY` message for an unknown property, worded as
/// Trino words it.
pub fn unknown_property_message(name: &str) -> String {
    format!("Session property '{name}' does not exist")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_codec_is_allow_listed_and_validated() {
        let prop = lookup("iceberg.compression_codec").expect("allow-listed");
        assert!(prop.validate("zstd").is_ok());
        assert!(prop.validate("SNAPPY").is_ok());
        let err = prop.validate("gzip").unwrap_err();
        assert!(err.contains("gzip"), "{err}");
        assert!(err.contains("ZSTD, SNAPPY, LZ4, NONE"), "{err}");
    }

    #[test]
    fn unknown_properties_are_not_found() {
        assert!(lookup("query_max_memory").is_none());
        assert!(lookup("query_max_run_time").is_none());
        // Names are case-sensitive, as in Trino.
        assert!(lookup("Iceberg.Compression_Codec").is_none());
        assert_eq!(
            unknown_property_message("bogus"),
            "Session property 'bogus' does not exist"
        );
    }
}
