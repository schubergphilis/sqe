//! SQL pipeline newtypes that mark the trust boundary across the
//! coordinator's pre-parse stages.
//!
//! Raw user input flows through several rewrites before it reaches the
//! parser. Each stage strips or transforms a clause that sqlparser-rs
//! cannot consume directly: `FOR INCREMENTAL BETWEEN SNAPSHOT`,
//! `FOR VERSION AS OF`, `PARTITIONED BY (...)`, Trino-compat shapes.
//! The original layout treated every stage as `&str`, so reordering the
//! pipeline (passing raw SQL straight into `parse_and_classify`) failed
//! only at query time. These wrappers give the compiler a way to enforce
//! the stage order without paying any runtime cost.

/// SQL as received from the user. Untrusted: may contain SQE-specific
/// clauses that sqlparser-rs cannot parse, may carry trailing semicolons,
/// may be empty.
#[derive(Debug, Clone)]
pub struct UserSql(String);

impl UserSql {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl From<String> for UserSql {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for UserSql {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// SQL after `extract_incremental_spec` and `extract_time_travel_spec`
/// have stripped clauses sqlparser does not understand, and after
/// `normalize_partitioned_by` has rewritten Hive-style PARTITIONED BY
/// to sqlparser-friendly PARTITION BY. The classifier accepts this
/// shape.
#[derive(Debug, Clone)]
pub struct ClassifiableSql(String);

impl ClassifiableSql {
    /// Construct a ClassifiableSql from a string the caller is asserting
    /// has already been pre-processed. Used by callers that already
    /// produced a normalized form via the per-stage extractors.
    pub fn from_normalized(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn into_inner(self) -> String {
        self.0
    }
}

/// Run the full pre-parse pipeline against a `UserSql` and return the
/// `ClassifiableSql` that the classifier accepts. The chain is total
/// and the stages cannot be reordered because each consumes the output
/// of the previous one by value or by reference. Callers should prefer
/// this helper over hand-rolling the pipeline when the intermediate
/// `IncrementalSpec` / `TimeTravelSpec` values are not needed.
pub fn pre_parse_pipeline(sql: &UserSql) -> sqe_core::Result<ClassifiableSql> {
    let (stripped, _incremental) = crate::time_travel::extract_incremental_spec(sql.as_str())?;
    let (stripped, _version) = crate::time_travel::extract_time_travel_spec(&stripped)?;
    let normalized = crate::partition::normalize_partitioned_by(&stripped);
    Ok(ClassifiableSql::from_normalized(normalized))
}
