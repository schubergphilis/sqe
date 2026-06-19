//! Session-context scalar UDFs.
//!
//! Each UDF bakes in a [`SessionIdentity`] at construction time, making it
//! `Volatility::Immutable` with no or fixed literal arguments. DataFusion
//! const-folds Immutable zero-arg (and Immutable literal-arg) calls to
//! scalars during logical optimization on the coordinator, so the baked
//! values never reach workers as a function invocation.
//!
//! Registration:
//! ```ignore
//! let id = Arc::new(SessionIdentity { username: "alice".into(), .. });
//! for udf in session_udfs(id) {
//!     ctx.register_udf(udf);
//! }
//! ```
//!
//! SQL names available after registration:
//! - `current_user()`
//! - `is_role_in_session(role TEXT)`
//! - `current_available_roles()`
//! - `current_database()`
//! - `current_schema()`

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, StringArray};
use arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;

// ---------------------------------------------------------------------------
// Identity carrier
// ---------------------------------------------------------------------------

/// The session-bound identity baked into each UDF instance.
///
/// All fields are captured at session-open time and never mutated.
/// The `#[derive]` gives deterministic PartialEq/Hash so DataFusion CSE
/// cannot conflate UDF instances from different sessions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct SessionIdentity {
    pub username: String,
    /// Sorted, deduplicated role set.
    pub roles: Vec<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
}

impl SessionIdentity {
    /// Construct a `SessionIdentity`, sorting and deduplicating `roles`.
    pub fn new(
        username: impl Into<String>,
        roles: impl IntoIterator<Item = impl Into<String>>,
        database: Option<String>,
        schema: Option<String>,
    ) -> Self {
        let mut roles: Vec<String> = roles.into_iter().map(Into::into).collect();
        roles.sort_unstable();
        roles.dedup();
        Self {
            username: username.into(),
            roles,
            database,
            schema,
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Render the role set as a sorted JSON array string.
/// e.g. `["analyst","engineer"]`
fn render_roles(roles: &[String]) -> String {
    serde_json::to_string(roles).unwrap_or_else(|_| "[]".to_string())
}

// ---------------------------------------------------------------------------
// current_user()
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct CurrentUserFunc {
    identity: Arc<SessionIdentity>,
    signature: Signature,
}

impl CurrentUserFunc {
    fn new(identity: Arc<SessionIdentity>) -> Self {
        Self {
            identity,
            signature: Signature::exact(vec![], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CurrentUserFunc {
    fn name(&self) -> &str {
        "current_user"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            self.identity.username.clone(),
        ))))
    }
}

// ---------------------------------------------------------------------------
// is_role_in_session(role Utf8)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct IsRoleInSessionFunc {
    identity: Arc<SessionIdentity>,
    signature: Signature,
}

impl IsRoleInSessionFunc {
    fn new(identity: Arc<SessionIdentity>) -> Self {
        Self {
            identity,
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IsRoleInSessionFunc {
    fn name(&self) -> &str {
        "is_role_in_session"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let roles = &self.identity.roles;
        let arg = &args.args[0];
        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(role))) => {
                let found = roles.binary_search(role).is_ok();
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(found))))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
            }
            ColumnarValue::Array(array) => {
                let str_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(
                            "is_role_in_session: expected Utf8 array".to_string(),
                        )
                    })?;
                let result: BooleanArray = str_array
                    .iter()
                    .map(|opt_role| opt_role.map(|r| roles.binary_search(&r.to_string()).is_ok()))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
            }
            ColumnarValue::Scalar(other) => Err(datafusion::error::DataFusionError::Internal(
                format!("is_role_in_session: expected Utf8 scalar, got {other:?}"),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// current_available_roles()
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct CurrentAvailableRolesFunc {
    identity: Arc<SessionIdentity>,
    signature: Signature,
}

impl CurrentAvailableRolesFunc {
    fn new(identity: Arc<SessionIdentity>) -> Self {
        Self {
            identity,
            signature: Signature::exact(vec![], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CurrentAvailableRolesFunc {
    fn name(&self) -> &str {
        "current_available_roles"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let rendered = render_roles(&self.identity.roles);
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(rendered))))
    }
}

// ---------------------------------------------------------------------------
// current_database()
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct CurrentDatabaseFunc {
    identity: Arc<SessionIdentity>,
    signature: Signature,
}

impl CurrentDatabaseFunc {
    fn new(identity: Arc<SessionIdentity>) -> Self {
        Self {
            identity,
            signature: Signature::exact(vec![], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CurrentDatabaseFunc {
    fn name(&self) -> &str {
        "current_database"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
            self.identity.database.clone(),
        )))
    }
}

// ---------------------------------------------------------------------------
// current_schema()
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct CurrentSchemaFunc {
    identity: Arc<SessionIdentity>,
    signature: Signature,
}

impl CurrentSchemaFunc {
    fn new(identity: Arc<SessionIdentity>) -> Self {
        Self {
            identity,
            signature: Signature::exact(vec![], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CurrentSchemaFunc {
    fn name(&self) -> &str {
        "current_schema"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
            self.identity.schema.clone(),
        )))
    }
}

// ---------------------------------------------------------------------------
// Public constructor
// ---------------------------------------------------------------------------

/// Return all five session-context UDFs backed by the given identity.
///
/// Pass each element to `ctx.register_udf(...)` once per session.
pub fn session_udfs(identity: Arc<SessionIdentity>) -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::from(CurrentUserFunc::new(Arc::clone(&identity))),
        ScalarUDF::from(IsRoleInSessionFunc::new(Arc::clone(&identity))),
        ScalarUDF::from(CurrentAvailableRolesFunc::new(Arc::clone(&identity))),
        ScalarUDF::from(CurrentDatabaseFunc::new(Arc::clone(&identity))),
        ScalarUDF::from(CurrentSchemaFunc::new(Arc::clone(&identity))),
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Array, BooleanArray, StringArray};
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::ColumnarValue;

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn make_args_noarg(return_dt: DataType, num_rows: usize) -> ScalarFunctionArgs {
        let return_field = Arc::new(Field::new("result", return_dt, true));
        ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: num_rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn make_args_utf8(
        arg: ColumnarValue,
        return_dt: DataType,
        num_rows: usize,
    ) -> ScalarFunctionArgs {
        let return_field = Arc::new(Field::new("result", return_dt, true));
        ScalarFunctionArgs {
            args: vec![arg],
            arg_fields: vec![Arc::new(Field::new("role", DataType::Utf8, true))],
            number_rows: num_rows,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    fn alice_identity() -> Arc<SessionIdentity> {
        Arc::new(SessionIdentity::new(
            "alice",
            vec!["analyst", "admin"],
            Some("sales_wh".to_string()),
            Some("public".to_string()),
        ))
    }

    fn empty_identity() -> Arc<SessionIdentity> {
        Arc::new(SessionIdentity::new(
            "bob",
            vec![] as Vec<String>,
            None,
            None,
        ))
    }

    // -----------------------------------------------------------------------
    // Helpers to unwrap ColumnarValue (no PartialEq on ColumnarValue in DF 54)
    // -----------------------------------------------------------------------

    fn scalar_utf8(cv: ColumnarValue) -> Option<String> {
        match cv {
            ColumnarValue::Scalar(ScalarValue::Utf8(v)) => v,
            other => panic!("expected Utf8 scalar, got {other:?}"),
        }
    }

    fn scalar_bool(cv: ColumnarValue) -> Option<bool> {
        match cv {
            ColumnarValue::Scalar(ScalarValue::Boolean(v)) => v,
            other => panic!("expected Boolean scalar, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // current_user()
    // -----------------------------------------------------------------------

    #[test]
    fn test_current_user_returns_username() {
        let func = CurrentUserFunc::new(alice_identity());
        let result = func
            .invoke_with_args(make_args_noarg(DataType::Utf8, 1))
            .unwrap();
        assert_eq!(scalar_utf8(result), Some("alice".to_string()));
    }

    #[test]
    fn test_current_user_volatility_immutable() {
        let func = CurrentUserFunc::new(alice_identity());
        assert_eq!(func.signature().volatility, Volatility::Immutable);
    }

    // -----------------------------------------------------------------------
    // is_role_in_session(role)
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_role_in_session_found() {
        let func = IsRoleInSessionFunc::new(alice_identity());
        let arg = ColumnarValue::Scalar(ScalarValue::Utf8(Some("admin".to_string())));
        let result = func
            .invoke_with_args(make_args_utf8(arg, DataType::Boolean, 1))
            .unwrap();
        assert_eq!(scalar_bool(result), Some(true));
    }

    #[test]
    fn test_is_role_in_session_not_found() {
        let func = IsRoleInSessionFunc::new(alice_identity());
        let arg = ColumnarValue::Scalar(ScalarValue::Utf8(Some("engineer".to_string())));
        let result = func
            .invoke_with_args(make_args_utf8(arg, DataType::Boolean, 1))
            .unwrap();
        assert_eq!(scalar_bool(result), Some(false));
    }

    #[test]
    fn test_is_role_in_session_empty_roles() {
        let func = IsRoleInSessionFunc::new(empty_identity());
        let arg = ColumnarValue::Scalar(ScalarValue::Utf8(Some("admin".to_string())));
        let result = func
            .invoke_with_args(make_args_utf8(arg, DataType::Boolean, 1))
            .unwrap();
        assert_eq!(scalar_bool(result), Some(false));
    }

    #[test]
    fn test_is_role_in_session_null_arg() {
        let func = IsRoleInSessionFunc::new(alice_identity());
        let arg = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        let result = func
            .invoke_with_args(make_args_utf8(arg, DataType::Boolean, 1))
            .unwrap();
        assert_eq!(scalar_bool(result), None);
    }

    #[test]
    fn test_is_role_in_session_array() {
        let func = IsRoleInSessionFunc::new(alice_identity());
        let array = Arc::new(StringArray::from(vec![
            Some("admin"),
            Some("engineer"),
            None,
            Some("analyst"),
        ])) as ArrayRef;
        let arg = ColumnarValue::Array(array);
        let result = func
            .invoke_with_args(make_args_utf8(arg, DataType::Boolean, 4))
            .unwrap();
        if let ColumnarValue::Array(arr) = result {
            let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert!(bool_arr.value(0));            // admin found
            assert!(!bool_arr.value(1));           // engineer not found
            assert!(bool_arr.is_null(2));          // null -> null
            assert!(bool_arr.value(3));            // analyst found
        } else {
            panic!("expected Boolean array");
        }
    }

    #[test]
    fn test_is_role_in_session_volatility_immutable() {
        let func = IsRoleInSessionFunc::new(alice_identity());
        assert_eq!(func.signature().volatility, Volatility::Immutable);
    }

    // -----------------------------------------------------------------------
    // current_available_roles()
    // -----------------------------------------------------------------------

    #[test]
    fn test_current_available_roles_sorted_json() {
        // alice_identity sorts ["analyst","admin"] -> ["admin","analyst"]
        let func = CurrentAvailableRolesFunc::new(alice_identity());
        let result = func
            .invoke_with_args(make_args_noarg(DataType::Utf8, 1))
            .unwrap();
        assert_eq!(
            scalar_utf8(result),
            Some(r#"["admin","analyst"]"#.to_string())
        );
    }

    #[test]
    fn test_current_available_roles_engineer_analyst_sorted() {
        let id = Arc::new(SessionIdentity::new(
            "charlie",
            vec!["engineer", "analyst"],
            None,
            None,
        ));
        let func = CurrentAvailableRolesFunc::new(id);
        let result = func
            .invoke_with_args(make_args_noarg(DataType::Utf8, 1))
            .unwrap();
        assert_eq!(
            scalar_utf8(result),
            Some(r#"["analyst","engineer"]"#.to_string())
        );
    }

    #[test]
    fn test_current_available_roles_empty() {
        let func = CurrentAvailableRolesFunc::new(empty_identity());
        let result = func
            .invoke_with_args(make_args_noarg(DataType::Utf8, 1))
            .unwrap();
        assert_eq!(scalar_utf8(result), Some("[]".to_string()));
    }

    #[test]
    fn test_current_available_roles_volatility_immutable() {
        let func = CurrentAvailableRolesFunc::new(alice_identity());
        assert_eq!(func.signature().volatility, Volatility::Immutable);
    }

    // -----------------------------------------------------------------------
    // current_database()
    // -----------------------------------------------------------------------

    #[test]
    fn test_current_database_some() {
        let func = CurrentDatabaseFunc::new(alice_identity());
        let result = func
            .invoke_with_args(make_args_noarg(DataType::Utf8, 1))
            .unwrap();
        assert_eq!(scalar_utf8(result), Some("sales_wh".to_string()));
    }

    #[test]
    fn test_current_database_none() {
        let func = CurrentDatabaseFunc::new(empty_identity());
        let result = func
            .invoke_with_args(make_args_noarg(DataType::Utf8, 1))
            .unwrap();
        assert_eq!(scalar_utf8(result), None);
    }

    #[test]
    fn test_current_database_volatility_immutable() {
        let func = CurrentDatabaseFunc::new(alice_identity());
        assert_eq!(func.signature().volatility, Volatility::Immutable);
    }

    // -----------------------------------------------------------------------
    // current_schema()
    // -----------------------------------------------------------------------

    #[test]
    fn test_current_schema_some() {
        let func = CurrentSchemaFunc::new(alice_identity());
        let result = func
            .invoke_with_args(make_args_noarg(DataType::Utf8, 1))
            .unwrap();
        assert_eq!(scalar_utf8(result), Some("public".to_string()));
    }

    #[test]
    fn test_current_schema_none() {
        let func = CurrentSchemaFunc::new(empty_identity());
        let result = func
            .invoke_with_args(make_args_noarg(DataType::Utf8, 1))
            .unwrap();
        assert_eq!(scalar_utf8(result), None);
    }

    #[test]
    fn test_current_schema_volatility_immutable() {
        let func = CurrentSchemaFunc::new(alice_identity());
        assert_eq!(func.signature().volatility, Volatility::Immutable);
    }

    // -----------------------------------------------------------------------
    // Inequality: different identities produce unequal UDFs
    // -----------------------------------------------------------------------

    #[test]
    fn test_current_user_different_identities_not_equal() {
        let id_alice = Arc::new(SessionIdentity::new("alice", vec![] as Vec<String>, None, None));
        let id_bob = Arc::new(SessionIdentity::new("bob", vec![] as Vec<String>, None, None));
        let func_a = CurrentUserFunc::new(id_alice);
        let func_b = CurrentUserFunc::new(id_bob);
        assert_ne!(func_a, func_b);
    }

    #[test]
    fn test_is_role_different_identities_not_equal() {
        let id_alice = alice_identity();
        let id_bob = empty_identity();
        let func_a = IsRoleInSessionFunc::new(id_alice);
        let func_b = IsRoleInSessionFunc::new(id_bob);
        assert_ne!(func_a, func_b);
    }

    // -----------------------------------------------------------------------
    // session_udfs() constructor
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_udfs_returns_five() {
        let udfs = session_udfs(alice_identity());
        assert_eq!(udfs.len(), 5);
    }

    #[test]
    fn test_session_udfs_names() {
        let udfs = session_udfs(alice_identity());
        let names: Vec<&str> = udfs.iter().map(|u| u.name()).collect();
        assert!(names.contains(&"current_user"));
        assert!(names.contains(&"is_role_in_session"));
        assert!(names.contains(&"current_available_roles"));
        assert!(names.contains(&"current_database"));
        assert!(names.contains(&"current_schema"));
    }

    // -----------------------------------------------------------------------
    // SessionIdentity::new deduplicates and sorts
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_identity_sorts_and_deduplicates_roles() {
        let id = SessionIdentity::new(
            "x",
            vec!["c", "a", "b", "a", "c"],
            None,
            None,
        );
        assert_eq!(id.roles, vec!["a", "b", "c"]);
    }
}
