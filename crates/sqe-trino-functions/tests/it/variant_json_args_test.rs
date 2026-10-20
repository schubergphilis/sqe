//! `sqe_core::variant_json_args` against the real Trino UDF registry.
//!
//! This lived in `sqe-core` as a `#[cfg(test)]` module, which forced the base
//! crate to carry a dev-dependency on `sqe-trino-functions` — a feature crate
//! that sits *above* it. The test only needs public API from both sides, so it
//! belongs here, where the dependency points the right way.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use sqe_core::variant::{from_arrow, storage_type};
use sqe_core::variant_json_args::accept_variant_in_json_functions;

/// A session with the JSON functions and a one-row table `t(v)` where
/// `v` is a variant holding `{"a": {"b": 2}, "s": "x"}`.
async fn session() -> SessionContext {
    let mut ctx = SessionContext::new();
    sqe_trino_functions::register_trino_functions(&ctx);
    sqe_trino_functions::register_extended_trino_functions(&ctx);
    datafusion_functions_json::register_all(&mut ctx).unwrap();
    accept_variant_in_json_functions(&ctx);

    let text: ArrayRef = Arc::new(StringArray::from(vec![Some(
        r#"{"a": {"b": 2}, "s": "x"}"#,
    )]));
    let variant = from_arrow(text.as_ref(), &storage_type()).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        variant.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![variant]).unwrap();
    ctx.register_batch("t", batch).unwrap();
    ctx
}

async fn one(ctx: &SessionContext, sql: &str) -> String {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let col = batches[0].column(0);
    datafusion::arrow::util::display::array_value_to_string(col, 0).unwrap()
}

#[tokio::test]
async fn json_functions_read_a_variant_as_json_text() {
    let ctx = session().await;
    assert_eq!(
        one(&ctx, "SELECT json_extract(v, '$.a') FROM t").await,
        r#"{"b":2}"#
    );
    assert_eq!(
        one(&ctx, "SELECT json_extract_scalar(v, '$.s') FROM t").await,
        "x"
    );
    assert_eq!(
        one(&ctx, "SELECT json_get_int(v, 'a', 'b') FROM t").await,
        "2"
    );
    assert_eq!(one(&ctx, "SELECT json_get_str(v, 's') FROM t").await, "x");
}

#[tokio::test]
async fn json_functions_still_take_text() {
    let ctx = session().await;
    assert_eq!(
        one(&ctx, r#"SELECT json_extract_scalar('{"s": "y"}', '$.s')"#).await,
        "y"
    );
    assert_eq!(
        one(&ctx, r#"SELECT json_get_int('{"n": 7}', 'n')"#).await,
        "7"
    );
}

#[tokio::test]
async fn wrapping_twice_is_a_no_op() {
    let ctx = session().await;
    accept_variant_in_json_functions(&ctx);
    assert_eq!(
        one(&ctx, "SELECT json_get_int(v, 'a', 'b') FROM t").await,
        "2"
    );
}

/// `to_json`, the target of `CAST(x AS JSON)`, embeds a variant as its JSON
/// value, also inside an array or a row, instead of encoding the storage
/// struct (#848).
#[tokio::test]
async fn to_json_embeds_a_variant() {
    let ctx = session().await;
    let doc = r#"{"a":{"b":2},"s":"x"}"#;
    assert_eq!(one(&ctx, "SELECT to_json(v) FROM t").await, doc);
    assert_eq!(
        one(&ctx, "SELECT to_json(make_array(v, v)) FROM t").await,
        format!("[{doc},{doc}]")
    );
    assert_eq!(
        one(&ctx, "SELECT to_json(named_struct('k', v)) FROM t").await,
        format!(r#"{{"k":{doc}}}"#)
    );
}

/// `typeof` names a variant `variant`, by its extension tag or, when an
/// expression dropped the tag, by its storage shape (#849, #867).
#[tokio::test]
async fn typeof_names_a_variant() {
    let ctx = session().await;
    assert_eq!(one(&ctx, "SELECT typeof(v) FROM t").await, "variant");
    assert_eq!(
        one(&ctx, "SELECT typeof(make_array(v)) FROM t").await,
        "array(json)"
    );
}
