//! Tests pinning Trino's result types for decimal `sum`, `avg`, `abs`,
//! and `round`, and `real` statistics, through the engine
//! context. The rules live in this module's siblings (`TrinoSum`,
//! `RoundedAvg`) and in `sqe-trino-functions`.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Decimal128Array, Float32Array};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::SessionContext;

    use crate::session_tuning::{engine_context, engine_session_config, register_engine_functions};

    fn ctx() -> SessionContext {
        let mut ctx = engine_context(engine_session_config(), Arc::new(RuntimeEnv::default()));
        register_engine_functions(&mut ctx).unwrap();
        ctx
    }

    /// Arrow type and unscaled values of the first column.
    async fn dec(ctx: &SessionContext, sql: &str) -> (DataType, Vec<Option<i128>>) {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut vals = vec![];
        let mut ty = None;
        for b in &batches {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap_or_else(|| panic!("not decimal: {}", b.column(0).data_type()));
            ty = Some(a.data_type().clone());
            vals.extend(a.iter());
        }
        (ty.unwrap(), vals)
    }

    const T: &str = "(VALUES (1, CAST('12.34' AS DECIMAL(4,2))), (1, CAST('1.01' AS DECIMAL(4,2))), \
                     (2, CAST(NULL AS DECIMAL(4,2))), (2, CAST('-0.50' AS DECIMAL(4,2)))) AS t(g, x)";

    #[tokio::test]
    async fn sum_is_decimal_38() {
        let ctx = ctx();
        let (t, v) = dec(&ctx, "SELECT sum(CAST('12.34' AS DECIMAL(4,2)))").await;
        assert_eq!((t, v), (DataType::Decimal128(38, 2), vec![Some(1234)]));
        let (t, v) = dec(&ctx, &format!("SELECT sum(x) FROM {T}")).await;
        assert_eq!((t, v), (DataType::Decimal128(38, 2), vec![Some(1285)]));
        let (t, v) = dec(
            &ctx,
            &format!("SELECT sum(x) FROM {T} GROUP BY g ORDER BY g"),
        )
        .await;
        assert_eq!(
            (t, v),
            (DataType::Decimal128(38, 2), vec![Some(1335), Some(-50)])
        );
    }

    #[tokio::test]
    async fn avg_is_input_decimal() {
        let ctx = ctx();
        let (t, v) = dec(&ctx, "SELECT avg(CAST('12.34' AS DECIMAL(4,2)))").await;
        assert_eq!((t, v), (DataType::Decimal128(4, 2), vec![Some(1234)]));
        // (12.34 + 1.01) / 2 = 6.675 -> 6.68 (half away from zero).
        let (t, v) = dec(&ctx, &format!("SELECT avg(x) FROM {T} WHERE g = 1")).await;
        assert_eq!((t, v), (DataType::Decimal128(4, 2), vec![Some(668)]));
        let (t, v) = dec(
            &ctx,
            &format!("SELECT avg(x) FROM {T} GROUP BY g ORDER BY g"),
        )
        .await;
        assert_eq!(
            (t, v),
            (DataType::Decimal128(4, 2), vec![Some(668), Some(-50)])
        );
    }

    #[tokio::test]
    async fn abs_keeps_decimal_type() {
        let ctx = ctx();
        let (t, v) = dec(&ctx, "SELECT abs(CAST('-12.34' AS DECIMAL(4,2)))").await;
        assert_eq!((t, v), (DataType::Decimal128(4, 2), vec![Some(1234)]));
        let (t, v) = dec(&ctx, &format!("SELECT abs(x) FROM {T} ORDER BY g, x")).await;
        assert_eq!(t, DataType::Decimal128(4, 2));
        assert_eq!(v, vec![Some(101), Some(1234), Some(50), None]);
    }

    #[tokio::test]
    async fn round_takes_trino_types() {
        let ctx = ctx();
        let (t, v) = dec(&ctx, "SELECT round(CAST('12.34' AS DECIMAL(4,2)))").await;
        assert_eq!((t, v), (DataType::Decimal128(3, 0), vec![Some(12)]));
        let (t, v) = dec(&ctx, "SELECT round(CAST('12.34' AS DECIMAL(4,2)), 1)").await;
        assert_eq!((t, v), (DataType::Decimal128(5, 2), vec![Some(1230)]));
        let (t, v) = dec(&ctx, &format!("SELECT round(x) FROM {T} ORDER BY g, x")).await;
        assert_eq!(t, DataType::Decimal128(3, 0));
        // 1.01 -> 1, 12.34 -> 12, -0.50 -> -1 (half away from zero), NULL.
        assert_eq!(v, vec![Some(1), Some(12), Some(-1), None]);
    }

    #[tokio::test]
    async fn real_statistics_return_real_through_the_engine() {
        let ctx = ctx();
        let batches = ctx
            .sql(
                "SELECT g, corr(y, x), covar_pop(y, x) FROM (VALUES \
                 (1, CAST(1 AS REAL), CAST(2 AS REAL)), (1, CAST(2 AS REAL), CAST(4 AS REAL)), \
                 (2, CAST(1 AS REAL), CAST(NULL AS REAL))) AS t(g, x, y) GROUP BY g ORDER BY g",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let b = &batches[0];
        assert_eq!(b.column(1).data_type(), &DataType::Float32);
        assert_eq!(b.column(2).data_type(), &DataType::Float32);
        let corr = b.column(1).as_any().downcast_ref::<Float32Array>().unwrap();
        let cov = b.column(2).as_any().downcast_ref::<Float32Array>().unwrap();
        assert!((corr.value(0) - 1.0).abs() < 1e-6);
        assert!((cov.value(0) - 0.5).abs() < 1e-6);
        assert!(corr.is_null(1));
    }
}
