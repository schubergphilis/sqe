//! The shredded variant fields a query reads (sqe#870).
//!
//! A query that reads a variant column only through `variant_get` /
//! `try_variant_get` object paths (`$.a`, `$.b.c`, `$['d']`) needs only the
//! shredded `typed_value` fields named by the first step of each path. The
//! scan then reads `metadata`, the residual `value` and those fields instead
//! of every shredded field (see `iceberg::arrow::variant_projection`), and
//! unshreds just them.
//!
//! The analysis runs on the final logical plan and works by column name,
//! erring towards reading the whole column: a name qualifies only when
//! every expression that mentions it is such a call, and no variant column
//! of that name leaves the plan, or enters a node that consumes its input
//! rows whole (a `DISTINCT`, a `UNION`, a write, an extension), without
//! passing through an expression. Two tables with a variant column of the
//! same name share one set of fields, which reads more but never less.

use std::collections::{BTreeSet, HashMap, HashSet};

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::DFSchema;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::scalar::ScalarValue;

/// The object fields each variant column is read through, by column name.
/// Carried to the table scans as a session config extension.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VariantReadHints(pub HashMap<String, BTreeSet<String>>);

impl VariantReadHints {
    /// The hints for `plan`; empty when no variant column narrows.
    pub fn of(plan: &LogicalPlan) -> Self {
        let mut walk = Walk::default();
        walk.leaving(plan.schema());
        let walked = plan.apply_with_subqueries(|node| {
            walk.node(node);
            Ok(TreeNodeRecursion::Continue)
        });
        if walked.is_err() {
            return Self::default();
        }
        let Walk { mut fields, whole } = walk;
        fields.retain(|name, _| !whole.contains(name));
        Self(fields)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Default)]
struct Walk {
    fields: HashMap<String, BTreeSet<String>>,
    /// Names some use reads whole.
    whole: HashSet<String>,
}

impl Walk {
    fn node(&mut self, node: &LogicalPlan) {
        // These nodes read their input columns only through their own
        // expressions, or pass them unchanged to a parent that does.
        let passes_through = matches!(
            node,
            LogicalPlan::Projection(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::Aggregate(_)
                | LogicalPlan::Window(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Limit(_)
                | LogicalPlan::Join(_)
                | LogicalPlan::SubqueryAlias(_)
                | LogicalPlan::Repartition(_)
                | LogicalPlan::TableScan(_)
                | LogicalPlan::EmptyRelation(_)
                | LogicalPlan::Values(_)
        );
        if !passes_through {
            for input in node.inputs() {
                self.leaving(input.schema());
            }
        }
        // `expressions()` holds a TableScan's pushed filters too.
        for expr in node.expressions() {
            let _ = expr.apply(|e| Ok(self.expr(e)));
        }
    }

    fn expr(&mut self, expr: &Expr) -> TreeNodeRecursion {
        match expr {
            Expr::ScalarFunction(call) => {
                if let Some((column, field)) = field_read(call) {
                    self.fields.entry(column).or_default().insert(field);
                    return TreeNodeRecursion::Jump;
                }
            }
            Expr::Column(column) => {
                self.whole.insert(column.name.clone());
            }
            Expr::OuterReferenceColumn(_, column) => {
                self.whole.insert(column.name.clone());
            }
            Expr::ScalarSubquery(subquery) => self.leaving(subquery.subquery.schema()),
            Expr::InSubquery(in_subquery) => self.leaving(in_subquery.subquery.subquery.schema()),
            Expr::Exists(exists) => self.leaving(exists.subquery.subquery.schema()),
            _ => {}
        }
        TreeNodeRecursion::Continue
    }

    /// Every variant column in `schema` is read whole.
    fn leaving(&mut self, schema: &DFSchema) {
        for (_, field) in schema.iter() {
            if sqe_core::variant::is_variant_storage(field.data_type()) {
                self.whole.insert(field.name().clone());
            }
        }
    }
}

/// `(column, first field)` for `variant_get(column, '$.field...', ...)` or
/// `try_variant_get`, the variant a bare column and the path a literal
/// whose first step is an object field.
fn field_read(call: &ScalarFunction) -> Option<(String, String)> {
    if !matches!(call.func.name(), "variant_get" | "try_variant_get") {
        return None;
    }
    let [Expr::Column(column), path, rest @ ..] = call.args.as_slice() else {
        return None;
    };
    if !rest.iter().all(|e| matches!(e, Expr::Literal(..))) {
        return None;
    }
    let Expr::Literal(
        ScalarValue::Utf8(Some(path)) | ScalarValue::LargeUtf8(Some(path)) | ScalarValue::Utf8View(Some(path)),
        _,
    ) = path
    else {
        return None;
    };
    Some((column.name.clone(), first_field(path)?))
}

/// The object field a Spark-style JSON path starts with: `a` for `$.a`,
/// `$.a.b`, `$.a[0]`, `$['a']` and `$["a"]`. `None` for `$` and an array
/// step.
fn first_field(path: &str) -> Option<String> {
    let rest = path.trim().strip_prefix('$')?;
    if let Some(rest) = rest.strip_prefix('.') {
        let end = rest.find(['.', '[']).unwrap_or(rest.len());
        let name = &rest[..end];
        return (!name.is_empty() && name != "*").then(|| name.to_string());
    }
    let rest = rest.strip_prefix('[')?;
    let quote = rest.chars().next().filter(|c| matches!(c, '\'' | '"'))?;
    let rest = &rest[1..];
    let end = rest.find(quote)?;
    rest[end + 1..].starts_with(']').then(|| rest[..end].to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[test]
    fn first_field_reads_the_first_object_step() {
        assert_eq!(first_field("$.a").as_deref(), Some("a"));
        assert_eq!(first_field(" $.a.b ").as_deref(), Some("a"));
        assert_eq!(first_field("$.a[0]").as_deref(), Some("a"));
        assert_eq!(first_field("$['a b'].c").as_deref(), Some("a b"));
        assert_eq!(first_field("$[\"a\"]").as_deref(), Some("a"));
        assert_eq!(first_field("$"), None);
        assert_eq!(first_field("$[0]"), None);
        assert_eq!(first_field("$.*"), None);
        assert_eq!(first_field("$['a"), None);
        assert_eq!(first_field("a"), None);
    }

    async fn hints(sql: &str) -> VariantReadHints {
        let ctx = SessionContext::new();
        sqe_core::variant_get::register_variant_functions(&ctx);
        let storage = sqe_core::variant::storage_type();
        for table in ["t", "u"] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("v", storage.clone(), true),
                Field::new("w", storage.clone(), true),
            ]));
            let mem = MemTable::try_new(schema, vec![vec![]]).unwrap();
            ctx.register_table(table, Arc::new(mem)).unwrap();
        }
        let plan = ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap();
        VariantReadHints::of(&plan)
    }

    fn fields(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[tokio::test]
    async fn object_paths_name_their_first_fields() {
        let h = hints(
            "SELECT variant_get(v, '$.a', 'int'), try_variant_get(v, '$.b.c', 'string') \
             FROM t WHERE variant_get(v, '$[''d'']', 'int') > 5",
        )
        .await;
        assert_eq!(h.0.get("v"), Some(&fields(&["a", "b", "d"])));
        assert_eq!(h.0.len(), 1);
    }

    #[tokio::test]
    async fn any_whole_use_reads_the_column_whole() {
        for sql in [
            "SELECT v FROM t",
            "SELECT * FROM t",
            "SELECT variant_get(v, '$.a', 'int'), v FROM t",
            "SELECT variant_get(v, '$', 'string') FROM t",
            "SELECT variant_get(v, '$[0]', 'int') FROM t",
            "SELECT count(DISTINCT v) FROM t",
            "SELECT id FROM t WHERE variant_get(v, '$.a', 'int') > 1 \
             UNION ALL SELECT id FROM u WHERE v IS NOT NULL",
            "SELECT v FROM t UNION SELECT v FROM u",
            "SELECT variant_get(v, '$.a', 'int') FROM t ORDER BY v",
        ] {
            assert!(!hints(sql).await.0.contains_key("v"), "{sql}");
        }
    }

    #[tokio::test]
    async fn an_alias_reads_only_the_fields_read_through_it() {
        // The optimizer inlines `v AS x`, so the call reads `v` directly; a
        // kept alias would carry a bare `v` and read the column whole.
        let h = hints("SELECT variant_get(x, '$.a', 'int') FROM (SELECT v AS x FROM t)").await;
        if let Some(read) = h.0.get("v") {
            assert_eq!(read, &fields(&["a"]));
        }
        assert!(!h.0.contains_key("x"));
    }

    #[tokio::test]
    async fn other_columns_keep_their_hints() {
        let h = hints("SELECT w, variant_get(v, '$.a', 'int') FROM t").await;
        assert_eq!(h.0.get("v"), Some(&fields(&["a"])));
        assert!(!h.0.contains_key("w"));
    }

    #[tokio::test]
    async fn a_join_passes_the_column_to_the_projection() {
        let h = hints(
            "SELECT variant_get(t.v, '$.a', 'int'), variant_get(u.v, '$.b', 'int') \
             FROM t JOIN u ON t.id = u.id",
        )
        .await;
        assert_eq!(h.0.get("v"), Some(&fields(&["a", "b"])));
    }
}
