# dbt-sqe Adapter and ALTER TABLE Schema Evolution — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add full ALTER TABLE schema evolution (ADD/DROP/RENAME COLUMN, SET/DROP NOT NULL, type widening) and a production-ready dbt-sqe Python adapter with all materialization strategies.

**Architecture:** Two independent features. ALTER TABLE (7.3) wires sqlparser's AlterTableOperation variants through the existing classifier → query_handler → catalog_ops pipeline to iceberg-rust's `update_table()` API. dbt-sqe (7.1) is a standard dbt namespace package under `adapters/dbt-sqe/` using ADBC Flight SQL for connection.

**Tech Stack:** Rust (ALTER TABLE), Python 3.10+ (dbt adapter), adbc-driver-flightsql, dbt-core 1.8+, dbt-adapters 1.16+

---

## Part A: ALTER TABLE Schema Evolution

### File Map

| File | Action | Purpose |
|---|---|---|
| `crates/sqe-sql/src/classifier.rs` | Modify | Add `AlterSchema` variant, route ADD/DROP/RENAME/ALTER COLUMN |
| `crates/sqe-coordinator/src/query_handler.rs` | Modify | Route `AlterSchema` to `catalog_ops.alter_table_schema()` |
| `crates/sqe-coordinator/src/catalog_ops.rs` | Modify | Implement `alter_table_schema()` — load table, modify schema, commit |

---

### Task A1: Add AlterSchema to classifier

**Files:**
- Modify: `crates/sqe-sql/src/classifier.rs:8-32` (enum), `crates/sqe-sql/src/classifier.rs:136-147` (match arm), tests

- [ ] **Step 1: Update existing tests to expect new variant**

Change the two existing ALTER TABLE tests at lines 403-415 to expect `AlterSchema` instead of `Utility`:

```rust
#[test]
fn test_alter_table_add_column_is_alter_schema() {
    let result = parse_and_classify("ALTER TABLE foo ADD COLUMN bar INT");
    assert!(matches!(result, Ok(StatementKind::AlterSchema(_))));
}

#[test]
fn test_alter_table_rename_column_is_alter_schema() {
    let result = parse_and_classify("ALTER TABLE foo RENAME COLUMN old_col TO new_col");
    assert!(matches!(result, Ok(StatementKind::AlterSchema(_))));
}
```

Add new tests:

```rust
#[test]
fn test_alter_table_drop_column_is_alter_schema() {
    let result = parse_and_classify("ALTER TABLE foo DROP COLUMN bar");
    assert!(matches!(result, Ok(StatementKind::AlterSchema(_))));
}

#[test]
fn test_alter_table_alter_column_set_not_null() {
    let result = parse_and_classify("ALTER TABLE foo ALTER COLUMN bar SET NOT NULL");
    assert!(matches!(result, Ok(StatementKind::AlterSchema(_))));
}

#[test]
fn test_alter_table_alter_column_drop_not_null() {
    let result = parse_and_classify("ALTER TABLE foo ALTER COLUMN bar DROP NOT NULL");
    assert!(matches!(result, Ok(StatementKind::AlterSchema(_))));
}

#[test]
fn test_alter_table_alter_column_set_data_type() {
    let result = parse_and_classify("ALTER TABLE foo ALTER COLUMN bar SET DATA TYPE BIGINT");
    assert!(matches!(result, Ok(StatementKind::AlterSchema(_))));
}

#[test]
fn test_alter_table_rename_still_works() {
    let result = parse_and_classify("ALTER TABLE foo RENAME TO bar");
    assert!(matches!(result, Ok(StatementKind::Rename(_))));
}

#[test]
fn test_alter_schema_name() {
    let kind = parse_and_classify("ALTER TABLE foo ADD COLUMN bar INT").unwrap();
    assert_eq!(kind.name(), "alterschema");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p sqe-sql -- --nocapture`
Expected: FAIL — `AlterSchema` variant doesn't exist yet, and existing tests expect `Utility`.

- [ ] **Step 3: Add AlterSchema variant to StatementKind enum**

In `crates/sqe-sql/src/classifier.rs`, add variant to the enum (after `Rename`):

```rust
AlterSchema(Box<Statement>),
```

Add to the `name()` match:

```rust
StatementKind::AlterSchema(_) => "alterschema",
```

Update the `AlterTable` match arm (lines 136-147) to detect schema-change operations:

```rust
Statement::AlterTable {
    ref operations, ..
} => {
    let is_rename = operations.iter().any(|op| {
        matches!(op, AlterTableOperation::RenameTable { .. })
    });
    let is_schema_change = operations.iter().any(|op| {
        matches!(
            op,
            AlterTableOperation::AddColumn { .. }
                | AlterTableOperation::DropColumn { .. }
                | AlterTableOperation::RenameColumn { .. }
                | AlterTableOperation::AlterColumn { .. }
        )
    });
    if is_rename {
        Ok(StatementKind::Rename(Box::new(stmt)))
    } else if is_schema_change {
        Ok(StatementKind::AlterSchema(Box::new(stmt)))
    } else {
        Ok(StatementKind::Utility(Box::new(stmt)))
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p sqe-sql -- --nocapture`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-sql/src/classifier.rs
git commit -m "feat(sql): classify ALTER TABLE ADD/DROP/RENAME/ALTER COLUMN as AlterSchema"
```

---

### Task A2: Route AlterSchema in query_handler

**Files:**
- Modify: `crates/sqe-coordinator/src/query_handler.rs:274` (add match arm after `Rename`)

- [ ] **Step 1: Add the match arm**

After the `StatementKind::Rename` arm (around line 277), add:

```rust
StatementKind::AlterSchema(stmt) => {
    self.catalog_ops.alter_table_schema(session, stmt).await?;
    Ok(vec![])
}
```

- [ ] **Step 2: Verify it compiles (will fail — method doesn't exist yet)**

Run: `cargo check -p sqe-coordinator`
Expected: FAIL — `alter_table_schema` not found on `CatalogOps`.

- [ ] **Step 3: Add stub method to catalog_ops**

In `crates/sqe-coordinator/src/catalog_ops.rs`, add after `rename_table()` (after line 238):

```rust
/// Alter table schema: ADD/DROP/RENAME COLUMN, SET/DROP NOT NULL, type widening.
pub async fn alter_table_schema(
    &self,
    session: &Session,
    stmt: &Statement,
) -> sqe_core::Result<()> {
    let _ = (session, stmt);
    Err(SqeError::NotImplemented("ALTER TABLE schema evolution not yet implemented".to_string()))
}
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check -p sqe-coordinator`
Expected: OK — compiles with stub.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/query_handler.rs crates/sqe-coordinator/src/catalog_ops.rs
git commit -m "feat(coordinator): route AlterSchema to catalog_ops stub"
```

---

### Task A3: Implement alter_table_schema

**Files:**
- Modify: `crates/sqe-coordinator/src/catalog_ops.rs` — replace stub with full implementation

- [ ] **Step 1: Add imports**

At the top of `catalog_ops.rs`, add/extend imports:

```rust
use iceberg::spec::NestedField;
use iceberg::arrow::arrow_type_to_type;
use sqlparser::ast::{AlterColumnOperation, ColumnDef};
use std::sync::Arc as StdArc;
```

Also add (for the sql_type_to_arrow reuse — make it pub(crate) in write_handler.rs):

In `crates/sqe-coordinator/src/write_handler.rs` line 1413, change:
```rust
fn sql_type_to_arrow(sql_type: &sqlparser::ast::DataType) -> sqe_core::Result<arrow_schema::DataType> {
```
to:
```rust
pub(crate) fn sql_type_to_arrow(sql_type: &sqlparser::ast::DataType) -> sqe_core::Result<arrow_schema::DataType> {
```

Then in catalog_ops.rs add:
```rust
use crate::write_handler::sql_type_to_arrow;
```

- [ ] **Step 2: Implement alter_table_schema**

Replace the stub with the full implementation:

```rust
/// Alter table schema: ADD/DROP/RENAME COLUMN, SET/DROP NOT NULL, type widening.
///
/// Loads the current table, applies each AlterTableOperation to the schema,
/// and commits via `catalog.update_table()`.
#[instrument(skip(self, session, stmt), fields(username = %session.user.username))]
pub async fn alter_table_schema(
    &self,
    session: &Session,
    stmt: &Statement,
) -> sqe_core::Result<()> {
    let (table_name, operations) = match stmt {
        Statement::AlterTable {
            name, operations, ..
        } => (name, operations),
        other => {
            return Err(SqeError::Execution(format!(
                "Expected ALTER TABLE statement, got: {other}"
            )));
        }
    };

    let (namespace, name) = parse_table_ref(table_name)?;
    let table_ident = TableIdent::new(namespace, name.clone());

    info!(
        username = %session.user.username,
        table = %table_ident,
        operations = operations.len(),
        "Altering table schema"
    );

    let catalog = self.create_catalog_bridge(session).await?;
    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| SqeError::Catalog(format!("Failed to load table: {e}")))?;

    let current_schema = table.metadata().current_schema();
    let current_fields: Vec<StdArc<NestedField>> =
        current_schema.as_struct().fields().to_vec();
    let mut max_field_id = current_fields
        .iter()
        .map(|f| f.id)
        .max()
        .unwrap_or(0);

    let mut fields = current_fields;

    for op in operations {
        match op {
            AlterTableOperation::AddColumn { column_def, .. } => {
                let col = column_def;
                max_field_id += 1;
                let arrow_type = sql_type_to_arrow(&col.data_type)?;
                let iceberg_type =
                    arrow_type_to_type(&arrow_type).map_err(|e| {
                        SqeError::Execution(format!(
                            "Cannot convert type for column '{}': {e}",
                            col.name
                        ))
                    })?;

                let is_not_null = col.options.iter().any(|opt| {
                    matches!(
                        opt.option,
                        sqlparser::ast::ColumnOption::NotNull
                    )
                });

                let field = if is_not_null {
                    NestedField::required(
                        max_field_id,
                        col.name.value.clone(),
                        iceberg_type,
                    )
                } else {
                    NestedField::optional(
                        max_field_id,
                        col.name.value.clone(),
                        iceberg_type,
                    )
                };
                fields.push(StdArc::new(field));
                info!(column = %col.name, "Added column");
            }

            AlterTableOperation::DropColumn {
                column_name, ..
            } => {
                let col_name = &column_name.value;
                let original_len = fields.len();
                fields.retain(|f| f.name != *col_name);
                if fields.len() == original_len {
                    return Err(SqeError::Execution(format!(
                        "Column '{col_name}' not found in table '{name}'"
                    )));
                }
                info!(column = %col_name, "Dropped column");
            }

            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                let old_name = &old_column_name.value;
                let new_name = &new_column_name.value;
                let mut found = false;
                fields = fields
                    .into_iter()
                    .map(|f| {
                        if f.name == *old_name {
                            found = true;
                            StdArc::new(NestedField {
                                id: f.id,
                                name: new_name.clone(),
                                required: f.required,
                                field_type: f.field_type.clone(),
                                doc: f.doc.clone(),
                                initial_default: f.initial_default.clone(),
                                write_default: f.write_default.clone(),
                            })
                        } else {
                            f
                        }
                    })
                    .collect();
                if !found {
                    return Err(SqeError::Execution(format!(
                        "Column '{old_name}' not found in table '{name}'"
                    )));
                }
                info!(old = %old_name, new = %new_name, "Renamed column");
            }

            AlterTableOperation::AlterColumn {
                column_name, op, ..
            } => {
                let col_name = &column_name.value;
                match op {
                    AlterColumnOperation::SetNotNull => {
                        let mut found = false;
                        fields = fields
                            .into_iter()
                            .map(|f| {
                                if f.name == *col_name {
                                    found = true;
                                    StdArc::new(NestedField {
                                        id: f.id,
                                        name: f.name.clone(),
                                        required: true,
                                        field_type: f.field_type.clone(),
                                        doc: f.doc.clone(),
                                        initial_default: f.initial_default.clone(),
                                        write_default: f.write_default.clone(),
                                    })
                                } else {
                                    f
                                }
                            })
                            .collect();
                        if !found {
                            return Err(SqeError::Execution(format!(
                                "Column '{col_name}' not found in table '{name}'"
                            )));
                        }
                        info!(column = %col_name, "Set NOT NULL");
                    }
                    AlterColumnOperation::DropNotNull => {
                        let mut found = false;
                        fields = fields
                            .into_iter()
                            .map(|f| {
                                if f.name == *col_name {
                                    found = true;
                                    StdArc::new(NestedField {
                                        id: f.id,
                                        name: f.name.clone(),
                                        required: false,
                                        field_type: f.field_type.clone(),
                                        doc: f.doc.clone(),
                                        initial_default: f.initial_default.clone(),
                                        write_default: f.write_default.clone(),
                                    })
                                } else {
                                    f
                                }
                            })
                            .collect();
                        if !found {
                            return Err(SqeError::Execution(format!(
                                "Column '{col_name}' not found in table '{name}'"
                            )));
                        }
                        info!(column = %col_name, "Dropped NOT NULL");
                    }
                    AlterColumnOperation::SetDataType { data_type, .. } => {
                        let arrow_type = sql_type_to_arrow(data_type)?;
                        let new_iceberg_type =
                            arrow_type_to_type(&arrow_type).map_err(|e| {
                                SqeError::Execution(format!(
                                    "Cannot convert type for column '{col_name}': {e}"
                                ))
                            })?;
                        let mut found = false;
                        fields = fields
                            .into_iter()
                            .map(|f| {
                                if f.name == *col_name {
                                    found = true;
                                    StdArc::new(NestedField {
                                        id: f.id,
                                        name: f.name.clone(),
                                        required: f.required,
                                        field_type: Box::new(new_iceberg_type.clone()),
                                        doc: f.doc.clone(),
                                        initial_default: f.initial_default.clone(),
                                        write_default: f.write_default.clone(),
                                    })
                                } else {
                                    f
                                }
                            })
                            .collect();
                        if !found {
                            return Err(SqeError::Execution(format!(
                                "Column '{col_name}' not found in table '{name}'"
                            )));
                        }
                        info!(column = %col_name, new_type = %data_type, "Changed data type");
                    }
                    other => {
                        return Err(SqeError::NotImplemented(format!(
                            "ALTER COLUMN operation not supported: {other:?}"
                        )));
                    }
                }
            }

            other => {
                return Err(SqeError::NotImplemented(format!(
                    "ALTER TABLE operation not supported: {other}"
                )));
            }
        }
    }

    // Build new schema and commit
    let new_schema = iceberg::spec::Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| {
            SqeError::Execution(format!("Failed to build updated schema: {e}"))
        })?;

    let transaction = iceberg::transaction::Transaction::new(&table);
    let updated_table = transaction
        .update_schema()
        .set_schema(new_schema)
        .apply()
        .and_then(|t| t.commit(&*catalog))
        .await
        .map_err(|e| {
            SqeError::Catalog(format!("Failed to commit schema change: {e}"))
        })?;

    info!(
        table = %table_ident,
        schema_id = updated_table.metadata().current_schema().schema_id(),
        "Schema updated successfully"
    );

    Ok(())
}
```

**Note:** The exact iceberg-rust Transaction API may differ from the above. The RisingWave fork (rev `1978911ec4`) may use a different method chain. If `Transaction::new(&table).update_schema().set_schema(schema).apply()` doesn't compile, check the iceberg crate's `Transaction` and `SchemaUpdate` types. The alternative approach is to build a `TableCommit` directly with `TableUpdate::SetCurrentSchema` or similar. The engineer should grep for `Transaction` and `update_schema` in the iceberg crate source to find the correct API.

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p sqe-coordinator`
Expected: OK. If the iceberg Transaction API doesn't match, adjust the commit section.

- [ ] **Step 4: Run all unit tests**

Run: `cargo test -p sqe-sql -p sqe-coordinator -- --nocapture`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/sqe-coordinator/src/catalog_ops.rs crates/sqe-coordinator/src/write_handler.rs
git commit -m "feat(coordinator): implement ALTER TABLE schema evolution (ADD/DROP/RENAME COLUMN, SET/DROP NOT NULL, type widening)"
```

---

### Task A4: Integration test

**Files:**
- Modify: `crates/sqe-coordinator/tests/integration_test.rs`

- [ ] **Step 1: Add integration test**

Add a test that creates a table, alters its schema, and verifies via information_schema:

```rust
#[tokio::test]
#[ignore] // Requires running quickstart stack
async fn test_alter_table_add_drop_column() {
    let client = setup_flight_client().await;

    // Create test table
    execute(&client, "CREATE TABLE test_ns.alter_test AS SELECT 1 AS id, 'hello' AS name").await;

    // ADD COLUMN
    execute(&client, "ALTER TABLE test_ns.alter_test ADD COLUMN age INT").await;

    // Verify column exists
    let result = query(&client, "SELECT column_name FROM information_schema.columns WHERE table_name = 'alter_test' AND column_name = 'age'").await;
    assert_eq!(result.len(), 1);

    // DROP COLUMN
    execute(&client, "ALTER TABLE test_ns.alter_test DROP COLUMN age").await;

    // Verify column gone
    let result = query(&client, "SELECT column_name FROM information_schema.columns WHERE table_name = 'alter_test' AND column_name = 'age'").await;
    assert_eq!(result.len(), 0);

    // RENAME COLUMN
    execute(&client, "ALTER TABLE test_ns.alter_test RENAME COLUMN name TO full_name").await;

    // Verify rename
    let result = query(&client, "SELECT column_name FROM information_schema.columns WHERE table_name = 'alter_test' AND column_name = 'full_name'").await;
    assert_eq!(result.len(), 1);

    // Cleanup
    execute(&client, "DROP TABLE IF EXISTS test_ns.alter_test").await;
}
```

- [ ] **Step 2: Run integration test (requires stack)**

Run: `scripts/integration-test.sh` or `cargo test -p sqe-coordinator --test integration_test test_alter_table -- --ignored --nocapture`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/sqe-coordinator/tests/integration_test.rs
git commit -m "test: add integration test for ALTER TABLE schema evolution"
```

---

## Part B: dbt-sqe Adapter

### File Map

| File | Action | Purpose |
|---|---|---|
| `adapters/dbt-sqe/setup.cfg` | Create | Package metadata and dependencies |
| `adapters/dbt-sqe/setup.py` | Create | Minimal setup.py delegating to setup.cfg |
| `adapters/dbt-sqe/dbt/__init__.py` | Create | Empty namespace package |
| `adapters/dbt-sqe/dbt/adapters/__init__.py` | Create | Empty namespace package |
| `adapters/dbt-sqe/dbt/adapters/sqe/__init__.py` | Create | Plugin registration |
| `adapters/dbt-sqe/dbt/adapters/sqe/__version__.py` | Create | Version string |
| `adapters/dbt-sqe/dbt/adapters/sqe/connections.py` | Create | SQEConnectionManager + SQECredentials |
| `adapters/dbt-sqe/dbt/adapters/sqe/impl.py` | Create | SQEAdapter |
| `adapters/dbt-sqe/dbt/adapters/sqe/column.py` | Create | SQEColumn type mapping |
| `adapters/dbt-sqe/dbt/adapters/sqe/relation.py` | Create | SQERelation |
| `adapters/dbt-sqe/dbt/include/__init__.py` | Create | Empty namespace package |
| `adapters/dbt-sqe/dbt/include/sqe/__init__.py` | Create | PACKAGE_PATH |
| `adapters/dbt-sqe/dbt/include/sqe/dbt_project.yml` | Create | dbt project config |
| `adapters/dbt-sqe/dbt/include/sqe/sample_profiles.yml` | Create | Example profiles.yml |
| `adapters/dbt-sqe/dbt/include/sqe/macros/adapters.sql` | Create | Metadata + DDL macros |
| `adapters/dbt-sqe/dbt/include/sqe/macros/catalog.sql` | Create | dbt docs generate |
| `adapters/dbt-sqe/dbt/include/sqe/macros/materializations/table.sql` | Create | CTAS materialization |
| `adapters/dbt-sqe/dbt/include/sqe/macros/materializations/view.sql` | Create | View materialization |
| `adapters/dbt-sqe/dbt/include/sqe/macros/materializations/incremental.sql` | Create | Incremental (append/delete+insert/merge) |
| `adapters/dbt-sqe/dbt/include/sqe/macros/materializations/seed.sql` | Create | Seed batch INSERT |

---

### Task B1: Package scaffolding

**Files:**
- Create: `adapters/dbt-sqe/setup.cfg`, `setup.py`, all `__init__.py` files, `dbt_project.yml`, `sample_profiles.yml`, `__version__.py`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p adapters/dbt-sqe/dbt/adapters/sqe
mkdir -p adapters/dbt-sqe/dbt/include/sqe/macros/materializations
```

- [ ] **Step 2: Create setup.cfg**

```ini
[metadata]
name = dbt-sqe
version = attr: dbt.adapters.sqe.__version__.version
description = dbt adapter for SQE (Sovereign Query Engine) via ADBC Flight SQL
long_description = file: README.md
long_description_content_type = text/markdown
author = Jacob Verhoeks
license = Apache-2.0

[options]
install_requires =
    dbt-common>=1,<2
    dbt-adapters>=1.16,<2
    dbt-core>=1.8.0
    adbc-driver-flightsql>=0.10.0
    pyarrow>=14.0.0
python_requires = >=3.10
packages = find_namespace:
include_package_data = true

[options.packages.find]
include =
    dbt
    dbt.*
```

- [ ] **Step 3: Create setup.py**

```python
from setuptools import setup

setup()
```

- [ ] **Step 4: Create namespace __init__.py files**

All four `__init__.py` files (empty — namespace packages):
- `adapters/dbt-sqe/dbt/__init__.py` — empty
- `adapters/dbt-sqe/dbt/adapters/__init__.py` — empty
- `adapters/dbt-sqe/dbt/include/__init__.py` — empty

- [ ] **Step 5: Create __version__.py**

`adapters/dbt-sqe/dbt/adapters/sqe/__version__.py`:

```python
version = "1.0.0"
```

- [ ] **Step 6: Create Plugin registration**

`adapters/dbt-sqe/dbt/adapters/sqe/__init__.py`:

```python
from dbt.adapters.sqe.connections import SQEConnectionManager, SQECredentials
from dbt.adapters.sqe.impl import SQEAdapter
from dbt.adapters.base import AdapterPlugin
from dbt.include.sqe import PACKAGE_PATH

Plugin = AdapterPlugin(
    adapter=SQEAdapter,
    credentials=SQECredentials,
    include_path=PACKAGE_PATH,
)
```

- [ ] **Step 7: Create include package**

`adapters/dbt-sqe/dbt/include/sqe/__init__.py`:

```python
import os
PACKAGE_PATH = os.path.dirname(__file__)
```

- [ ] **Step 8: Create dbt_project.yml**

`adapters/dbt-sqe/dbt/include/sqe/dbt_project.yml`:

```yaml
name: dbt_sqe
version: 1.0.0
config-version: 2
macro-paths: ["macros"]
```

- [ ] **Step 9: Create sample_profiles.yml**

`adapters/dbt-sqe/dbt/include/sqe/sample_profiles.yml`:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: sqe
      host: localhost
      port: 50051
      user: admin
      password: "{{ env_var('SQE_PASSWORD') }}"
      catalog: warehouse
      schema: analytics
      threads: 4
```

- [ ] **Step 10: Commit**

```bash
git add adapters/
git commit -m "feat(dbt): scaffold dbt-sqe adapter package"
```

---

### Task B2: Connection manager and credentials

**Files:**
- Create: `adapters/dbt-sqe/dbt/adapters/sqe/connections.py`

- [ ] **Step 1: Write connections.py**

```python
"""SQE connection manager using ADBC Flight SQL."""

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Tuple

import agate
import pyarrow as pa
from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    Credentials,
)
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError


@dataclass
class SQECredentials(Credentials):
    """Connection credentials for SQE."""

    host: str = "localhost"
    port: int = 50051
    user: Optional[str] = None
    password: Optional[str] = None
    database: str = "warehouse"  # catalog name
    schema: str = "default"

    _ALIASES = {"catalog": "database"}

    @property
    def type(self) -> str:
        return "sqe"

    @property
    def unique_field(self) -> str:
        return self.host

    def _connection_keys(self) -> Tuple[str, ...]:
        return ("host", "port", "database", "schema", "user")


class SQEConnectionManager(SQLConnectionManager):
    """Manages ADBC Flight SQL connections to SQE."""

    TYPE = "sqe"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            return connection

        credentials = connection.credentials

        try:
            from adbc_driver_flightsql.dbapi import connect

            uri = f"grpc://{credentials.host}:{credentials.port}"

            kwargs = {"uri": uri}
            if credentials.user:
                kwargs["db_kwargs"] = {
                    "username": credentials.user,
                    "password": credentials.password or "",
                }

            handle = connect(**kwargs)
            connection.handle = handle
            connection.state = "open"
        except Exception as e:
            connection.handle = None
            connection.state = "fail"
            raise DbtRuntimeError(f"Failed to connect to SQE at {uri}: {e}") from e

        return connection

    def cancel(self, connection: Connection):
        if connection.handle:
            try:
                connection.handle.close()
            except Exception:
                pass

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except Exception as e:
            msg = str(e)
            raise DbtDatabaseError(msg) from e

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        rowcount = cursor.rowcount if cursor.rowcount >= 0 else -1
        return AdapterResponse(
            _message="OK",
            rows_affected=rowcount,
        )

    @classmethod
    def get_result_from_cursor(cls, cursor, limit=None) -> agate.Table:
        """Convert ADBC Arrow result to agate table for dbt."""
        try:
            table = cursor.fetch_arrow_table()
        except Exception:
            return agate.Table(rows=[], column_names=[], column_types=[])

        if table.num_rows == 0:
            names = [field.name for field in table.schema]
            return agate.Table(rows=[], column_names=names, column_types=[])

        # Convert Arrow table to Python dicts via PyArrow
        columns = {}
        for i, field in enumerate(table.schema):
            col = table.column(i)
            columns[field.name] = col.to_pylist()

        num_rows = table.num_rows
        if limit:
            num_rows = min(num_rows, limit)

        rows = []
        col_names = list(columns.keys())
        for i in range(num_rows):
            rows.append([columns[name][i] for name in col_names])

        return agate.Table(rows=rows, column_names=col_names)
```

- [ ] **Step 2: Verify it imports**

```bash
cd adapters/dbt-sqe && pip install -e . && python -c "from dbt.adapters.sqe.connections import SQECredentials; print(SQECredentials.type)" && cd ../..
```

Expected: `sqe`

- [ ] **Step 3: Commit**

```bash
git add adapters/dbt-sqe/dbt/adapters/sqe/connections.py
git commit -m "feat(dbt): add SQE connection manager with ADBC Flight SQL"
```

---

### Task B3: Adapter, Column, and Relation classes

**Files:**
- Create: `adapters/dbt-sqe/dbt/adapters/sqe/impl.py`, `column.py`, `relation.py`

- [ ] **Step 1: Write column.py**

```python
"""SQE column type mapping."""

from dbt.adapters.base.column import Column


class SQEColumn(Column):
    """Maps Iceberg/Arrow type names to SQL standard types for dbt."""

    TYPE_LABELS = {
        "STRING": "VARCHAR",
        "LONG": "BIGINT",
        "SHORT": "SMALLINT",
        "BYTE": "TINYINT",
        "FLOAT": "REAL",
    }

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        return cls.TYPE_LABELS.get(dtype.upper(), dtype.upper())

    def is_string(self) -> bool:
        return self.dtype.upper() in ("VARCHAR", "TEXT", "STRING", "CHAR", "UTF8")

    def is_integer(self) -> bool:
        return self.dtype.upper() in (
            "INT",
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "INT32",
            "INT64",
            "INT16",
            "INT8",
            "LONG",
        )

    def is_float(self) -> bool:
        return self.dtype.upper() in ("FLOAT", "DOUBLE", "REAL", "FLOAT32", "FLOAT64")

    def is_numeric(self) -> bool:
        return self.is_integer() or self.is_float() or self.dtype.upper().startswith("DECIMAL")
```

- [ ] **Step 2: Write relation.py**

```python
"""SQE relation (table/view reference)."""

from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationType


@dataclass(frozen=True, eq=False, repr=False)
class SQERelation(BaseRelation):
    quote_character: str = '"'

    def render(self) -> str:
        parts = []
        if self.database:
            parts.append(self.quoted(self.database))
        if self.schema:
            parts.append(self.quoted(self.schema))
        if self.identifier:
            parts.append(self.quoted(self.identifier))
        return ".".join(parts)

    def quoted(self, identifier: str) -> str:
        return f'{self.quote_character}{identifier}{self.quote_character}'
```

- [ ] **Step 3: Write impl.py**

```python
"""SQE adapter implementation."""

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.sqe.connections import SQEConnectionManager
from dbt.adapters.sqe.column import SQEColumn
from dbt.adapters.sqe.relation import SQERelation


class SQEAdapter(SQLAdapter):
    """dbt adapter for SQE (Sovereign Query Engine)."""

    ConnectionManager = SQEConnectionManager
    Column = SQEColumn
    Relation = SQERelation

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def is_cancelable(cls) -> bool:
        return True

    def valid_incremental_strategies(self):
        return ["append", "delete+insert", "merge"]

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "VARCHAR"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        if decimals and decimals > 0:
            return "DOUBLE"
        else:
            return "BIGINT"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx):
        return "BOOLEAN"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "TIMESTAMP"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        return "DATE"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "VARCHAR"
```

Add the missing import at the top of impl.py:

```python
import agate
```

- [ ] **Step 4: Verify adapter loads**

```bash
cd adapters/dbt-sqe && pip install -e . && python -c "from dbt.adapters.sqe import Plugin; print(Plugin.adapter.__name__)" && cd ../..
```

Expected: `SQEAdapter`

- [ ] **Step 5: Commit**

```bash
git add adapters/dbt-sqe/dbt/adapters/sqe/
git commit -m "feat(dbt): add SQE adapter, column, and relation classes"
```

---

### Task B4: SQL macros — metadata and DDL

**Files:**
- Create: `adapters/dbt-sqe/dbt/include/sqe/macros/adapters.sql`

- [ ] **Step 1: Write adapters.sql**

```sql
{# ── Metadata discovery ─────────────────────────────────────────────── #}

{% macro sqe__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations', fetch_result=True) %}
    SELECT
      table_catalog AS "database",
      table_schema AS "schema",
      table_name AS "name",
      CASE table_type
        WHEN 'BASE TABLE' THEN 'table'
        WHEN 'VIEW' THEN 'view'
        ELSE 'table'
      END AS "type"
    FROM information_schema.tables
    WHERE table_schema = '{{ schema_relation.schema }}'
      AND table_catalog = '{{ schema_relation.database }}'
  {% endcall %}
  {{ return(load_result('list_relations').table) }}
{% endmacro %}

{% macro sqe__get_columns_in_relation(relation) %}
  {% call statement('get_columns', fetch_result=True) %}
    SELECT
      column_name,
      data_type,
      character_maximum_length,
      numeric_precision,
      numeric_scale
    FROM information_schema.columns
    WHERE table_name = '{{ relation.identifier }}'
      AND table_schema = '{{ relation.schema }}'
      AND table_catalog = '{{ relation.database }}'
    ORDER BY ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro sqe__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True) %}
    SELECT DISTINCT schema_name
    FROM information_schema.schemata
    WHERE catalog_name = '{{ database }}'
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro sqe__check_schema_exists(information_schema, schema) %}
  {% call statement('check_schema', fetch_result=True) %}
    SELECT COUNT(*) AS num_schemas
    FROM information_schema.schemata
    WHERE catalog_name = '{{ information_schema.database }}'
      AND schema_name = '{{ schema }}'
  {% endcall %}
  {{ return(load_result('check_schema').table) }}
{% endmacro %}

{# ── DDL generation ──────────────────────────────────────────────────── #}

{% macro sqe__create_table_as(temporary, relation, compiled_code) %}
  CREATE OR REPLACE TABLE {{ relation }}
  AS (
    {{ compiled_code }}
  )
{% endmacro %}

{% macro sqe__create_view_as(relation, sql) %}
  CREATE OR REPLACE VIEW {{ relation }}
  AS (
    {{ sql }}
  )
{% endmacro %}

{% macro sqe__drop_relation(relation) %}
  {% if relation.type == 'view' %}
    DROP VIEW IF EXISTS {{ relation }}
  {% else %}
    DROP TABLE IF EXISTS {{ relation }}
  {% endif %}
{% endmacro %}

{% macro sqe__rename_relation(from_relation, to_relation) %}
  ALTER TABLE {{ from_relation }} RENAME TO {{ to_relation }}
{% endmacro %}

{% macro sqe__create_schema(relation) %}
  CREATE SCHEMA IF NOT EXISTS {{ relation.without_identifier() }}
{% endmacro %}

{% macro sqe__drop_schema(relation) %}
  DROP SCHEMA IF EXISTS {{ relation.without_identifier() }}
{% endmacro %}

{% macro sqe__current_timestamp() %}
  now()
{% endmacro %}

{% macro sqe__make_temp_relation(base_relation, suffix) %}
  {% set tmp_identifier = base_relation.identifier ~ suffix %}
  {% do return(base_relation.incorporate(path={"identifier": tmp_identifier})) %}
{% endmacro %}
```

- [ ] **Step 2: Commit**

```bash
git add adapters/dbt-sqe/dbt/include/sqe/macros/adapters.sql
git commit -m "feat(dbt): add metadata and DDL SQL macros"
```

---

### Task B5: Materialization macros

**Files:**
- Create: `adapters/dbt-sqe/dbt/include/sqe/macros/materializations/table.sql`, `view.sql`, `incremental.sql`, `seed.sql`
- Create: `adapters/dbt-sqe/dbt/include/sqe/macros/catalog.sql`

- [ ] **Step 1: Write table.sql**

```sql
{% materialization table, adapter='sqe' %}
  {%- set existing_relation = load_cached_relation(this) -%}

  {% if existing_relation is not none %}
    {{ adapter.drop_relation(existing_relation) }}
  {% endif %}

  {% call statement('main') %}
    {{ sqe__create_table_as(False, this, compiled_code) }}
  {% endcall %}

  {{ return({'relations': [this]}) }}
{% endmaterialization %}
```

- [ ] **Step 2: Write view.sql**

```sql
{% materialization view, adapter='sqe' %}
  {%- set existing_relation = load_cached_relation(this) -%}

  {% if existing_relation is not none and existing_relation.type != 'view' %}
    {{ adapter.drop_relation(existing_relation) }}
  {% endif %}

  {% call statement('main') %}
    {{ sqe__create_view_as(this, compiled_code) }}
  {% endcall %}

  {{ return({'relations': [this]}) }}
{% endmaterialization %}
```

- [ ] **Step 3: Write incremental.sql**

```sql
{% materialization incremental, adapter='sqe' %}
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set strategy = config.get('incremental_strategy', 'append') -%}
  {%- set unique_key = config.get('unique_key') -%}

  {% if existing_relation is none %}
    {# First run — create the table #}
    {% call statement('main') %}
      {{ sqe__create_table_as(False, this, compiled_code) }}
    {% endcall %}
  {% else %}
    {# Incremental run #}
    {% if strategy == 'append' %}
      {% call statement('main') %}
        INSERT INTO {{ this }}
        ({{ compiled_code }})
      {% endcall %}

    {% elif strategy == 'delete+insert' %}
      {% if unique_key is none %}
        {{ exceptions.raise_compiler_error("delete+insert strategy requires a unique_key") }}
      {% endif %}
      {% set tmp_relation = make_temp_relation(this) %}
      {% call statement('create_tmp') %}
        {{ sqe__create_table_as(False, tmp_relation, compiled_code) }}
      {% endcall %}
      {% call statement('delete') %}
        DELETE FROM {{ this }}
        WHERE {{ unique_key }} IN (
          SELECT {{ unique_key }} FROM {{ tmp_relation }}
        )
      {% endcall %}
      {% call statement('insert') %}
        INSERT INTO {{ this }}
        (SELECT * FROM {{ tmp_relation }})
      {% endcall %}
      {{ adapter.drop_relation(tmp_relation) }}

    {% elif strategy == 'merge' %}
      {% if unique_key is none %}
        {{ exceptions.raise_compiler_error("merge strategy requires a unique_key") }}
      {% endif %}
      {% set dest_columns = adapter.get_columns_in_relation(this) %}
      {% set merge_update_columns = dest_columns | map(attribute='name') | list %}
      {% call statement('main') %}
        MERGE INTO {{ this }} AS target
        USING ({{ compiled_code }}) AS source
        ON target.{{ unique_key }} = source.{{ unique_key }}
        WHEN MATCHED THEN UPDATE SET
          {% for col in merge_update_columns %}
            target.{{ col }} = source.{{ col }}{% if not loop.last %},{% endif %}
          {% endfor %}
        WHEN NOT MATCHED THEN INSERT (
          {% for col in merge_update_columns %}
            {{ col }}{% if not loop.last %},{% endif %}
          {% endfor %}
        ) VALUES (
          {% for col in merge_update_columns %}
            source.{{ col }}{% if not loop.last %},{% endif %}
          {% endfor %}
        )
      {% endcall %}
    {% else %}
      {{ exceptions.raise_compiler_error("Invalid incremental strategy: " ~ strategy) }}
    {% endif %}
  {% endif %}

  {{ return({'relations': [this]}) }}
{% endmaterialization %}
```

- [ ] **Step 4: Write seed.sql**

```sql
{% macro sqe__load_csv_rows(model, agate_table) %}
  {% set batch_size = 1000 %}
  {% set cols = agate_table.column_names %}
  {% set col_list = cols | join(', ') %}

  {% for batch_start in range(0, agate_table.rows | length, batch_size) %}
    {% set batch_end = [batch_start + batch_size, agate_table.rows | length] | min %}
    {% call statement('seed_batch_' ~ loop.index) %}
      INSERT INTO {{ this }} ({{ col_list }})
      VALUES
      {% for row_idx in range(batch_start, batch_end) %}
        {% set row = agate_table.rows[row_idx] %}
        ({% for value in row %}
          {% if value is none %}NULL
          {% elif value is number %}{{ value }}
          {% elif value is string %}'{{ value | replace("'", "''") }}'
          {% else %}'{{ value }}'
          {% endif %}
          {% if not loop.last %},{% endif %}
        {% endfor %}){% if row_idx < batch_end - 1 %},{% endif %}
      {% endfor %}
    {% endcall %}
  {% endfor %}
{% endmacro %}

{% macro sqe__create_csv_table(model, agate_table) %}
  {% set column_override = config.get('column_types', {}) %}
  {% set cols %}
    {% for col_name in agate_table.column_names %}
      {% set col_type = column_override.get(col_name, adapter.convert_type(agate_table, loop.index0)) %}
      {{ col_name }} {{ col_type }}{% if not loop.last %},{% endif %}
    {% endfor %}
  {% endset %}

  {% call statement('create_seed_table') %}
    CREATE TABLE IF NOT EXISTS {{ this }} ({{ cols }})
  {% endcall %}
{% endmacro %}
```

- [ ] **Step 5: Write catalog.sql**

```sql
{% macro sqe__get_catalog(information_schema, schemas) %}
  {% call statement('catalog', fetch_result=True) %}
    SELECT
      table_catalog AS "table_database",
      table_schema AS "table_schema",
      table_name AS "table_name",
      table_type AS "table_type",
      NULL AS "table_comment",
      column_name AS "column_name",
      ordinal_position AS "column_index",
      data_type AS "column_type",
      NULL AS "column_comment"
    FROM information_schema.columns
    WHERE table_schema IN (
      {% for schema in schemas %}
        '{{ schema }}'{% if not loop.last %},{% endif %}
      {% endfor %}
    )
    ORDER BY table_schema, table_name, ordinal_position
  {% endcall %}
  {{ return(load_result('catalog').table) }}
{% endmacro %}
```

- [ ] **Step 6: Commit**

```bash
git add adapters/dbt-sqe/dbt/include/sqe/macros/
git commit -m "feat(dbt): add materialization macros (table, view, incremental, seed, catalog)"
```

---

### Task B6: Smoke test with dbt debug

**Files:** None (manual verification)

- [ ] **Step 1: Install adapter**

```bash
cd adapters/dbt-sqe && pip install -e . && cd ../..
```

- [ ] **Step 2: Create a minimal dbt project for testing**

```bash
mkdir -p /tmp/dbt-sqe-test/models
```

Create `/tmp/dbt-sqe-test/dbt_project.yml`:
```yaml
name: sqe_test
version: 1.0.0
config-version: 2
profile: sqe_test
```

Create `/tmp/dbt-sqe-test/profiles.yml`:
```yaml
sqe_test:
  target: dev
  outputs:
    dev:
      type: sqe
      host: localhost
      port: 50051
      user: admin
      password: ""
      catalog: warehouse
      schema: test_dbt
      threads: 1
```

- [ ] **Step 3: Run dbt debug (requires running SQE)**

```bash
cd /tmp/dbt-sqe-test && dbt debug --profiles-dir .
```

Expected: Connection test passes. If SQE is not running, at minimum dbt should recognize the adapter type and show "Could not connect" (not "adapter not found").

- [ ] **Step 4: Run a simple dbt model (requires running SQE stack)**

Create `/tmp/dbt-sqe-test/models/test_model.sql`:
```sql
SELECT 1 AS id, 'hello' AS name
```

```bash
cd /tmp/dbt-sqe-test && dbt run --profiles-dir .
```

Expected: Creates table `test_dbt.test_model` via CTAS.

- [ ] **Step 5: Commit any fixes**

```bash
git add adapters/
git commit -m "fix(dbt): smoke test fixes for dbt debug and basic run"
```

---

## Final: Update docs and nextsteps

### Task C1: Update project tracking

**Files:**
- Modify: `nextsteps.md`, `README.md`

- [ ] **Step 1: Update nextsteps.md**

Add after Step 4d line:
```
Step 7.1: dbt-sqe adapter   ✅ DONE (ADBC Flight SQL, table/view/incremental/seed materializations)
Step 7.3: ALTER TABLE schema ✅ DONE (ADD/DROP/RENAME COLUMN, SET/DROP NOT NULL, type widening)
```

- [ ] **Step 2: Update README.md roadmap**

Change:
```markdown
- [ ] dbt adapter (dbt-sqe via ADBC Flight SQL)
```
to:
```markdown
- [x] dbt adapter (dbt-sqe via ADBC Flight SQL — table, view, incremental, seed)
- [x] ALTER TABLE schema evolution (ADD/DROP/RENAME COLUMN, SET/DROP NOT NULL, type widening)
```

- [ ] **Step 3: Commit**

```bash
git add nextsteps.md README.md
git commit -m "docs: mark dbt-sqe adapter and ALTER TABLE schema evolution as done"
```
