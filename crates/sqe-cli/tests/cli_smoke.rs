//! Binary-level smoke tests for the `sqe-cli` executable.
//!
//! These exercise the actual compiled binary via `assert_cmd`. The library
//! tests in `src/` cover module behaviour; this file pins flag parsing,
//! exit codes, stderr formatting, mutually-exclusive flag rejection, and
//! the happy path of `--embedded --memory -e <sql>`.

use assert_cmd::Command;
use predicates::prelude::*;

fn bin() -> Command {
    Command::cargo_bin("sqe-cli").expect("sqe-cli binary builds")
}

#[test]
fn help_flag_exits_zero_and_prints_usage() {
    bin()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("sqe-cli"))
        .stdout(predicate::str::contains("Usage:"));
}

#[test]
fn version_flag_exits_zero_and_prints_version() {
    bin()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("sqe-cli"));
}

#[test]
fn unknown_flag_exits_two() {
    // Clap returns exit code 2 for argument parsing errors.
    bin()
        .arg("--definitely-not-a-flag")
        .assert()
        .code(2)
        .stderr(
            predicate::str::contains("unexpected argument").or(predicate::str::contains("error:")),
        );
}

#[test]
fn invalid_protocol_value_exits_two() {
    bin()
        .args(["--protocol", "carrier-pigeon"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("invalid value").or(predicate::str::contains("error:")));
}

#[test]
fn catalog_spec_missing_equals_rejected() {
    // No `=` separator: `parse_catalog_spec` returns Err, clap surfaces it.
    bin()
        .args(["--embedded", "--catalog", "noequals"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("expected NAME=PATH"));
}

#[test]
fn catalog_spec_empty_name_rejected() {
    bin()
        .args(["--embedded", "--catalog", "=/tmp/x"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("catalog name is empty"));
}

#[test]
fn catalog_spec_with_dot_in_name_rejected() {
    bin()
        .args(["--embedded", "--catalog", "prod.eu=/tmp/x"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("cannot contain"));
}

#[test]
fn memory_and_warehouse_are_mutually_exclusive() {
    // Clap enforces `conflicts_with_all` and returns exit code 2.
    bin()
        .args(["--embedded", "--memory", "--warehouse", "/tmp/sqe-wh"])
        .assert()
        .code(2)
        .stderr(
            predicate::str::contains("cannot be used with").or(predicate::str::contains("error:")),
        );
}

#[test]
fn embedded_memory_runs_select_one() {
    // Happy path: in-memory embedded engine runs `SELECT 1` end-to-end and
    // exits zero. The banner goes to stderr; the table goes to stdout.
    bin()
        .args(["--embedded", "--memory", "-e", "SELECT 1 AS one"])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .success()
        .stderr(predicate::str::contains("embedded engine"))
        .stdout(predicate::str::contains("one").and(predicate::str::contains("1")));
}

#[test]
fn embedded_memory_syntax_error_exits_nonzero() {
    // Bad SQL must propagate a non-zero exit from main.
    bin()
        .args(["--embedded", "--memory", "-e", "SELEC"])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .failure();
}

#[test]
fn embedded_warehouse_delete_update_merge() {
    // DELETE, UPDATE and MERGE run through the server's write handlers and
    // commit to the warehouse: a second process reads the result back.
    let wh = tempfile::tempdir().expect("tempdir");
    let wh_path = wh.path().to_str().expect("utf-8 path");
    let script = wh.path().join("dml.sql");
    std::fs::write(
        &script,
        "CREATE SCHEMA iceberg.s;\n\
         CREATE TABLE iceberg.s.t (id INT, v VARCHAR);\n\
         INSERT INTO iceberg.s.t VALUES (1, 'a'), (2, 'b'), (3, 'c');\n\
         DELETE FROM iceberg.s.t WHERE id = 1;\n\
         UPDATE iceberg.s.t SET v = 'z' WHERE id = 2;\n\
         MERGE INTO iceberg.s.t t USING (SELECT * FROM (VALUES (3, 'm'), (4, 'n')) AS s(id, v)) s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);\n",
    )
    .expect("write script");
    bin()
        .args(["--embedded", "--warehouse", wh_path, "--file"])
        .arg(&script)
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let out = bin()
        .args([
            "--embedded",
            "--warehouse",
            wh_path,
            "-e",
            "SELECT string_agg(CAST(id AS VARCHAR) || v, ',' ORDER BY id) AS rows FROM iceberg.s.t",
        ])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(stdout.contains("2z,3m,4n"), "unexpected rows: {stdout}");
}

/// The newest `*.metadata.json` of `iceberg.<ns>.<table>` under `wh`.
fn latest_metadata(wh: &std::path::Path, ns: &str, table: &str) -> serde_json::Value {
    let dir = wh.join("iceberg").join(ns).join(table).join("metadata");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.to_string_lossy().ends_with(".metadata.json"))
        .collect();
    files.sort();
    let last = files.last().expect("a metadata file");
    serde_json::from_slice(&std::fs::read(last).expect("read metadata")).expect("metadata json")
}

#[test]
fn embedded_warehouse_create_table_keeps_partitioning_and_properties() {
    // CREATE TABLE runs through the server's DDL handler: the partition
    // transform, the table property, nested columns and the format version
    // all land in the table metadata, and the table is usable at once.
    let wh = tempfile::tempdir().expect("tempdir");
    let wh_path = wh.path().to_str().expect("utf-8 path");
    let script = wh.path().join("ddl.sql");
    std::fs::write(
        &script,
        "CREATE SCHEMA iceberg.s;\n\
         CREATE TABLE iceberg.s.ev (id INT, ts TIMESTAMP, info STRUCT<a: INT, b: VARCHAR>, \
         tags ARRAY<VARCHAR>) PARTITIONED BY (day(ts)) \
         TBLPROPERTIES ('write.delete.mode' = 'merge-on-read');\n\
         INSERT INTO iceberg.s.ev SELECT 1, TIMESTAMP '2026-01-01 10:00:00', \
         named_struct('a', 7, 'b', 'x'), ['p', 'q'];\n\
         CREATE TABLE iceberg.s.v2 (id INT) WITH ('format-version' = '2');\n\
         CREATE TABLE IF NOT EXISTS iceberg.s.v2 (id INT);\n",
    )
    .expect("write script");
    bin()
        .args(["--embedded", "--warehouse", wh_path, "--file"])
        .arg(&script)
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let out = bin()
        .args([
            "--embedded",
            "--warehouse",
            wh_path,
            "-e",
            "SELECT CAST(id AS VARCHAR) || ':' || CAST(info['a'] AS VARCHAR) || ':' || tags[2] AS r \
             FROM iceberg.s.ev",
        ])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(stdout.contains("1:7:q"), "unexpected rows: {stdout}");

    let ev = latest_metadata(wh.path(), "s", "ev");
    assert_eq!(ev["format-version"], 3, "{ev}");
    assert_eq!(
        ev["properties"]["write.delete.mode"], "merge-on-read",
        "{ev}"
    );
    let spec = &ev["partition-specs"][0]["fields"][0];
    assert_eq!(spec["transform"], "day", "{ev}");
    // The INSERT computed the partition value (vendored `PartitionExpr`).
    let day = wh.path().join("iceberg/s/ev/data/ts_day=2026-01-01");
    assert!(day.is_dir(), "no partition directory {}", day.display());
    // The local catalog refuses reserved properties; the option still picks
    // the format version.
    assert_eq!(latest_metadata(wh.path(), "s", "v2")["format-version"], 2);
}

#[test]
fn embedded_warehouse_insert_refuses_null_in_not_null_column() {
    // A NULL from a query or an omitted column used to be stored as 0.
    let wh = tempfile::tempdir().expect("tempdir");
    let wh_path = wh.path().to_str().expect("utf-8 path");
    let script = wh.path().join("setup.sql");
    std::fs::write(
        &script,
        "CREATE SCHEMA iceberg.s;\n\
         CREATE TABLE iceberg.s.t (id INT NOT NULL, v VARCHAR);\n\
         CREATE TABLE iceberg.s.p (id INT NOT NULL, ts TIMESTAMP) PARTITIONED BY (day(ts));\n\
         INSERT INTO iceberg.s.t VALUES (1, 'a');\n\
         INSERT INTO iceberg.s.p SELECT id, TIMESTAMP '2026-01-01 10:00:00' FROM iceberg.s.t;\n",
    )
    .expect("write script");
    bin()
        .args(["--embedded", "--warehouse", wh_path, "--file"])
        .arg(&script)
        .arg("--stop-on-error")
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    for sql in [
        "INSERT INTO iceberg.s.t SELECT CAST(NULL AS INT), 'b'",
        "INSERT INTO iceberg.s.t (v) VALUES ('c')",
        "INSERT INTO iceberg.s.p SELECT CAST(NULL AS INT), TIMESTAMP '2026-01-02 10:00:00'",
    ] {
        bin()
            .args(["--embedded", "--warehouse", wh_path, "-e", sql])
            .timeout(std::time::Duration::from_secs(60))
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "NULL value not allowed for NOT NULL column: id",
            ));
    }

    let out = bin()
        .args([
            "--embedded",
            "--warehouse",
            wh_path,
            "-e",
            "SELECT (SELECT string_agg(CAST(id AS VARCHAR), ',') FROM iceberg.s.t) || '|' || \
             (SELECT string_agg(CAST(id AS VARCHAR), ',') FROM iceberg.s.p) AS r",
        ])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(stdout.contains("1|1"), "unexpected rows: {stdout}");
}

#[test]
fn embedded_warehouse_insert_values_and_filter_on_array_column() {
    // INSERT ... VALUES typed the rows against a list type without the
    // element's field id and failed; a predicate on a list column failed to
    // bind in the Iceberg scan.
    let wh = tempfile::tempdir().expect("tempdir");
    let wh_path = wh.path().to_str().expect("utf-8 path");
    let script = wh.path().join("arrays.sql");
    std::fs::write(
        &script,
        "CREATE SCHEMA iceberg.s;\n\
         CREATE TABLE iceberg.s.t (id INT, tags ARRAY<VARCHAR>, v VARCHAR);\n\
         INSERT INTO iceberg.s.t VALUES (1, ARRAY['a', 'b'], 'x');\n\
         INSERT INTO iceberg.s.t (id, tags) VALUES (5, ARRAY['z']);\n\
         INSERT INTO iceberg.s.t VALUES (2, NULL, 'y'), (3, ARRAY[], 'w');\n\
         CREATE TABLE iceberg.s.c AS SELECT id, tags FROM iceberg.s.t WHERE tags IS NOT NULL;\n\
         INSERT INTO iceberg.s.c SELECT id + 10, tags FROM iceberg.s.t WHERE tags IS NOT NULL;\n",
    )
    .expect("write script");
    bin()
        .args(["--embedded", "--warehouse", wh_path, "--file"])
        .arg(&script)
        .arg("--stop-on-error")
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let out = bin()
        .args([
            "--embedded",
            "--warehouse",
            wh_path,
            "-e",
            "SELECT string_agg(CAST(id AS VARCHAR) || '=' || array_to_string(tags, '+'), ',' \
             ORDER BY id) AS r FROM iceberg.s.c",
        ])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(
        stdout.contains("1=a+b,3=,5=z,11=a+b,13=,15=z"),
        "unexpected rows: {stdout}"
    );
}

#[test]
fn embedded_warehouse_alter_table() {
    // ALTER TABLE runs on the server's catalog DDL: columns, nested columns,
    // properties, partition evolution, identifier fields, tags and a rename
    // all reach the table metadata, and the table stays readable.
    let wh = tempfile::tempdir().expect("tempdir");
    let wh_path = wh.path().to_str().expect("utf-8 path");
    let script = wh.path().join("alter.sql");
    std::fs::write(
        &script,
        "CREATE SCHEMA iceberg.s;\n\
         CREATE TABLE iceberg.s.t (id INT NOT NULL, v VARCHAR, info STRUCT<a: INT, b: INT>);\n\
         INSERT INTO iceberg.s.t SELECT 1, 'a', named_struct('a', 1, 'b', 2);\n\
         ALTER TABLE iceberg.s.t ADD COLUMN n BIGINT;\n\
         ALTER TABLE iceberg.s.t RENAME COLUMN v TO label;\n\
         ALTER TABLE iceberg.s.t ALTER COLUMN info.a SET DATA TYPE BIGINT;\n\
         ALTER TABLE iceberg.s.t DROP COLUMN info.b;\n\
         ALTER TABLE iceberg.s.t SET TBLPROPERTIES ('write.delete.mode' = 'merge-on-read');\n\
         ALTER TABLE iceberg.s.t ADD PARTITION FIELD bucket(4, id);\n\
         ALTER TABLE iceberg.s.t SET IDENTIFIER FIELDS id;\n\
         ALTER TABLE iceberg.s.t CREATE TAG v1;\n\
         ALTER TABLE iceberg.s.t RENAME TO t2;\n\
         INSERT INTO iceberg.s.t2 SELECT 2, 'b', named_struct('a', CAST(5 AS BIGINT)), 7;\n",
    )
    .expect("write script");
    bin()
        .args(["--embedded", "--warehouse", wh_path, "--file"])
        .arg(&script)
        .arg("--stop-on-error")
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let out = bin()
        .args([
            "--embedded",
            "--warehouse",
            wh_path,
            "-e",
            "SELECT string_agg(CAST(id AS VARCHAR) || ':' || label || ':' || \
             CAST(info['a'] AS VARCHAR) || ':' || COALESCE(CAST(n AS VARCHAR), '-'), ',' \
             ORDER BY id) AS r FROM iceberg.s.t2",
        ])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(
        stdout.contains("1:a:1:-,2:b:5:7"),
        "unexpected rows: {stdout}"
    );

    // The rename moves the catalog entry, not the files.
    let meta = latest_metadata(wh.path(), "s", "t");
    assert_eq!(
        meta["properties"]["write.delete.mode"], "merge-on-read",
        "{meta}"
    );
    assert!(meta["refs"]["v1"]["type"] == "tag", "{meta}");
    // The `schemas` array is not ordered by id; pick the current one.
    let schema = meta["schemas"]
        .as_array()
        .and_then(|s| {
            s.iter()
                .find(|s| s["schema-id"] == meta["current-schema-id"])
        })
        .expect("the current schema");
    let names: Vec<&str> = schema["fields"]
        .as_array()
        .expect("fields")
        .iter()
        .filter_map(|f| f["name"].as_str())
        .collect();
    assert_eq!(names, ["id", "label", "info", "n"], "{schema}");
    assert_eq!(
        schema["identifier-field-ids"],
        serde_json::json!([1]),
        "{schema}"
    );
    let info = &schema["fields"][2]["type"]["fields"];
    assert_eq!(info.as_array().map(Vec::len), Some(1), "{schema}");
    assert_eq!(info[0]["type"], "long", "{schema}");
    let spec = meta["partition-specs"]
        .as_array()
        .and_then(|s| s.iter().find(|s| s["spec-id"] == meta["default-spec-id"]))
        .expect("the default spec");
    assert_eq!(spec["fields"][0]["transform"], "bucket[4]", "{meta}");
}

#[test]
fn embedded_warehouse_ctas_refuses_partitioning_it_would_drop() {
    let wh = tempfile::tempdir().expect("tempdir");
    let wh_path = wh.path().to_str().expect("utf-8 path");
    bin()
        .args([
            "--embedded",
            "--warehouse",
            wh_path,
            "-e",
            "CREATE SCHEMA iceberg.s",
        ])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .success();
    bin()
        .args([
            "--embedded",
            "--warehouse",
            wh_path,
            "-e",
            "CREATE TABLE iceberg.s.t PARTITIONED BY (id) AS SELECT 1 AS id",
        ])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "CREATE TABLE ... AS with PARTITIONED BY",
        ));
}

#[test]
fn embedded_warehouse_partitioned_by_bare_columns() {
    // `PARTITIONED BY (region, id)` parsed as a Hive partition column list,
    // which the DDL handler never reads: the table came out unpartitioned
    // with no error.
    let wh = tempfile::tempdir().expect("tempdir");
    let wh_path = wh.path().to_str().expect("utf-8 path");
    let script = wh.path().join("parts.sql");
    std::fs::write(
        &script,
        "CREATE SCHEMA iceberg.s;\n\
         CREATE TABLE iceberg.s.t (id INT, region VARCHAR) PARTITIONED BY (region, id);\n\
         INSERT INTO iceberg.s.t VALUES (1, 'eu'), (2, 'us');\n",
    )
    .expect("write script");
    bin()
        .args(["--embedded", "--warehouse", wh_path, "--file"])
        .arg(&script)
        .arg("--stop-on-error")
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let t = latest_metadata(wh.path(), "s", "t");
    let fields = &t["partition-specs"][0]["fields"];
    assert_eq!(fields[0]["name"], "region", "{t}");
    assert_eq!(fields[0]["transform"], "identity", "{t}");
    assert_eq!(fields[1]["name"], "id", "{t}");
    let eu = wh.path().join("iceberg/s/t/data/region=eu/id=1");
    assert!(eu.is_dir(), "no partition directory {}", eu.display());
}

#[test]
fn embedded_warehouse_metadata_tables() {
    // `cat.ns."t$snapshots"` and friends read the named catalog's metadata;
    // the TVFs they rewrite to were never registered in embedded mode.
    let wh = tempfile::tempdir().expect("tempdir");
    let wh_path = wh.path().to_str().expect("utf-8 path");
    let script = wh.path().join("meta.sql");
    std::fs::write(
        &script,
        "CREATE SCHEMA iceberg.s;\n\
         CREATE TABLE iceberg.s.t (id INT, v VARCHAR);\n\
         INSERT INTO iceberg.s.t VALUES (1, 'a');\n\
         INSERT INTO iceberg.s.t VALUES (2, 'b');\n",
    )
    .expect("write script");
    bin()
        .args(["--embedded", "--warehouse", wh_path, "--file"])
        .arg(&script)
        .arg("--stop-on-error")
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let out = bin()
        .args([
            "--embedded",
            "--warehouse",
            wh_path,
            "-e",
            "SELECT 'n=' || CAST((SELECT count(*) FROM iceberg.s.\"t$snapshots\") AS VARCHAR) \
             || '/' || CAST((SELECT count(*) FROM iceberg.s.\"t$history\") AS VARCHAR) \
             || '/' || CAST((SELECT count(*) FROM iceberg.s.\"t$files\") AS VARCHAR) \
             || '/' || CAST((SELECT count(*) FROM iceberg.s.\"t$refs\") AS VARCHAR) AS r",
        ])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(stdout.contains("n=2/2/2/1"), "unexpected counts: {stdout}");

    bin()
        .args([
            "--embedded",
            "--warehouse",
            wh_path,
            "-e",
            "SELECT * FROM nope.s.\"t$snapshots\"",
        ])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .failure()
        .stderr(predicate::str::contains("'nope' is not an Iceberg catalog"));
}

/// The DDL `SHOW CREATE TABLE` prints for `table`, read from the JSON output.
fn show_create(wh_path: &str, table: &str) -> String {
    let out = bin()
        .args(["--embedded", "--warehouse", wh_path, "-f", "json", "-e"])
        .arg(format!("SHOW CREATE TABLE {table}"))
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    let row: serde_json::Value = serde_json::from_str(stdout.trim()).expect("json row");
    row["Create Table"]
        .as_str()
        .expect("Create Table")
        .to_string()
}

#[test]
fn embedded_warehouse_show_create_table_round_trips() {
    // SHOW CREATE TABLE on an Iceberg table prints the columns, partition
    // spec and properties as the server does, and the printed DDL creates
    // the same table again.
    let wh = tempfile::tempdir().expect("tempdir");
    let wh_path = wh.path().to_str().expect("utf-8 path");
    let script = wh.path().join("ddl.sql");
    std::fs::write(
        &script,
        "CREATE SCHEMA iceberg.s;\n\
         CREATE TABLE iceberg.s.t (id INT NOT NULL, \"Name\" VARCHAR, ts TIMESTAMP(6), \
         amt DECIMAL(10,2)) PARTITIONED BY (day(ts), bucket(4, id)) \
         TBLPROPERTIES ('write.format.default' = 'parquet');\n",
    )
    .expect("write script");
    bin()
        .args([
            "--embedded",
            "--warehouse",
            wh_path,
            "--stop-on-error",
            "--file",
        ])
        .arg(&script)
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let ddl = show_create(wh_path, "iceberg.s.t");
    assert_eq!(
        ddl,
        "CREATE TABLE iceberg.s.t (\n   id integer NOT NULL,\n   \"Name\" varchar,\n   \
         ts timestamp(6),\n   amt decimal(10,2)\n)\n\
         PARTITIONED BY (day(ts), bucket(4, id))\n\
         TBLPROPERTIES (\n   'format-version' = '3',\n   'write.format.default' = 'parquet'\n)"
    );

    let copy = ddl.replacen("iceberg.s.t ", "iceberg.s.t2 ", 1);
    bin()
        .args(["--embedded", "--warehouse", wh_path, "-e"])
        .arg(&copy)
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .success();
    assert_eq!(show_create(wh_path, "iceberg.s.t2"), copy);

    bin()
        .args(["--embedded", "--warehouse", wh_path, "-e"])
        .arg("SHOW CREATE TABLE iceberg.s.nope")
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .failure()
        .stderr(predicate::str::contains("table not found: iceberg.s.nope"));
}

#[test]
fn embedded_warehouse_time_travel() {
    // `FOR VERSION AS OF` / `FOR SYSTEM_TIME AS OF` failed to parse
    // ("Expected: one of UPDATE or SHARE") in embedded mode.
    let wh = tempfile::tempdir().expect("tempdir");
    let wh_path = wh.path().to_str().expect("utf-8 path");
    let script = wh.path().join("tt.sql");
    std::fs::write(
        &script,
        "CREATE SCHEMA iceberg.s;\n\
         CREATE TABLE iceberg.s.t (id INT);\n\
         INSERT INTO iceberg.s.t VALUES (1);\n\
         ALTER TABLE iceberg.s.t CREATE TAG first;\n\
         INSERT INTO iceberg.s.t VALUES (2);\n",
    )
    .expect("write script");
    bin()
        .args(["--embedded", "--warehouse", wh_path, "--file"])
        .arg(&script)
        .arg("--stop-on-error")
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let run = |sql: &str| {
        bin()
            .args([
                "--embedded",
                "--warehouse",
                wh_path,
                "-f",
                "json",
                "-e",
                sql,
            ])
            .timeout(std::time::Duration::from_secs(60))
            .assert()
    };
    let stdout =
        |sql: &str| String::from_utf8_lossy(&run(sql).success().get_output().stdout).to_string();
    let first: serde_json::Value = serde_json::from_str(
        stdout(
            "SELECT CAST(snapshot_id AS VARCHAR) AS id FROM iceberg.s.\"t$snapshots\" \
             ORDER BY committed_at LIMIT 1",
        )
        .lines()
        .find(|l| l.starts_with('{'))
        .expect("snapshot row"),
    )
    .expect("json row");
    let first = first["id"].as_str().expect("snapshot id").to_string();

    let count = |sql: &str| {
        stdout(&format!(
            "SELECT 'n=' || CAST(count(*) AS VARCHAR) AS r FROM {sql}"
        ))
    };
    assert!(count(&format!("iceberg.s.t FOR VERSION AS OF {first}")).contains("n=1"));
    assert!(count("iceberg.s.t FOR VERSION AS OF 'first'").contains("n=1"));
    assert!(count("iceberg.s.t FOR SYSTEM_TIME AS OF '2999-01-01'").contains("n=2"));
    // The pinned provider is gone after the statement: a plain read is current.
    assert!(count("iceberg.s.t").contains("n=2"));

    run("SELECT * FROM iceberg.s.t FOR SYSTEM_TIME AS OF '2000-01-01'")
        .failure()
        .stderr(predicate::str::contains("No Iceberg snapshot found"));
    run("SELECT * FROM iceberg.s.t FOR VERSION AS OF 42")
        .failure()
        .stderr(predicate::str::contains(
            "snapshot id not found on iceberg.s.t",
        ));
    run("SELECT * FROM default.default.m FOR VERSION AS OF 1")
        .failure()
        .stderr(predicate::str::contains(
            "time travel reads Iceberg tables only",
        ));
}

#[test]
fn embedded_warehouse_write_branch() {
    // SET WRITE_BRANCH was cluster-only (#875). `main` and `dev` diverge
    // first, so a write that read or committed `main` shows in the rows.
    let wh = tempfile::tempdir().expect("tempdir");
    let wh_path = wh.path().to_str().expect("utf-8 path");
    let script = wh.path().join("wb.sql");
    std::fs::write(
        &script,
        "CREATE SCHEMA iceberg.s;\n\
         CREATE TABLE iceberg.s.t (id INT, v VARCHAR);\n\
         INSERT INTO iceberg.s.t VALUES (1, 'a'), (2, 'b'), (3, 'c');\n\
         ALTER TABLE iceberg.s.t CREATE BRANCH dev;\n\
         INSERT INTO iceberg.s.t VALUES (99, 'late');\n\
         SET WRITE_BRANCH = 'dev';\n\
         INSERT INTO iceberg.s.t VALUES (4, 'd');\n\
         DELETE FROM iceberg.s.t WHERE id = 2;\n\
         UPDATE iceberg.s.t SET v = 'z' WHERE id = 1;\n\
         MERGE INTO iceberg.s.t t USING (SELECT * FROM (VALUES (3, 'm'), (99, 'new')) AS s(id, v)) s \
         ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v \
         WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);\n\
         RESET WRITE_BRANCH;\n\
         INSERT INTO iceberg.s.t VALUES (5, 'main');\n",
    )
    .expect("write script");
    bin()
        .args(["--embedded", "--warehouse", wh_path, "--file"])
        .arg(&script)
        .arg("--stop-on-error")
        .timeout(std::time::Duration::from_secs(120))
        .assert()
        .success();

    let rows = |from: &str| {
        let out = bin()
            .args([
                "--embedded",
                "--warehouse",
                wh_path,
                "-e",
                &format!(
                    "SELECT 'rows=' || string_agg(CAST(id AS VARCHAR) || v, ',' ORDER BY id) \
                     AS r FROM {from}"
                ),
            ])
            .timeout(std::time::Duration::from_secs(60))
            .assert()
            .success();
        String::from_utf8_lossy(&out.get_output().stdout).to_string()
    };
    // id 99 was on main only, so the MERGE inserts it on dev.
    let dev = rows("iceberg.s.t FOR VERSION AS OF 'dev'");
    assert!(dev.contains("rows=1z,3m,4d,99new"), "dev: {dev}");
    let main = rows("iceberg.s.t");
    assert!(main.contains("rows=1a,2b,3c,5main,99late"), "main: {main}");

    // CTAS on a branch is refused before the table is created, and a
    // branch the table lacks is refused instead of created.
    let refused = |sql: &str, needle: &str| {
        let script = wh.path().join("refused.sql");
        std::fs::write(&script, format!("SET WRITE_BRANCH = 'dev';\n{sql};\n")).expect("write");
        bin()
            .args(["--embedded", "--warehouse", wh_path, "--file"])
            .arg(&script)
            .arg("--stop-on-error")
            .timeout(std::time::Duration::from_secs(60))
            .assert()
            .failure()
            .stderr(predicate::str::contains(needle));
    };
    refused(
        "CREATE TABLE iceberg.s.c AS SELECT 1 AS id",
        "has no branch to write to",
    );
    refused(
        "CREATE TABLE iceberg.s.u (id INT);\nINSERT INTO iceberg.s.u VALUES (1)",
        "CREATE BRANCH dev",
    );
    bin()
        .args([
            "--embedded",
            "--warehouse",
            wh_path,
            "-e",
            "SELECT * FROM iceberg.s.c",
        ])
        .timeout(std::time::Duration::from_secs(60))
        .assert()
        .failure();
}

#[test]
fn embedded_warehouse_refuses_create_view() {
    // The SQLite catalog stores no views. CREATE VIEW used to "succeed" by
    // registering an empty Iceberg table under the view's name.
    let wh = tempfile::tempdir().expect("tempdir");
    let wh_path = wh.path().to_str().expect("utf-8 path");
    let run = |sql: &str| {
        bin()
            .args(["--embedded", "--warehouse", wh_path, "-e", sql])
            .timeout(std::time::Duration::from_secs(60))
            .assert()
    };
    run("CREATE SCHEMA iceberg.s").success();
    run("CREATE VIEW iceberg.s.v AS SELECT 1 AS x")
        .failure()
        .stderr(predicate::str::contains("stores no Iceberg views"));
    run("SHOW TABLES IN iceberg.s")
        .success()
        .stdout(predicate::str::contains(" v ").not());
    run("DROP VIEW IF EXISTS iceberg.s.v").success();
    run("DROP VIEW iceberg.s.v")
        .failure()
        .stderr(predicate::str::contains("stores no views"));
    // A session view is still a DataFusion view.
    run("CREATE VIEW sv AS SELECT 1 AS x").success();
}
