# Embedded and single-node CLI

`sqe-cli` can run the engine in-process, with no server and no network. The
same DataFusion engine, the same Iceberg reader, the same SQL: it just runs
inside the CLI process. This is the fastest way to query a warehouse from a
laptop, a CI job, or a script.

Three storage modes:

- `--memory`: an in-memory DataFusion catalog. Nothing is persisted. Good for
  ad-hoc SQL and testing functions.
- `--warehouse PATH`: a filesystem Iceberg warehouse with **no catalog
  service**. SQE walks the path for `metadata.json` and treats the prefix as
  the catalog. This is the "Iceberg without a catalog" case.
- `--catalog NAME=PATH`: one or more named Iceberg warehouses, addressable as
  `NAME.namespace.table`.

A persistent SQLite catalog (the JDBC backend pointed at a local file) is also
available for a durable single-node catalog.

## In-memory

```bash
sqe-cli --embedded --memory -e "SELECT 1 AS one"
```

```text
+-----+
| one |
+-----+
| 1   |
+-----+
```

## Filesystem warehouse (no catalog service)

Point at a directory; SQE reads the Iceberg metadata directly. No Polaris, no
Glue, no metastore.

```bash
sqe-cli --embedded --warehouse /data/warehouse \
    -e "SELECT COUNT(*) FROM sales.orders"
```

This is the catalog-free Hadoop mode. Writes need atomic rename, which object
stores do not all provide, so this mode is read-oriented; for writes use a real
catalog. The same backend powers the `[catalog.backend] type = "hadoop"` server
config.

## Cloud catalogs embedded

The embedded engine takes the same catalog config as the server, so it can run
directly against Glue or S3 Tables without a coordinator. Credentials come from
the standard AWS provider chain (`AWS_PROFILE`, instance profile, SSO):

```bash
AWS_PROFILE=analytics sqe-cli --embedded \
    --catalog glue=s3://my-bucket/warehouse \
    -e "SELECT * FROM analytics.events LIMIT 10"
```

## How it is tested

- `crates/sqe-cli/tests/cli_smoke.rs`: binary-level flag parsing, exit codes,
  mutually-exclusive flag rejection, and the `--embedded --memory` happy path.
- The catalog spec parser (`NAME=PATH`) is validated for empty names, missing
  separators, and dotted names.

## Notes

- `--memory` and `--warehouse` are mutually exclusive.
- Local-path TVFs (`read_csv` and friends) work in embedded mode; the embedded
  engine enables `allow_local_paths` so a laptop user can read local files.
- Embedded mode authenticates the OS user against the configured catalog's
  credential source, not OIDC; there is no server to pass tokens through.
