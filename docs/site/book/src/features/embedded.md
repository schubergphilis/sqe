# Embedded mode

SQE can run the full query engine in-process, with no server and no network.
`sqe-cli` in embedded mode starts DataFusion, the Iceberg reader, and the same
SQL planner locally, inside the CLI process. This is the fastest path for
querying a warehouse from a laptop, a CI job, or a script.

Four warehouse modes:

- **In-memory** (`--memory`): a transient DataFusion catalog. Nothing is
  persisted. Good for ad-hoc SQL and testing SQL functions.
- **Filesystem warehouse** (`--warehouse PATH`): an Iceberg warehouse on local
  disk or object storage with **no catalog service**. SQE walks the path for
  `metadata.json` files and treats the prefix as the catalog. The "Iceberg
  without a catalog" case.
- **Persistent SQLite catalog** (`--catalog-backend sqlite`): a durable
  single-node catalog backed by a local SQLite file. Survives restarts.
- **Cloud catalogs embedded**: Glue and S3 Tables can be attached directly,
  with no coordinator, using the standard AWS credential chain.

See the quickstarts:

- [Embedded: query local and remote files](../quickstart/embedded-files.md)
- [Embedded: persistent local catalog (SQLite)](../quickstart/embedded-sqlite-catalog.md)
- [Embedded: attach multiple catalogs](../quickstart/attach-catalogs.md)

---

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

The embedded engine can attach a Glue or S3 Tables catalog directly, with no
coordinator. Pass `--catalog-backend` plus the cloud warehouse; credentials come
from the standard AWS provider chain (`AWS_PROFILE`, instance profile, SSO).
These catalogs attach read-only (query, not write); use the server for writes.
Requires the `aws` cargo feature (default-on).

```bash
# AWS Glue Data Catalog (warehouse is an s3:// prefix)
AWS_PROFILE=analytics sqe-cli --embedded \
    --catalog-backend glue \
    --catalog-warehouse s3://my-bucket/warehouse --region eu-central-1 \
    -e "SELECT * FROM glue.analytics.events LIMIT 10"

# AWS S3 Tables (warehouse is the table-bucket ARN)
AWS_PROFILE=analytics sqe-cli --embedded \
    --catalog-backend s3tables \
    --catalog-warehouse arn:aws:s3tables:eu-central-1:ACCOUNT:bucket/NAME \
    --region eu-central-1 \
    -e "SHOW SCHEMAS"
```

The catalog mounts under the backend name by default (`glue.` / `s3tables.`);
override with `--catalog-name`.

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
