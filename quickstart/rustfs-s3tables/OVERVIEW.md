# RustFS S3 Tables (Iceberg REST catalog inside the object store)

## Goal

Connect SQE to RustFS S3 Tables, the Iceberg REST catalog that RustFS 1.0.0-rc.6 serves from the object store itself. There is no separate catalog service: a bucket is turned into a table bucket, RustFS stores Iceberg metadata and data as objects, and answers the Iceberg REST protocol at `/iceberg` on the S3 port. The catalog is authenticated like an S3 call: SigV4 with the S3 keys under the service name `s3`.

This quickstart is about the catalog integration, not authentication. SQE uses its `anonymous` provider, so there is no identity provider to configure.

## Components

| Service | Image | Role |
|---|---|---|
| `rustfs` | `rustfs/rustfs:1.0.0-rc.6` | Object store and Iceberg REST catalog. |
| `bucket-init` | `rustfs/rc` | One-shot: creates the `warehouse` bucket. |
| `s3tables-enable` | `curlimages/curl` | One-shot: `PUT /iceberg/v1/buckets/warehouse` enables S3 Tables on it. |
| `sqe` | built from this repo | The query engine, running in anonymous auth mode. |

## Configuration

### Backend (sqe.toml)

```toml
[[auth.providers]]
type = "anonymous"
user = "anonymous"
roles = ["admin"]

[catalogs.rustfs]
catalog_url = "http://rustfs:9000/iceberg"
warehouse = "warehouse"
require_vended_credentials = false

[catalogs.rustfs.auth]
type = "aws"
signing_name = "s3"
signing_region = "us-east-1"

[storage]
s3_endpoint = "http://rustfs:9000"
s3_region = "us-east-1"
s3_access_key = "root"
s3_secret_key = "s3cr3t"
s3_path_style = true
s3_allow_http = true
```

RustFS does not advertise `rest.sigv4-enabled` in `/v1/config`, so the `auth` block tells SQE to sign client-side. With `signing_name = "s3"` the signer also adds the `x-amz-content-sha256` payload hash header, which RustFS requires on every catalog request. The `[storage]` keys sign the catalog and read and write the data files; RustFS vends no credentials.

### SQL (queries.sql)

```sql
CREATE SCHEMA IF NOT EXISTS rustfs.demo;

DROP TABLE IF EXISTS rustfs.demo.events;
CREATE TABLE rustfs.demo.events (
    id     BIGINT,
    kind   VARCHAR,
    amount DOUBLE
);

INSERT INTO rustfs.demo.events VALUES
    (1, 'click',    1.50),
    (2, 'purchase', 42.00),
    (3, 'click',    0.75),
    (4, 'purchase', 13.25);

SHOW SCHEMAS FROM rustfs;

SELECT kind, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM rustfs.demo.events
GROUP BY kind
ORDER BY total DESC;

DROP TABLE IF EXISTS rustfs.demo.purchases;
CREATE TABLE rustfs.demo.purchases AS
SELECT id, amount FROM rustfs.demo.events WHERE kind = 'purchase';

SELECT COUNT(*) AS purchases, ROUND(SUM(amount), 2) AS total FROM rustfs.demo.purchases;

SHOW TABLES FROM rustfs.demo;
```

The table bucket starts empty, so `CREATE SCHEMA` is part of the script. SQE maps it to an Iceberg `create_namespace` call.

## The test

`run.sh` brings the stack up with `docker compose up --wait` and runs `queries.sql` as the anonymous user over Flight SQL. The `--check` mode asserts the namespace is listed, the purchase total reads back as 55.25, the CTAS table appears in `SHOW TABLES`, and no error is printed. Output is captured to `OUTPUT.md`. Tear down with `./run.sh --down`.

## Output

See `OUTPUT.md` for the captured run.
