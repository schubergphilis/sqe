---
slug: rustfs-s3tables
title: "RustFS S3 Tables (Iceberg REST catalog inside the object store)"
description: "Run SQE against RustFS S3 Tables, the Iceberg REST catalog built into RustFS. No separate catalog service: the table bucket is the warehouse, and SQE signs the catalog calls with the S3 keys (SigV4, service name s3). Auth-less stack: SQE runs in anonymous dev mode."
---

# RustFS S3 Tables (Iceberg REST catalog inside the object store)

[RustFS](https://rustfs.com/) 1.0.0-rc.6 ships **S3 Tables**: an Iceberg REST
catalog served by the object store itself, at `/iceberg` on the S3 port. There
is no Polaris, Nessie or Glue in this stack. A bucket becomes a *table bucket*
with one management call, and from then on RustFS stores the Iceberg metadata
and data as S3 objects and answers the Iceberg REST protocol for them.

The catalog is authenticated the way every other RustFS request is: AWS SigV4
with the S3 keys, under the service name `s3`. RustFS does not advertise
`rest.sigv4-enabled` in its `/v1/config` answer, so SQE is told to sign
client-side with a three-line `[catalogs.rustfs.auth]` block. The same
`[storage]` keys sign the catalog and read and write the data files.

This quickstart is about the **catalog**, not auth. SQE uses its `anonymous`
provider, so there is no Keycloak and no token to manage. For the auth story
(real identities, RBAC, token passthrough) see the
[polaris-keycloak-client-id](../polaris-keycloak-client-id/) and
[polaris-keycloak-user-token](../polaris-keycloak-user-token/) quickstarts.
The stack mirrors the community demo at
[fdass/iceberg-rustfs](https://github.com/fdass/iceberg-rustfs), which drives
the same catalog from PyIceberg and Trino.

## What you get

`docker compose up` starts:

| Service | Image | Role |
|---|---|---|
| `rustfs` | `rustfs/rustfs:1.0.0-rc.6` | S3-compatible object store **and** the Iceberg REST catalog (`/iceberg`). |
| `bucket-init` | `rustfs/rc` | One-shot: creates the `warehouse` bucket, then exits. |
| `s3tables-enable` | `curlimages/curl` | One-shot: `PUT /iceberg/v1/buckets/warehouse` turns it into a table bucket. |
| `sqe` | built from this repo | The query engine, in anonymous auth mode. |

## Prerequisites

- Docker (with Compose v2).

## Run it

```bash
cd quickstart/rustfs-s3tables
cp .env.example .env
./run.sh             # up -> queries -> capture output
./run.sh --down      # tear everything down
./run.sh --check     # up -> queries -> assert key invariants
```

`run.sh` brings the stack up and runs [`queries.sql`](./queries.sql) as the
anonymous user, capturing the result to [`OUTPUT.md`](./OUTPUT.md). Tear down
with `./run.sh --down`.

By hand once it is up:

```bash
docker compose exec -e SQE_PASSWORD=anonymous sqe \
  sqe-cli --port 50051 --user anonymous -e "SHOW SCHEMAS FROM rustfs"
```

Endpoints: Flight SQL `grpc://localhost:60061`, RustFS S3 API and catalog
`http://localhost:19100`, RustFS console `http://localhost:19101`.

## How it works

SQE has one Iceberg REST client, and RustFS S3 Tables speaks the Iceberg REST
protocol, so SQE talks to it through the code path it uses for Polaris. SQE
issues `GET /v1/config?warehouse=warehouse` against the `/iceberg` mount, then
lists, creates and commits over the same protocol. Every request, including
SQE's own HEAD authorization check and the commit POSTs, carries a SigV4
signature with the `x-amz-content-sha256` payload hash, which RustFS requires
because it validates the catalog like an S3 call.

RustFS owns the table metadata and hands table locations back
(`s3://warehouse/tables/<uuid>/...`). SQE reads and writes the data files
directly on RustFS with the `[storage]` keys; RustFS vends no credentials, so
`require_vended_credentials` is `false`. The query path is: client -> SQE
(anonymous) -> RustFS for metadata -> RustFS for data.

## Configuration explained

### The catalog (the whole point)

```toml
[catalogs.rustfs]
catalog_url = "http://rustfs:9000/iceberg"   # RustFS S3 Tables REST mount
warehouse = "warehouse"                       # the table bucket
require_vended_credentials = false

[catalogs.rustfs.auth]
type = "aws"
signing_name = "s3"
signing_region = "us-east-1"
```

`catalog_url` is the Iceberg REST base URL. Against Polaris it is
`.../api/catalog`; against RustFS it is `.../iceberg` on the S3 port. The
`auth` block is what makes RustFS different from AWS Glue or Amazon S3 Tables:
those advertise SigV4 in `/v1/config` and SQE switches on its own, while RustFS
says nothing and rejects unsigned requests with 403 "Signature is required".
`signing_name = "s3"` engages the signer, and because the service is `s3` the
signer also adds the payload hash header that RustFS demands.

`signing_region` may be left out; it then defaults to `[storage] s3_region`.
The signature uses the `[storage]` access keys. Without them SQE falls back to
the AWS SDK credential chain (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`).

### Auth (anonymous, dev only)

```toml
[[auth.providers]]
type = "anonymous"
user = "anonymous"
roles = ["admin"]
```

Every connection is accepted as a single `anonymous` identity. SQE logs a
security warning on startup because this disables authentication entirely. Do
not use it in production.

### Storage

```toml
[storage]
s3_endpoint = "http://rustfs:9000"
s3_region = "us-east-1"
s3_access_key = "root"
s3_secret_key = "s3cr3t"
s3_path_style = true
s3_allow_http = true
```

One endpoint and one key pair serve both planes. Change the keys in `.env`
and here together.

## What RustFS S3 Tables does and does not do

- Namespaces, tables, commits, CTAS, `DROP TABLE` and view listing work over
  the standard protocol.
- `/v1/config` reports `"credential-vending": "unsupported"`: keep
  `require_vended_credentials = false`.
- RustFS uses the unit separator (`%1F`) between namespace levels, the Iceberg
  REST default, so multi-level namespaces round-trip.
- The `PUT /iceberg/v1/buckets/<bucket>` management call is RustFS-specific;
  the compose file performs it once with `curl --aws-sigv4`.

## Output

Captured from a clean run (`./run.sh`), committed in [`OUTPUT.md`](./OUTPUT.md).
