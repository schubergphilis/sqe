---
title: "SQE Talks to Five Catalogs Now: HMS, Nessie, Glue, JDBC, S3 Tables"
description: "We claimed the engine was catalog-agnostic. Time to prove it. One branch, five live integration tests, one small AWS SigV4 patch, and a matrix score that moved from 153 to 158."
pubDate: "2026-04-29"
author: "Jacob Verhoeks"
tags: ["iceberg", "catalog", "aws", "s3-tables", "hive-metastore", "nessie"]
---

The pitch we have been making for six months goes like this. SQE speaks the Iceberg REST protocol. The catalog is configurable. If you swap Polaris for Nessie or Glue or Hive Metastore, the engine code does not change, just the config. Catalog-agnostic.

The pitch had a problem. We had only ever run it against Polaris.

This is the story of one branch that proved the claim. Five catalogs, five live integration tests, one small AWS SigV4 patch in the vendored REST client. A week of work, ending with the engine talking to every catalog the lakehouse market actually uses.

## The plan

The plan was the smallest version of "verify it works." For each catalog, write one integration test that creates a namespace, lists it, and drops it. The round-trip exercises the auth path, the wire protocol, and the catalog's view of the world. If those three work, the rest of the iceberg-rust `Catalog` trait is just more endpoints over the same plumbing.

Five catalogs to cover:

- **Hive Metastore.** Thrift, the legacy elephant. Still dominant in Hadoop shops.
- **Project Nessie.** Open-source REST catalog with git-like branching.
- **JDBC Postgres.** The iceberg-catalog-sql variant. Useful for shops that want to put metadata next to their OLTP database.
- **AWS Glue.** Mandatory if you live on AWS.
- **AWS S3 Tables.** The new managed Iceberg service AWS announced in late 2024.

Polaris was already in production, so we left it alone. Unity Catalog stayed on the list for next time.

## Hive Metastore

Easy in theory. We have a vendored `iceberg-catalog-hms` crate from apache/iceberg-rust v0.9.0. We have a docker image: `apache/hive:standalone-metastore-4.1.0`. The metastore listens on port 9083 and speaks Thrift.

We brought the container up with bundled Derby as the metadata database (the upstream image does not ship a Postgres JDBC driver, so this was the path of least friction) and a local-filesystem warehouse (`file:///opt/hive/warehouse`). The default S3A warehouse fails on `CREATE DATABASE` because the image does not bundle the S3A JARs either, and our smoke test only needs the Thrift surface to work.

First test run: `Connection reset by peer`.

This is the macOS IPv6 trap. `localhost` resolves to `::1` first. Docker's port forwarding answers IPv4 only. The TCP handshake completes against `::1`, then the connection drops because nothing is listening there.

The fix is one character: change `localhost` to `127.0.0.1` in the test. The round-trip works on the second try.

```rust
let uri = std::env::var("SQE_TEST_HMS_URI")
    .unwrap_or_else(|_| "127.0.0.1:19083".into());
```

The vendored HMS code handles everything else. We never wrote a line of Thrift.

## Project Nessie

Same theory. Nessie speaks Iceberg REST, so we should be able to point our existing REST client at the Nessie endpoint and have it work.

We picked the latest tag at the time, `projectnessie/nessie:0.76.6`. The container started, the health check went green, then `curl http://localhost:19121/iceberg/v1/config` returned 404.

It turns out the 0.76 line shipped a partial Iceberg REST adapter. The endpoints are there but several core paths are not implemented. The adapter became fully usable around the 0.85 line. We jumped to `ghcr.io/projectnessie/nessie:0.107.5`, the latest tag.

The 0.107 image needed a different healthcheck (`/q/health` is not exposed; we use `/api/v2/config` instead). After that, the same `iceberg-catalog-rest` client we use against Polaris connected, listed namespaces, created one, dropped it. Zero engine code changes.

The Nessie config response carries an interesting prefix:

```json
"overrides": {
    "nessie.core-base-uri": "http://127.0.0.1:19121/api/",
    "nessie.catalog-base-uri": "http://127.0.0.1:19121/catalog/v1/",
    "nessie.iceberg-base-uri": "http://127.0.0.1:19121/iceberg/",
    "uri": "http://127.0.0.1:19121/iceberg/",
    "nessie.is-nessie-catalog": "true",
    "nessie.prefix-pattern": "{ref}|{warehouse}",
    "nessie.default-branch.name": "main"
}
```

The `prefix` field encodes the active branch and warehouse together. The iceberg-rust REST client reads it and prepends it to every subsequent request automatically. We did not have to thread anything through.

## JDBC Postgres

The least dramatic of the five. The test was already written. Earlier work had pulled in `iceberg-catalog-sql` (the upstream JDBC catalog implementation) and added a smoke test against the docker-compose Postgres service. The test was sitting on `#[ignore]` waiting for someone to run it.

We ran it. It passed. Two minutes of work.

## AWS Glue

This is where the test stack stops being self-contained. Glue is a managed service. There is no docker image. You need a real AWS account.

Jacob has one. We created an S3 bucket in eu-central-1 to use as the warehouse, dropped the AWS profile name and bucket URI into a gitignored `.env` file, and updated the test to read from environment.

```bash
AWS_PROFILE=your-aws-profile
AWS_REGION=eu-central-1
SQE_TEST_GLUE_WAREHOUSE=s3://your-glue-it-bucket/wh/
```

The `.env.example` file is the committed template. The actual `.env` is in `.gitignore`. Adding `!.env.example` as an exception means `cp .env.example .env` works without a second thought.

The test creates a Glue database, lists databases, drops the one it just created. Real AWS, real credentials, real round-trip. The vendored `iceberg-catalog-glue` crate does the heavy lifting; we just configured it.

```rust
let backend = GlueBackend::new(GlueConfig::new(region, warehouse));
let catalog = backend.build_catalog(storage_factory).await?;
catalog.create_namespace(&ns, HashMap::new()).await?;
```

It worked on the first try.

## The S3 Tables surprise

S3 Tables was supposed to be the hardest. AWS announced it in late 2024 as a managed Iceberg service. It uses SigV4 auth, which is a different model from the OAuth bearer tokens SQE was built around. We expected to write a separate backend, maybe a week of work.

Then we read the docs more carefully.

S3 Tables exposes itself through the same Iceberg REST protocol that Polaris speaks. The endpoint is `https://glue.<region>.amazonaws.com/iceberg` (the federated Glue path) or `https://s3tables.<region>.amazonaws.com/iceberg` (per-bucket). Same wire format. Same JSON. Same paths.

The only difference is the signature. AWS wants SigV4 on every request. The Java reference implementation handles this with a `RESTSigV4AuthManager` that swaps in when the server advertises `rest.sigv4-enabled=true` in its `/v1/config` defaults. iceberg-rust did not have an equivalent.

We added one.

```rust
// vendor/iceberg-rust/crates/catalog/rest/src/client.rs
async fn authenticate(&self, req: &mut Request) -> Result<()> {
    #[cfg(feature = "aws-sigv4")]
    if let Some(signer) = self.sigv4_signer.as_ref() {
        return signer.sign(req).await;
    }
    // ... existing OAuth/Bearer path
}
```

The SigV4 path is gated behind a new `aws-sigv4` cargo feature. Default-on for SQE, default-off upstream so a Polaris-only build does not pull in the AWS SDK. The signer reads credentials from the standard AWS provider chain (env vars, `AWS_PROFILE`, instance profile, SSO) and signs each outgoing `reqwest::Request` before it leaves the client.

Three properties trigger the new path: `rest.sigv4-enabled=true`, `rest.signing-name=glue` (or `s3tables`), `rest.signing-region=<region>`. AWS advertises these in the server's `/v1/config` response anyway. We just have to set them on the user config so the very first call (the config fetch itself) is signed.

The whole patch is one new module: `vendor/iceberg-rust/crates/catalog/rest/src/sigv4.rs`. About 200 lines, most of them comments and error mapping. The actual signing is a pure function from `(request, region, service)` to a signed request. The `aws-sigv4` crate did all the canonical-request and HMAC-SHA256 work.

The test passes. We list namespaces in a real S3 Tables bucket. The catalog returns `table_demo_analytics`. We list tables. One table, `table_user_events`. All through SQE's existing `iceberg::Catalog` interface. Zero engine changes outside the vendored REST crate.

```
running 1 test
S3 Tables round-trip ok: namespace=table_demo_analytics tables=["table_demo_analytics.table_user_events"]
test s3_tables::list_namespaces_via_glue_rest ... ok
```

That readout is the punchline. SQE talking to AWS S3 Tables, listing real customer data, through the same code path it uses for Polaris in production.

## What changed and what did not

Code changes:

- New cargo feature `aws-sigv4` on the vendored `iceberg-catalog-rest`. About 250 lines of new code (the signer module + auth-path branch + config readers).
- Default features on `sqe-catalog` expanded from `["rest", "sql-postgres"]` to `["rest", "sql-postgres", "hms", "glue", "hadoop"]`. Picking a backend stays a runtime config concern, not a build-time one. The release binary lands at about 180 MB regardless of which catalogs are configured.
- Five new test functions in `crates/sqe-catalog/tests/backends_integration.rs`. Each is `#[ignore]` because they need external services.
- Two new docker-compose overlays: `docker-compose.hms.yml` and `docker-compose.nessie.yml`. They layer on the existing `docker-compose.test.yml` so the rustfs S3 service is reused.
- `.env.example` template + `.gitignore` allowlist so `cp .env.example .env` works for the AWS tests.

Code that did not change:

- The coordinator. Still constructs a REST catalog from config.
- The query path. Still routes through the same `SessionCatalog`.
- The auth providers. Still the same eleven-provider chain.
- The information_schema virtual tables. Still backed by the same catalog interface.

The reason engine code did not change is that S3 Tables IS Iceberg REST. Same wire protocol. Same Catalog trait. Same client crate. The auth shim lives entirely inside the vendored REST library. Once the request leaves the auth method, every byte after that looks identical to a Polaris request.

## The score

The Iceberg Matrix tracks 63 capabilities per engine, three levels each: full, partial, none. SQE was sitting at 153 out of 189 before this branch. Five cells flipped from `partial` to `full`:

| Cell | Evidence |
|---|---|
| `sqe:hive-metastore:v2` | Live Thrift round-trip vs `apache/hive:standalone-metastore-4.1.0` |
| `sqe:hive-metastore:v3` | Same code path, format-version agnostic |
| `sqe:nessie:v3` | Live REST round-trip vs `ghcr.io/projectnessie/nessie:0.107.5` |
| `sqe:aws-glue-catalog:v2` | Live AWS round-trip in eu-central-1 |
| `sqe:aws-glue-catalog:v3` | Same |

Score moves to 158/189. The capability gain is bigger than the score reflects. The score counts cells, not catalogs. The capability is "this engine talks to every catalog the lakehouse market actually uses, plus the new AWS managed service." That capability did not exist a week earlier.

The architectural bet from 2024 paid off. We chose Polaris because it speaks the open Iceberg REST protocol and stays out of the way. Two years later, every other catalog we wanted to support either spoke the same protocol natively (Nessie) or could be reached through it with a small auth shim (Glue REST, S3 Tables). The catalog choice is reversible. We tested it. It is.

## What is still open

Three things stay on the deferred list.

**Engine session-manager wiring.** SQE's `SessionCatalog::new` is called in 13 places across the coordinator crates. All of them construct a REST catalog, even when the configured backend is HMS or Glue. Refactoring to dispatch through a `CatalogBackend` trait closes a real gap. Not needed for the live tests because they hit the iceberg-rust catalog libraries directly, but needed before SQL queries can route through HMS or Glue end-to-end. Tracked as the next phase.

**Unity Catalog.** The OIDC M2M auth provider for Unity has been in `sqe-auth` since Phase A. We just have not run a live Unity test yet. The protocol and auth model are simpler than S3 Tables (it is straight bearer-token Iceberg REST), so the test should be the easiest of the bunch when we get to it.

**Delta Lake support and multi-cloud storage (Azure, GCS).** Both deferred to separate changes. Iceberg-only is enough for the catalogs we care about today.

The branch is `feat/matrix-phase-o-live-catalogs`. The MR is !113. Score is 158/189 (83.6%).
