# 305: Catalog error classification is substring matching on Display/Debug text

- Scope: sqe-catalog (rest_catalog.rs), sqe-core error.rs (classifier), catalog_provider.rs, schema_provider.rs
- Severity: medium
- Category: reliability
- Effort: M (days)
- Status: fixed (branch `fix/errors-audit-observability`, 2026-09-05)

## Problem

The catalog crate wraps every `iceberg::Error` into `SqeError::Catalog(String)` or `catalog_src(String, e)` and then decides authorization, visibility, retry and degrade behaviour from the text:

- `crates/sqe-catalog/src/rest_catalog.rs:796-799`: `iceberg_error_is_forbidden` is `format!("{e:?}").to_ascii_lowercase()` then `contains("403") || contains("forbidden")`. This feeds `namespace_visible_from_probe` (`rest_catalog.rs:845-856`), which hides a namespace from `SHOW SCHEMAS`, and `classify_iceberg_catalog_error` (`818-831`), which turns the error into `AccessDenied`.
- `crates/sqe-core/src/error.rs:507-575`: `classify_catalog_error` matches `"401"`, `"403"`, `"not found"` plus `"namespace"`, `"timeout"`, `"connection refused"`, and so on. `crates/sqe-catalog/src/catalog_provider.rs:804-824` has a test documenting that a Polaris `/v1/config` 404 for a missing warehouse lands on `SchemaNotFound` because the message happens to contain both "not found" and "namespaces".
- `catalog_provider.rs:48-70`: `should_degrade_to_empty_namespaces` decides to register an empty catalog based on that code.
- `schema_provider.rs:36-38`: the view fallback runs only when the code is `TableNotFound`, again from text.
- `rest_catalog.rs:1670-1678` shows a typed alternative already exists: `SqeError::CatalogHttp { status, op, body }`, used only by `authorize_table_access`.

The Debug rendering of an `iceberg::Error` includes the table identifier, the URL and the server's JSON body. A table named `forbidden_words`, a namespace `ns_403`, a Polaris error body that quotes such a name, or a 500 whose body says "forbidden by policy engine" all classify as `AccessDenied`.

## Why it matters

Classification here is not cosmetic. `AccessDenied` on a namespace probe hides the namespace silently (`filter_visible_namespaces`, `catalog_provider.rs:129-163`); `AccessDenied` on a listing degrades the whole catalog to empty without failing the session (`catalog_provider.rs:306-321`); `TableNotFound` diverts to view lookup and then to "table not found" (`schema_provider.rs:304-326`); the breaker treats `AccessDenied` as success (`rest_catalog.rs:109-122`) so a misclassified 5xx never trips it. Every new message shape from Polaris, Nessie, Glue REST or Unity (the crate supports all four) is a new chance to misroute. The 2026-06-26 analysis already flagged `SqeError::Catalog(String)` losing its source; this is the concrete cost.

## Proposed change

1. In the vendored REST client (`vendor/iceberg-rust/crates/catalog/rest/src/catalog.rs`), attach the HTTP status as a typed field: an `iceberg::Error` context key with a fixed name is the minimum; a `RestError { status: u16, op }` source is better. The fork is already patched for other reasons.
2. In `rest_catalog.rs`, map that typed status to `SqeError::CatalogHttp { status, op, body }` in `classify_iceberg_catalog_error`; delete `iceberg_error_is_forbidden`'s string matching and make `namespace_visible_from_probe` match on `status == 403`.
3. Make `SqeErrorCode` derivation for `CatalogHttp` a `match status` in `sqe-core`, and keep `classify_catalog_error(msg)` only as the fallback for legacy `Catalog(String)` sites, with a deprecation note.
4. Non-REST backends (Glue, HMS, SQL) have their own error kinds; map them at `build_backend_catalog` boundary rather than by text.

## Acceptance criteria

- Test: an `iceberg::Error` for table `forbidden_words` with status 500 classifies as `CatalogUnavailable`, not `AccessDenied`; the namespace probe fails the listing rather than hiding the name.
- `grep -n 'contains("403")\|contains("forbidden")' crates/sqe-catalog/src crates/sqe-core/src` returns nothing outside tests.
- Existing tests in `rest_catalog.rs:2631-2761` and `catalog_provider.rs:782-844` pass against typed statuses.

## Related

- 2026-06-26 codebase analysis item on `SqeError` string variants (still open).
- 104 and 800 file the same classifier from the sqe-core and cross-cutting side; this issue is the catalog-side evidence and the vendored-client fix. One MR can close all three.
- 306.

## Resolution (2026-09-05)

`iceberg::Error::http_status()` reads the `status`/`code` context entries. `iceberg_error_is_forbidden` is `http_status() == Some(403)`. `classify_iceberg_catalog_error` builds `CatalogHttp` from the typed status, so a table named `forbidden_words` with HTTP 500 is `CatalogUnavailable`. Tests in `rest_catalog.rs`.
