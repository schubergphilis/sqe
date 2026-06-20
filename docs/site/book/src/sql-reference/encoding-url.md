# Encoding, hashing, URL

Three small, related families: binary encoding (base64 / hex), cryptographic hashes, URL parsing. SQE inherits the encoding and crypto helpers from DataFusion and adds Trino-named aliases plus eight URL extractors.

## Binary encoding

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `encode(input, encoding)` | `datafusion-builtin` | Encode binary to text. `encoding`: `'base64'` or `'hex'`. | yes | yes | yes | yes |
| `decode(input, encoding)` | `datafusion-builtin` | Decode text to binary. Same `encoding` set. | yes | yes | yes | yes |
| `from_base64(s)` | `sqe-trino-functions` | Trino-named base64 decode. `trino_functions.rs:78` | yes | yes (`base64_decode_string`) | yes (`unbase64`) | yes |
| `to_base64(b)` | `sqe-trino-functions` | Trino-named base64 encode. `trino_functions.rs:79` | yes | yes (`base64_encode`) | yes (`base64`) | yes |
| `from_hex(s)` | `sqe-trino-functions` | Trino-named hex decode. `trino_functions.rs:80` | yes | yes (`hex_decode_string`) | yes (`unhex`) | yes |
| `to_hex(n)` | `datafusion-builtin` | Encode integer or binary as hex. NOT registered as Trino UDF: DataFusion already has it. | yes | yes | yes | yes |

```sql
SELECT to_base64(CAST('hello' AS bytea));         -- 'aGVsbG8='
SELECT from_base64('aGVsbG8=');                   -- bytea -> 'hello'
SELECT encode(CAST('hi' AS bytea), 'hex');        -- '6869'
SELECT decode('6869', 'hex');                     -- bytea -> 'hi'
```

## Cryptographic hashes

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `md5(s)` | `datafusion-builtin` | 128-bit hex string. | yes | yes | yes | yes |
| `sha224(s)` | `datafusion-builtin` | SHA-224 hex. | yes | - | yes | - |
| `sha256(s)` | `datafusion-builtin` + `sqe-policy` | SHA-256 hex. SQE registers an additional UDF used by column masks. | yes | yes | yes | yes |
| `sha384(s)` | `datafusion-builtin` | SHA-384 hex. | yes | - | yes | - |
| `sha512(s)` | `datafusion-builtin` | SHA-512 hex. | yes | yes | yes | yes |
| `digest(s, algo)` | `datafusion-builtin` | Generic. Algos: `md5`, `sha224`, `sha256`, `sha384`, `sha512`, `blake2s`, `blake2b`, `blake3`. | - | - | - | - |
| `checksum(b)` | `sqe-trino-functions (ext)` | xxHash of bytes; cheaper than crypto hashes. `trino_functions_ext.rs:69` | yes | - | - | - |

```sql
SELECT sha256(email) AS email_hash FROM users;
SELECT digest('hello', 'blake3');       -- 64-char blake3 hex
SELECT to_hex(checksum(payload)) FROM events;
```

## URL parsing

All eight URL functions live in `sqe-trino-functions`. They wrap the `url` crate from crates.io for correct RFC 3986 handling.

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `url_extract_host(url)` | `sqe-trino-functions` | Hostname only, no port. `trino_functions.rs:67` | yes | `parse_url(..., 'HOST')` | `parse_url(..., 'HOST')` | - |
| `url_extract_path(url)` | `sqe-trino-functions` | Path component (after host, before query). `trino_functions.rs:68` | yes | `parse_url(..., 'PATH')` | `parse_url(..., 'PATH')` | - |
| `url_extract_port(url)` | `sqe-trino-functions` | Port as INT. NULL when absent. `trino_functions.rs:69` | yes | - | - | - |
| `url_extract_protocol(url)` | `sqe-trino-functions` | Scheme (`https`, `http`, `s3`, etc). `trino_functions.rs:70` | yes | `parse_url(..., 'PROTOCOL')` | `parse_url(..., 'PROTOCOL')` | - |
| `url_extract_query(url)` | `sqe-trino-functions` | Query string (after `?`, no leading `?`). `trino_functions.rs:71` | yes | `parse_url(..., 'QUERY')` | `parse_url(..., 'QUERY')` | - |
| `url_extract_parameter(url, name)` | `sqe-trino-functions` | First value of named parameter. `trino_functions.rs:72` | yes | `parse_url(..., 'QUERY:name')` | `parse_url(..., 'QUERY', 'name')` | - |
| `url_encode(s)` | `sqe-trino-functions` | Percent-encode. `trino_functions.rs:73` | yes | yes | yes | - |
| `url_decode(s)` | `sqe-trino-functions` | Percent-decode. `trino_functions.rs:74` | yes | yes | yes | - |

```sql
SELECT
    url,
    url_extract_protocol(url) AS proto,
    url_extract_host(url)     AS host,
    url_extract_port(url)     AS port,
    url_extract_path(url)     AS path,
    url_extract_query(url)    AS query,
    url_extract_parameter(url, 'utm_source') AS utm
FROM events;
```

For example, on `https://example.com:8443/api?utm_source=newsletter&page=2`:

| Component | Value |
|---|---|
| `url_extract_protocol` | `https` |
| `url_extract_host` | `example.com` |
| `url_extract_port` | `8443` |
| `url_extract_path` | `/api` |
| `url_extract_query` | `utm_source=newsletter&page=2` |
| `url_extract_parameter(..., 'utm_source')` | `newsletter` |

## When to use which hash

| Use case | Recommendation |
|---|---|
| Equality lookups, deduplication | `xxhash` via `checksum()`. ~10x faster than crypto hashes. |
| Cryptographic integrity, signatures | `sha256` or `sha512`. Avoid `md5` for new uses. |
| Column masking via grants | `sha256` (used in `MASKED WITH` clauses). Deterministic, no collisions in practice. |
| Hashing UTF-8 strings | All accept `VARCHAR` or `BINARY` arguments. The result type is `VARCHAR` (hex) for crypto hashes, `BINARY` for `digest()`. |
| Salting | Concatenate the salt: `sha256(concat(salt, password))`. SQE has no `pbkdf2`-style helpers. |

## When to use which URL parser

`url_extract_host`, `url_extract_query`, `url_extract_parameter` use a real RFC 3986 parser, so they handle internationalised domain names (IDN), unusual ports, percent-encoded query values, and trailing fragments correctly. The regex equivalent (`regexp_extract(url, 'https?://([^/]+)', 1)`) breaks on these edge cases.

Use the regex form only when the input is known to be a fixed shape that the regex covers.

## Why no Snowflake-style `PARSE_URL`

Snowflake's `parse_url(url [, permissive])` returns an OBJECT with named keys. The eight separate `url_extract_*` functions are equivalent in expressive power and easier to compile statically. dbt-snowflake to dbt-sqe migrations need the rewrite, but it is mechanical: `parse_url(u, 'HOST')` becomes `url_extract_host(u)`, etc.

## Why DataFusion's `decode` is not shadowed

A Snowflake-style `DECODE(expr, search1, result1, ...)` would conflict with DataFusion's binary `decode(input, encoding)`. SQE keeps the binary one and exposes the conditional logic via `CASE WHEN`. See [Conditional](./conditional.md) for the trade-off.
