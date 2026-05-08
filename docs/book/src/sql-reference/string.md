# String functions

DataFusion contributes ~35 string functions plus a unicode submodule and a regex submodule. SQE adds Trino-name aliases and a few extras.

## Concatenation

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `concat(a, b, ...)` | `datafusion-builtin` | Variadic. NULL inputs become empty string. | yes | yes | yes | yes |
| `concat_ws(sep, a, b, ...)` | `datafusion-builtin` | Concat with separator; skips NULL args. | yes | yes | yes | yes |
| `a \|\| b` | `datafusion-builtin` | SQL standard concat. NULL propagates. | yes | yes | yes | yes |

## Length and offsets

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `length(s)` / `char_length(s)` / `character_length(s)` | `datafusion-builtin` | Number of characters. UTF-8-aware. | yes | yes | yes | yes |
| `octet_length(s)` | `datafusion-builtin` | Number of bytes. | yes | yes | yes | yes |
| `bit_length(s)` | `datafusion-builtin` | `octet_length * 8`. | yes | yes | yes | yes |
| `position(needle in haystack)` / `strpos(haystack, needle)` | `datafusion-builtin` | 1-based offset of `needle`. 0 if not found. | yes | yes | yes | yes |
| `find_in_set(needle, comma_list)` | `datafusion-builtin` | 1-based offset in a comma-separated list. | - | - | yes | - |
| `codepoint(s)` | `sqe-trino-functions` | Unicode codepoint of the first character. `trino_functions.rs:101` | yes | - | - | - |
| `chr(n)` / `char(n)` | `datafusion-builtin` | Codepoint -> character. | yes | yes | yes | yes |
| `ascii(s)` | `datafusion-builtin` | Codepoint of the first character. | yes | yes | yes | yes |

## Slicing

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `substring(s from start [for length])` | `datafusion-builtin` | SQL standard. 1-based. | yes | yes | yes | yes |
| `substr(s, start [, length])` | `datafusion-builtin` | Function form. | yes | yes | yes | yes |
| `left(s, n)` | `datafusion-builtin` | Leftmost n chars. | yes | yes | yes | yes |
| `right(s, n)` | `datafusion-builtin` | Rightmost n chars. | yes | yes | yes | yes |
| `split_part(s, delim, n)` | `datafusion-builtin` | Nth part after splitting by `delim`. | yes | yes | yes | yes |
| `split(s, delim)` | `sqe-trino-functions` | Returns array of parts. `trino_functions.rs:118` | yes | yes | yes | yes |
| `split_part(s, delim, n)` | `datafusion-builtin` | Single part by index. | yes | yes | yes | yes |

## Trimming

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `trim(s)` / `trim(both ' ' from s)` | `datafusion-builtin` | Default trims whitespace. | yes | yes | yes | yes |
| `ltrim(s [, chars])` | `datafusion-builtin` | Trim from left. | yes | yes | yes | yes |
| `rtrim(s [, chars])` | `datafusion-builtin` | Trim from right. | yes | yes | yes | yes |
| `btrim(s [, chars])` | `datafusion-builtin` | Trim both sides; explicit alias of `trim`. | - | yes | yes | yes |

## Case and padding

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `lower(s)` / `upper(s)` | `datafusion-builtin` | Case conversion. | yes | yes | yes | yes |
| `initcap(s)` | `datafusion-builtin` | Title-case each word. | yes | yes | yes | yes |
| `lpad(s, n [, fill])` | `datafusion-builtin` | Pad on left to length `n`. | yes | yes | yes | yes |
| `rpad(s, n [, fill])` | `datafusion-builtin` | Pad on right. | yes | yes | yes | yes |
| `repeat(s, n)` | `datafusion-builtin` | Repeat `n` times. | yes | yes | yes | yes |
| `reverse(s)` | `datafusion-builtin` | Reverse the string. | yes | yes | yes | yes |
| `replace(s, from, to)` | `datafusion-builtin` | Replace all occurrences. | yes | yes | yes | yes |
| `translate(s, from, to)` | `datafusion-builtin` | Per-character substitution. | yes | yes | yes | yes |
| `overlay(s placing rep from start [for length])` | `datafusion-builtin` | Splice substring. | yes | yes | yes | yes |
| `format(pattern, args...)` | `sqe-trino-functions (ext)` | C-style printf. `trino_functions_ext.rs:79` | yes | - | yes | - |

## Predicates

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `starts_with(s, prefix)` | `datafusion-builtin` | True if `s` begins with `prefix`. | yes | yes | yes | yes |
| `ends_with(s, suffix)` | `datafusion-builtin` | True if `s` ends with `suffix`. | yes | yes | yes | yes |
| `contains(s, sub)` | `datafusion-builtin` | True if `s` contains `sub`. | - | yes | yes | yes |
| `s LIKE pattern` | `datafusion-builtin` | SQL standard pattern. `_` and `%`. | yes | yes | yes | yes |
| `s ILIKE pattern` | `datafusion-builtin` | Case-insensitive LIKE. | yes | yes | partial | yes |
| `s SIMILAR TO pattern` | `datafusion-builtin` | SQL/POSIX-light regex. | yes | yes | - | yes |

## Regular expressions

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `regexp_like(s, pattern)` | `datafusion-builtin` | True if `pattern` matches anywhere. | yes | yes | yes | yes |
| `regexp_match(s, pattern)` | `datafusion-builtin` | Returns array of capture groups. | yes | yes | yes | yes |
| `regexp_replace(s, pattern, repl [, flags])` | `datafusion-builtin` | Replace matches. Flags `g` (global), `i` (insensitive). | yes | yes | yes | yes |
| `regexp_count(s, pattern)` | `datafusion-builtin` | Count of non-overlapping matches. | yes | yes | yes | yes |
| `regexp_extract(s, pattern [, group])` | `sqe-trino-functions (ext)` | Extract first match (or capture group N). `trino_functions_ext.rs:46` | yes | yes | yes | yes |
| `regexp_extract_all(s, pattern [, group])` | `sqe-trino-functions (ext)` | All matches as array. `trino_functions_ext.rs:47` | yes | - | partial | - |
| `regexp_split(s, pattern)` | `sqe-trino-functions (ext)` | Split by regex. `trino_functions_ext.rs:48` | yes | - | - | - |

## Hashing

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `md5(s)` | `datafusion-builtin` | 128-bit hex. | yes | yes | yes | yes |
| `sha224(s)` | `datafusion-builtin` | SHA-224 hex. | yes | - | yes | - |
| `sha256(s)` | `datafusion-builtin` + `sqe-policy` | SHA-256 hex. SQE registers an alias used by column masks. | yes | yes | yes | yes |
| `sha384(s)` | `datafusion-builtin` | SHA-384 hex. | yes | - | yes | - |
| `sha512(s)` | `datafusion-builtin` | SHA-512 hex. | yes | yes | yes | yes |
| `digest(s, algorithm)` | `datafusion-builtin` | Generic digest. Algos: `md5`, `sha224`, `sha256`, `sha384`, `sha512`, `blake2s`, `blake2b`, `blake3`. | - | - | - | - |

## Distance / phonetic

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `levenshtein(a, b)` | `datafusion-builtin` | Edit distance. | yes | yes | yes | yes |
| `hamming_distance(a, b)` | `sqe-trino-functions (ext)` | Hamming distance for equal-length strings. `trino_functions_ext.rs:35` | yes | - | - | - |
| `soundex(s)` | `sqe-trino-functions (ext)` | Soundex code. `trino_functions_ext.rs:34` | yes | yes | yes | - |
| `word_stem(s [, lang])` | `sqe-trino-functions (ext)` | Stemmer. Default English. `trino_functions_ext.rs:61` | yes | - | - | - |

## Encoding

See [Encoding, hashing, URL](./encoding-url.md) for `from_base64`, `to_base64`, `from_hex`, `to_hex`, `encode`, `decode`.

## Unicode normalization

| Function | Origin | Notes | Trino | Snowflake | Spark SQL | DuckDB |
|---|---|---|---|---|---|---|
| `normalize(s [, form])` | `sqe-trino-functions (ext)` | Unicode normalization. Forms: `NFC` (default), `NFD`, `NFKC`, `NFKD`. `trino_functions_ext.rs:49` | yes | - | - | - |
| `from_utf8(bytes)` | `sqe-trino-functions` | Convert binary to UTF-8 string. `trino_functions.rs:85` | yes | - | - | - |
| `to_utf8(s)` | `sqe-trino-functions` | Convert string to UTF-8 binary. `trino_functions.rs:86` | yes | - | - | - |

## Examples

### Cleanse user input

```sql
SELECT
    initcap(trim(lower(name))) AS name_clean,
    regexp_replace(email, '\\s+', '') AS email_clean,
    coalesce(nullif(trim(comment), ''), 'no comment') AS comment_clean
FROM users;
```

### Extract domain from URL

```sql
SELECT
    url,
    regexp_extract(url, 'https?://([^/]+)', 1) AS host_via_regex,
    url_extract_host(url) AS host_via_helper
FROM access_logs;
```

`url_extract_host` is dramatically faster than the regex version. Prefer it whenever the input is well-formed URLs.

### Tokenise a sentence

```sql
SELECT id, token
FROM articles, UNNEST(split(body, ' ')) AS t(token)
WHERE length(token) > 3;
```

### Mask sensitive data

```sql
SELECT
    user_id,
    sha256(email) AS email_hash,
    concat('***', right(phone, 4)) AS phone_masked
FROM users;
```

(For declarative masking via grants, see [GRANT and REVOKE](./grant-revoke.md).)

## What is NOT supported

- **`SOUNDEX_DIFFERENCE`** (T-SQL). use `levenshtein(soundex(a), soundex(b))`.
- **`PARSE_URL` family with named parts**. Use the `url_extract_*` family in [Encoding, hashing, URL](./encoding-url.md).
- **`STRING_SPLIT_TO_ARRAY`**. use `split(s, delim)`.
