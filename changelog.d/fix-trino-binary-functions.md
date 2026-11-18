### Added

- Trino varbinary hash and encoding functions

    `sha1`, `crc32`, `xxhash64`, `murmur3`, `hmac_md5`, `hmac_sha1`,
    `hmac_sha256`, `hmac_sha512`, `to_base64url` / `from_base64url`,
    `to_base32` / `from_base32`, `to_ieee754_32` / `_64`,
    `from_ieee754_32` / `_64`, `to_big_endian_32` / `_64` and
    `from_big_endian_32` / `_64`. Hashes return `varbinary`, `crc32` returns
    `bigint`. `spooky_hash_v2_32` / `_64` are not implemented yet.

### Changed

- `to_utf8`, `from_utf8`, `from_hex`, `to_base64` and `from_base64` work on
  `varbinary` like Trino

    They used to return hex text in place of bytes. `to_utf8` and `from_hex`
    now return `varbinary`; `from_utf8` takes `varbinary` and the replacement
    overloads. `to_hex` takes `varbinary` as well as integers. `md5`, `lpad`,
    `rpad` and `substr` / `substring` keep `varbinary` in and out; `md5` on
    `varchar` keeps DataFusion's hex text.
