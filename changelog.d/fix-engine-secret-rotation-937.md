### Added

- Catalog `client_credentials` blocks accept `client_secret_file`, read on every token request, so a rotated engine secret takes effect without a restart (#937)

    The token cache is keyed on a fingerprint of the secret, and the first token on a new engine secret is audited as `engine_secret_rotated`. Startup refuses a block with both sources set or with an unreadable or empty file.
