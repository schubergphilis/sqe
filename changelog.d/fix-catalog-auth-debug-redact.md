### Security

- A catalog's configured `static` token and `client_credentials` secret are redacted in `Debug` output

    They were plain strings, so formatting a catalog config with `{:?}` printed them in clear; nothing logged it today (#944).
