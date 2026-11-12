### Documentation

- Connecting: Flight SQL clients can send an existing token as `Authorization: Bearer` (ADBC `authorization_header`) without a handshake. The page explains what a `bearer_token` provider and a `bearer_passthrough` provider each do with that token, and that clients sharing a token share one session. The provider table in Configuration gains `bearer_passthrough`.
