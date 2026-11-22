### Added

- dbt-sqe: extra Flight SQL call headers from the profile (#772)

    New profile option `call_headers` (header name to value) sends each header on every Flight SQL call, next to the credentials, so an orchestrator can pass a run's signed catalog scope (`[session.catalog_scope] header_name`, #908). Names are lower-cased; `authorization`, `content-type`, `host`, `te`, `user-agent`, `grpc-*` and `*-bin` are refused, so a profile cannot replace the credentials. A value must be printable ASCII, and an empty value sends nothing. Values never appear in `dbt debug`, the credentials' repr or dbt-sqe's errors. dbt-sqe 1.2.0.
