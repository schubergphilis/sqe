### Added

- Signed catalog scopes are checked for their format (`typ`) (#908)

    New `[session.catalog_scope] token_type` (default `sqe-catalog-scope+jwt`) names the scope format SQE reads. A scope whose JOSE header `typ` is that value, the generic `JWT`, or absent is read as before. Any other `typ` is refused with a reason and an audit record, so a later version of an orchestrator's scope format is never read as the current one. The scope's `iss` was already required to equal `issuer`; `iat` is accepted. An orchestrator that sends its own `typ` sets `token_type` to it, as it sets `header_name`.
