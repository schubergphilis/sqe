### Added

- `[metrics.audit] omit_query_text` and `omit_actor_identity` keep the SQL text, the actor email and the roles out of the audit log

    With `omit_query_text = true` no native or OCSF record holds statement text: `query_hash`, `statement_type` and the text's byte length (`query.text_len`, OCSF `unmapped.query_text_len`) stay. The legacy path for secret-bearing statements honours it too. `omit_actor_identity = true` drops the actor's `email` and `roles` and keeps the username and subject. Both apply before the record is chained, so the chain verifies and OCSF `metadata.uid` still equals the native `integrity.hash`. Both default to false.
