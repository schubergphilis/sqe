### Added

- Column tags keep their value: `SET TAGS ('pii' = 'ssn')` (Databricks) and
  `SET TAG PII = 'true'` (Snowflake) store the value next to the key

    `sqe.column-tags` list entries are now either a key (`"gdpr"`) or a
    one-entry object (`{"pii": "ssn"}`). A table without values is written
    byte for byte as before. Policies, the Ranger tag projection and Spark
    still see keys only. Setting a tag again replaces its value, setting it
    without a value clears the value, and an empty value (`'k' = ''`) stores
    the key alone. A rename carries the values. A change to a value alone
    counts as a tag change for tag authority.

### Changed

- Upgrade note: an SQE older than this release refuses queries on a table
  whose `sqe.column-tags` holds a tag value, because it reads the property
  as unknown tag state and fails closed. Upgrade every coordinator before
  writing a tag with a value, including Snowflake `SET TAG k = 'v'`, which
  used to drop the value.
