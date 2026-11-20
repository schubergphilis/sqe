### Added

- Audit records carry the session's resolved catalog and namespace (#910)

    The native event has an optional `session` object and the OCSF line has `unmapped.session_catalog` and `unmapped.session_namespace`. Statement records set them; authentication, maintenance and system records do not. Existing records hash unchanged.

### Fixed

- `USE CATALOG <name>` now takes effect over Flight SQL (#910)

    The statement did not parse and the Flight handler ignored it, so later statements ran in the old catalog.
