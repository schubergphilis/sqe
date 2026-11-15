## Context

Hive external tables (`[[hive.roots]]` and `ATTACH ... TYPE hive_external`) keep their definitions in sidecar manifests or Glue and read the data with the engine's storage credentials or a named `SECRET`. Nothing on the storage side scopes a read to the user, so SQE gates locations itself. Today that gate is the file TVF allow-list:

- `HiveManifestStore::{check_location, put, get, ...}` call `TvfPolicy::check_path(location, caller, false)` on the table location and every partition-index location (`sqe-catalog/src/hive/store.rs`).
- A `SELECT` and Hive DDL run the check as the session user (`hive_session_caller`, `sqe-coordinator/src/hive_ddl_handler.rs`). The attached store itself carries the attaching user's caller, or `trusted_startup_config()` for a configured root, which skips the allow-list; manifest reads use it.

So the allow-list says both "a user may `read_parquet` here" and "a Hive table may live here". Every deployment with a Hive table has to grant the first to get the second, and the policy layer only sees the second.

## Goals / Non-Goals

**Goals**
- A Hive table's location never has to be a file TVF prefix.
- A file TVF read under Hive table data is refused, whoever asks, unless it brings its own credentials.
- Who may read or create a Hive table does not change: the same caller, `{user}` and `{catalog}` placeholders and canonicalisation apply, against a different list.

**Non-Goals**
- Per-user storage credentials for Hive reads.
- Changing Glue roots' credential handling or Hive caching.
- The Iceberg overlap guard itself (`tvf-prefix-table-overlap`); this change shares its registry and refusal point.

## Decisions

### D1. Two lists, one matcher
`[hive] allowed_data_prefixes` applies to every root; `data_prefixes` on a `[[hive.roots]]` entry adds prefixes for that root. They use the TVF prefix grammar and matcher (`canonicalize_object_url`, `{user}`, `{catalog}`), so the Hive gate is `check_path` against another prefix set. A `HiveDataPolicy` built from `HiveConfig` holds it; `HiveManifestStore` takes it in place of the `TvfPolicy` it holds now. Admin roles and `allow_all_object_stores` keep their meaning for the Hive gate only if D5 says so.

*Alternative:* derive the allowed area from the root (data must live under the root `location`). Rejected: sidecar roots routinely point at data written by other engines elsewhere, and Glue roots have no location.

### D2. Protected-location registry, shared with Iceberg
The overlap change adds a process-wide registry of locations file TVFs must refuse. Hive adds two kinds of entry: every Hive data prefix (static, from config, with placeholder segments as wildcards) and every Hive table location SQE loads (dynamic, so a table under a broad prefix is covered precisely). The refusal sits in `check_object_store` after the trusted-local and inline-credential exits and before allow-all, admin roles and the allow-list, exactly where the Iceberg refusal goes.

As built (`sqe-core/src/config/hive_data.rs`):

- `ProtectedLocations::global()` is the registry; `TvfPolicy` reaches it through a `#[serde(skip)]` field defaulted to the global one, so no policy can be built without it. Unit tests inject a private one.
- The refusal and the allow-list are split: `check_object_store` runs `refuse_protected` and then `check_object_store_allowlist`. The Hive gate never calls the refusal, so a Hive table is never refused by its own entry; under the fallback it calls the allow-list half only.
- Matching is by path segment: scheme plus segments, the bucket compared case-insensitively, empty segments dropped, `s3a` read as `s3`. A segment holding a placeholder matches any segment. A TVF path's glob is cut at its first segment holding `*?[{`. A path is refused when either it or the entry is a segment prefix of the other, so a directory or glob above the data is refused as well as a file in it.
- With a non-empty registry, a path that does not canonicalise is refused.
- A configured prefix is named in the refusal; a table location never is ("overlaps the location of a Hive table"), so the error does not disclose a table to someone who cannot see it.
- Entries are recorded where the Hive gate passes: store `put` (create), `get` (load, sidecar roots), `check_location` (`ADD PARTITION`), and the data-store factory's `store_for` (load, the only gate for Glue roots). The table location comes first on load, so partitions under it add nothing; a location already covered is not recorded, so the registry grows per table, not per partition. Root locations and local paths are not recorded.
- Static prefixes are registered when a `QueryHandler` is built, so every coordinator, in tests as well, enforces them.
- Dynamic entries live in the process. After a restart a table is protected again from its first load; until then only the static prefixes cover it. Each coordinator in a fleet records its own loads.

### D3. Startup overlap check
At startup, every allowed TVF prefix is compared with every Hive data prefix. An overlap is logged at error level naming both; startup continues, and D2 refuses the reads. Refusing to start would turn a config upgrade into an outage.

### D4. Migration fallback
When neither `allowed_data_prefixes` nor any root's `data_prefixes` is set, the Hive gate uses the TVF list, as today, and startup logs a deprecation warning that names the new keys. D2's dynamic entries still apply, so a raw read of a table SQE has loaded is refused during the fallback. The fallback is removed one release later (decided).

The fallback is a property of the whole config: as soon as any Hive prefix is set, a root without its own `data_prefixes` (and every `ATTACH`ed root) gets the global list, not the TVF list. Embedded mode (`sqe-cli`) has no `[hive]` section and stays on the TVF list.

Known trade-off of the fallback: anyone who may create an external table at a TVF prefix makes that location unreadable to file TVFs for the whole process. Setting `allowed_data_prefixes` ends it.

### D5. ATTACH (decided: option 1, bypass kept)
An attached root's tables are checked per reading session (unchanged). Anyone who may `ATTACH` may attach a root; each location is still checked against the Hive list for every reader, so under a `{user}` prefix other users cannot read it. Admin-only ATTACH would break the self-service staging use.

Admin roles and `allow_all_object_stores` bypass the Hive gate, as they do the TVF allow-list: the Hive gate controls where tables may live, not raw reads. They do not bypass the TVF refusal (D2).

## Risks / Trade-offs

- **Existing TVF reads of Hive data stop working** once D2 lands. Intended; the changelog says so under Security.
- **Broad Hive prefixes block TVF staging below them.** A deployment that wants both keeps them apart; the startup check (D3) names the overlap.
- **Dynamic registry growth** is bounded by the number of Hive tables loaded, as for Iceberg.
- **Dynamic entries are per process** (D2): a restart or another coordinator protects a table again from its first load there. Static prefixes have no such gap, which is one more reason to set them.

## Open Questions

None. D5 and the D4 fallback length were settled on 2026-11-14.
