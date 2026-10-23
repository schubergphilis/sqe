**STATUS 2026-10-23: table maintenance as the table's writer (`feat/maintenance-table-owner`, AI Portal request §2, portal #225).**

The four parts of the request:

- (a) `expire_snapshots` moves from the admin tier to the table-write tier. SQE admits any session whose roles are not explicitly read-only. That role-name check (`session_is_explicitly_read_only`) is a heuristic; the real gate is the catalog commit made with the caller's token, and `expire_snapshots` deletes files only after that commit succeeded. `access_control_e2e::expire_snapshots_runs_as_writer_and_refuses_reader` proves both sides on live Polaris + Ranger: alice (SELECT only) is refused and the snapshot count is unchanged; bob (INSERT through role engineer) expires down to one snapshot. A deleting `remove_orphan_files` stays admin: it deletes from storage with no commit the catalog could refuse. Only its dry run moves (to read-only). This part of the request is therefore not met for the deleting form.
- (b) `remove_orphan_files(..., dry_run => true)` returns `path` + `action = 'would_delete'` and deletes nothing. `[maintenance] orphan_min_age_secs` (default 86400) is a server-side floor: a deleting call with a newer `older_than` is refused, naming the setting.
- (c) A `CALL` table name without a catalog resolves in the session's catalog. A `[catalogs.*]` entry is built with its own config (`build_session_catalog`, now `pub(crate)`); any other name goes through Polaris discovery under `polaris-auto`.
- (d) `TableRef` takes 4+ parts as catalog + nested namespace + table (`cat.a.b.t`); `NamespaceRef` takes 3+ parts the same way. Empty segments (`a..t`) are refused. `to_table_ident`, the audit resource, `reproject_column_tags` and the scheduler's `table_ref_from_ident` build multi-level `NamespaceIdent`s. The scheduler used to parse `ident.to_string()`, which read a discovered `a.b.t` as catalog `a`.

Tests:
- unit: `RUST_MIN_STACK=33554432 cargo test -p sqe-coordinator --features test-sqlite --lib -- maintenance catalog_ops` 168 passed, including `a_four_part_name_loads_from_a_nested_namespace` (memory catalog), `table_ref_from_ident_keeps_a_nested_namespace`, `unqualified_procedure_tables_use_the_session_catalog`, `orphan_cutoff_enforces_the_floor_on_deleting_calls`; `cargo test -p sqe-sql --lib procedures::` 56 passed.
- integration: `make test-integration FILTER=maintenance_call_uses_the_session_catalog` 1 passed; `FILTER=maintenance_procedures` 14 passed (includes `remove_orphan_files_dry_run_lists_and_floor_refuses`). The scheduler ITs are plain `#[tokio::test]`, which the gate skips: `cargo test -p sqe-coordinator --features test-sqlite --test it maintenance_scheduler` 13 passed.
- access control: `scripts/access-control-test.sh expire_snapshots_runs_as_writer_and_refuses_reader` 1 passed; alice's refusal is Polaris's 403 on the commit, asserted.

Open: the portal's acceptance asks that writers can run a deleting `remove_orphan_files`. It would need a write check that does not depend on a commit, for example a Polaris privilege probe on the table before deleting.

**NEXT:** unchanged
