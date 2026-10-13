**STATUS 2026-10-13: audit log rotation keeps the hash chain; no export spool without a shipper (`fix/audit-log-rotation-838`, #838).**

Rotation: `[metrics.audit] max_file_bytes` (0 = off), `max_files` (0 = keep all) and `min_free_bytes` (0 = never delete for space); defaults below. The writer thread checks sizes once per loop iteration, after every arm has flushed, and renames the native file and (Ocsf/Both formats) the OCSF file to `<path>.<generation>` with the same zero-padded generation. The generation counts rotations rather than seqs, because a `BROKEN:` resume restarts seq at 0 and seq-named files could collide. A failed reopen renames the file back so the path and the live sink agree and the next iteration retries. The in-memory chain is never reset. `resume_chain_from_path` falls back to the newest rotated file only when the active file is empty or missing; a torn active file still opens a `BROKEN:` segment. `--verify-audit <active>` verifies all generations oldest first, then the active file (skipped when empty right after a rotation), and links each pair with `link_rotated_reports`. A retention-pruned oldest file is accepted as a non-genesis start.

Spool: main.rs resolves the OTLP endpoint and builds the exporter before `with_export_spool`; with neither, no spool is attached, so nothing grows uncapped.

For #812: rotated files are `audit.jsonl.<generation>` (`rotated_audit_files` lists them in write order), so the history reader can walk generations newest first and stop at its time bound.

Tests: seven unit tests in `audit::logger` (chain across files, restart on an empty active file, torn active file still breaks, Both-format pairing, `max_files` pruning, low-disk pruning never deletes the newest, pruning is recorded in the chain). All `sqe-metrics` audit tests pass. Not run: `make test-integration` (no integration test covers the audit writer).

Rotation is on by default (`max_file_bytes` 256 MiB, `max_files` 0 = keep all). History is deleted only to keep the volume alive: every 10 s the writer checks free space (rustix `statvfs`, unix only), and below `min_free_bytes` (default 1 GiB) deletes the oldest rotated generations, never the newest, writing a `Maintenance` / `PRUNED` record into the chain. `max_files` pruning writes the same record. `AuditLoggerOptions::default()` (library and tests) still never rotates.

**NEXT:** #812 durable query history on top of rotated files; dependency/CVE batch (#800, !1133, !1107, !1389); SF100 memory safety (#788, #784, #755).
