**STATUS 2026-10-17: bounded waits in the Ranger quickstart and the access-control harnesses (`fix/bounded-waits`).**

`make test-access-control-spark` hung for three hours. Two defects together:

- `scripts/spark-access-control-test.sh` brought `spark` up with `--wait`, which also started its `depends_on` `data-seed` (the script said it did not), and `--wait` has no default timeout.
- `data-seed`'s readiness probe sent one `POST /v1/statement` and grepped for `"state":"FINISHED"`. Since #767 a `POST` never carries a terminal state, so the probe could not pass. Each abandoned `SELECT 1` held 256 MB of carol's 1 GB per-user budget until the 5-minute idle eviction, which did release it (admissions and "query memory at completion" lines pair up 8:8 per 10 minutes in the SQE log; the first two, admitted 13:01:09 and 13:01:13, completed 13:06:11 and 13:06:14). The loop took each freed slot right away, so carol stayed at the limit. There is no reservation leak.

Fixes:
- The seed follows `nextUri` to the end, with `--max-time 30` per request, a 600-page cap per statement, and 60 readiness attempts.
- `spark` starts with `--no-deps`.
- Every `up --wait` in `access-control-test.sh`, `spark-access-control-test.sh`, `access-control-demo.sh` and `access-control-parity-demo.sh` has `--wait-timeout`.
- The harness's `spark-sql` probe runs under a perl `alarm`.
- The integration tests' `docker` / `spark-sql` / `trino-cli` / `docker compose` calls go through `common::process::output_with_timeout`.

Checked the new seed against the live stack: ready, seeded, exit 0.

**NEXT:**
- An abandoned finished Trino statement holds its per-user reservation for the full 300 s idle window, so four abandoned `POST`s lock a user out for five minutes. Consider releasing the reservation once the result is fully buffered, or a shorter idle window for unpolled results.
- A V3-aware delete trigger for compaction and `table_health` (deletion-vector cardinality / record count).
- Fix `test_distributed_select`: zero the thresholds and match `node_id` against the worker URLs.
