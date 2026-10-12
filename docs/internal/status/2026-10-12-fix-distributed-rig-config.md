**STATUS 2026-10-12: `make test-distributed` boots again (`fix/distributed-rig-config`).**

The coordinator in the 2-worker rig timed out at startup. Its config had drifted behind four startup guards added since the rig was last used:

- Plaintext transport: `[security] allow_insecure_transport = true` on coordinator and workers (test rig only).
- Worker secret: the rig sets none, so the worker now sets `allow_unauthenticated = true`.
- Memory: the coordinator's default `memory_limit` plus the 2 GiB process headroom exceeded the 8g container. Set `memory_limit = "4GB"`.
- Scan-ticket audience: each worker sets `SQE_WORKER__ADVERTISE_URL` to its compose hostname, so tickets addressed to `worker-N:50052` validate.

Verified: the rig comes up, and with `distribution_threshold = "0"` scans are dispatched to the workers (`sqe_scheduler_decisions_total`). That run is how #831 was found.

**NEXT:** #831 (worker reshape plus `initial_default`).
