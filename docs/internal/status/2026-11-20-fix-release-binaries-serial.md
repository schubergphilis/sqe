**STATUS 2026-11-20: release-binary jobs run one arch at a time, one binary per link (`fix/release-binaries-serial`).**

The v0.55.1 tag pipeline never produced binaries. Both runs put the amd64 and
arm64 `sqe-release-binaries` jobs on the same runner host (run05, then run01).
Both went silent once the final thin-LTO link started, and the retry ended
with `no_updates_running` after 60 minutes, meaning the runner stopped reporting
rather than a timeout. !1866 raised the job limit to 180 minutes; that was
needed (a native release link alone is silent for 33 minutes) but it does not
stop two links from sharing a host.

- `resource_group: sqe-release-binaries` runs the two arch jobs one after the other.
- `scripts/ci-release-binaries.sh` builds `sqe-cli`, then `sqe-server`, so two
  LTO links never overlap inside one job, and prints cgroup memory every 60 s.
- The `ci::multiarch-release` MR label runs the dry run with the release profile.

Cause not proven: memory exhaustion is the likely reading, and the `[mem]` lines
of the next run will confirm or refute it.

**NEXT:** label this MR `ci::multiarch-release`, check both arches go green and read
the `[mem]` peak, merge, then release v0.55.2.
