**STATUS 2026-11-20: release binaries get a 180-minute job limit (`fix/release-binaries-timeout`).**

v0.55.1 fixed the zig open-file error, but both `sqe-release-binaries` jobs then
ran into their 90-minute limit. Compilation took 20 to 40 minutes on the shared
runner, and the final thin-LTO link of `sqe-server` and `sqe-cli` was still
running more than 50 minutes later, with no output. MR pipelines never caught
this because they build with `dev-release`, which has no LTO. The job limit is
now 180 minutes.

**NEXT:** tag v0.55.2 from main once this merges, and record how long the link
takes. If it is close to the limit, test `codegen-units` or LTO changes for the
release profile, measured against query performance.
