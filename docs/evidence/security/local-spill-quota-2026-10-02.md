# Local spill quota enforcement — #797

Base: `7be79b90`. The baseline failed both new admission regressions: opening concurrent writers did not reserve quota, and a batch larger than the quota reached disk successfully.

The local backend now holds an atomic reservation throughout each writer's lifetime and admits serialized IPC bytes before filesystem writes. Publication settles the reservation to the file size. Failed writes cannot later publish a truncated segment. Failed/cancelled writers remove partial files and refund capacity; scope deletion excludes active reservations to avoid double refunds. Cleanup failures retain the charge for the orphan.

Validation: all 52 `sqe-spill` library tests pass, including five new regressions for admission, growth, settlement, cancellation/scope deletion and failed publication. `cargo clippy -p sqe-spill --tests -- -D warnings` passes. Existing vendor warnings are outside the changed crate. Local builds use the temporary macOS SDK linker wrapper; no build configuration changed.

This bounds serialized local segment bytes, including open writers. It does not introduce a process RSS limit or alter DataFusion's separate spill manager. SF10 integration qualification is tracked separately; SF100 is deferred at the user's request.
