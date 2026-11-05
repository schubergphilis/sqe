**STATUS 2026-11-04: Quack error routing uses `error_code()` (`fix/quack-storage-refusal-policy`, follow-up to !1644 / #880).**

!1644 refined `SqeError::error_code()` so a `Sourced { ExecutionFailed }` whose text carries the store's refusal (`PermissionDenied`, `AccessDenied`) reports `AccessDenied`. `quack_executor::sqe_error_to_query_error` matched the raw `Sourced.code`, so over Quack the same refusal went out as `SQE-EXEC: Close writer error: ...` and skipped the Policy masking. The `Sourced` arm now matches `err.error_code()`. An engine-credential failure (`ExpiredToken`, ...) refines to `StorageCredentialsUnavailable` and stays Execution.

Gates: `cargo test -p sqe-coordinator --lib -- quack_executor` 11 passed (new: `storage_refusal_routes_on_the_refined_code`); clippy `--tests -D warnings` clean.

**NEXT:** #880 part 1: refuse DELETE / UPDATE / MERGE before any file is written when Polaris would refuse the commit.
