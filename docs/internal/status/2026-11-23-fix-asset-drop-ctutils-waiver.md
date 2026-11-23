# fix/asset-drop-ctutils-waiver

**Status:** MR open. Removes the expired `reputation-unresolved` waiver for
`cargo/ctutils` from `.asset`.

- The waiver was added for ctutils 0.4.5 while deps.dev had not indexed it; it
  expired on 2026-11-06.
- `Cargo.lock` pins ctutils 0.4.8. deps.dev returns that version (published
  2026-11-02, Apache-2.0 OR MIT, RustCrypto/utils).
- The last aikido-reputation run (on the #937 MR, waiver already expired)
  reported 0 unresolved packages; the only ctutils line was the expiry warning.
- No CI job definition changed. aikido-reputation runs only when `Cargo.lock`
  changes, so this MR's pipeline does not exercise it.

**NEXT:** the next `Cargo.lock` MR should show aikido-reputation without the
expired-suppression warning. Its 72 very-low findings are pre-existing and
advisory.
