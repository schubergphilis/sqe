### Fixed

- Acceptance tests for the signed catalog scope, the `system` catalog split and the shutdown drain (#908, #901, #902)

    These are tests only; behaviour is unchanged.
    - The signed catalog scope is now tested against a test issuer serving a JWKS over https.
    - A Ranger tag mask on the Polaris table `system.access.audit` is now tested end to end, together with the refusal for a user who has no grant.
    - A process test SIGTERMs a running `sqe-server`. It checks that the running query finishes, that a later statement is refused with `draining`, and that the process exits right after.
