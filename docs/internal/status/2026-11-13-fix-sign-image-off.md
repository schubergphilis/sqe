# fix/sign-image-off

`AIKIDO_SIGN` set to `"0"`. The v0.54.0 tag pipeline ran `aikido-sign-image` for the first time (enabled 2026-09-25, skipped on v0.53.0) and it failed: public Fulcio returned HTTP 400 on this GitLab's ID token. Image, Harbor mirror and GitLab Release had already been published, so the release itself is fine; it is unsigned, as every earlier one was. `release.md` gains two sections: signing status, and that CI images are amd64 only.

Not verified: the next tag pipeline skipping the job. It is guarded by `$AIKIDO_SIGN == "1"` in the aikido template at the pinned ref, so it should not be created.

**NEXT:** key-based signing (cosign key in a protected CI variable or KMS, `--tlog-upload=false`), preferably in the aikido template; then set `AIKIDO_SIGN` back to `"1"`. In parallel: aarch64 cross-build spike for a CI multi-arch image.
