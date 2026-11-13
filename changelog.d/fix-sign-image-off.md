### Changed

- CI: release image signing is off until it signs with a key

    Keyless `cosign sign` against public Sigstore cannot use this GitLab's ID token, so `aikido-sign-image` failed the v0.54.0 tag pipeline after the image and Release were published. No release image was ever signed. `release.md` records why and what turns it back on.
