### Fixed

- The license gate passes again on `allocator-api2` 0.2.21

    deps.dev has no record of the crate (it comes in through hashbrown),
    so the license check could never find its license and failed with
    "license evidence missing". The shared CI pin moves to the aikido
    revision that records its reviewed license (MIT OR Apache-2.0).
