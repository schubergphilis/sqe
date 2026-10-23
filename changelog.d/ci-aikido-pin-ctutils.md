### Fixed

- The license gate passes again on `ctutils` 0.4.5

    ctutils 0.4.5 (the RUSTSEC-2026-0385 fix) was published before deps.dev
    indexed it, so the license check failed with "license evidence missing"
    on main and every merge request. The shared CI pin moves to the aikido
    revision that records its reviewed license for 0.4.x (Apache-2.0 OR MIT).
