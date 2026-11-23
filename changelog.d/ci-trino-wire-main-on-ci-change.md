### Changed

- CI: the Trino wire test job also runs on main when `.gitlab-ci.yml` changes

    The main-branch rule only matched Rust changes, so a CI-only merge never ran the job on main.
