### Fixed

- The `ci-deps-image` job builds again

    The aikido deps-image script pruned the workspace with a Python helper,
    and the deps image has no Python, so the job failed (job 5660962). The
    shared CI pin moves to the aikido revision that prunes with a shell
    script.
