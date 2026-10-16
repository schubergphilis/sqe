### Fixed

- A scan memory wait test no longer fails on a loaded CI runner

    `a_later_denial_keeps_the_budget_an_earlier_wait_left` measured its
    300 ms wait budget in real time, so oversleep on a busy runner spent
    the margin and the second admission failed. It now runs on tokio's
    paused clock, where the waits are exact.
