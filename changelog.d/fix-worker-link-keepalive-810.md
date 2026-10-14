### Fixed

- The coordinator-to-worker link uses the configured keepalive and frame size (#810)

    The pooled worker channel set HTTP/2 keepalive to a hard-coded 30 s
    ping with a 30 s timeout. It ignored `[coordinator.transport]`, had no
    TCP keepalive and kept h2's 16 KB frames. It now takes
    `http2_keepalive_interval_secs`, `http2_keepalive_timeout_secs`,
    `tcp_keepalive_secs` and `max_frame_size` from the same section the
    Flight SQL server uses. With the defaults, an unanswered ping drops the
    link after 10 s instead of 30 s.
