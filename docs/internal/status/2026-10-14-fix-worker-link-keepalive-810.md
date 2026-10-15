**STATUS 2026-10-14: the coordinator-to-worker channel takes keepalive and frame size from `[coordinator.transport]` (`fix/worker-link-keepalive-810`, #810 item 4).**

`apply_grpc_transport_endpoint` (`transport.rs`) set only the HTTP/2 windows. `ChannelPool::get` then added a hard-coded 30 s keepalive interval and a 30 s timeout. The worker link therefore ignored the configured keepalive, had no TCP keepalive and used h2's 16 KB frames. The endpoint now applies `max_frame_size`, `http2_keepalive_interval_secs`, `http2_keepalive_timeout_secs` and `tcp_keepalive_secs` (0 disables), the same knobs `apply_grpc_transport` gives the server. `ChannelPoolConfig::keep_alive` is gone. The pool-less dispatch path in `distributed_scan.rs` gets the same settings through `GrpcTransportConfig::default()`.

Behaviour change: with the default 10 s ping timeout, a dead worker link is detected after 10 s instead of 30 s.

Tests: new `transport::tests::worker_endpoint_takes_tcp_keepalive_from_config`. The `transport::` and `channel_pool::` tests pass (9). Clippy reports nothing new for `sqe-coordinator`, and `sqe-server` bins check clean.

**NEXT:** #810 items 1-3 are open: measure the single-stream export throughput, then either ship multi-endpoint results or delete `build_flight_info_distributed`.
