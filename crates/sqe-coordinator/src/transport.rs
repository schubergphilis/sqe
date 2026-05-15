//! Shared HTTP/2 transport tuning for tonic Server builders.
//!
//! tonic's defaults (64 KB stream window, 64 KB connection window,
//! 16 KB frame size, no TCP keepalive) cap Flight SQL DoGet throughput
//! on multi-GB result sets and let long-lived connections behind NAT
//! get dropped silently. The defaults here lift the windows into the
//! MB range and turn on HTTP/2 + TCP keepalive.

use std::time::Duration;

use sqe_core::config::GrpcTransportConfig;
use tonic::transport::Server;

/// Apply the configured HTTP/2 + TCP knobs to a tonic Server builder.
pub fn apply_grpc_transport(server: Server, cfg: &GrpcTransportConfig) -> Server {
    let mut s = server
        .initial_stream_window_size(Some(cfg.initial_stream_window_size))
        .initial_connection_window_size(Some(cfg.initial_connection_window_size))
        .max_frame_size(Some(cfg.max_frame_size))
        .http2_keepalive_interval(Some(Duration::from_secs(
            cfg.http2_keepalive_interval_secs,
        )))
        .http2_keepalive_timeout(Some(Duration::from_secs(
            cfg.http2_keepalive_timeout_secs,
        )));
    if cfg.tcp_keepalive_secs > 0 {
        s = s.tcp_keepalive(Some(Duration::from_secs(cfg.tcp_keepalive_secs)));
    }
    s
}
