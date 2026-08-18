//! Shared HTTP/2 transport tuning for tonic Server builders.
//!
//! tonic's defaults (64 KB stream window, 64 KB connection window,
//! 16 KB frame size, no TCP keepalive) cap Flight SQL DoGet throughput
//! on multi-GB result sets and let long-lived connections behind NAT
//! get dropped silently. The defaults here lift the windows into the
//! MB range and turn on HTTP/2 + TCP keepalive.

use std::time::Duration;

use sqe_core::config::GrpcTransportConfig;
use tonic::transport::{Endpoint, Server};

/// Apply the configured HTTP/2 + TCP knobs to a tonic Server builder.
pub fn apply_grpc_transport(server: Server, cfg: &GrpcTransportConfig) -> Server {
    let mut s = server
        .initial_stream_window_size(Some(cfg.initial_stream_window_size))
        .initial_connection_window_size(Some(cfg.initial_connection_window_size))
        .max_frame_size(Some(cfg.max_frame_size))
        .http2_keepalive_interval(Some(Duration::from_secs(cfg.http2_keepalive_interval_secs)))
        .http2_keepalive_timeout(Some(Duration::from_secs(cfg.http2_keepalive_timeout_secs)));
    if cfg.tcp_keepalive_secs > 0 {
        s = s.tcp_keepalive(Some(Duration::from_secs(cfg.tcp_keepalive_secs)));
    }
    s
}

/// Apply the same HTTP/2 windows to an outbound `Endpoint`.
///
/// The server-side twin above governs client -> coordinator (Flight SQL).
/// This one governs coordinator -> worker, which carries every distributed
/// scan result, and it was left on tonic's defaults: `init_stream_window_size`
/// and `init_connection_window_size` are both `None`, so h2 falls back to
/// 65,535 bytes for each.
///
/// A receive window caps single-stream throughput at `window / RTT`
/// regardless of link bandwidth. At 64 KB that is ~64 MB/s on a 1 ms LAN
/// hop and ~6 MB/s across a 10 ms one, and because the *connection* window
/// is shared, every scan fragment multiplexed onto one pooled channel to a
/// worker draws from the same 64 KB. The configured defaults (8 MB stream,
/// 16 MB connection) move that ceiling out by two orders of magnitude.
///
/// Loopback rigs barely show this: at ~50 us RTT even a 64 KB window clears
/// 1 GB/s. It is a LAN and WAN fix, which is why it matters for the
/// separate-host SF10/SF100 qualification (#416) and not for a laptop A/B.
pub fn apply_grpc_transport_endpoint(endpoint: Endpoint, cfg: &GrpcTransportConfig) -> Endpoint {
    endpoint
        .initial_stream_window_size(Some(cfg.initial_stream_window_size))
        .initial_connection_window_size(Some(cfg.initial_connection_window_size))
}
