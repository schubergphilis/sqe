pub mod advertise;
pub mod bootstrap;
pub mod compaction;
pub mod config_reload;
pub mod credential_channel;
pub mod executor;
pub mod flight_service;
pub mod heartbeat;
pub mod hive_partition;
pub mod hive_reader;
pub mod operator_memory;
pub mod runtime;
/// Shuffle receivers, attempt gating and partitioners for the DoExchange
/// intake. Gated behind `worker-shuffle`: no query plan produces shuffle
/// traffic, so by default the worker carries no registry keyed by a
/// network-supplied query_id/stage_id (review finding 600).
#[cfg(feature = "worker-shuffle")]
pub mod shuffle;
/// Spillable partition buffer backing the DoExchange intake. Gated behind
/// `worker-shuffle` for the same reason as [`shuffle`].
#[cfg(feature = "worker-shuffle")]
pub mod spill_buffer;
pub mod ticket_pin;
