//! One integration-test binary for sqe-worker. Add new tests as a module
//! here, not as `tests/*.rs`: each extra file is another link of DataFusion.
//!
//! `protocol`: worker protocol scenarios (review finding 611 / #618):
//! registration, credential refresh, error classification and cancellation,
//! end to end through the Flight service, with no external services.
//! `slow_consumer` / `zero_pruning_memory`: the bounded-memory gates. Their
//! assertions are on per-test pools and budgets, not process RSS (which is
//! only recorded), so sharing the process with other tests does not skew them.

mod common;

mod hive_partition_materialize_test;
mod protocol;
mod slow_consumer;
mod zero_pruning_memory;
