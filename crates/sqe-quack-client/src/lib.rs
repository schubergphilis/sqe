//! Client side of the DuckDB Quack RPC.
//!
//! Mirror of [`sqe_quack_server`]: this crate POSTs `application/vnd.duckdb`
//! requests to a remote `quack_serve()` endpoint (either a real DuckDB
//! instance or another sqe-server) and surfaces the response as Arrow
//! [`RecordBatch`]es.
//!
//! ```no_run
//! use sqe_quack_client::QuackClient;
//!
//! let mut client = QuackClient::connect("quack:localhost:9494", Some("token"))?;
//! let result = client.execute("SELECT 1 AS a, 'hi' AS b")?;
//! for batch in &result.batches {
//!     println!("{} rows, {} cols", batch.num_rows(), batch.num_columns());
//! }
//! # Ok::<_, sqe_quack_client::ClientError>(())
//! ```
//!
//! [`RecordBatch`]: arrow_array::RecordBatch

mod client;

pub use client::{ClientError, QuackClient};
