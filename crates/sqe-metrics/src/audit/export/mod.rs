//! Audit export pipeline: a durable [`cursor`] for resumable shipping, wire
//! [`record`] shaping, an [`otlp`] exporter, and the background [`shipper`].

mod cursor;
mod otlp;
mod record;
mod shipper;

pub use cursor::SeqCursor;
pub use otlp::OtlpExporter;
pub use record::{ocsf_to_ship_record, LogShipExporter, Severity, ShipRecord};
pub use shipper::{OtlpLogShipper, ShipOutcome, ShipperMetrics, StartAt};
