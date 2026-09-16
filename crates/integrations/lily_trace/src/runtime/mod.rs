//! Configuration, ownership, shutdown, and exporter outcome types.

mod config;
mod correlation;
#[cfg(test)]
mod correlation_tests;
mod file_worker;
mod json_format;
mod method_format;
mod metric_reader;
mod otlp_log_layer;
mod otlp_span_processor;
mod subscriber;
pub(crate) mod tasks;
pub(crate) use subscriber::reconcile_shutdown_before;

pub use config::*;
pub use otlp_log_layer::{LogExportMetricSnapshot, LogExportReport};
pub use otlp_span_processor::{SpanExportMetricSnapshot, SpanExportReport};
pub use subscriber::{
    tracing_runtime_status, ExportTaskShutdownStatus, FileExportInitError,
    FileExportMetricSnapshot, FileExportShutdownStatus, SpanExportTaskShutdownStatus,
    TraceInstallError, TraceInstallOutcome, TraceShutdownReport, TracingRuntimeOwner,
    TracingRuntimeStatus,
};
