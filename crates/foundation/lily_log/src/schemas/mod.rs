//! OpenTelemetry Schemas Module
//!
//! This module contains all OTEL table schemas with their corresponding
//! Table and Repository implementations.

pub mod otel_logs;
pub mod otel_metrics_exponential_histogram;
pub mod otel_metrics_gauge;
pub mod otel_metrics_histogram;
pub mod otel_metrics_sum;
pub mod otel_metrics_summary;
pub mod otel_traces;

// Re-export all entities, tables, and repositories
pub use otel_logs::{OtelLog, OtelLogRepository, OtelLogTable};
pub use otel_metrics_exponential_histogram::{
    OtelMetricExponentialHistogram, OtelMetricExponentialHistogramRepository,
    OtelMetricExponentialHistogramTable,
};
pub use otel_metrics_gauge::{OtelMetricGauge, OtelMetricGaugeRepository, OtelMetricGaugeTable};
pub use otel_metrics_histogram::{
    OtelMetricHistogram, OtelMetricHistogramRepository, OtelMetricHistogramTable,
};
pub use otel_metrics_sum::{OtelMetricSum, OtelMetricSumRepository, OtelMetricSumTable};
pub use otel_metrics_summary::{
    OtelMetricSummary, OtelMetricSummaryRepository, OtelMetricSummaryTable,
};
pub use otel_traces::{OtelTrace, OtelTraceRepository, OtelTraceTable};
