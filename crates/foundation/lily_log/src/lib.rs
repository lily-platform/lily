//! # Lily Log - OpenTelemetry ClickHouse Schema Management
//!
//! This crate provides Rust schema definitions for OpenTelemetry data
//! stored in ClickHouse via OTEL Collector.
//!
//! ## OTEL Tables
//!
//! - **otel_traces**: Distributed tracing data (spans, events, links)
//! - **otel_logs**: Application logs with structured attributes  
//! - **otel_metrics_sum**: Sum/counter metrics
//! - **otel_metrics_gauge**: Gauge metrics
//! - **otel_metrics_histogram**: Histogram metrics with buckets
//! - **otel_metrics_summary**: Summary metrics with quantiles
//! - **otel_metrics_exponential_histogram**: Exponential histogram metrics
//!
//! ## Usage
//!
//! ```rust,ignore
//! use lily_log::schemas::{OtelTrace, OtelTraceTable, OtelTraceRepository};
//! use lily_log::models::traces::{TraceTree, TraceAnalytics};
//! use std::sync::Arc;
//!
//! // Inject repository via DI
//! let repository: Arc<OtelTraceRepository> = container.get_service(None).await?;
//!
//! // Query raw traces
//! let raw_traces: Vec<OtelTrace> = repository.find_all().await?;
//!
//! // Convert to analysis-ready models
//! let trace_tree: TraceTree = raw_traces.clone().into();
//! let analytics: TraceAnalytics = raw_traces.into();
//! ```

pub mod log_service;
pub mod migrations;
pub mod schemas;

// Re-export all schemas for convenience
pub use schemas::*;

// Re-export the scoped analytics surface.
pub use lily_clickhouse::ClickhouseError;
pub use log_service::{AnalyticsPageRequest, AnalyticsQuery, AnalyticsScope, LogService};
pub use migrations::{OtelRetentionPolicy, otel_schema_migrations};
