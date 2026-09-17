//! OpenTelemetry Entity Definitions
//!
//! This module contains Rust struct definitions for all OTEL tables in ClickHouse.
//! These are pure data structures without any repository logic.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// OpenTelemetry Trace Span
///
/// Maps to `otel.otel_traces` table in ClickHouse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtelTrace {
    #[serde(rename = "Timestamp")]
    pub timestamp: i64,
    #[serde(rename = "TraceId")]
    pub trace_id: String,
    #[serde(rename = "SpanId")]
    pub span_id: String,
    #[serde(rename = "ParentSpanId")]
    pub parent_span_id: String,
    #[serde(rename = "TraceState")]
    pub trace_state: String,
    #[serde(rename = "SpanName")]
    pub span_name: String,
    #[serde(rename = "SpanKind")]
    pub span_kind: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: HashMap<String, String>,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "SpanAttributes")]
    pub span_attributes: HashMap<String, String>,
    #[serde(rename = "Duration")]
    pub duration: u64,
    #[serde(rename = "StatusCode")]
    pub status_code: String,
    #[serde(rename = "StatusMessage")]
    pub status_message: String,
    #[serde(rename = "Events.Timestamp")]
    pub events_timestamp: Vec<i64>,
    #[serde(rename = "Events.Name")]
    pub events_name: Vec<String>,
    #[serde(rename = "Events.Attributes")]
    pub events_attributes: Vec<HashMap<String, String>>,
    #[serde(rename = "Links.TraceId")]
    pub links_trace_id: Vec<String>,
    #[serde(rename = "Links.SpanId")]
    pub links_span_id: Vec<String>,
    #[serde(rename = "Links.TraceState")]
    pub links_trace_state: Vec<String>,
    #[serde(rename = "Links.Attributes")]
    pub links_attributes: Vec<HashMap<String, String>>,
}

/// OpenTelemetry Log Record
///
/// Maps to `otel.otel_logs` table in ClickHouse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtelLog {
    #[serde(rename = "Timestamp")]
    pub timestamp: i64,
    #[serde(rename = "TimestampTime")]
    pub timestamp_time: i64,
    #[serde(rename = "TraceId")]
    pub trace_id: String,
    #[serde(rename = "SpanId")]
    pub span_id: String,
    #[serde(rename = "TraceFlags")]
    pub trace_flags: u8,
    #[serde(rename = "SeverityText")]
    pub severity_text: String,
    #[serde(rename = "SeverityNumber")]
    pub severity_number: u8,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "Body")]
    pub body: String,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: HashMap<String, String>,
    #[serde(rename = "ScopeSchemaUrl")]
    pub scope_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    pub scope_attributes: HashMap<String, String>,
    #[serde(rename = "LogAttributes")]
    pub log_attributes: HashMap<String, String>,
}

/// OpenTelemetry Sum Metric
///
/// Maps to `otel.otel_metrics_sum` table in ClickHouse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtelMetricSum {
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: HashMap<String, String>,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    pub scope_attributes: HashMap<String, String>,
    #[serde(rename = "ScopeDroppedAttrCount")]
    pub scope_dropped_attr_count: u32,
    #[serde(rename = "ScopeSchemaUrl")]
    pub scope_schema_url: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "MetricName")]
    pub metric_name: String,
    #[serde(rename = "MetricDescription")]
    pub metric_description: String,
    #[serde(rename = "MetricUnit")]
    pub metric_unit: String,
    #[serde(rename = "Attributes")]
    pub attributes: HashMap<String, String>,
    #[serde(rename = "StartTimeUnix")]
    pub start_time_unix: i64,
    #[serde(rename = "TimeUnix")]
    pub time_unix: i64,
    #[serde(rename = "Value")]
    pub value: f64,
    #[serde(rename = "Flags")]
    pub flags: u32,
    #[serde(rename = "Exemplars.FilteredAttributes")]
    pub exemplars_filtered_attributes: Vec<HashMap<String, String>>,
    #[serde(rename = "Exemplars.TimeUnix")]
    pub exemplars_time_unix: Vec<i64>,
    #[serde(rename = "Exemplars.Value")]
    pub exemplars_value: Vec<f64>,
    #[serde(rename = "Exemplars.SpanId")]
    pub exemplars_span_id: Vec<String>,
    #[serde(rename = "Exemplars.TraceId")]
    pub exemplars_trace_id: Vec<String>,
    #[serde(rename = "AggregationTemporality")]
    pub aggregation_temporality: i32,
    #[serde(rename = "IsMonotonic")]
    pub is_monotonic: bool,
}

/// OpenTelemetry Gauge Metric
///
/// Maps to `otel.otel_metrics_gauge` table in ClickHouse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtelMetricGauge {
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: HashMap<String, String>,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    pub scope_attributes: HashMap<String, String>,
    #[serde(rename = "ScopeDroppedAttrCount")]
    pub scope_dropped_attr_count: u32,
    #[serde(rename = "ScopeSchemaUrl")]
    pub scope_schema_url: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "MetricName")]
    pub metric_name: String,
    #[serde(rename = "MetricDescription")]
    pub metric_description: String,
    #[serde(rename = "MetricUnit")]
    pub metric_unit: String,
    #[serde(rename = "Attributes")]
    pub attributes: HashMap<String, String>,
    #[serde(rename = "StartTimeUnix")]
    pub start_time_unix: i64,
    #[serde(rename = "TimeUnix")]
    pub time_unix: i64,
    #[serde(rename = "Value")]
    pub value: f64,
    #[serde(rename = "Flags")]
    pub flags: u32,
    #[serde(rename = "Exemplars.FilteredAttributes")]
    pub exemplars_filtered_attributes: Vec<HashMap<String, String>>,
    #[serde(rename = "Exemplars.TimeUnix")]
    pub exemplars_time_unix: Vec<i64>,
    #[serde(rename = "Exemplars.Value")]
    pub exemplars_value: Vec<f64>,
    #[serde(rename = "Exemplars.SpanId")]
    pub exemplars_span_id: Vec<String>,
    #[serde(rename = "Exemplars.TraceId")]
    pub exemplars_trace_id: Vec<String>,
}

/// OpenTelemetry Histogram Metric
///
/// Maps to `otel.otel_metrics_histogram` table in ClickHouse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtelMetricHistogram {
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: HashMap<String, String>,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    pub scope_attributes: HashMap<String, String>,
    #[serde(rename = "ScopeDroppedAttrCount")]
    pub scope_dropped_attr_count: u32,
    #[serde(rename = "ScopeSchemaUrl")]
    pub scope_schema_url: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "MetricName")]
    pub metric_name: String,
    #[serde(rename = "MetricDescription")]
    pub metric_description: String,
    #[serde(rename = "MetricUnit")]
    pub metric_unit: String,
    #[serde(rename = "Attributes")]
    pub attributes: HashMap<String, String>,
    #[serde(rename = "StartTimeUnix")]
    pub start_time_unix: i64,
    #[serde(rename = "TimeUnix")]
    pub time_unix: i64,
    #[serde(rename = "Count")]
    pub count: u64,
    #[serde(rename = "Sum")]
    pub sum: f64,
    #[serde(rename = "BucketCounts")]
    pub bucket_counts: Vec<u64>,
    #[serde(rename = "ExplicitBounds")]
    pub explicit_bounds: Vec<f64>,
    #[serde(rename = "Exemplars.FilteredAttributes")]
    pub exemplars_filtered_attributes: Vec<HashMap<String, String>>,
    #[serde(rename = "Exemplars.TimeUnix")]
    pub exemplars_time_unix: Vec<i64>,
    #[serde(rename = "Exemplars.Value")]
    pub exemplars_value: Vec<f64>,
    #[serde(rename = "Exemplars.SpanId")]
    pub exemplars_span_id: Vec<String>,
    #[serde(rename = "Exemplars.TraceId")]
    pub exemplars_trace_id: Vec<String>,
    #[serde(rename = "Flags")]
    pub flags: u32,
    #[serde(rename = "Min")]
    pub min: f64,
    #[serde(rename = "Max")]
    pub max: f64,
    #[serde(rename = "AggregationTemporality")]
    pub aggregation_temporality: i32,
}

/// OpenTelemetry Summary Metric
///
/// Maps to `otel.otel_metrics_summary` table in ClickHouse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtelMetricSummary {
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: HashMap<String, String>,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    pub scope_attributes: HashMap<String, String>,
    #[serde(rename = "ScopeDroppedAttrCount")]
    pub scope_dropped_attr_count: u32,
    #[serde(rename = "ScopeSchemaUrl")]
    pub scope_schema_url: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "MetricName")]
    pub metric_name: String,
    #[serde(rename = "MetricDescription")]
    pub metric_description: String,
    #[serde(rename = "MetricUnit")]
    pub metric_unit: String,
    #[serde(rename = "Attributes")]
    pub attributes: HashMap<String, String>,
    #[serde(rename = "StartTimeUnix")]
    pub start_time_unix: i64,
    #[serde(rename = "TimeUnix")]
    pub time_unix: i64,
    #[serde(rename = "Count")]
    pub count: u64,
    #[serde(rename = "Sum")]
    pub sum: f64,
    #[serde(rename = "ValueAtQuantiles.Quantile")]
    pub value_at_quantiles_quantile: Vec<f64>,
    #[serde(rename = "ValueAtQuantiles.Value")]
    pub value_at_quantiles_value: Vec<f64>,
    #[serde(rename = "Flags")]
    pub flags: u32,
}

/// OpenTelemetry Exponential Histogram Metric
///
/// Maps to `otel.otel_metrics_exponential_histogram` table in ClickHouse.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtelMetricExponentialHistogram {
    #[serde(rename = "ResourceAttributes")]
    pub resource_attributes: HashMap<String, String>,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    pub scope_attributes: HashMap<String, String>,
    #[serde(rename = "ScopeDroppedAttrCount")]
    pub scope_dropped_attr_count: u32,
    #[serde(rename = "ScopeSchemaUrl")]
    pub scope_schema_url: String,
    #[serde(rename = "ServiceName")]
    pub service_name: String,
    #[serde(rename = "MetricName")]
    pub metric_name: String,
    #[serde(rename = "MetricDescription")]
    pub metric_description: String,
    #[serde(rename = "MetricUnit")]
    pub metric_unit: String,
    #[serde(rename = "Attributes")]
    pub attributes: HashMap<String, String>,
    #[serde(rename = "StartTimeUnix")]
    pub start_time_unix: i64,
    #[serde(rename = "TimeUnix")]
    pub time_unix: i64,
    #[serde(rename = "Count")]
    pub count: u64,
    #[serde(rename = "Sum")]
    pub sum: f64,
    #[serde(rename = "Scale")]
    pub scale: i32,
    #[serde(rename = "ZeroCount")]
    pub zero_count: u64,
    #[serde(rename = "PositiveOffset")]
    pub positive_offset: i32,
    #[serde(rename = "PositiveBucketCounts")]
    pub positive_bucket_counts: Vec<u64>,
    #[serde(rename = "NegativeOffset")]
    pub negative_offset: i32,
    #[serde(rename = "NegativeBucketCounts")]
    pub negative_bucket_counts: Vec<u64>,
    #[serde(rename = "Exemplars.FilteredAttributes")]
    pub exemplars_filtered_attributes: Vec<HashMap<String, String>>,
    #[serde(rename = "Exemplars.TimeUnix")]
    pub exemplars_time_unix: Vec<i64>,
    #[serde(rename = "Exemplars.Value")]
    pub exemplars_value: Vec<f64>,
    #[serde(rename = "Exemplars.SpanId")]
    pub exemplars_span_id: Vec<String>,
    #[serde(rename = "Exemplars.TraceId")]
    pub exemplars_trace_id: Vec<String>,
    #[serde(rename = "Flags")]
    pub flags: u32,
    #[serde(rename = "Min")]
    pub min: f64,
    #[serde(rename = "Max")]
    pub max: f64,
    #[serde(rename = "AggregationTemporality")]
    pub aggregation_temporality: i32,
}
