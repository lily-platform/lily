//! OpenTelemetry Exponential Histogram Metrics Schema
//!
//! Exponential histogram metrics stored in ClickHouse.

use lily_clickhouse::{ClickhouseSchema, DatabaseService};
use lily_injectable_derive::Injectable;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// OpenTelemetry Exponential Histogram Metric
///
/// Maps to `otel.otel_metrics_exponential_histogram` table in ClickHouse.
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row, ClickhouseSchema)]
#[clickhouse(
    table = "otel_metrics_exponential_histogram",
    order_by = "ServiceName, MetricName, TimeUnix",
    engine = "MergeTree()"
)]
pub struct OtelMetricExponentialHistogram {
    #[serde(rename = "ResourceAttributes")]
    #[clickhouse(type = "Map(String, String)")]
    pub resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    #[clickhouse(type = "Map(String, String)")]
    pub scope_attributes: Vec<(String, String)>,
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
    #[clickhouse(type = "Map(String, String)")]
    pub attributes: Vec<(String, String)>,
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
    #[clickhouse(type = "Array(Map(String, String))")]
    pub exemplars_filtered_attributes: Vec<Vec<(String, String)>>,
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

/// Table abstraction for OtelMetricExponentialHistogram
#[derive(Injectable, lily_clickhouse_derive::ClickhouseTable, Default)]
#[entity_type(OtelMetricExponentialHistogram)]
#[service(lifetime = "Singleton")]
pub struct OtelMetricExponentialHistogramTable {
    #[inject]
    pub db: Arc<DatabaseService>,
}

/// Repository for OtelMetricExponentialHistogram CRUD operations
#[derive(Injectable, lily_clickhouse_derive::ClickhouseRepository, Default)]
#[table_type(OtelMetricExponentialHistogramTable)]
#[entity_type(OtelMetricExponentialHistogram)]
#[service(lifetime = "Singleton")]
pub struct OtelMetricExponentialHistogramRepository {
    #[inject]
    pub table: Arc<OtelMetricExponentialHistogramTable>,
}
