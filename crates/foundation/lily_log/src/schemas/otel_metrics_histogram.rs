//! OpenTelemetry Histogram Metrics Schema
//!
//! Histogram metrics stored in ClickHouse.

use lily_clickhouse::{ClickhouseSchema, DatabaseService};
use lily_injectable_derive::Injectable;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// OpenTelemetry Histogram Metric
///
/// Maps to `otel.otel_metrics_histogram` table in ClickHouse.
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row, ClickhouseSchema)]
#[clickhouse(
    table = "otel_metrics_histogram",
    order_by = "ServiceName, MetricName, TimeUnix",
    engine = "MergeTree()"
)]
pub struct OtelMetricHistogram {
    #[serde(rename = "ResourceAttributes")]
    #[clickhouse(type = "Map(LowCardinality(String), String)")]
    pub resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ResourceSchemaUrl")]
    pub resource_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    #[clickhouse(type = "Map(LowCardinality(String), String)")]
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
    #[clickhouse(type = "Map(LowCardinality(String), String)")]
    pub attributes: Vec<(String, String)>,
    #[serde(rename = "StartTimeUnix")]
    #[clickhouse(type = "DateTime64(9)")]
    pub start_time_unix: i64,
    #[serde(rename = "TimeUnix")]
    #[clickhouse(type = "DateTime64(9)")]
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
    #[clickhouse(type = "Array(Map(LowCardinality(String), String))")]
    pub exemplars_filtered_attributes: Vec<Vec<(String, String)>>,
    #[serde(rename = "Exemplars.TimeUnix")]
    #[clickhouse(type = "Array(DateTime64(9))")]
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

/// Table abstraction for OtelMetricHistogram
#[derive(Injectable, lily_clickhouse_derive::ClickhouseTable, Default)]
#[entity_type(OtelMetricHistogram)]
#[service(lifetime = "Singleton")]
pub struct OtelMetricHistogramTable {
    #[inject]
    pub db: Arc<DatabaseService>,
}

/// Repository for OtelMetricHistogram CRUD operations
#[derive(Injectable, lily_clickhouse_derive::ClickhouseRepository, Default)]
#[table_type(OtelMetricHistogramTable)]
#[entity_type(OtelMetricHistogram)]
#[service(lifetime = "Singleton")]
pub struct OtelMetricHistogramRepository {
    #[inject]
    pub table: Arc<OtelMetricHistogramTable>,
}
