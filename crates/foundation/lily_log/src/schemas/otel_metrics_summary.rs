//! OpenTelemetry Summary Metrics Schema
//!
//! Summary metrics stored in ClickHouse.

use lily_clickhouse::{ClickhouseSchema, DatabaseService};
use lily_injectable_derive::Injectable;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// OpenTelemetry Summary Metric
///
/// Maps to `otel.otel_metrics_summary` table in ClickHouse.
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row, ClickhouseSchema)]
#[clickhouse(
    table = "otel_metrics_summary",
    order_by = "ServiceName, MetricName, TimeUnix",
    engine = "MergeTree()"
)]
pub struct OtelMetricSummary {
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
    #[serde(rename = "ValueAtQuantiles.Quantile")]
    pub value_at_quantiles_quantile: Vec<f64>,
    #[serde(rename = "ValueAtQuantiles.Value")]
    pub value_at_quantiles_value: Vec<f64>,
    #[serde(rename = "Flags")]
    pub flags: u32,
}

/// Table abstraction for OtelMetricSummary
#[derive(Injectable, lily_clickhouse_derive::ClickhouseTable, Default)]
#[entity_type(OtelMetricSummary)]
#[service(lifetime = "Singleton")]
pub struct OtelMetricSummaryTable {
    #[inject]
    pub db: Arc<DatabaseService>,
}

/// Repository for OtelMetricSummary CRUD operations
#[derive(Injectable, lily_clickhouse_derive::ClickhouseRepository, Default)]
#[table_type(OtelMetricSummaryTable)]
#[entity_type(OtelMetricSummary)]
#[service(lifetime = "Singleton")]
pub struct OtelMetricSummaryRepository {
    #[inject]
    pub table: Arc<OtelMetricSummaryTable>,
}
