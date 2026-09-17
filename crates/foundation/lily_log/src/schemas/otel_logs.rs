//! OpenTelemetry Logs Schema
//!
//! Application logs with structured attributes stored in ClickHouse.

use lily_clickhouse::{ClickhouseSchema, DatabaseService};
use lily_injectable_derive::Injectable;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// OpenTelemetry Log Record
///
/// Maps to `otel.otel_logs` table in ClickHouse.
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row, ClickhouseSchema)]
#[clickhouse(
    table = "otel_logs",
    order_by = "Timestamp, ServiceName",
    engine = "MergeTree()"
)]
pub struct OtelLog {
    #[serde(rename = "Timestamp")]
    #[clickhouse(type = "DateTime64(9)")]
    pub timestamp: i64,

    #[serde(rename = "TimestampTime")]
    #[clickhouse(type = "DateTime")]
    pub timestamp_time: u32,
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
    #[clickhouse(type = "Map(LowCardinality(String), String)")]
    pub resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeSchemaUrl")]
    pub scope_schema_url: String,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "ScopeAttributes")]
    #[clickhouse(type = "Map(LowCardinality(String), String)")]
    pub scope_attributes: Vec<(String, String)>,
    #[serde(rename = "LogAttributes")]
    #[clickhouse(type = "Map(LowCardinality(String), String)")]
    pub log_attributes: Vec<(String, String)>,
}

/// Table abstraction for OtelLog
#[derive(Injectable, lily_clickhouse_derive::ClickhouseTable, Default)]
#[entity_type(OtelLog)]
#[service(lifetime = "Singleton")]
pub struct OtelLogTable {
    #[inject]
    pub db: Arc<DatabaseService>,
}

/// Repository for OtelLog CRUD operations
#[derive(Injectable, lily_clickhouse_derive::ClickhouseRepository, Default)]
#[table_type(OtelLogTable)]
#[entity_type(OtelLog)]
#[service(lifetime = "Singleton")]
pub struct OtelLogRepository {
    #[inject]
    pub table: Arc<OtelLogTable>,
}
