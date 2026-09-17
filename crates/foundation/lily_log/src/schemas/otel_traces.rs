//! OpenTelemetry Traces Schema
//!
//! Distributed tracing data stored in ClickHouse.

use lily_clickhouse::{ClickhouseSchema, DatabaseService};
use lily_injectable_derive::Injectable;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// OpenTelemetry Trace Span
///
/// Maps to `otel.otel_traces` table in ClickHouse.
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row, ClickhouseSchema)]
#[clickhouse(
    table = "otel_traces",
    order_by = "TraceId, Timestamp",
    engine = "MergeTree()"
)]
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
    #[clickhouse(type = "Map(String, String)")]
    pub resource_attributes: Vec<(String, String)>,
    #[serde(rename = "ScopeName")]
    pub scope_name: String,
    #[serde(rename = "ScopeVersion")]
    pub scope_version: String,
    #[serde(rename = "SpanAttributes")]
    #[clickhouse(type = "Map(String, String)")]
    pub span_attributes: Vec<(String, String)>,
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
    #[clickhouse(type = "Array(Map(String, String))")]
    pub events_attributes: Vec<Vec<(String, String)>>,
    #[serde(rename = "Links.TraceId")]
    pub links_trace_id: Vec<String>,
    #[serde(rename = "Links.SpanId")]
    pub links_span_id: Vec<String>,
    #[serde(rename = "Links.TraceState")]
    pub links_trace_state: Vec<String>,
    #[serde(rename = "Links.Attributes")]
    #[clickhouse(type = "Array(Map(String, String))")]
    pub links_attributes: Vec<Vec<(String, String)>>,
}

/// Table abstraction for OtelTrace
#[derive(Injectable, lily_clickhouse_derive::ClickhouseTable, Default)]
#[entity_type(OtelTrace)]
#[service(lifetime = "Singleton")]
pub struct OtelTraceTable {
    #[inject]
    pub db: Arc<DatabaseService>,
}

/// Repository for OtelTrace CRUD operations
#[derive(Injectable, lily_clickhouse_derive::ClickhouseRepository, Default)]
#[table_type(OtelTraceTable)]
#[entity_type(OtelTrace)]
#[service(lifetime = "Singleton")]
pub struct OtelTraceRepository {
    #[inject]
    pub table: Arc<OtelTraceTable>,
}
