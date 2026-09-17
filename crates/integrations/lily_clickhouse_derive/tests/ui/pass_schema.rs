extern crate self as lily_clickhouse;

use lily_clickhouse_derive::ClickhouseSchema;

pub trait ClickhouseSchemaProvider {
    fn schema() -> &'static str;
    fn columns() -> &'static [&'static str];
    fn table_name() -> &'static str;
    fn order_by() -> &'static str;
    fn engine() -> &'static str;
}

#[derive(ClickhouseSchema)]
#[clickhouse(table = "audit_events", order_by = "id", engine = "MergeTree()")]
struct AuditEvent {
    id: u64,
    message: String,
}

fn main() {
    assert_eq!(AuditEvent::table_name(), "audit_events");
    assert_eq!(AuditEvent::schema(), "`id` UInt64, `message` String");
    assert_eq!(AuditEvent::columns(), &["id", "message"]);
}
