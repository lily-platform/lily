use lily_clickhouse_derive::ClickhouseSchema;

#[derive(ClickhouseSchema)]
#[clickhouse(table = "events;drop", order_by = "id", engine = "MergeTree()")]
struct AuditEvent {
    id: u64,
}

fn main() {}
