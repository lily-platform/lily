use lily_clickhouse_derive::ClickhouseSchema;

#[derive(ClickhouseSchema)]
#[clickhouse(table = "events", order_by = "id", engine = "MergeTree()")]
struct AuditEvent {
    #[clickhouse(type = "String); DROP TABLE users; --")]
    id: u64,
}

fn main() {}
