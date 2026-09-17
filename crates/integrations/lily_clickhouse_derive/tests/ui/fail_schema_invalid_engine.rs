use lily_clickhouse_derive::ClickhouseSchema;

#[derive(ClickhouseSchema)]
#[clickhouse(table = "events", order_by = "id", engine = "UnsafeEngine()")]
struct AuditEvent {
    id: u64,
}

fn main() {}
