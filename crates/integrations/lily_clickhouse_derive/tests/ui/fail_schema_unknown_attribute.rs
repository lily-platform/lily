use lily_clickhouse_derive::ClickhouseSchema;

#[derive(ClickhouseSchema)]
#[clickhouse(unknown = "value")]
struct AuditEvent {
    id: u64,
}

fn main() {}
