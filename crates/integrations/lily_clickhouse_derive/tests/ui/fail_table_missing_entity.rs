use lily_clickhouse_derive::ClickhouseTable;

#[derive(ClickhouseTable)]
struct AuditTable {
    db: (),
}

fn main() {}
