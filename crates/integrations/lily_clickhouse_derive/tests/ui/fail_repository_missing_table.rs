use lily_clickhouse_derive::ClickhouseRepository;

struct AuditEvent;

#[derive(ClickhouseRepository)]
#[entity_type(AuditEvent)]
struct AuditRepository {
    table: (),
}

fn main() {}
