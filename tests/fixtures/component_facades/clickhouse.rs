use runtime::{
    ClickhouseRepository, ClickhouseSchema, ClickhouseSchemaProvider, ClickhouseTable,
    DatabaseService,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize, clickhouse::Row, ClickhouseSchema)]
#[clickhouse(table = "events", order_by = "id")]
struct Event {
    id: u64,
    message: String,
}

#[derive(Default, ClickhouseTable)]
#[entity_type(Event)]
#[cfg_attr(feature = "factory", cell_name("analytics"))]
struct EventTable {
    db: Arc<DatabaseService>,
    #[cfg(feature = "factory")]
    clickhouse_factory: Arc<runtime::ClickhouseFactory>,
}

#[derive(Default, ClickhouseRepository)]
#[table_type(EventTable)]
#[entity_type(Event)]
struct EventRepository {
    table: Arc<EventTable>,
}

#[test]
fn all_reexported_derives_preserve_schema_and_migration_metadata() {
    let _repository = EventRepository::default();
    assert_eq!(Event::columns(), &["id", "message"]);
    assert_eq!(EventTable::table_name(), "events");
    assert_eq!(EventTable::columns(), Event::columns());
    let sql = EventTable::migration_sql("app").unwrap();
    assert!(sql.contains("`events`"));
    assert!(sql.contains("`id` UInt64"));
    assert!(sql.contains("ORDER BY (`id`)"));
}
