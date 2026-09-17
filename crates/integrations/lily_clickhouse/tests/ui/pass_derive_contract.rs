use std::sync::Arc;

use lily_clickhouse::{
    ClickhouseRepository, ClickhouseSchema, ClickhouseTable, DatabaseService,
};
use serde::{Deserialize, Serialize};

#[cfg(feature = "factory")]
use lily_clickhouse::ClickhouseFactory;
use lily_injectable_derive::Injectable;

#[derive(Clone, Serialize, Deserialize, clickhouse::Row, ClickhouseSchema)]
#[clickhouse(
    table = "derive_contract",
    order_by = "event_id",
    engine = "MergeTree()"
)]
struct Event {
    event_id: String,
    status: String,
}

#[cfg(feature = "single")]
#[derive(Injectable, ClickhouseTable, Default)]
#[entity_type(Event)]
#[service(lifetime = "Singleton")]
struct EventTable {
    #[inject]
    db: Arc<DatabaseService>,
}

#[cfg(feature = "factory")]
#[derive(Injectable, ClickhouseTable, Default)]
#[entity_type(Event)]
#[cell_name("analytics")]
#[service(lifetime = "Singleton")]
struct EventTable {
    #[inject]
    clickhouse_factory: Arc<ClickhouseFactory>,
    db: Arc<DatabaseService>,
}

#[derive(Injectable, ClickhouseRepository, Default)]
#[table_type(EventTable)]
#[entity_type(Event)]
#[service(lifetime = "Singleton")]
struct EventRepository {
    #[inject]
    table: Arc<EventTable>,
}

fn assert_generated_contract(repository: &EventRepository) {
    let _count = EventRepository::count;
    let _ = repository;
    let _ = EventTable::table_name();
}

fn main() {}
