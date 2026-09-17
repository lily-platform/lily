#![allow(dead_code)]

use std::sync::Arc;

use lily_clickhouse::{ClickhouseRepository, ClickhouseSchema, ClickhouseTable, DatabaseService};
use lily_injectable_derive::Injectable;
use serde::{Deserialize, Serialize};

#[cfg(feature = "factory")]
use lily_clickhouse::ClickhouseFactory;

#[derive(Clone, Serialize, Deserialize, clickhouse::Row, ClickhouseSchema)]
#[clickhouse(
    table = "external_derive_contract",
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

fn generated_contract_is_public() {
    let _count = EventRepository::count;
    let _ = EventTable::table_name();
}

#[cfg(feature = "wrong-service")]
#[derive(ClickhouseTable, Default)]
#[entity_type(Event)]
struct WrongServiceTable {
    db: Arc<String>,
}

#[cfg(feature = "manual-collision")]
#[async_trait::async_trait]
impl lily_injection::ServiceTrait for EventRepository {
    async fn initialize(&mut self) -> Result<(), lily_error::injection::InjectionError> {
        Ok(())
    }
}
