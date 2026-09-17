use std::sync::Arc;

#[cfg(feature = "factory")]
use lily_clickhouse::ClickhouseFactory;
use lily_clickhouse::{ClickhouseRepository, ClickhouseSchema, ClickhouseTable, DatabaseService};
use lily_injectable_derive::Injectable;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, clickhouse::Row, ClickhouseSchema,
)]
#[clickhouse(
    table = "derive_compile_events",
    order_by = "event_id",
    engine = "MergeTree()"
)]
struct DeriveCompileEvent {
    event_id: String,
    status: String,
}

#[cfg(feature = "single")]
#[derive(Injectable, ClickhouseTable, Default)]
#[entity_type(DeriveCompileEvent)]
#[service(lifetime = "Singleton")]
struct DeriveCompileTable {
    #[inject]
    #[allow(dead_code)]
    db: Arc<DatabaseService>,
}

#[cfg(feature = "factory")]
#[derive(Injectable, ClickhouseTable, Default)]
#[entity_type(DeriveCompileEvent)]
#[cell_name("analytics")]
#[service(lifetime = "Singleton")]
struct DeriveCompileTable {
    #[inject]
    clickhouse_factory: Arc<ClickhouseFactory>,
    db: Arc<DatabaseService>,
}

#[derive(Injectable, ClickhouseRepository, Default)]
#[table_type(DeriveCompileTable)]
#[entity_type(DeriveCompileEvent)]
#[service(lifetime = "Singleton")]
struct DeriveCompileRepository {
    #[inject]
    #[allow(dead_code)]
    table: Arc<DeriveCompileTable>,
}

#[test]
fn generated_table_and_repository_contracts_compile() {
    assert_eq!(DeriveCompileTable::table_name(), "derive_compile_events");
    assert_eq!(
        std::any::type_name::<DeriveCompileRepository>(),
        concat!(module_path!(), "::DeriveCompileRepository")
    );
}

#[test]
fn downstream_derive_contract_compile_passes() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/pass_derive_contract.rs");
}
