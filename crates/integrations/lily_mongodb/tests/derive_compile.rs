//! Low-level derive expansion checks.
//!
//! `AuditCollection` and `AuditRepository` deliberately omit `Injectable` so
//! this test isolates the two MongoDB macros. Application composition is
//! covered by `tests/ui/pass_derive_contract.rs`, which is the
//! repository-backed DI shape documented by the crate.

#[cfg(feature = "factory")]
use lily_injectable_derive::Injectable;
#[cfg(feature = "factory")]
use lily_mongodb::MongoFactory;
use lily_mongodb::{Collection, DatabaseService};
use lily_mongodb_derive::{MongoCollection, Repository};
use mongodb::bson::oid::ObjectId;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
struct AuditEntity {
    _id: Option<ObjectId>,
    message: String,
    created_at: i64,
}

#[derive(MongoCollection)]
#[collection("audit_events")]
#[collection_type(AuditEntity)]
#[index(field = "created_at")]
#[unique_field("message")]
#[unique_combination("message", "created_at")]
struct AuditCollection {
    db: Arc<DatabaseService>,
    collection: Option<Collection<AuditEntity>>,
}

#[derive(Repository)]
#[entity_type(AuditEntity)]
#[collection_type(AuditCollection)]
struct AuditRepository {
    collection: Arc<AuditCollection>,
}

#[cfg(feature = "factory")]
#[derive(Injectable, MongoCollection, Default)]
#[collection("factory_audit_events")]
#[collection_type(AuditEntity)]
#[cell_name("orders")]
#[service(lifetime = "Singleton")]
struct FactoryAuditCollection {
    #[inject]
    mongo_factory: Arc<MongoFactory>,
    db: Arc<DatabaseService>,
    collection: Option<Collection<AuditEntity>>,
}

#[test]
fn generated_collection_contract_exposes_configured_name() {
    let _repository_contract = std::any::type_name::<AuditRepository>();
    assert_eq!(AuditCollection::collection_name(), "audit_events");
    let steps = AuditCollection::migration_steps().expect("validated derive migration steps");
    assert_eq!(steps.len(), 4);
}

#[test]
fn downstream_derive_contract_compile_passes() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/pass_derive_contract.rs");
}

#[cfg(feature = "factory")]
#[test]
fn factory_collection_contract_compiles_with_named_cell_state() {
    assert_eq!(
        FactoryAuditCollection::collection_name(),
        "factory_audit_events"
    );
}
