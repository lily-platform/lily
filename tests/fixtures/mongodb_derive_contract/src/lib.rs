#![allow(dead_code)]

//! External macro ABI fixture, not an application composition example.
//!
//! Single mode intentionally omits `Injectable` to prove that the collection
//! derive's generated contract compiles independently. Canonical DI composition
//! is exercised in `crates/integrations/lily_mongodb/tests/ui/pass_derive_contract.rs`.

use std::sync::Arc;

use lily_mongodb::{Collection, DatabaseService, MongoCollection, Repository};
use mongodb::bson::oid::ObjectId;
use serde::{Deserialize, Serialize};

#[cfg(feature = "factory")]
use lily_injectable_derive::Injectable;
#[cfg(feature = "factory")]
use lily_mongodb::MongoFactory;

#[derive(Clone, Serialize, Deserialize)]
struct Entity {
    #[serde(skip_serializing_if = "Option::is_none")]
    _id: Option<ObjectId>,
    value: String,
}

#[cfg(feature = "single")]
#[derive(MongoCollection)]
#[collection("external_derive_contract")]
#[collection_type(Entity)]
struct EntityCollection {
    db: Arc<DatabaseService>,
    collection: Option<Collection<Entity>>,
}

#[cfg(feature = "factory")]
#[derive(Injectable, MongoCollection, Default)]
#[collection("external_derive_contract")]
#[collection_type(Entity)]
#[cell_name("orders")]
#[service(lifetime = "Singleton")]
struct EntityCollection {
    #[inject]
    mongo_factory: Arc<MongoFactory>,
    db: Arc<DatabaseService>,
    collection: Option<Collection<Entity>>,
}

#[derive(Repository)]
#[entity_type(Entity)]
#[collection_type(EntityCollection)]
struct EntityRepository {
    collection: Arc<EntityCollection>,
}

fn generated_contract_is_public() {
    fn implements_repository<T: lily_mongo_repository::MongoRepository<Entity>>() {}
    implements_repository::<EntityRepository>();
    let _ = EntityCollection::collection_name();
}

#[cfg(feature = "wrong-service")]
#[derive(MongoCollection)]
#[collection("wrong_service")]
#[collection_type(Entity)]
struct WrongServiceCollection {
    db: Arc<String>,
    collection: Option<Collection<Entity>>,
}

#[cfg(feature = "manual-collision")]
#[async_trait::async_trait]
impl lily_injection::ServiceTrait for EntityCollection {
    async fn initialize(&mut self) -> Result<(), lily_error::injection::InjectionError> {
        Ok(())
    }
}
