use std::sync::Arc;

use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;
use lily_mongodb::{Collection, DatabaseService, MongoCollection, Repository};
use mongodb::bson::oid::ObjectId;
use serde::{Deserialize, Serialize};

#[cfg(feature = "factory")]
use lily_mongodb::MongoFactory;

#[derive(Clone, Serialize, Deserialize)]
struct Entity {
    #[serde(skip_serializing_if = "Option::is_none")]
    _id: Option<ObjectId>,
    value: String,
}

#[cfg(feature = "single")]
#[derive(Injectable, MongoCollection, Default)]
#[collection("derive_contract")]
#[collection_type(Entity)]
#[service(lifetime = "Singleton")]
struct EntityCollection {
    #[inject]
    db: Arc<DatabaseService>,
    collection: Option<Collection<Entity>>,
}

#[cfg(feature = "factory")]
#[derive(Injectable, MongoCollection, Default)]
#[collection("derive_contract")]
#[collection_type(Entity)]
#[cell_name("orders")]
#[service(lifetime = "Singleton")]
struct EntityCollection {
    #[inject]
    mongo_factory: Arc<MongoFactory>,
    db: Arc<DatabaseService>,
    collection: Option<Collection<Entity>>,
}

#[derive(Injectable, Repository, Default)]
#[entity_type(Entity)]
#[collection_type(EntityCollection)]
#[service(lifetime = "Singleton")]
struct EntityRepository {
    #[inject]
    collection: Arc<EntityCollection>,
}

impl ServiceTrait for EntityRepository {}

#[derive(Clone, Serialize, Deserialize)]
struct StringIdEntity {
    #[serde(rename = "_id")]
    id: String,
    value: String,
}

#[cfg(feature = "single")]
#[derive(Injectable, MongoCollection, Default)]
#[collection("string_id_derive_contract")]
#[collection_type(StringIdEntity)]
#[service(lifetime = "Singleton")]
struct StringIdCollection {
    #[inject]
    db: Arc<DatabaseService>,
    collection: Option<Collection<StringIdEntity>>,
}

#[cfg(feature = "factory")]
#[derive(Injectable, MongoCollection, Default)]
#[collection("string_id_derive_contract")]
#[collection_type(StringIdEntity)]
#[cell_name("orders")]
#[service(lifetime = "Singleton")]
struct StringIdCollection {
    #[inject]
    mongo_factory: Arc<MongoFactory>,
    db: Arc<DatabaseService>,
    collection: Option<Collection<StringIdEntity>>,
}

#[derive(Injectable, Repository, Default)]
#[entity_type(StringIdEntity)]
#[collection_type(StringIdCollection)]
#[service(lifetime = "Singleton")]
struct StringIdRepository {
    #[inject]
    collection: Arc<StringIdCollection>,
}

impl ServiceTrait for StringIdRepository {}

fn assert_generated_contract(repository: &EntityRepository) {
    fn implements_repository<T: lily_mongo_repository::MongoRepository<Entity>>() {}
    implements_repository::<EntityRepository>();
    fn implements_string_id_repository<
        T: lily_mongo_repository::MongoRepository<StringIdEntity>,
    >() {
    }
    implements_string_id_repository::<StringIdRepository>();
    let _ = repository;
    let _ = EntityCollection::collection_name();
    let _ = StringIdCollection::collection_name();
}

fn main() {}
