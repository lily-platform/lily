use runtime::{
    BaseServiceError, BaseServiceOperation, Collection, CrudService, DatabaseService,
    MongoCollection, MongoRepositoryError, Repository, bson::oid::ObjectId,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
struct Entity {
    _id: Option<ObjectId>,
    value: String,
}

#[derive(Default, MongoCollection)]
#[collection("facade_contract")]
#[collection_type(Entity)]
#[index(field = "value")]
#[cfg_attr(feature = "factory", cell_name("primary"))]
struct EntityCollection {
    db: Arc<DatabaseService>,
    #[cfg(feature = "factory")]
    mongo_factory: Arc<runtime::MongoFactory>,
    collection: Option<Collection<Entity>>,
}

#[derive(Default, Repository)]
#[entity_type(Entity)]
#[collection_type(EntityCollection)]
struct EntityRepository {
    collection: Arc<EntityCollection>,
}

#[derive(Default, CrudService)]
#[entity_type(Entity)]
#[dto_type(Entity)]
#[repository_type(EntityRepository)]
struct FirstService {
    repository: Arc<EntityRepository>,
}

// Two derives in the same module must not introduce duplicate trait imports.
#[derive(Default)]
struct Gateway;
impl Gateway {
    async fn notify_created(&self, _: &Entity) -> Result<(), &'static str> {
        Ok(())
    }
    async fn notify_created_many(&self, _: &[Entity]) -> Result<(), &'static str> {
        Ok(())
    }
    async fn notify_updated(&self, _: &Entity) -> Result<(), &'static str> {
        Ok(())
    }
    async fn notify_deleted_id(&self, _: &str) -> Result<(), &'static str> {
        Ok(())
    }
}

#[derive(Default, CrudService)]
#[gateway(Gateway)]
#[entity_type(Entity)]
#[dto_type(Entity)]
#[repository_type(EntityRepository)]
struct SecondService {
    repository: Arc<EntityRepository>,
    gateway: Arc<Gateway>,
}

#[test]
fn migration_and_repository_contracts_are_available_from_the_facade() {
    fn repository<T: runtime::MongoRepository<Entity>>() {}
    repository::<EntityRepository>();
    assert_eq!(EntityCollection::collection_name(), "facade_contract");
    assert_eq!(EntityCollection::migration_steps().unwrap().len(), 2);
    assert_eq!(runtime::MAX_MONGO_PAGE_SIZE, 100);
    assert!(runtime::MongoPageRequest::new(0, runtime::MAX_MONGO_PAGE_SIZE + 1).is_err());
}

#[cfg(test)]
#[tokio::test]
async fn generated_services_preserve_typed_failures_without_a_live_database() {
    for error in [
        FirstService::default()
            .find_by_id("invalid")
            .await
            .err()
            .unwrap(),
        SecondService::default()
            .find_by_id("invalid")
            .await
            .err()
            .unwrap(),
    ] {
        assert!(matches!(
            error,
            BaseServiceError::OperationFailed {
                operation: BaseServiceOperation::FindById,
                source: MongoRepositoryError::InvalidDocumentId(_),
                ..
            }
        ));
    }
    let error = FirstService::default()
        .create(Entity {
            _id: None,
            value: "audit".into(),
        })
        .await
        .err()
        .unwrap();
    assert!(matches!(
        error,
        BaseServiceError::OperationFailed {
            operation: BaseServiceOperation::Create,
            source: MongoRepositoryError::InternalError(_),
            ..
        }
    ));
}
