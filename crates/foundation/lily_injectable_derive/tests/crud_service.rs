use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lily_mongo_repository::{
    MongoDocumentId, MongoFilter, MongoIdBatch, MongoOperationContext, MongoPage, MongoPageRequest,
    MongoRepository, MongoRepositoryError, MongoWriteBatch,
};
use lily_mongo_service::{BaseServiceError, BaseServiceOperation};
use lily_injectable_derive::CrudService;
use mongodb::bson::{oid::ObjectId, Document};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct UserEntity {
    _id: Option<ObjectId>,
    name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UserDto {
    id: Option<String>,
    name: String,
}

impl From<UserDto> for UserEntity {
    fn from(dto: UserDto) -> Self {
        Self {
            _id: dto.id.and_then(|id| ObjectId::parse_str(id).ok()),
            name: dto.name,
        }
    }
}

impl From<UserEntity> for UserDto {
    fn from(entity: UserEntity) -> Self {
        Self {
            id: entity._id.map(|id| id.to_hex()),
            name: entity.name,
        }
    }
}

#[derive(Default)]
struct UserRepository {
    delete_filter: Mutex<Option<Document>>,
    fail_next: Mutex<bool>,
}

impl UserRepository {
    fn arrange_failure(&self) {
        *self.fail_next.lock().unwrap() = true;
    }

    fn fail_if_arranged(&self) -> Result<(), MongoRepositoryError> {
        let mut fail_next = self.fail_next.lock().unwrap();
        if std::mem::take(&mut *fail_next) {
            Err(MongoRepositoryError::QueryFailed(
                "arranged repository failure".to_string(),
            ))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl MongoRepository<UserEntity> for UserRepository {
    async fn create(
        &self,
        mut document: UserEntity,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<UserEntity, MongoRepositoryError> {
        self.fail_if_arranged()?;
        document._id.get_or_insert_with(ObjectId::new);
        Ok(document)
    }

    async fn create_many(
        &self,
        documents: MongoWriteBatch<UserEntity>,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<Vec<UserEntity>, MongoRepositoryError> {
        self.fail_if_arranged()?;
        Ok(documents.into_inner())
    }

    async fn update(
        &self,
        document: UserEntity,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<UserEntity, MongoRepositoryError> {
        self.fail_if_arranged()?;
        Ok(document)
    }

    async fn delete_by_id(
        &self,
        _id: MongoDocumentId,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<bool, MongoRepositoryError> {
        self.fail_if_arranged()?;
        Ok(true)
    }

    async fn delete_one(
        &self,
        _filter: MongoFilter,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<bool, MongoRepositoryError> {
        unreachable!("BaseService does not use filter-based delete_one")
    }

    async fn delete_many(
        &self,
        filter: MongoFilter,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<u64, MongoRepositoryError> {
        self.fail_if_arranged()?;
        let document = filter.into_document();
        let count = document
            .get_document("_id")
            .and_then(|id| id.get_array("$in"))
            .map(|ids| ids.len() as u64)
            .unwrap_or_default();
        *self.delete_filter.lock().unwrap() = Some(document);
        Ok(count)
    }

    async fn find_by_id(
        &self,
        id: MongoDocumentId,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<Option<UserEntity>, MongoRepositoryError> {
        self.fail_if_arranged()?;
        if id.as_object_id().bytes() == [0; 12] {
            return Ok(None);
        }
        Ok(Some(UserEntity {
            _id: Some(id.into_object_id()),
            name: "found".to_string(),
        }))
    }

    async fn find_by_ids(
        &self,
        ids: MongoIdBatch,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<Vec<UserEntity>, MongoRepositoryError> {
        self.fail_if_arranged()?;
        Ok(ids
            .as_slice()
            .iter()
            .map(|id| UserEntity {
                _id: Some(*id.as_object_id()),
                name: "found".to_string(),
            })
            .collect())
    }

    async fn find_one(
        &self,
        _filter: MongoFilter,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<Option<UserEntity>, MongoRepositoryError> {
        unreachable!("BaseService does not expose find_one")
    }

    async fn find_page(
        &self,
        _filter: MongoFilter,
        _page: MongoPageRequest,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<MongoPage<UserEntity>, MongoRepositoryError> {
        unreachable!("BaseService does not expose find_page")
    }

    async fn count(
        &self,
        _filter: MongoFilter,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<u64, MongoRepositoryError> {
        unreachable!("BaseService does not expose count")
    }

    async fn exists(
        &self,
        _filter: MongoFilter,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<bool, MongoRepositoryError> {
        unreachable!("BaseService does not expose exists")
    }
}

#[derive(CrudService)]
#[entity_type(UserEntity)]
#[dto_type(UserDto)]
#[repository_type(UserRepository)]
struct UserService {
    repository: Arc<UserRepository>,
}

#[tokio::test]
async fn generated_service_maps_dtos_ids_batches_and_required_reads() {
    let repository = Arc::new(UserRepository::default());
    let service = UserService {
        repository: repository.clone(),
    };

    let created = service
        .create(UserDto {
            id: None,
            name: "created".to_string(),
        })
        .await
        .unwrap();
    assert!(created.id.is_some());

    let id_one = ObjectId::new().to_hex();
    let id_two = ObjectId::new().to_hex();
    let found = service.find_by_id(&id_one).await.unwrap();
    assert_eq!(found.id.as_deref(), Some(id_one.as_str()));

    let found_many = service
        .find_by_ids(vec![id_one.clone(), id_two.clone()])
        .await
        .unwrap();
    assert_eq!(found_many.len(), 2);

    assert!(service
        .delete(UserDto {
            id: Some(id_one.clone()),
            name: "ignored".to_string(),
        })
        .await
        .unwrap());
    assert_eq!(
        service
            .delete_many(vec![
                UserDto {
                    id: Some(id_one),
                    name: "must not become a predicate".to_string(),
                },
                UserDto {
                    id: Some(id_two),
                    name: "also ignored".to_string(),
                },
            ])
            .await
            .unwrap(),
        2
    );
    let filter = repository.delete_filter.lock().unwrap().clone().unwrap();
    assert_eq!(filter.len(), 1);
    assert!(filter.contains_key("_id"));

    let missing_id = ObjectId::from_bytes([0; 12]).to_hex();
    assert!(matches!(
        service.find_by_id(&missing_id).await,
        Err(BaseServiceError::NotFound { entity, identifier })
            if entity == "UserEntity" && identifier == missing_id
    ));
}

#[tokio::test]
async fn generated_service_rejects_invalid_ids_and_batches_before_repository_io() {
    let service = UserService {
        repository: Arc::new(UserRepository::default()),
    };

    assert!(matches!(
        service.find_by_id("not-an-object-id").await,
        Err(BaseServiceError::OperationFailed {
            operation: BaseServiceOperation::FindById,
            source: MongoRepositoryError::InvalidDocumentId(_),
            ..
        })
    ));
    assert!(matches!(
        service
            .delete(UserDto {
                id: None,
                name: "missing id".to_string(),
            })
            .await,
        Err(BaseServiceError::OperationFailed {
            operation: BaseServiceOperation::Delete,
            source: MongoRepositoryError::InvalidDocumentId(_),
            ..
        })
    ));
    assert!(matches!(
        service.create_many(Vec::new()).await,
        Err(BaseServiceError::OperationFailed {
            operation: BaseServiceOperation::CreateMany,
            source: MongoRepositoryError::InvalidBatch(_),
            ..
        })
    ));
    assert!(matches!(
        service.find_by_ids(Vec::new()).await,
        Err(BaseServiceError::OperationFailed {
            operation: BaseServiceOperation::FindByIds,
            source: MongoRepositoryError::InvalidBatch(_),
            ..
        })
    ));
}

fn dto(id: &str) -> UserDto {
    UserDto {
        id: Some(id.to_string()),
        name: "operation".to_string(),
    }
}

fn assert_operation(error: BaseServiceError, expected: BaseServiceOperation) {
    assert_eq!(error.entity_name(), "UserEntity");
    assert_eq!(error.operation(), expected);
    assert!(matches!(
        error.repository_error(),
        Some(MongoRepositoryError::QueryFailed(message))
            if message == "arranged repository failure"
    ));
}

#[tokio::test]
async fn generated_service_labels_repository_failures_with_the_exact_method() {
    let repository = Arc::new(UserRepository::default());
    let service = UserService {
        repository: repository.clone(),
    };
    let id = ObjectId::new().to_hex();

    repository.arrange_failure();
    assert_operation(
        service.create(dto(&id)).await.unwrap_err(),
        BaseServiceOperation::Create,
    );

    repository.arrange_failure();
    assert_operation(
        service.create_many(vec![dto(&id)]).await.unwrap_err(),
        BaseServiceOperation::CreateMany,
    );

    repository.arrange_failure();
    assert_operation(
        service.update(dto(&id)).await.unwrap_err(),
        BaseServiceOperation::Update,
    );

    repository.arrange_failure();
    assert_operation(
        service.delete(dto(&id)).await.unwrap_err(),
        BaseServiceOperation::Delete,
    );

    repository.arrange_failure();
    assert_operation(
        service.delete_many(vec![dto(&id)]).await.unwrap_err(),
        BaseServiceOperation::DeleteMany,
    );

    repository.arrange_failure();
    assert_operation(
        service.find_by_id(&id).await.unwrap_err(),
        BaseServiceOperation::FindById,
    );

    repository.arrange_failure();
    assert_operation(
        service.delete_by_id(&id).await.unwrap_err(),
        BaseServiceOperation::DeleteById,
    );

    repository.arrange_failure();
    assert_operation(
        service.find_by_ids(vec![id]).await.unwrap_err(),
        BaseServiceOperation::FindByIds,
    );
}
