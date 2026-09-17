#![allow(dead_code)]

use async_trait::async_trait;
use lily_injection::Injectable;
use lily_injection::ServiceTrait;
use lily_mongodb::bson::oid::ObjectId;
use lily_mongodb::{
    MongoDocumentId, MongoFilter, MongoIdBatch, MongoOperationContext, MongoPage, MongoPageRequest,
    MongoRepository, MongoRepositoryError, MongoWriteBatch,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct Entity {
    pub _id: Option<ObjectId>,
}

pub struct GoodDto;

impl From<GoodDto> for Entity {
    fn from(_: GoodDto) -> Self {
        Self { _id: None }
    }
}

impl From<Entity> for GoodDto {
    fn from(_: Entity) -> Self {
        Self
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
pub struct Repository;

impl ServiceTrait for Repository {}

#[async_trait]
impl MongoRepository<Entity> for Repository {
    async fn create(
        &self,
        _document: Entity,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<Entity, MongoRepositoryError> {
        todo!()
    }

    async fn create_many(
        &self,
        _documents: MongoWriteBatch<Entity>,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<Vec<Entity>, MongoRepositoryError> {
        todo!()
    }

    async fn update(
        &self,
        _document: Entity,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<Entity, MongoRepositoryError> {
        todo!()
    }

    async fn delete_by_id(
        &self,
        _id: MongoDocumentId,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<bool, MongoRepositoryError> {
        todo!()
    }

    async fn delete_one(
        &self,
        _filter: MongoFilter,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<bool, MongoRepositoryError> {
        todo!()
    }

    async fn delete_many(
        &self,
        _filter: MongoFilter,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<u64, MongoRepositoryError> {
        todo!()
    }

    async fn find_by_id(
        &self,
        _id: MongoDocumentId,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<Option<Entity>, MongoRepositoryError> {
        todo!()
    }

    async fn find_by_ids(
        &self,
        _ids: MongoIdBatch,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<Vec<Entity>, MongoRepositoryError> {
        todo!()
    }

    async fn find_one(
        &self,
        _filter: MongoFilter,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<Option<Entity>, MongoRepositoryError> {
        todo!()
    }

    async fn find_page(
        &self,
        _filter: MongoFilter,
        _page: MongoPageRequest,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<MongoPage<Entity>, MongoRepositoryError> {
        todo!()
    }

    async fn count(
        &self,
        _filter: MongoFilter,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<u64, MongoRepositoryError> {
        todo!()
    }

    async fn exists(
        &self,
        _filter: MongoFilter,
        _operation: &MongoOperationContext<'_>,
    ) -> Result<bool, MongoRepositoryError> {
        todo!()
    }
}
