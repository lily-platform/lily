mod crud_support;

use std::sync::Arc;

use crud_support::{Entity, GoodDto, Repository};
use lily_injectable_derive::{CrudService, Injectable};
use lily_injection::ServiceTrait;

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct Gateway;

impl ServiceTrait for Gateway {}

impl Gateway {
    async fn notify_created(&self, _dto: &GoodDto) -> Result<(), &'static str> {
        Ok(())
    }

    async fn notify_created_many(&self, _dtos: &[GoodDto]) -> Result<(), &'static str> {
        Ok(())
    }

    async fn notify_updated(&self, _dto: &GoodDto) -> Result<(), &'static str> {
        Ok(())
    }

    async fn notify_deleted_id(&self, _id: &str) -> Result<(), &'static str> {
        Ok(())
    }
}

#[derive(Injectable, CrudService, Default)]
#[gateway(Gateway)]
#[entity_type(Entity)]
#[dto_type(GoodDto)]
#[repository_type(Repository)]
#[service(lifetime = "Singleton")]
struct Service {
    #[inject]
    gateway: Arc<Gateway>,
    #[inject]
    repository: Arc<Repository>,
}

impl ServiceTrait for Service {}

fn main() {
    let _ = std::any::type_name::<Service>();
}
