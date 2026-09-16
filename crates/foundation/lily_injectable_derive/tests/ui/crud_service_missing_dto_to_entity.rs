mod crud_support;

use std::sync::Arc;

use crud_support::{Entity, Repository};
use lily_injectable_derive::CrudService;

struct ReadOnlyDto;

impl From<Entity> for ReadOnlyDto {
    fn from(_: Entity) -> Self {
        Self
    }
}

#[derive(CrudService)]
#[entity_type(Entity)]
#[dto_type(ReadOnlyDto)]
#[repository_type(Repository)]
struct Service {
    repository: Arc<Repository>,
}

fn main() {}
