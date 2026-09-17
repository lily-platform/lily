mod crud_support;

use std::sync::Arc;

use crud_support::{Entity, Repository};
use lily_injectable_derive::CrudService;

struct WriteOnlyDto;

impl From<WriteOnlyDto> for Entity {
    fn from(_: WriteOnlyDto) -> Self {
        Self { _id: None }
    }
}

#[derive(CrudService)]
#[entity_type(Entity)]
#[dto_type(WriteOnlyDto)]
#[repository_type(Repository)]
struct Service {
    repository: Arc<Repository>,
}

fn main() {}
