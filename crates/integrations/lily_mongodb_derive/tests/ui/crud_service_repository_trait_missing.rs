mod crud_support;

use std::sync::Arc;

use crud_support::{Entity, GoodDto};
use lily_mongodb::CrudService;

struct NotARepository;

#[derive(CrudService)]
#[entity_type(Entity)]
#[dto_type(GoodDto)]
#[repository_type(NotARepository)]
struct Service {
    repository: Arc<NotARepository>,
}

fn main() {}
