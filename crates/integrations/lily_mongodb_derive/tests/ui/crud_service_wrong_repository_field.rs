mod crud_support;

use crud_support::{Entity, GoodDto, Repository};
use lily_mongodb::CrudService;

#[derive(CrudService)]
#[entity_type(Entity)]
#[dto_type(GoodDto)]
#[repository_type(Repository)]
struct Service {
    repository: String,
}

fn main() {}
