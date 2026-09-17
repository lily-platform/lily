use std::sync::Arc;

use diesel::prelude::*;
use lily_postgresql::{PgDatabaseService, PgRepository};

#[cfg(feature = "factory")]
use lily_postgresql::{PgError, PgFactory};
#[cfg(feature = "factory")]
use std::sync::OnceLock;

diesel::table! {
    orders (id) {
        id -> BigInt,
        status -> Text,
    }
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Insertable, AsChangeset)]
#[diesel(table_name = orders)]
#[diesel(check_for_backend(diesel::pg::Pg))]
struct Order {
    #[diesel(skip_insertion)]
    #[diesel(skip_update)]
    id: i64,
    status: String,
}

#[cfg(feature = "single")]
#[derive(PgRepository)]
#[pg(entity = Order)]
struct OrderRepository {
    database: Arc<PgDatabaseService>,
}

#[cfg(feature = "factory")]
#[derive(Default, PgRepository)]
#[pg(entity = Order)]
struct OrderRepository {
    #[allow(dead_code)]
    factory: Arc<PgFactory>,
    database: OnceLock<Arc<PgDatabaseService>>,
}

#[cfg(feature = "single")]
#[test]
fn single_repository_delegates_to_the_injected_database_instance() {
    let database = Arc::new(PgDatabaseService::default());
    let repository = OrderRepository {
        database: Arc::clone(&database),
    };
    let selected =
        <OrderRepository as lily_postgresql::PgRepository>::database_service(&repository).unwrap();
    assert!(Arc::ptr_eq(&database, &selected));
}

#[cfg(feature = "factory")]
#[test]
fn factory_repository_fails_typed_when_service_initializer_did_not_set_database() {
    let repository = OrderRepository::default();
    assert!(matches!(
        <OrderRepository as lily_postgresql::PgRepository>::database_service(&repository),
        Err(PgError::RepositoryDatabaseNotSet)
    ));
}
