use diesel::prelude::*;
use runtime::{PgDatabaseService, PgRepository, diesel};
use std::sync::Arc;

diesel::table! { events (id) { id -> BigInt, message -> Text, } }

#[derive(Clone, Queryable, Selectable, Identifiable, Insertable, AsChangeset)]
#[diesel(table_name = events)]
#[diesel(check_for_backend(diesel::pg::Pg))]
struct Event {
    id: i64,
    message: String,
}

#[derive(Default, PgRepository)]
#[pg(entity = Event)]
struct EventRepository {
    #[cfg(feature = "single")]
    database: Arc<PgDatabaseService>,
    #[cfg(feature = "factory")]
    database: std::sync::OnceLock<Arc<PgDatabaseService>>,
    #[cfg(feature = "factory")]
    factory: Arc<runtime::PgFactory>,
}

#[test]
fn generated_repository_preserves_database_ownership() {
    let repository = EventRepository::default();
    #[cfg(feature = "single")]
    assert!(Arc::ptr_eq(
        &repository.database,
        &runtime::PgRepository::database_service(&repository).unwrap()
    ));
    #[cfg(feature = "factory")]
    {
        assert!(matches!(
            runtime::PgRepository::database_service(&repository),
            Err(runtime::PgError::RepositoryDatabaseNotSet)
        ));
        let db = Arc::new(PgDatabaseService::default());
        assert!(repository.database.set(Arc::clone(&db)).is_ok());
        assert!(Arc::ptr_eq(
            &db,
            &runtime::PgRepository::database_service(&repository).unwrap()
        ));
    }
}

#[cfg(test)]
#[tokio::test]
async fn generated_empty_batch_keeps_the_no_acquisition_contract() {
    let repository = EventRepository::default();
    assert!(
        repository
            .find_by_ids(Vec::<i64>::new())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(repository.create_many(Vec::new()).await.unwrap().is_empty());
}
