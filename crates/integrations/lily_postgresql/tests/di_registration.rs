use std::any::TypeId;

use lily_injection_registry::get_all_service_metadata;
use lily_postgresql::{PgDatabaseService, PgDbContext};

#[cfg(feature = "factory")]
use lily_postgresql::PgFactory;

#[test]
fn feature_mode_publishes_exactly_one_postgresql_owner() {
    let metadata = get_all_service_metadata();
    let database_count = metadata
        .iter()
        .filter(|metadata| metadata.type_id == TypeId::of::<PgDatabaseService>())
        .count();
    let contexts: Vec<_> = metadata
        .iter()
        .filter(|metadata| metadata.type_id == TypeId::of::<PgDbContext>())
        .collect();

    #[cfg(feature = "single")]
    {
        assert_eq!(database_count, 1);
        assert_eq!(contexts.len(), 1);
        assert_eq!(
            contexts[0].lifetime,
            lily_injection_registry::ServiceLifetime::Scoped
        );
    }

    #[cfg(feature = "factory")]
    {
        let factory_count = metadata
            .iter()
            .filter(|metadata| metadata.type_id == TypeId::of::<PgFactory>())
            .count();
        assert_eq!(factory_count, 1);
        assert!(
            contexts.is_empty(),
            "factory mode must not silently select a context database"
        );
        assert_eq!(
            database_count, 0,
            "factory child database services must not become global descriptors"
        );
    }
}
