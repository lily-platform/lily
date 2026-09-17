//! Named MongoDB cell ownership for the `factory` feature.

#[cfg(feature = "factory")]
use async_trait::async_trait;
#[cfg(feature = "factory")]
use lily_config::{ConfigService, DatabaseCellConfig, LilyConfig};
#[cfg(feature = "factory")]
use lily_error::injection::InjectionError;
#[cfg(feature = "factory")]
use lily_injectable_derive::Injectable;
#[cfg(feature = "factory")]
use lily_injection::ServiceTrait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::database::DatabaseService;
#[cfg(feature = "factory")]
use crate::options::MongoClientPlan;

/// Singleton owner for the MongoDB cells declared in `lily_config`.
///
/// Initialization validates every configured cell before opening clients,
/// performs one readiness ping per cell and publishes the completed immutable
/// name-to-service map only after all connections succeed. Collection derives
/// select a cell through `#[cell_name("...")]`; application code may use
/// [`MongoFactory::get`] for an explicit dynamic selection.
#[cfg(feature = "factory-api")]
#[cfg_attr(not(feature = "factory"), doc(hidden))]
#[cfg_attr(feature = "factory", derive(Injectable))]
#[cfg_attr(feature = "factory", service(lifetime = "Singleton"))]
#[derive(Default)]
pub struct MongoFactory {
    cells: RwLock<HashMap<String, Arc<DatabaseService>>>,
    #[cfg(feature = "factory")]
    #[inject]
    config_service: Arc<ConfigService>,
    #[cfg(feature = "test-support")]
    test_container_fixture: bool,
}

#[cfg(feature = "factory-api")]
impl MongoFactory {
    /// Builds an inert singleton seed for transport-free framework tests.
    ///
    /// No client or cell is created; dynamic lookup therefore remains empty.
    /// Only the DI initializer is bypassed for tests which exercise unrelated
    /// composition and shutdown behavior.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    #[must_use]
    pub fn test_container_fixture() -> Self {
        Self {
            test_container_fixture: true,
            ..Self::default()
        }
    }

    /// Returns the initialized database service registered under `name`.
    ///
    /// `None` means the cell was not present in the validated startup snapshot.
    /// The returned `Arc` shares the factory-owned service; the caller does not
    /// close it independently. Transactional adapters select the exact cell
    /// here, verify its capability before listener admission and continue to
    /// share the factory-owned client lifecycle.
    pub fn get(&self, name: &str) -> Option<Arc<DatabaseService>> {
        self.cells.read().ok()?.get(name).cloned()
    }

    /// Idempotently releases every client cell owned by this factory.
    ///
    /// Container-managed applications rely on `ServiceTrait::dispose`. This
    /// method exists for manually owned factories and shutdown adapters.
    pub fn close(&self) -> Result<(), lily_error::application::mongodb::MongoDbError> {
        let cells = {
            let mut cells = self.cells.write().map_err(|_| {
                lily_error::application::mongodb::MongoDbError::InternalError(
                    "MongoFactory cell lock poisoned".into(),
                )
            })?;
            std::mem::take(&mut *cells)
        };
        for service in cells.values() {
            service.close()?;
        }
        Ok(())
    }

    /// Selects the immutable, typed factory cells from one config snapshot.
    ///
    /// Keeping this boundary independent from client construction makes the
    /// startup contract testable without a live MongoDB server and prevents a
    /// return to derived `database.cells.N.*` key scanning.
    #[cfg(feature = "factory")]
    fn configured_cells(config: &LilyConfig) -> Result<&[DatabaseCellConfig], InjectionError> {
        let database = config.database.as_ref();
        let mode = database
            .and_then(|database| database.mode.as_deref())
            .unwrap_or("single");

        if mode != "factory" {
            return Err(InjectionError::InitError(
                "MongoFactory requires database.mode = \"factory\" in configuration".to_string(),
            ));
        }

        let cells = database
            .and_then(|database| database.cells.as_deref())
            .filter(|cells| !cells.is_empty())
            .ok_or_else(|| {
                InjectionError::InitError(
                    "No database cells configured. Please add [[database.cells]] sections to lily.toml"
                        .to_string(),
                )
            })?;

        for cell in cells {
            if cell.database_type != "mongodb" {
                return Err(InjectionError::InitError(format!(
                    "MongoFactory only supports 'mongodb' database type, got: {}",
                    cell.database_type
                )));
            }
        }

        Ok(cells)
    }
}

#[cfg(feature = "factory")]
#[async_trait]
impl ServiceTrait for MongoFactory {
    #[lily_trace::lily_trace(name = "mongo_factory.initialize", skip(self), env = "development")]
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        #[cfg(feature = "test-support")]
        if self.test_container_fixture {
            return Ok(());
        }
        lily_trace::prelude::info!("Initializing MongoFactory");

        let config = self.config_service.get_lily_config().await;
        let configured_cells = Self::configured_cells(&config)?;
        let plans = configured_cells
            .iter()
            .map(MongoClientPlan::from_cell)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| InjectionError::InitError(error.to_string()))?;
        let mut cells = HashMap::with_capacity(configured_cells.len());

        for (cell, plan) in configured_cells.iter().zip(plans) {
            lily_trace::prelude::info!("Configuring database cell: {}", cell.name);

            let db_service = DatabaseService::connect(plan).await.map_err(|error| {
                tracing::error!(
                    cell = %cell.name,
                    error = %error,
                    "MongoDB factory cell readiness probe failed"
                );
                InjectionError::InitError(format!(
                    "MongoDB factory cell '{}' readiness probe failed",
                    cell.name
                ))
            })?;

            cells.insert(cell.name.clone(), Arc::new(db_service));

            lily_trace::prelude::info!(
                "✅ Database cell '{}' configured for database: {}",
                cell.name,
                cell.database_name
            );
        }

        let cell_count = cells.len();
        self.cells = RwLock::new(cells);
        lily_trace::prelude::info!("MongoFactory initialized with {} cells", cell_count);
        Ok(())
    }

    #[lily_trace::lily_trace(name = "mongo_factory.dispose", skip(self), env = "development")]
    async fn dispose(&self) -> Result<(), InjectionError> {
        lily_trace::prelude::info!("Disposing MongoFactory");
        self.close()
            .map_err(|error| InjectionError::General(error.to_string()))?;
        lily_trace::prelude::info!("MongoFactory disposed");
        Ok(())
    }
}

#[cfg(all(test, feature = "factory"))]
mod tests {
    use super::*;
    use lily_config::DatabaseConfig;

    fn mongodb_cell(name: &str) -> DatabaseCellConfig {
        DatabaseCellConfig {
            name: name.to_string(),
            database_type: "mongodb".to_string(),
            connection_string: Some("mongodb://localhost:27017".to_string()),
            pool_size: Some(17),
            connection_timeout_secs: Some(3),
            query_timeout_secs: None,
            pooling_enabled: Some(true),
            database_name: "typed_database".to_string(),
            host: None,
            port: None,
            username: None,
            password: None,
            auth_database: None,
            use_tls: None,
            app_name: Some("typed-app".to_string()),
        }
    }

    fn config_with(mode: &str, cells: Option<Vec<DatabaseCellConfig>>) -> LilyConfig {
        let mut config = LilyConfig::default();
        let database = DatabaseConfig {
            mode: Some(mode.to_string()),
            cells,
            ..DatabaseConfig::default()
        };
        config.database = Some(database);
        config
    }

    #[test]
    fn typed_snapshot_cells_do_not_require_flat_index_keys() {
        let config = config_with("factory", Some(vec![mongodb_cell("primary")]));

        let cells = MongoFactory::configured_cells(&config).unwrap();

        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].name, "primary");
        assert_eq!(cells[0].database_name, "typed_database");
        assert_eq!(cells[0].pool_size, Some(17));
    }

    #[test]
    fn typed_snapshot_rejects_wrong_mode_and_empty_cells_without_a_database() {
        let wrong_mode = MongoFactory::configured_cells(&config_with("single", None)).unwrap_err();
        assert!(matches!(
            wrong_mode,
            InjectionError::InitError(message) if message.contains("database.mode = \"factory\"")
        ));

        let empty =
            MongoFactory::configured_cells(&config_with("factory", Some(Vec::new()))).unwrap_err();
        assert!(matches!(
            empty,
            InjectionError::InitError(message) if message.contains("No database cells configured")
        ));
    }
}
