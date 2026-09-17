use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

#[cfg(feature = "factory")]
use async_trait::async_trait;
#[cfg(feature = "factory")]
use lily_config::ConfigService;
#[cfg(feature = "factory")]
use lily_error::injection::InjectionError;
#[cfg(feature = "factory")]
use lily_injectable_derive::Injectable;
#[cfg(feature = "factory")]
use lily_injection::ServiceTrait;

#[cfg(feature = "factory")]
use crate::plan::{PgConnectionPlan, validate_factory_config};
use crate::{PgDatabaseService, PgError, PgResult};

/// Application-owned immutable map of named Diesel PostgreSQL services.
///
/// Child database services are owned exclusively by this factory and are not
/// published as additional global DI descriptors. Application services inject
/// the factory, select a configured cell with [`Self::get`], and keep the
/// returned `Arc<PgDatabaseService>` for their singleton lifetime.
///
/// `Default` exists only for DI construction. A manually default-constructed
/// factory returns [`PgError::NotInitialized`].
#[cfg_attr(feature = "factory", derive(Injectable))]
#[cfg_attr(feature = "factory", service(lifetime = "Singleton"))]
#[cfg_attr(not(feature = "factory"), doc(hidden))]
#[derive(Default)]
pub struct PgFactory {
    #[cfg(feature = "factory")]
    #[inject]
    config_service: Arc<ConfigService>,
    cells: OnceLock<HashMap<String, Arc<PgDatabaseService>>>,
    #[cfg(all(feature = "test-support", feature = "factory"))]
    test_container_fixture: bool,
}

impl PgFactory {
    /// Builds an inert singleton seed for transport-free framework tests.
    ///
    /// No connection pool or named cell is created. Only the DI initializer is
    /// bypassed while lookup remains fail-closed as uninitialized.
    #[cfg(all(feature = "test-support", feature = "factory"))]
    #[doc(hidden)]
    #[must_use]
    pub fn test_container_fixture() -> Self {
        Self {
            test_container_fixture: true,
            ..Self::default()
        }
    }

    /// Returns the initialized database service for an exact configured name.
    ///
    /// Before DI initialization this returns [`PgError::NotInitialized`]; an
    /// unknown configured name returns [`PgError::UnknownCell`]. Child services
    /// share ownership through `Arc` but their shutdown remains factory-owned.
    pub fn get(&self, name: &str) -> PgResult<Arc<PgDatabaseService>> {
        self.cells
            .get()
            .ok_or(PgError::NotInitialized)?
            .get(name)
            .cloned()
            .ok_or_else(|| PgError::UnknownCell {
                name: name.to_owned(),
            })
    }

    /// Returns configured cell names in deterministic lexical order.
    ///
    /// Before DI initialization this returns [`PgError::NotInitialized`].
    pub fn cell_names(&self) -> PgResult<Vec<String>> {
        let mut names: Vec<_> = self
            .cells
            .get()
            .ok_or(PgError::NotInitialized)?
            .keys()
            .cloned()
            .collect();
        names.sort();
        Ok(names)
    }

    #[cfg(feature = "factory")]
    async fn close_cells(cells: &HashMap<String, Arc<PgDatabaseService>>) -> Vec<String> {
        let mut failures = Vec::new();
        for (name, service) in cells {
            if service.close().await.is_err() {
                failures.push(name.clone());
            }
        }
        failures.sort();
        failures
    }

    #[cfg(feature = "factory")]
    async fn close(&self) -> PgResult<()> {
        let Some(cells) = self.cells.get() else {
            return Ok(());
        };
        let failures = Self::close_cells(cells).await;
        if failures.is_empty() {
            Ok(())
        } else {
            Err(PgError::FactoryShutdown { cells: failures })
        }
    }
}

#[cfg(feature = "factory")]
#[async_trait]
impl ServiceTrait for PgFactory {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        #[cfg(feature = "test-support")]
        if self.test_container_fixture {
            return Ok(());
        }
        if self.cells.get().is_some() {
            return Err(InjectionError::InitError(
                PgError::AlreadyInitialized.to_string(),
            ));
        }
        let lily = self.config_service.get_lily_config().await;
        let config = lily.postgresql.as_ref().ok_or_else(|| {
            InjectionError::InitError("PostgreSQL configuration is missing".into())
        })?;

        // Complete validation and immutable plan construction happen before
        // any connection is opened.
        validate_factory_config(config)
            .map_err(|error| InjectionError::InitError(error.to_string()))?;
        let plans = config
            .cells
            .iter()
            .map(|cell| PgConnectionPlan::from_cell(cell).map(|plan| (cell.name.clone(), plan)))
            .collect::<PgResult<Vec<_>>>()
            .map_err(|error| InjectionError::InitError(error.to_string()))?;

        let mut cells = HashMap::with_capacity(plans.len());
        for (name, plan) in plans {
            match PgDatabaseService::connect(plan).await {
                Ok(service) => {
                    cells.insert(name, Arc::new(service));
                }
                Err(_) => {
                    let rollback_failures = Self::close_cells(&cells).await;
                    let error = if rollback_failures.is_empty() {
                        PgError::FactoryStartup { cell: name }
                    } else {
                        PgError::FactoryStartupRollback {
                            cell: name,
                            cells: rollback_failures,
                        }
                    };
                    tracing::error!(
                        error_code = "factory_startup_failed",
                        "PostgreSQL factory readiness failed"
                    );
                    return Err(InjectionError::InitError(error.to_string()));
                }
            }
        }

        if let Err(unpublished) = self.cells.set(cells) {
            let _ = Self::close_cells(&unpublished).await;
            return Err(InjectionError::InitError(
                PgError::AlreadyInitialized.to_string(),
            ));
        }
        tracing::info!(
            cell_count = config.cells.len(),
            "PostgreSQL factory initialized"
        );
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        self.close().await.map_err(|_| {
            InjectionError::DisposeError("PostgreSQL factory shutdown failed".into())
        })?;
        tracing::info!("PostgreSQL factory disposed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_factory_is_uninitialized_and_typed() {
        let factory = PgFactory::default();
        assert!(matches!(
            factory.get("orders"),
            Err(PgError::NotInitialized)
        ));
        assert_eq!(factory.cell_names(), Err(PgError::NotInitialized));
    }
}
