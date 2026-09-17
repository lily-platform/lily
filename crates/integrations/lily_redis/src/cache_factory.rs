//! DI-owned Redis cache cells for the mutually exclusive `factory` profile.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

#[cfg(feature = "factory")]
use async_trait::async_trait;
#[cfg(feature = "factory")]
use lily_config::ConfigService;
#[cfg(any(feature = "factory", test))]
use lily_config::LilyConfig;
#[cfg(feature = "factory")]
use lily_error::injection::InjectionError;
#[cfg(feature = "factory")]
use lily_injectable_derive::Injectable;
#[cfg(feature = "factory")]
use lily_injection::ServiceTrait;

use crate::{CacheError, CacheService, ICache, RedisCachePlan};

enum FactoryState {
    Uninitialized,
    Ready(HashMap<String, Arc<CacheService>>),
    Disposed,
}

#[cfg_attr(feature = "factory", derive(Injectable))]
#[cfg_attr(feature = "factory", service(lifetime = "Singleton"))]
/// Owner and lookup registry for named Redis cache cells.
///
/// In normal factory mode Lily DI initializes it from `[[cache.cells]]`.
pub struct CacheFactory {
    state: RwLock<FactoryState>,
    #[cfg(feature = "factory")]
    #[cfg_attr(feature = "factory", inject)]
    config_service: Arc<ConfigService>,
}

impl Default for CacheFactory {
    fn default() -> Self {
        Self {
            state: RwLock::new(FactoryState::Uninitialized),
            #[cfg(feature = "factory")]
            config_service: Arc::new(ConfigService::default()),
        }
    }
}

impl CacheFactory {
    /// Connects a standalone factory from explicitly named, validated plans.
    ///
    /// The caller must eventually invoke [`CacheFactory::dispose_all`] or
    /// register a [`crate::CacheShutdownHandle`].
    pub async fn connect(
        cells: impl IntoIterator<Item = (String, RedisCachePlan)>,
    ) -> Result<Self, CacheError> {
        let factory = Self::default();
        factory
            .initialize_from_plans(cells.into_iter().collect())
            .await?;
        Ok(factory)
    }

    /// Looks up a named cell, returning `None` when it is absent or the factory
    /// is not usable.
    ///
    /// Use [`Self::try_get`] when uninitialized/disposed state must remain
    /// distinguishable from a missing name.
    pub fn get(&self, name: &str) -> Option<Arc<CacheService>> {
        self.try_get(name).ok().flatten()
    }

    /// Looks up a named cell while preserving lifecycle errors.
    pub fn try_get(&self, name: &str) -> Result<Option<Arc<CacheService>>, CacheError> {
        match &*self.state.read().map_err(|_| CacheError::StatePoisoned)? {
            FactoryState::Uninitialized => Err(CacheError::NotInitialized),
            FactoryState::Ready(cells) => Ok(cells.get(name).cloned()),
            FactoryState::Disposed => Err(CacheError::Disposed),
        }
    }

    async fn initialize_from_plans(
        &self,
        plans: Vec<(String, RedisCachePlan)>,
    ) -> Result<(), CacheError> {
        if plans.is_empty() {
            return Err(CacheError::InvalidConfiguration(
                "at least one Redis cache cell is required".into(),
            ));
        }
        let mut names = HashSet::with_capacity(plans.len());
        for (name, _) in &plans {
            if name.is_empty() || !names.insert(name.clone()) {
                return Err(CacheError::InvalidConfiguration(format!(
                    "cache cell name {name:?} is empty or duplicated"
                )));
            }
        }
        let mut cells = HashMap::with_capacity(plans.len());
        for (name, plan) in plans {
            match CacheService::connect(plan).await {
                Ok(service) => {
                    cells.insert(name, Arc::new(service));
                }
                Err(error) => {
                    let mut cleanup_failures = Vec::new();
                    for service in cells.values() {
                        if let Err(cleanup_error) = ICache::dispose(service.as_ref()).await {
                            cleanup_failures.push(cleanup_error.to_string());
                        }
                    }
                    if !cleanup_failures.is_empty() {
                        return Err(CacheError::InitializationRollbackFailed {
                            initialization: Box::new(error),
                            cleanup: cleanup_failures.join("; "),
                        });
                    }
                    return Err(error);
                }
            }
        }

        let mut state = self.state.write().map_err(|_| CacheError::StatePoisoned)?;
        match &*state {
            FactoryState::Uninitialized => {
                *state = FactoryState::Ready(cells);
                Ok(())
            }
            FactoryState::Ready(_) => Err(CacheError::InvalidConfiguration(
                "cache factory was initialized more than once".into(),
            )),
            FactoryState::Disposed => Err(CacheError::Disposed),
        }
    }

    /// Idempotently stops and drains every Redis cell owned by this factory.
    pub async fn dispose_all(&self) -> Result<(), CacheError> {
        let cells = {
            let mut state = self.state.write().map_err(|_| CacheError::StatePoisoned)?;
            match std::mem::replace(&mut *state, FactoryState::Disposed) {
                FactoryState::Uninitialized | FactoryState::Disposed => HashMap::new(),
                FactoryState::Ready(cells) => cells,
            }
        };

        let mut failures = Vec::new();
        for (name, service) in cells {
            if let Err(error) = ICache::dispose(service.as_ref()).await {
                failures.push(format!("{name}: {error}"));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(CacheError::Backend(format!(
                "cache factory disposal failed: {}",
                failures.join(", ")
            )))
        }
    }
}

#[cfg(any(feature = "factory", test))]
fn configured_factory_plans(
    config: &LilyConfig,
) -> Result<Vec<(String, RedisCachePlan)>, CacheError> {
    let cache = config.cache.as_ref().ok_or_else(|| {
        CacheError::InvalidConfiguration("[cache] configuration is required".into())
    })?;
    if cache.mode.as_deref().unwrap_or("single") != "factory" {
        return Err(CacheError::InvalidConfiguration(
            "CacheFactory requires cache.mode = \"factory\"".into(),
        ));
    }
    let cells = cache
        .cells
        .as_deref()
        .filter(|cells| !cells.is_empty())
        .ok_or_else(|| {
            CacheError::InvalidConfiguration(
                "at least one [[cache.cells]] entry is required".into(),
            )
        })?;

    let mut names = HashSet::with_capacity(cells.len());
    cells
        .iter()
        .map(|cell| {
            if cell.name.is_empty() || !names.insert(cell.name.clone()) {
                return Err(CacheError::InvalidConfiguration(format!(
                    "cache cell name {:?} is empty or duplicated",
                    cell.name
                )));
            }
            Ok((cell.name.clone(), RedisCachePlan::from_cell(cell)?))
        })
        .collect()
}

#[cfg(feature = "factory")]
#[async_trait]
impl ServiceTrait for CacheFactory {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        let config = self.config_service.get_lily_config().await;
        let plans = configured_factory_plans(&config)
            .map_err(|error| InjectionError::InitError(error.to_string()))?;
        self.initialize_from_plans(plans)
            .await
            .map_err(|error| InjectionError::InitError(error.to_string()))
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        self.dispose_all()
            .await
            .map_err(|error| InjectionError::General(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lily_config::{CacheCellConfig, CacheConfig, LilyConfig};

    fn cell(name: &str, url: &str) -> CacheCellConfig {
        CacheCellConfig {
            name: name.into(),
            provider: "redis".into(),
            redis_url: Some(url.into()),
            use_tls: Some(false),
            additional_ca_bundle: None,
            key_namespace: Some(name.into()),
            default_ttl_secs: Some(45),
            pool_size: Some(4),
            connection_timeout_secs: Some(2),
            operation_timeout_secs: Some(1),
            scan_page_size: Some(20),
            max_scan_results: Some(100),
        }
    }

    fn config(mode: &str, cells: Option<Vec<CacheCellConfig>>) -> LilyConfig {
        LilyConfig {
            cache: Some(CacheConfig {
                mode: Some(mode.into()),
                cells,
                ..CacheConfig::default()
            }),
            ..LilyConfig::default()
        }
    }

    #[test]
    fn standard_typed_cells_are_prevalidated_without_io() {
        let config = config(
            "factory",
            Some(vec![
                cell("sessions", "redis://cache-a:6379/1"),
                cell("ratelimits", "redis://cache-b:6379/2"),
            ]),
        );
        let plans = configured_factory_plans(&config).unwrap();
        assert_eq!(plans.len(), 2);
    }

    #[test]
    fn invalid_or_duplicate_cells_fail_before_any_socket() {
        assert!(configured_factory_plans(&config("factory", Some(Vec::new()))).is_err());
        assert!(configured_factory_plans(&config("single", None)).is_err());
        assert!(
            configured_factory_plans(&config(
                "factory",
                Some(vec![
                    cell("sessions", "redis://cache-a:6379/0"),
                    cell("sessions", "redis://cache-b:6379/0"),
                ]),
            ))
            .is_err()
        );
    }

    #[test]
    fn invalid_private_ca_path_fails_before_any_redis_socket() {
        let mut configured = cell(
            "sessions",
            "rediss://cache-user:cache-password@cache.internal:6380/0",
        );
        configured.use_tls = Some(true);
        configured.additional_ca_bundle = Some("relative-ca.pem".into());
        let error =
            configured_factory_plans(&config("factory", Some(vec![configured]))).unwrap_err();
        assert!(matches!(error, CacheError::InvalidConfiguration(_)));
    }

    #[cfg(feature = "factory")]
    #[test]
    fn factory_feature_publishes_exactly_one_link_time_descriptor() {
        let descriptors = lily_injection_registry::get_all_service_metadata();
        assert_eq!(
            descriptors
                .iter()
                .filter(|metadata| metadata.type_id == std::any::TypeId::of::<CacheFactory>())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn direct_factory_connect_rejects_duplicates_before_io() {
        let plan = RedisCachePlan::from_cell(&cell(
            "sessions",
            "redis://this-host-must-not-be-resolved.invalid:6379/0",
        ))
        .unwrap();
        let result =
            CacheFactory::connect([("sessions".into(), plan.clone()), ("sessions".into(), plan)])
                .await;
        let error = match result {
            Ok(_) => panic!("duplicate direct cells must fail before provider I/O"),
            Err(error) => error,
        };
        assert!(matches!(error, CacheError::InvalidConfiguration(_)));
    }
}
