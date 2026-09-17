//! Framework lifecycle adapter for Redis-only cache services.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lily_shutdown::{FrameworkShutdownComponent, FrameworkShutdownPhase, ShutdownError};

use crate::{ICache, cache_service::CacheService};

enum CacheShutdownTarget {
    Service(Arc<CacheService>),
    #[cfg(feature = "factory")]
    Factory(Arc<crate::CacheFactory>),
}

/// Adapter that registers a standalone cache owner with framework shutdown.
pub struct CacheShutdownHandle {
    target: CacheShutdownTarget,
    timeout: Duration,
}

impl CacheShutdownHandle {
    /// Wraps one standalone cache service for dependency-phase shutdown.
    pub fn service(service: Arc<CacheService>, timeout: Duration) -> Self {
        Self {
            target: CacheShutdownTarget::Service(service),
            timeout,
        }
    }

    #[cfg(feature = "factory")]
    /// Wraps one standalone cache factory for dependency-phase shutdown.
    pub fn factory(factory: Arc<crate::CacheFactory>, timeout: Duration) -> Self {
        Self {
            target: CacheShutdownTarget::Factory(factory),
            timeout,
        }
    }
}

#[async_trait]
impl FrameworkShutdownComponent for CacheShutdownHandle {
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        let result = match &self.target {
            CacheShutdownTarget::Service(service) => service.dispose().await,
            #[cfg(feature = "factory")]
            CacheShutdownTarget::Factory(factory) => factory.dispose_all().await,
        };
        result.map_err(|error| ShutdownError::Component(error.to_string()))
    }

    fn name(&self) -> &str {
        match &self.target {
            CacheShutdownTarget::Service(_) => "redis-cache",
            #[cfg(feature = "factory")]
            CacheShutdownTarget::Factory(_) => "redis-cache-factory",
        }
    }

    fn phase(&self) -> FrameworkShutdownPhase {
        FrameworkShutdownPhase::DisposeDependencies
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }
}
