//! Framework lifecycle adapter for ClickHouse clients.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lily_shutdown::{FrameworkShutdownComponent, FrameworkShutdownPhase, ShutdownError};

use crate::DatabaseService;

enum ClickhouseShutdownTarget {
    Service(Arc<DatabaseService>),
    #[cfg(feature = "factory")]
    Factory(Arc<crate::ClickhouseFactory>),
}

/// Adapter that registers a standalone ClickHouse owner with framework shutdown.
pub struct ClickhouseShutdownHandle {
    target: ClickhouseShutdownTarget,
    timeout: Duration,
}

impl ClickhouseShutdownHandle {
    /// Wraps one standalone database service for dependency-phase shutdown.
    pub fn service(service: Arc<DatabaseService>, timeout: Duration) -> Self {
        Self {
            target: ClickhouseShutdownTarget::Service(service),
            timeout,
        }
    }

    #[cfg(feature = "factory")]
    /// Wraps one factory for dependency-phase shutdown.
    pub fn factory(factory: Arc<crate::ClickhouseFactory>, timeout: Duration) -> Self {
        Self {
            target: ClickhouseShutdownTarget::Factory(factory),
            timeout,
        }
    }
}

#[async_trait]
impl FrameworkShutdownComponent for ClickhouseShutdownHandle {
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        let result = match &self.target {
            ClickhouseShutdownTarget::Service(service) => service.close(),
            #[cfg(feature = "factory")]
            ClickhouseShutdownTarget::Factory(factory) => factory.close(),
        };
        result.map_err(|error| ShutdownError::Component(error.to_string()))
    }

    fn name(&self) -> &str {
        match &self.target {
            ClickhouseShutdownTarget::Service(_) => "clickhouse-client",
            #[cfg(feature = "factory")]
            ClickhouseShutdownTarget::Factory(_) => "clickhouse-client-factory",
        }
    }

    fn phase(&self) -> FrameworkShutdownPhase {
        FrameworkShutdownPhase::DisposeDependencies
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }
}
