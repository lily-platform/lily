//! Framework lifecycle adapter for MongoDB clients.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lily_shutdown::{FrameworkShutdownComponent, FrameworkShutdownPhase, ShutdownError};

use crate::DatabaseService;

enum MongoShutdownTarget {
    Service(Arc<DatabaseService>),
    #[cfg(feature = "factory-api")]
    Factory(Arc<crate::MongoFactory>),
}

/// Adapter that places a manually owned MongoDB client in Lily's shutdown
/// coordinator.
///
/// A DI-owned `DatabaseService` or `MongoFactory` already disposes through its
/// `ServiceTrait` lifecycle and does not need a second shutdown handle. Use
/// this adapter only when another composition root constructed the target and
/// transfers shutdown responsibility to `lily_shutdown`.
pub struct MongoShutdownHandle {
    target: MongoShutdownTarget,
    timeout: Duration,
}

impl MongoShutdownHandle {
    /// Wraps a manually owned single-database service.
    ///
    /// `timeout` is the maximum duration granted by the framework shutdown
    /// coordinator to this dependency phase.
    pub fn service(service: Arc<DatabaseService>, timeout: Duration) -> Self {
        Self {
            target: MongoShutdownTarget::Service(service),
            timeout,
        }
    }

    #[cfg(feature = "factory-api")]
    /// Wraps a manually owned named-cell factory.
    ///
    /// Do not register this in addition to the factory's DI lifecycle.
    pub fn factory(factory: Arc<crate::MongoFactory>, timeout: Duration) -> Self {
        Self {
            target: MongoShutdownTarget::Factory(factory),
            timeout,
        }
    }
}

#[async_trait]
impl FrameworkShutdownComponent for MongoShutdownHandle {
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        let result = match &self.target {
            MongoShutdownTarget::Service(service) => service.close(),
            #[cfg(feature = "factory-api")]
            MongoShutdownTarget::Factory(factory) => factory.close(),
        };
        result.map_err(|error| ShutdownError::Component(error.to_string()))
    }

    fn name(&self) -> &str {
        match &self.target {
            MongoShutdownTarget::Service(_) => "mongodb-client",
            #[cfg(feature = "factory-api")]
            MongoShutdownTarget::Factory(_) => "mongodb-client-factory",
        }
    }

    fn phase(&self) -> FrameworkShutdownPhase {
        FrameworkShutdownPhase::DisposeDependencies
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }
}
