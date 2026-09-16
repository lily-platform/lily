//! Background services whose execution and scope cleanup belong to their host.
//!
//! Register services with `lily_http_api::AppBuilder::add_background_service`.
//! Other hosts can use [`BackgroundServices`] and [`BackgroundServiceRuntime`]
//! directly, projecting their own absolute shutdown deadlines.
//! Import DI scope types from `lily_injection` or the host's DI re-exports.

mod runtime;
pub use lily_cancellation::ExecutionCancellation;
pub use runtime::{
    BackgroundServiceRuntime, BackgroundServiceSnapshot, BackgroundShutdownDeadlines,
};

use futures::{future::BoxFuture, FutureExt};
use lily_injection::ApplicationScopeFactory;
use std::{
    any::{type_name, TypeId},
    error::Error,
    sync::Arc,
};

/// One host-owned worker. `execute_async` is called once; the worker owns any
/// polling/retry loop. `Ok(())` is a valid one-shot or disabled completion.
/// Unhandled errors and panics request host shutdown; there is no auto-restart.
#[async_trait::async_trait]
pub trait BackgroundServiceTrait: Send + 'static {
    type Error: Error + Send + Sync + 'static;

    /// Construct the worker during application build. Resolve dependencies via
    /// short-lived factory scopes; do not start detached execution here.
    async fn new(scopes: Arc<ApplicationScopeFactory>) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Run after the host has bound its listener. The token is cancelled only
    /// when host shutdown starts. Returning does not stop a healthy host.
    async fn execute_async(
        &mut self,
        stopping_token: ExecutionCancellation,
    ) -> Result<(), Self::Error>;
}

/// Safe lifecycle failure metadata. Application error text is deliberately
/// excluded from host logs and health; workers can log their own safe context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BackgroundServiceError {
    #[error("background service {service} initialization failed")]
    Initialization { service: &'static str },
    #[error("background service {service} initialization panicked")]
    InitializationPanicked { service: &'static str },
    #[error("background runtime is stopping")]
    Stopping,
    #[error("background runtime has not finished initialization")]
    NotInitialized,
    #[error("background supervisor failed")]
    SupervisorFailed,
}

trait Worker: Send {
    fn execute(self: Box<Self>, token: ExecutionCancellation) -> BoxFuture<'static, bool>;
}

impl<T: BackgroundServiceTrait> Worker for T {
    fn execute(mut self: Box<Self>, token: ExecutionCancellation) -> BoxFuture<'static, bool> {
        async move { self.execute_async(token).await.is_ok() }.boxed()
    }
}

type ConstructWorker = fn(
    Arc<ApplicationScopeFactory>,
) -> BoxFuture<'static, Result<Box<dyn Worker>, BackgroundServiceError>>;

struct Registration {
    type_id: TypeId,
    name: &'static str,
    construct: ConstructWorker,
}

/// Typed, deduplicated registrations. Registering the same concrete type twice
/// creates one worker, preserving its first registration order.
#[derive(Default)]
pub struct BackgroundServices {
    registrations: Vec<Registration>,
}

impl BackgroundServices {
    pub fn add<T: BackgroundServiceTrait>(&mut self) {
        if self
            .registrations
            .iter()
            .any(|r| r.type_id == TypeId::of::<T>())
        {
            return;
        }
        self.registrations.push(Registration {
            type_id: TypeId::of::<T>(),
            name: type_name::<T>(),
            construct: |scopes| {
                async move {
                    T::new(scopes)
                        .await
                        .map(|worker| Box::new(worker) as Box<dyn Worker>)
                        .map_err(|_| BackgroundServiceError::Initialization {
                            service: type_name::<T>(),
                        })
                }
                .boxed()
            },
        });
    }

    pub fn is_empty(&self) -> bool {
        self.registrations.is_empty()
    }

    /// Create an owner before awaiting initialization, so cancelled startup
    /// can still shut down and reconcile constructors and their scopes.
    pub fn into_runtime(
        self,
        container: &lily_injection::ApplicationContainer,
    ) -> Arc<BackgroundServiceRuntime> {
        BackgroundServiceRuntime::new(
            self.registrations,
            Arc::new(ApplicationScopeFactory::new(container)),
        )
    }
}
