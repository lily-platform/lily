//! Owned job scopes without exposing the container or an unscoped provider.

use crate::{ApplicationContainer, ApplicationScope, Extensions, InjectionError};
use futures::{
    FutureExt, StreamExt,
    future::{BoxFuture, Shared},
    stream::FuturesUnordered,
};
use lily_process::ProcessContext;
use std::{
    collections::BTreeMap,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};
use tokio::time::Instant;

type Cleanup = Shared<BoxFuture<'static, Result<(), InjectionError>>>;

struct ScopeRecord {
    observation: crate::__private::ScopeCleanupObservation,
    cleanup: Option<Cleanup>,
}

#[derive(Default)]
struct ScopeRegistry {
    sealed: bool,
    next: u64,
    completed: usize,
    failed: usize,
    scopes: BTreeMap<u64, ScopeRecord>,
}

/// Fixed-size evidence for scopes owned by one factory, including dropped runs.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ServiceScopeSnapshot {
    pub registered: usize,
    pub completed: usize,
    pub failed: usize,
    pub outstanding: usize,
    pub sealed: bool,
}

impl ServiceScopeSnapshot {
    pub fn reconciles(self) -> bool {
        self.registered == self.completed + self.failed + self.outstanding
    }

    pub fn is_terminal(self) -> bool {
        self.sealed && self.outstanding == 0 && self.reconciles()
    }
}

/// Creates isolated application scopes for jobs and other non-request work.
///
/// Share one `Arc<ApplicationScopeFactory>` with workers. The factory itself
/// is not cloneable, and does not expose registration or container shutdown.
/// Each returned scope must run one operation; its disposal is awaited before
/// returning. Dropping a scope/run still starts container-owned cleanup.
pub struct ApplicationScopeFactory {
    extensions: Arc<Extensions>,
    registry: Mutex<ScopeRegistry>,
}

impl ApplicationScopeFactory {
    /// Bind a factory to an existing container without taking shutdown authority.
    pub fn new(container: &ApplicationContainer) -> Self {
        Self {
            extensions: container.services(),
            registry: Mutex::default(),
        }
    }

    /// Create a lazy scope. No services are resolved until `ServiceScope::run`.
    pub fn create_scope(
        self: &Arc<Self>,
        context: ProcessContext,
    ) -> Result<ServiceScope, InjectionError> {
        let mut registry = self.registry.lock().unwrap_or_else(|p| p.into_inner());
        Self::reap(&mut registry);
        if registry.sealed || !self.extensions.accepts_new_scopes() {
            return Err(InjectionError::ContainerClosing);
        }
        let scope = ApplicationScope::new(self.extensions.scope_manager_handle(), context)?;
        let key = registry.next;
        registry.next += 1;
        registry.scopes.insert(
            key,
            ScopeRecord {
                observation: scope.cleanup_observation(),
                cleanup: None,
            },
        );
        Ok(ServiceScope {
            scope,
            factory: Arc::clone(self),
            key,
        })
    }

    fn reap(registry: &mut ScopeRegistry) {
        // Completed entries are retired during normal job traffic too. The
        // registry retains only live work, never an unbounded per-job history.
        registry.scopes.retain(|_, record| {
            match record
                .cleanup
                .as_ref()
                .and_then(|cleanup| cleanup.clone().now_or_never())
            {
                Some(Ok(())) => {
                    registry.completed += 1;
                    false
                }
                Some(Err(_)) => {
                    registry.failed += 1;
                    false
                }
                None => true,
            }
        });
    }

    #[doc(hidden)]
    pub fn snapshot(&self) -> ServiceScopeSnapshot {
        let mut registry = self.registry.lock().unwrap_or_else(|p| p.into_inner());
        Self::reap(&mut registry);
        ServiceScopeSnapshot {
            registered: registry.next as usize,
            completed: registry.completed,
            failed: registry.failed,
            outstanding: registry.scopes.len(),
            sealed: registry.sealed,
        }
    }

    #[doc(hidden)]
    pub fn seal(&self) {
        self.registry
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .sealed = true;
    }

    /// Reconcile only this factory's exact scope generations. Expiry requests
    /// disposer cancellation, but this future returns only after actual joins.
    /// Hosts must retain their owner if an outer deadline stops waiting here.
    #[doc(hidden)]
    pub async fn drain_before(&self, deadline: Instant) -> ServiceScopeSnapshot {
        self.seal();
        let observations: Vec<_> = self
            .registry
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .scopes
            .values()
            .map(|r| r.observation.clone())
            .collect();
        let mut waits: FuturesUnordered<_> = observations
            .into_iter()
            .map(|observation| async move {
                let _ =
                    crate::__private::wait_for_observed_scope_cleanup_before(observation, deadline)
                        .await;
            })
            .collect();
        while waits.next().await.is_some() {}
        self.snapshot()
    }
}

/// A single-use scope. `run` supplies the provider only within its ProcessContext.
///
/// The provider borrow cannot escape the run:
///
/// ```compile_fail
/// use std::sync::Arc;
/// use lily_injection::{ApplicationScopeFactory, Extensions, InjectionError, ProcessContext};
/// async fn escape(factory: Arc<ApplicationScopeFactory>) -> Result<(), InjectionError> {
///     let provider: &Extensions = factory.create_scope(ProcessContext::new())?
///         .run(|extensions| Box::pin(async move {
///             Ok::<_, InjectionError>(extensions)
///         })).await?;
///     let _ = provider;
///     Ok(())
/// }
/// ```
///
/// Resolved `Arc` values can still escape. Applications must not use a scoped
/// service after its scope has closed.
#[must_use = "run the scope to perform work; dropping it begins cleanup"]
pub struct ServiceScope {
    scope: ApplicationScope,
    factory: Arc<ApplicationScopeFactory>,
    key: u64,
}

impl ServiceScope {
    /// Run work and await scope disposal on both success and error. Cleanup
    /// errors take precedence over the application's result. Panics are
    /// resumed after disposal; task cancellation uses the owned Drop path.
    pub async fn run<T, E, F>(mut self, operation: F) -> Result<T, E>
    where
        E: From<InjectionError>,
        F: for<'scope> FnOnce(&'scope Extensions) -> BoxFuture<'scope, Result<T, E>>,
    {
        let result = AssertUnwindSafe(
            self.scope
                .run(async { operation(&self.factory.extensions).await }),
        )
        .catch_unwind()
        .await;
        let cleanup = self.begin_cleanup();
        let disposed = cleanup.await;
        self.factory.snapshot();
        match result {
            Err(panic) => std::panic::resume_unwind(panic),
            Ok(result) => {
                disposed.map_err(E::from)?;
                result.map_err(E::from)?
            }
        }
    }

    fn begin_cleanup(&mut self) -> Cleanup {
        let mut registry = self
            .factory
            .registry
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let Some(record) = registry.scopes.get_mut(&self.key) else {
            return futures::future::ready(Ok(())).boxed().shared();
        };
        record
            .cleanup
            .get_or_insert_with(|| {
                let ticket = self.scope.begin_cleanup();
                let observation = record.observation.clone();
                async move {
                    let result = match ticket {
                        Some(ticket) => ticket.wait().await,
                        None => Err(InjectionError::ContainerClosing),
                    };
                    crate::__private::wait_for_observed_scope_cleanup(observation).await;
                    result
                }
                .boxed()
                .shared()
            })
            .clone()
    }
}

impl Drop for ServiceScope {
    fn drop(&mut self) {
        drop(self.begin_cleanup());
    }
}
