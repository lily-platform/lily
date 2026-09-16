use crate::storage::{Extensions, ScopeResolutionGuard};
use futures::FutureExt;
use lily_error::injection::InjectionError;
use lily_injection_registry::ServiceDisposeFn;
use lily_injection_registry::ServiceLifetime;
use std::{
    any::{Any, TypeId},
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::Arc,
};
use tokio::sync::OnceCell;

/// Service descriptor containing all information needed to create and manage a service
/// Uses OnceLock for zero-cost singleton caching and boxed closure for flexible factory storage
pub(crate) struct ServiceDescriptor {
    type_name: &'static str,
    /// The lifetime management strategy
    lifetime: ServiceLifetime,

    /// Factory function to create instances (boxed closure)
    #[allow(clippy::type_complexity)]
    factory: Box<
        dyn Fn(
                Arc<Extensions>,
            ) -> Pin<
                Box<
                    dyn Future<
                            Output = Result<Box<dyn std::any::Any + Send + Sync>, InjectionError>,
                        > + Send
                        + 'static,
                >,
            > + Send
            + Sync,
    >,

    /// Zero-cost singleton instance cache (only used for Singleton lifetime)
    /// OnceLock provides thread-safe initialization with zero-cost concurrent reads
    singleton_instance: OnceCell<Result<Arc<dyn std::any::Any + Send + Sync>, InjectionError>>,
    /// Generated lifecycle callback used for singleton, scoped and transient cleanup.
    dispose_fn: Option<ServiceDisposeFn>,
    /// Makes singleton disposal idempotent even when close is called more than
    /// once or startup rollback races with an observer.
    singleton_disposal: OnceCell<Result<(), InjectionError>>,
}

impl ServiceDescriptor {
    /// Create from closure (used by service registry)
    pub(crate) fn from_closure<F>(
        type_name: &'static str,
        lifetime: ServiceLifetime,
        factory: F,
        dispose_fn: Option<ServiceDisposeFn>,
    ) -> Self
    where
        F: Fn(
                Arc<Extensions>,
            ) -> Pin<
                Box<
                    dyn Future<
                            Output = Result<Box<dyn std::any::Any + Send + Sync>, InjectionError>,
                        > + Send
                        + 'static,
                >,
            > + Send
            + Sync
            + 'static,
    {
        Self {
            type_name,
            lifetime,
            factory: Box::new(factory),
            singleton_instance: OnceCell::new(),
            dispose_fn,
            singleton_disposal: OnceCell::new(),
        }
    }

    /// Resolve a concrete service without erasing initialization or scope
    /// failures. Typed and metadata-driven callers share this exact lifetime
    /// path so they cannot observe different singleton/scoped semantics.
    pub(crate) async fn resolve_any(
        &self,
        type_id: TypeId,
        extensions: Arc<Extensions>,
        context: Option<&lily_process::ProcessContext>,
    ) -> Result<Arc<dyn std::any::Any + Send + Sync>, InjectionError> {
        match self.lifetime {
            ServiceLifetime::Singleton => self.get_singleton_any(extensions).await,
            ServiceLifetime::Scoped => self.get_scoped_any(type_id, extensions, context).await,
            ServiceLifetime::Transient => {
                self.create_transient_any(type_id, extensions, context)
                    .await
            }
        }
    }

    /// Get singleton instance (cached).
    async fn get_singleton_any(
        &self,
        extensions: Arc<Extensions>,
    ) -> Result<Arc<dyn std::any::Any + Send + Sync>, InjectionError> {
        let instance_result = self
            .singleton_instance
            .get_or_init(|| async {
                match self.create_boxed(extensions).await {
                    Ok(boxed_instance) => Ok(Arc::from(boxed_instance)),
                    Err(e) => Err(e),
                }
            })
            .await;

        let instance = instance_result.as_ref().map_err(|e| e.clone())?;
        Ok(Arc::clone(instance))
    }

    /// Get scoped instance (cached per process_id)
    async fn get_scoped_any(
        &self,
        type_id: TypeId,
        extensions: Arc<Extensions>,
        context: Option<&lily_process::ProcessContext>,
    ) -> Result<Arc<dyn std::any::Any + Send + Sync>, InjectionError> {
        if let Some(ctx) = context {
            // Scoped instances belong to the same application container that
            // owns the service provider.
            let scope_arc = extensions
                .scope_manager()
                .get_scope(&ctx.process_id_string())
                .ok_or_else(|| InjectionError::ScopeRequired {
                    service: format!("{type_id:?}"),
                })?;
            // Cleanup marks the scope as Closing before removing it from the
            // manager. This guard either attaches the complete resolution to
            // the still-open scope or rejects it; disposal waits until every
            // admitted factory has published its instance or failed.
            let _scope_resolution = ScopeResolutionGuard::enter(Arc::clone(&scope_arc))?;

            // Check cache first
            {
                let scope_context = scope_arc.read().unwrap();
                if let Some(cached_instance) = scope_context.get_cached_any(type_id) {
                    return Ok(cached_instance);
                }
            }

            let creation_lock = scope_arc
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .creation_lock(type_id);
            let _creation_guard = creation_lock.lock().await;

            // Another task in this scope may have initialized the service
            // while this task waited for the creation boundary.
            {
                let scope_context = scope_arc
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(cached_instance) = scope_context.get_cached_any(type_id) {
                    return Ok(cached_instance);
                }
            }

            // Create new instance and cache it
            let boxed_instance = self.create_boxed(extensions).await?;
            let arc_instance: Arc<dyn std::any::Any + Send + Sync> = Arc::from(boxed_instance);
            scope_arc
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .cache_any(
                    type_id,
                    self.type_name,
                    Arc::clone(&arc_instance),
                    self.dispose_fn,
                );
            Ok(arc_instance)
        } else {
            Err(InjectionError::ScopeRequired {
                service: format!("{type_id:?}"),
            })
        }
    }

    /// Create a transient instance and attach its lifecycle to the active
    /// scope. A transient still resolves to a new value on every call; the
    /// scope only retains a disposal ledger entry.
    async fn create_transient_any(
        &self,
        type_id: TypeId,
        extensions: Arc<Extensions>,
        context: Option<&lily_process::ProcessContext>,
    ) -> Result<Arc<dyn std::any::Any + Send + Sync>, InjectionError> {
        if let Some(context) = context {
            let scope = extensions
                .scope_manager()
                .get_scope(&context.process_id_string())
                .ok_or_else(|| InjectionError::ScopeRequired {
                    service: format!("transient {type_id:?}"),
                })?;
            let _scope_resolution = ScopeResolutionGuard::enter(Arc::clone(&scope))?;
            let boxed_instance = self.create_boxed(Arc::clone(&extensions)).await?;
            let arc_instance: Arc<dyn std::any::Any + Send + Sync> = Arc::from(boxed_instance);
            scope
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .track_transient(self.type_name, Arc::clone(&arc_instance), self.dispose_fn);
            Ok(arc_instance)
        } else {
            let boxed_instance = self.create_boxed(Arc::clone(&extensions)).await?;
            let arc_instance: Arc<dyn std::any::Any + Send + Sync> = Arc::from(boxed_instance);
            // This follows the same ownership rule as .NET's root provider:
            // a disposable transient resolved from the root lives until the
            // application container closes.
            extensions
                .track_root_transient(self.type_name, Arc::clone(&arc_instance), self.dispose_fn)
                .await?;
            Ok(arc_instance)
        }
    }

    /// Dispose an initialized singleton exactly once.
    pub(crate) async fn dispose_singleton(&self) -> Result<(), InjectionError> {
        self.singleton_disposal
            .get_or_init(|| async {
                let Some(instance) = self.singleton_instance.get() else {
                    return Ok(());
                };
                let instance = match instance {
                    Ok(instance) => Arc::clone(instance),
                    // A failed factory never published an instance. Generated
                    // factory code already cleans up partial initialization.
                    Err(_) => return Ok(()),
                };
                match self.dispose_fn {
                    Some(dispose_fn) => {
                        match AssertUnwindSafe(dispose_fn(instance)).catch_unwind().await {
                            Ok(result) => result,
                            Err(payload) => Err(InjectionError::DisposalPanicked {
                                service: self.type_name.to_string(),
                                message: panic_payload_message(payload),
                            }),
                        }
                    }
                    None => Ok(()),
                }
            })
            .await
            .clone()
    }

    async fn create_boxed(
        &self,
        extensions: Arc<Extensions>,
    ) -> Result<Box<dyn Any + Send + Sync>, InjectionError> {
        match AssertUnwindSafe((self.factory)(extensions))
            .catch_unwind()
            .await
        {
            Ok(result) => result,
            Err(payload) => Err(InjectionError::ServiceInitializationFailed {
                service: self.type_name.to_string(),
                source: Box::new(InjectionError::InitError(format!(
                    "service factory panicked: {}",
                    panic_payload_message(payload)
                ))),
            }),
        }
    }

    /// Get the lifetime of this service descriptor
    pub(crate) fn get_lifetime(&self) -> ServiceLifetime {
        self.lifetime
    }

    pub(crate) fn type_name(&self) -> &'static str {
        self.type_name
    }

    /// Get singleton instance if it exists
    pub(crate) fn get_singleton_instance(
        &self,
    ) -> Option<Result<Arc<dyn std::any::Any + Send + Sync>, InjectionError>> {
        self.singleton_instance.get().cloned()
    }
}

fn panic_payload_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}
