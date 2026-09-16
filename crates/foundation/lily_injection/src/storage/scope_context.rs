use lily_injection_registry::ServiceDisposeFn;
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::{Arc, RwLock},
};

use lily_error::injection::InjectionError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScopeState {
    Open,
    Closing,
    Closed,
}

/// Keeps one resolution attached to a live scope until its factory has either
/// published the instance to the ledger or failed. Cleanup marks the scope as
/// closing first and waits for these guards, closing the create-vs-dispose
/// race without holding a synchronous lock across `.await`.
pub(crate) struct ScopeResolutionGuard {
    scope: Arc<RwLock<ScopeContext>>,
}

/// Synchronous ownership handoff installed when a cleanup task is lost while
/// an admitted resolution can still publish into the scope ledger.
///
/// The final [`ScopeResolutionGuard`] detaches the complete ledger under the
/// scope lock and invokes this callback only after releasing that lock.
pub(crate) struct ScopeResolutionDrainHandoff {
    completion: Option<Box<dyn FnOnce(Vec<ScopedInstance>) + Send + Sync + 'static>>,
}

impl ScopeResolutionDrainHandoff {
    pub(crate) fn new(
        completion: impl FnOnce(Vec<ScopedInstance>) + Send + Sync + 'static,
    ) -> Self {
        Self {
            completion: Some(Box::new(completion)),
        }
    }

    fn complete(mut self, instances: Vec<ScopedInstance>) {
        if let Some(completion) = self.completion.take() {
            completion(instances);
        }
    }
}

impl std::fmt::Debug for ScopeResolutionDrainHandoff {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScopeResolutionDrainHandoff")
            .field("armed", &self.completion.is_some())
            .finish()
    }
}

impl ScopeResolutionGuard {
    pub(crate) fn enter(scope: Arc<RwLock<ScopeContext>>) -> Result<Self, InjectionError> {
        {
            let mut context = scope
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if context.state != ScopeState::Open {
                return Err(InjectionError::ScopeClosed {
                    scope_id: context.process_id.clone(),
                });
            }
            context.in_flight_resolutions += 1;
        }
        Ok(Self { scope })
    }
}

impl Drop for ScopeResolutionGuard {
    fn drop(&mut self) {
        let (notify, handoff) = {
            let mut context = self
                .scope
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            debug_assert!(context.in_flight_resolutions > 0);
            context.in_flight_resolutions = context.in_flight_resolutions.saturating_sub(1);
            if context.in_flight_resolutions == 0 {
                let notify = Arc::clone(&context.in_flight_changed);
                let handoff = context.resolution_drain_handoff.take().map(|handoff| {
                    context.mark_closed();
                    let instances = context.take_instances_for_disposal();
                    (handoff, instances)
                });
                (Some(notify), handoff)
            } else {
                (None, None)
            }
        };
        if let Some(notify) = notify {
            // There is exactly one cleanup owner per scope. `notify_one`
            // retains a permit when the waiter has not been polled yet, so a
            // resolution finishing between the count check and `.await`
            // cannot strand cleanup forever.
            notify.notify_one();
        }
        if let Some((handoff, instances)) = handoff {
            handoff.complete(instances);
        }
    }
}

pub(crate) struct ScopedInstance {
    pub type_name: &'static str,
    pub kind: ScopedInstanceKind,
    pub value: Arc<dyn Any + Send + Sync>,
    pub dispose_fn: Option<ServiceDisposeFn>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopedInstanceKind {
    Scoped,
    Transient,
    PartialInitialization,
}

#[derive(Debug)]
enum LifecycleEntry {
    /// Scoped values stay owned by the cache; the ledger stores only their
    /// creation order so it does not inflate the user-visible `Arc` count.
    Scoped(TypeId),
    /// Transients have no cache, therefore the ledger is their lifecycle owner.
    Transient(ScopedInstance),
}

impl std::fmt::Debug for ScopedInstance {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScopedInstance")
            .field("type_name", &self.type_name)
            .field("kind", &self.kind)
            .field("managed", &self.dispose_fn.is_some())
            .finish_non_exhaustive()
    }
}

/// Crate-internal cache and lifecycle ledger for one request/job scope.
#[derive(Debug)]
pub(crate) struct ScopeContext {
    /// Unique identifier for this scope (e.g., request_id, process_id)
    pub(crate) process_id: String,
    pub(super) owner_context: Option<lily_process::ProcessContext>,
    pub(super) trace: super::scope_trace::ScopeTrace,

    /// Cache of scoped service instances for this specific process/request
    /// Key: TypeId of the service, Value: Arc to the service instance
    scoped_instances: HashMap<TypeId, ScopedInstance>,
    /// Every resource-owning scoped or transient instance in exact creation
    /// order. Scoped instances also live in `scoped_instances` for reuse;
    /// transient instances appear only in this lifecycle ledger.
    lifecycle_instances: Vec<LifecycleEntry>,
    creation_locks: HashMap<TypeId, Arc<tokio::sync::Mutex<()>>>,
    state: ScopeState,
    in_flight_resolutions: usize,
    in_flight_changed: Arc<tokio::sync::Notify>,
    resolution_drain_handoff: Option<ScopeResolutionDrainHandoff>,
}

impl ScopeContext {
    /// Create a new scope context with the given process_id
    pub(crate) fn new(process_id: String) -> Self {
        Self {
            process_id,
            owner_context: None,
            trace: super::scope_trace::ScopeTrace::capture(),
            scoped_instances: HashMap::new(),
            lifecycle_instances: Vec::new(),
            creation_locks: HashMap::new(),
            state: ScopeState::Open,
            in_flight_resolutions: 0,
            in_flight_changed: Arc::new(tokio::sync::Notify::new()),
            resolution_drain_handoff: None,
        }
    }

    pub(crate) fn begin_closing(&mut self) {
        if self.state == ScopeState::Open {
            self.state = ScopeState::Closing;
        }
    }

    pub(crate) fn in_flight_resolution_count(&self) -> usize {
        self.in_flight_resolutions
    }

    pub(crate) fn in_flight_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.in_flight_changed)
    }

    /// Install the one owner that will reconcile this closing generation when
    /// its final admitted resolution terminates. The caller must hold the
    /// scope write lock while checking the in-flight count and installing the
    /// handoff, closing the zero-to-install race.
    pub(crate) fn install_resolution_drain_handoff(
        &mut self,
        handoff: ScopeResolutionDrainHandoff,
    ) -> Result<(), ScopeResolutionDrainHandoff> {
        if self.resolution_drain_handoff.is_some() {
            return Err(handoff);
        }
        self.resolution_drain_handoff = Some(handoff);
        Ok(())
    }

    pub(crate) fn mark_closed(&mut self) {
        self.state = ScopeState::Closed;
    }

    /// Get a cached scoped service instance if it exists
    #[allow(dead_code, reason = "retained for crate-local scope contract tests")]
    pub(crate) fn get_cached<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        let type_id = TypeId::of::<T>();
        self.scoped_instances
            .get(&type_id)?
            .value
            .clone()
            .downcast::<T>()
            .ok()
    }

    pub(crate) fn get_cached_any(&self, type_id: TypeId) -> Option<Arc<dyn Any + Send + Sync>> {
        self.scoped_instances
            .get(&type_id)
            .map(|instance| Arc::clone(&instance.value))
    }

    pub(crate) fn creation_lock(&mut self, type_id: TypeId) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(
            self.creation_locks
                .entry(type_id)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// Cache a scoped service instance for this process/request
    #[allow(dead_code, reason = "retained for crate-local scope contract tests")]
    pub(crate) fn cache_instance<T: Send + Sync + 'static>(&mut self, instance: Arc<T>) {
        self.cache_managed_instance(std::any::type_name::<T>(), instance, None);
    }

    #[allow(dead_code, reason = "retained for crate-local scope contract tests")]
    pub(crate) fn cache_managed_instance<T: Send + Sync + 'static>(
        &mut self,
        type_name: &'static str,
        instance: Arc<T>,
        dispose_fn: Option<ServiceDisposeFn>,
    ) {
        let type_id = TypeId::of::<T>();
        let any_instance: Arc<dyn Any + Send + Sync> = instance;
        self.cache_any(type_id, type_name, any_instance, dispose_fn);
    }

    pub(crate) fn cache_any(
        &mut self,
        type_id: TypeId,
        type_name: &'static str,
        instance: Arc<dyn Any + Send + Sync>,
        dispose_fn: Option<ServiceDisposeFn>,
    ) {
        if !self.scoped_instances.contains_key(&type_id) {
            self.lifecycle_instances
                .push(LifecycleEntry::Scoped(type_id));
        }
        self.scoped_instances.insert(
            type_id,
            ScopedInstance {
                type_name,
                kind: ScopedInstanceKind::Scoped,
                value: instance,
                dispose_fn,
            },
        );
    }

    /// Track a transient without caching it for future resolution. Each call
    /// remains a fresh instance while cleanup stays owned by this scope.
    pub(crate) fn track_transient(
        &mut self,
        type_name: &'static str,
        instance: Arc<dyn Any + Send + Sync>,
        dispose_fn: Option<ServiceDisposeFn>,
    ) {
        self.lifecycle_instances
            .push(LifecycleEntry::Transient(ScopedInstance {
                type_name,
                kind: ScopedInstanceKind::Transient,
                value: instance,
                dispose_fn,
            }));
    }

    /// Record a partially initialized service after its resolution future was
    /// cancelled. It is not resolvable or cached, but cleanup owns it and will
    /// invoke the same generated disposer exactly once.
    pub(crate) fn track_partial_initialization(
        &mut self,
        type_name: &'static str,
        instance: Arc<dyn Any + Send + Sync>,
        dispose_fn: ServiceDisposeFn,
    ) {
        self.lifecycle_instances
            .push(LifecycleEntry::Transient(ScopedInstance {
                type_name,
                kind: ScopedInstanceKind::PartialInitialization,
                value: instance,
                dispose_fn: Some(dispose_fn),
            }));
    }

    /// Get the number of cached instances in this scope
    #[allow(
        dead_code,
        reason = "retained for crate-local scope diagnostics and tests"
    )]
    pub(crate) fn cached_count(&self) -> usize {
        self.scoped_instances.len()
    }

    /// Clear all cached instances (useful for scope cleanup)
    #[allow(dead_code, reason = "retained for crate-local scope contract tests")]
    pub(crate) fn clear(&mut self) {
        self.scoped_instances.clear();
        self.lifecycle_instances.clear();
        self.creation_locks.clear();
    }

    /// Move instances out in reverse construction order so dependants are
    /// disposed before the dependencies they retain.
    pub(crate) fn take_instances_for_disposal(&mut self) -> Vec<ScopedInstance> {
        let mut instances = Vec::with_capacity(self.lifecycle_instances.len());
        for entry in self.lifecycle_instances.drain(..).rev() {
            match entry {
                LifecycleEntry::Scoped(type_id) => {
                    if let Some(instance) = self.scoped_instances.remove(&type_id) {
                        instances.push(instance);
                    }
                }
                LifecycleEntry::Transient(instance) => instances.push(instance),
            }
        }
        self.scoped_instances.clear();
        self.creation_locks.clear();
        instances
    }
}

impl Default for ScopeContext {
    fn default() -> Self {
        Self::new("default".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestService {
        value: i32,
    }

    #[test]
    fn test_scope_context_creation() {
        let context = ScopeContext::new("request-123".to_string());
        assert_eq!(context.process_id, "request-123");
        assert_eq!(context.cached_count(), 0);
    }

    #[test]
    fn test_cache_and_retrieve() {
        let mut context = ScopeContext::new("request-456".to_string());

        // Cache a service
        let service = Arc::new(TestService { value: 42 });
        context.cache_instance(Arc::clone(&service));

        assert_eq!(context.cached_count(), 1);

        // Retrieve the cached service
        let cached_service: Option<Arc<TestService>> = context.get_cached();
        assert!(cached_service.is_some());
        assert_eq!(cached_service.unwrap().value, 42);
    }

    #[test]
    fn test_scope_isolation() {
        let mut context1 = ScopeContext::new("request-1".to_string());
        let mut context2 = ScopeContext::new("request-2".to_string());

        // Cache service in context1
        let service1 = Arc::new(TestService { value: 100 });
        context1.cache_instance(service1);

        // Cache different service in context2
        let service2 = Arc::new(TestService { value: 200 });
        context2.cache_instance(service2);

        // Verify isolation
        let cached1: Option<Arc<TestService>> = context1.get_cached();
        let cached2: Option<Arc<TestService>> = context2.get_cached();

        assert_eq!(cached1.unwrap().value, 100);
        assert_eq!(cached2.unwrap().value, 200);
    }

    #[test]
    fn test_clear_cache() {
        let mut context = ScopeContext::new("request-clear".to_string());

        let service = Arc::new(TestService { value: 999 });
        context.cache_instance(service);
        assert_eq!(context.cached_count(), 1);

        context.clear();
        assert_eq!(context.cached_count(), 0);

        let cached: Option<Arc<TestService>> = context.get_cached();
        assert!(cached.is_none());
    }
}
