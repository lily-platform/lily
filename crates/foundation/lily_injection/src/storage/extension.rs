use crate::application_container::{DeadlineAwait, SeededSingleton, await_before_deadline};
use crate::service::ServiceDescriptor;
use crate::storage::registration_plan::RegistrationPlan;
use crate::storage::resolution_trace::{self, ResolutionMetadata};
use futures::FutureExt;
use lily_error::injection::{InjectionError, ShutdownOutcome, ShutdownOutcomeStatus};
use lily_injection_registry::{ServiceDisposeFn, ServiceLifetime, ServiceRouteMetadata};
use lily_process::ProcessContext;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{KeyValue, global};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::task::{Context, Poll};
use tokio::sync::Notify;

const PROVIDER_BUILDING: u8 = 0;
const PROVIDER_RUNNING: u8 = 1;
const PROVIDER_CLOSING: u8 = 2;
const PROVIDER_CLOSED: u8 = 3;
const PROVIDER_FAILED: u8 = 4;

enum RootManagedInstance {
    Singleton {
        type_id: TypeId,
        type_name: &'static str,
    },
    Transient {
        type_name: &'static str,
        value: Arc<dyn Any + Send + Sync>,
        dispose_fn: Option<ServiceDisposeFn>,
    },
}

struct ProviderResolutionGuard<'a> {
    provider: &'a Extensions,
}

impl Drop for ProviderResolutionGuard<'_> {
    fn drop(&mut self) {
        if self
            .provider
            .in_flight_resolutions
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            self.provider.resolutions_changed.notify_one();
        }
    }
}

/// Read-only service provider owned by one [`ApplicationContainer`](crate::ApplicationContainer).
///
/// Application code receives this type as `Arc<Extensions>` from a framework
/// boundary and uses only [`get_service`](Self::get_service). Registration,
/// cache mutation, scope creation and lifecycle ownership are deliberately not
/// public. Do not construct another container merely to obtain a provider.
pub struct Extensions {
    /// One descriptor per concrete implementation. Interface routes never
    /// create another descriptor, cache entry or lifecycle owner.
    service_descriptors: Arc<HashMap<TypeId, ServiceDescriptor>>,
    routes: Arc<HashMap<TypeId, ServiceRouteMetadata>>,
    /// Dependency-free framework handles attached by an application adapter
    /// before it materializes user code. These handles do not alter the
    /// validated business-service graph and have no independent lifecycle
    /// hooks; the provider owns their `Arc` until it is dropped.
    framework_singletons: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
    self_weak: std::sync::Weak<Extensions>,
    scope_manager: Arc<crate::storage::ScopeManager>,
    /// One creation-ordered ledger is required for correct mixed lifetime
    /// teardown. A transient created while constructing a singleton is placed
    /// before that singleton; a root transient resolved later is placed after
    /// it. Reversing this single ledger therefore closes dependants first in
    /// both cases.
    root_instances: Mutex<Vec<RootManagedInstance>>,
    state: AtomicU8,
    /// Covers the complete public resolution, including generated factory
    /// initialization and interface projection. Shutdown waits for admitted
    /// resolutions before taking the root lifecycle ledger.
    in_flight_resolutions: AtomicUsize,
    resolutions_changed: Notify,
    resolution_counter: Counter<u64>,
    cache_hit_counter: Counter<u64>,
    resolution_duration: Histogram<f64>,
}

impl Extensions {
    /// Resolve a concrete service or a derive-declared business interface.
    ///
    /// Both `get_service::<Concrete>()` and `get_service::<dyn Interface>()`
    /// travel through the same concrete descriptor. The latter only applies a
    /// compile-time generated safe `Arc` projection after lifetime resolution.
    ///
    /// `context` selects scope ownership. Passing `None` reuses the current
    /// task-local [`ProcessContext`] when available. A singleton and a
    /// root-owned transient may resolve without a context; a scoped service
    /// requires an active application scope. Prefer `#[inject] Arc<T>` when the
    /// dependency is known while constructing another service.
    pub async fn get_service<T: ?Sized + Send + Sync + 'static>(
        &self,
        context: Option<&ProcessContext>,
    ) -> Result<Arc<T>, InjectionError> {
        let requested_type_id = TypeId::of::<T>();
        let route = self.routes.get(&requested_type_id);
        let descriptor =
            route.and_then(|route| self.service_descriptors.get(&route.implementation_type_id));
        // Framework handles have no registration route, but are owned singletons.
        // Only consult their lock for the uncommon route-less lookup.
        let framework_handle = route.is_none()
            && self
                .framework_singletons
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .contains_key(&requested_type_id);
        let metadata = ResolutionMetadata {
            requested: std::any::type_name::<T>(),
            implementation: route
                .map(|route| route.implementation_type_name)
                .or_else(|| framework_handle.then_some(std::any::type_name::<T>())),
            lifetime: descriptor
                .map(ServiceDescriptor::get_lifetime)
                .or_else(|| framework_handle.then_some(ServiceLifetime::Singleton)),
        };
        resolution_trace::observe(metadata, self.resolve_service::<T>(context)).await
    }

    async fn resolve_service<T: ?Sized + Send + Sync + 'static>(
        &self,
        context: Option<&ProcessContext>,
    ) -> Result<Arc<T>, InjectionError> {
        let requested_type_id = TypeId::of::<T>();
        let requested_type_name = std::any::type_name::<T>();
        let owned_context = context.cloned().or_else(ProcessContext::current);
        let context = owned_context.as_ref();
        let started = std::time::Instant::now();

        let _resolution_guard = self.begin_resolution(context)?;
        self.resolution_counter.add(
            1,
            &[KeyValue::new(
                "service_type",
                requested_type_name.to_string(),
            )],
        );

        if let Some(instance) = self.framework_singleton::<T>(requested_type_id)? {
            self.resolution_duration.record(
                started.elapsed().as_secs_f64(),
                &[
                    KeyValue::new("service_type", requested_type_name.to_string()),
                    KeyValue::new("status", "success"),
                ],
            );
            return Ok(instance);
        }

        let route = self.routes.get(&requested_type_id).ok_or_else(|| {
            InjectionError::ServiceNotFound(format!(
                "Service '{requested_type_name}' is not registered in this application"
            ))
        })?;
        let descriptor = self
            .service_descriptors
            .get(&route.implementation_type_id)
            .ok_or_else(|| {
                InjectionError::InvalidRegistrationPlan(format!(
                    "route '{}' has no concrete descriptor",
                    route.requested_type_name
                ))
            })?;

        if descriptor.get_lifetime() == ServiceLifetime::Singleton
            && descriptor.get_singleton_instance().is_some()
        {
            self.cache_hit_counter.add(
                1,
                &[KeyValue::new(
                    "service_type",
                    requested_type_name.to_string(),
                )],
            );
        }

        let provider = self.provider_arc()?;
        let resolution = descriptor.resolve_any(route.implementation_type_id, provider, context);
        let concrete = if let Some(context) = context {
            ProcessContext::scope(context.clone(), resolution).await
        } else {
            resolution.await
        };

        let result = match concrete {
            Ok(instance) => (route.project_fn)(instance).and_then(|projected| {
                projected.downcast::<Arc<T>>().map(|arc| *arc).map_err(|_| {
                    InjectionError::InvalidRegistrationPlan(format!(
                        "route '{}' returned an incompatible projection",
                        route.requested_type_name
                    ))
                })
            }),
            Err(InjectionError::ScopeRequired { .. }) => Err(InjectionError::ScopeRequired {
                service: requested_type_name.to_string(),
            }),
            Err(error) => Err(error),
        };

        self.resolution_duration.record(
            started.elapsed().as_secs_f64(),
            &[
                KeyValue::new("service_type", requested_type_name.to_string()),
                KeyValue::new("status", if result.is_ok() { "success" } else { "error" }),
            ],
        );
        result
    }

    /// Resolve the canonical concrete instance for metadata-driven framework
    /// adapters. All failures, including not-found and factory failures, are
    /// preserved as a typed result.
    ///
    /// If `type_id` identifies an interface route, this method still returns
    /// its underlying concrete `Any` instance. Callers that know a Rust type at
    /// compile time should use [`get_service`](Self::get_service), which also
    /// applies the generated interface projection.
    pub(crate) async fn get_service_by_type_id(
        &self,
        type_id: TypeId,
        context: Option<&ProcessContext>,
    ) -> Result<Arc<dyn Any + Send + Sync>, InjectionError> {
        let owned_context = context.cloned().or_else(ProcessContext::current);
        let context = owned_context.as_ref();
        let _resolution_guard = self.begin_resolution(context)?;

        let route = self.routes.get(&type_id).ok_or_else(|| {
            InjectionError::ServiceNotFound(format!("Service {type_id:?} is not registered"))
        })?;
        let descriptor = self
            .service_descriptors
            .get(&route.implementation_type_id)
            .ok_or_else(|| {
                InjectionError::InvalidRegistrationPlan(format!(
                    "route '{}' has no concrete descriptor",
                    route.requested_type_name
                ))
            })?;
        let provider = self.provider_arc()?;
        let resolution = descriptor.resolve_any(route.implementation_type_id, provider, context);
        let result = if let Some(context) = context {
            ProcessContext::scope(context.clone(), resolution).await
        } else {
            resolution.await
        };
        match result {
            Err(InjectionError::ScopeRequired { .. }) => Err(InjectionError::ScopeRequired {
                service: route.requested_type_name.to_string(),
            }),
            result => result,
        }
    }

    /// Resolve one concrete singleton from the validated eager-start plan.
    ///
    /// Unlike public resolution this deliberately passes no application scope,
    /// preserving the existing root-build semantics. It still admits the
    /// complete factory through the provider resolution counter so rollback
    /// has an explicit drain barrier after cancellation.
    pub(crate) async fn resolve_eager_singleton(
        self: &Arc<Self>,
        type_id: TypeId,
    ) -> Result<(), InjectionError> {
        let _resolution_guard = self.begin_resolution(None)?;
        let descriptor = self.service_descriptors.get(&type_id).ok_or_else(|| {
            InjectionError::InvalidRegistrationPlan(format!(
                "singleton startup order contains missing descriptor {type_id:?}"
            ))
        })?;
        descriptor
            .resolve_any(type_id, Arc::clone(self), None)
            .await
            .map(drop)
    }

    /// Inspect the canonical lifetime behind a concrete or interface route
    /// without resolving the service or mutating provider state.
    pub(crate) fn service_registration_lifetime(&self, type_id: TypeId) -> Option<ServiceLifetime> {
        let route = self.routes.get(&type_id)?;
        self.service_descriptors
            .get(&route.implementation_type_id)
            .map(ServiceDescriptor::get_lifetime)
    }

    fn provider_arc(&self) -> Result<Arc<Self>, InjectionError> {
        self.self_weak
            .upgrade()
            .ok_or(InjectionError::ContainerClosed)
    }

    fn framework_singleton<T: ?Sized + Send + Sync + 'static>(
        &self,
        type_id: TypeId,
    ) -> Result<Option<Arc<T>>, InjectionError> {
        let instance = self
            .framework_singletons
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&type_id)
            .cloned();
        instance
            .map(|instance| {
                instance
                    .downcast::<Arc<T>>()
                    .map(|instance| Arc::clone(instance.as_ref()))
                    .map_err(|_| {
                        InjectionError::InvalidRegistrationPlan(format!(
                            "framework singleton '{}' has an incompatible concrete value",
                            std::any::type_name::<T>()
                        ))
                    })
            })
            .transpose()
    }

    /// Attach one dependency-free, immutable framework handle to this
    /// application provider.
    ///
    /// This is deliberately narrower than DI registration: it cannot declare
    /// interfaces, dependencies, lifetimes, initialization or disposal. The
    /// returned guard removes the handle unless the adapter commits it after
    /// its complete build succeeds.
    pub(crate) fn attach_framework_singleton<T>(
        self: &Arc<Self>,
        service: Arc<T>,
    ) -> Result<FrameworkSingletonAttachment<T>, InjectionError>
    where
        T: Send + Sync + 'static,
    {
        // Treat attachment as one admitted provider operation so shutdown
        // cannot pass this publication boundary while it is being mutated.
        let _resolution_guard = self.begin_resolution(None)?;
        let type_id = TypeId::of::<T>();
        let type_name = std::any::type_name::<T>();
        if self.routes.contains_key(&type_id) {
            return Err(InjectionError::InvalidRegistrationPlan(format!(
                "framework singleton '{type_name}' conflicts with a registered DI service"
            )));
        }

        let mut attached = self
            .framework_singletons
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if attached.contains_key(&type_id) {
            return Err(InjectionError::InvalidRegistrationPlan(format!(
                "framework singleton '{type_name}' is already attached to this application"
            )));
        }
        let erased: Arc<dyn Any + Send + Sync> = Arc::new(Arc::clone(&service));
        attached.insert(type_id, erased);
        drop(attached);

        Ok(FrameworkSingletonAttachment {
            provider: Arc::downgrade(self),
            committed: false,
            marker: PhantomData,
        })
    }

    fn detach_framework_singleton(&self, type_id: TypeId) {
        self.framework_singletons
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&type_id);
    }

    fn ensure_resolution_allowed(
        &self,
        context: Option<&ProcessContext>,
    ) -> Result<(), InjectionError> {
        match self.state.load(Ordering::Acquire) {
            PROVIDER_BUILDING | PROVIDER_RUNNING => Ok(()),
            PROVIDER_CLOSING => {
                // Work admitted before shutdown may finish and continue to
                // resolve dependencies from its still-live managed scope.
                if context
                    .and_then(|context| self.scope_manager.get_scope(&context.process_id_string()))
                    .is_some()
                {
                    Ok(())
                } else {
                    Err(InjectionError::ContainerClosing)
                }
            }
            PROVIDER_CLOSED | PROVIDER_FAILED => Err(InjectionError::ContainerClosed),
            _ => Err(InjectionError::ContainerClosed),
        }
    }

    fn begin_resolution(
        &self,
        context: Option<&ProcessContext>,
    ) -> Result<ProviderResolutionGuard<'_>, InjectionError> {
        self.ensure_resolution_allowed(context)?;
        self.in_flight_resolutions.fetch_add(1, Ordering::AcqRel);

        // Close the admission race with `begin_shutdown`: if shutdown changed
        // the state before the counter became visible, either the existing
        // managed scope still authorizes this work or the resolution is
        // rejected and the counter is immediately released.
        if let Err(error) = self.ensure_resolution_allowed(context) {
            if self.in_flight_resolutions.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.resolutions_changed.notify_one();
            }
            return Err(error);
        }
        Ok(ProviderResolutionGuard { provider: self })
    }

    pub(crate) fn in_flight_resolution_count(&self) -> usize {
        self.in_flight_resolutions.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_for_no_in_flight_resolutions(&self) {
        loop {
            if self.in_flight_resolution_count() == 0 {
                return;
            }
            self.resolutions_changed.notified().await;
        }
    }

    pub(crate) fn scope_manager(&self) -> &crate::storage::ScopeManager {
        &self.scope_manager
    }

    pub(crate) fn scope_manager_handle(&self) -> Arc<crate::storage::ScopeManager> {
        Arc::clone(&self.scope_manager)
    }

    pub(crate) fn active_scope_count(&self) -> usize {
        self.scope_manager.active_scope_count()
    }

    pub(crate) fn begin_shutdown(&self) -> Result<(), InjectionError> {
        match self.state.compare_exchange(
            PROVIDER_RUNNING,
            PROVIDER_CLOSING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(PROVIDER_CLOSING) => Ok(()),
            Err(PROVIDER_CLOSED | PROVIDER_FAILED) => Err(InjectionError::ContainerClosed),
            Err(_) => Err(InjectionError::ContainerClosing),
        }
    }

    pub(crate) fn accepts_new_scopes(&self) -> bool {
        self.state.load(Ordering::Acquire) == PROVIDER_RUNNING
    }

    pub(crate) fn mark_closed(&self) {
        self.state.store(PROVIDER_CLOSED, Ordering::Release);
    }

    pub(crate) fn mark_failed(&self) {
        self.state.store(PROVIDER_FAILED, Ordering::Release);
    }

    /// Move a service whose initialization future was cancelled into the
    /// nearest lifecycle ledger. Generated cancellation guards call this
    /// synchronously from `Drop`, before the enclosing resolution guard is
    /// released, so scope/root cleanup cannot overtake publication.
    pub(crate) fn track_cancelled_initialization(
        &self,
        context: Option<&ProcessContext>,
        type_name: &'static str,
        value: Arc<dyn Any + Send + Sync>,
        dispose_fn: ServiceDisposeFn,
    ) {
        if let Some(context) = context
            && let Some(scope) = self
                .scope_manager
                .get_scope_for_lifecycle_registration(&context.process_id_string())
        {
            scope
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .track_partial_initialization(type_name, value, dispose_fn);
            return;
        }

        // Root resolution (including eager singleton construction) has no
        // request scope. The provider-wide resolution guard makes shutdown
        // wait before taking this ledger.
        self.root_instances
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(RootManagedInstance::Transient {
                type_name,
                value,
                dispose_fn: Some(dispose_fn),
            });
    }

    pub(crate) async fn track_root_transient(
        &self,
        type_name: &'static str,
        value: Arc<dyn Any + Send + Sync>,
        dispose_fn: Option<ServiceDisposeFn>,
    ) -> Result<(), InjectionError> {
        {
            let mut instances = self
                .root_instances
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if matches!(
                self.state.load(Ordering::Acquire),
                PROVIDER_BUILDING | PROVIDER_RUNNING
            ) {
                instances.push(RootManagedInstance::Transient {
                    type_name,
                    value,
                    dispose_fn,
                });
                return Ok(());
            }
        }

        // Shutdown won the race after the factory initialized the transient.
        // Clean that value immediately instead of leaving an unowned resource.
        if let Some(dispose_fn) = dispose_fn {
            match AssertUnwindSafe(dispose_fn(value)).catch_unwind().await {
                Ok(result) => result?,
                Err(_) => {
                    return Err(InjectionError::DisposeError(
                        "root transient cleanup panicked while shutdown won a resolution race"
                            .to_string(),
                    ));
                }
            }
        }
        Err(InjectionError::ContainerClosing)
    }

    fn track_root_singleton(&self, type_id: TypeId, type_name: &'static str) {
        self.root_instances
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(RootManagedInstance::Singleton { type_id, type_name });
    }

    pub(crate) fn root_lifecycle_entry_count(&self) -> usize {
        self.root_instances
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// Dispose singleton and root-transient instances in one reverse creation
    /// order. The optional deadline is applied per lifecycle hook so one hung
    /// disposer cannot keep the lifecycle owner alive indefinitely. Entries
    /// that have not started when the aggregate deadline expires are reported
    /// as cancelled rather than being invoked outside the owner's budget.
    pub(crate) async fn dispose_root_instances(
        &self,
        deadline: Option<tokio::time::Instant>,
    ) -> Vec<ShutdownOutcome> {
        let instances = {
            let mut instances = self
                .root_instances
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *instances)
        };
        let mut outcomes = Vec::with_capacity(instances.len());
        for instance in instances.into_iter().rev() {
            if deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
                let (component, detail) = match &instance {
                    RootManagedInstance::Singleton { type_name, .. } => (
                        format!("singleton:{type_name}"),
                        format!(
                            "singleton '{type_name}' disposal was not started because the aggregate deadline was exhausted"
                        ),
                    ),
                    RootManagedInstance::Transient { type_name, .. } => (
                        format!("root-transient:{type_name}"),
                        format!(
                            "root transient '{type_name}' disposal was not started because the aggregate deadline was exhausted"
                        ),
                    ),
                };
                outcomes.push(ShutdownOutcome::with_detail(
                    component,
                    ShutdownOutcomeStatus::Cancelled,
                    detail,
                ));
                continue;
            }

            match instance {
                RootManagedInstance::Singleton { type_id, type_name } => {
                    let component = format!("singleton:{type_name}");
                    let Some(descriptor) = self.service_descriptors.get(&type_id) else {
                        outcomes.push(ShutdownOutcome::with_detail(
                            component,
                            ShutdownOutcomeStatus::Failed,
                            format!("root singleton '{type_name}' has no service descriptor"),
                        ));
                        continue;
                    };
                    let disposal = descriptor.dispose_singleton();
                    let outcome = match deadline {
                        Some(deadline) => match await_before_deadline(deadline, disposal).await {
                            DeadlineAwait::Completed(Ok(())) => {
                                ShutdownOutcome::completed(component)
                            }
                            DeadlineAwait::Completed(Err(InjectionError::DisposalPanicked {
                                message,
                                ..
                            })) => ShutdownOutcome::with_detail(
                                component,
                                ShutdownOutcomeStatus::Panicked,
                                message,
                            ),
                            DeadlineAwait::Completed(Err(error)) => ShutdownOutcome::with_detail(
                                component,
                                ShutdownOutcomeStatus::Failed,
                                error.to_string(),
                            ),
                            DeadlineAwait::TimedOut => ShutdownOutcome::with_detail(
                                component,
                                ShutdownOutcomeStatus::TimedOut,
                                format!("singleton '{type_name}' disposal timed out"),
                            ),
                            DeadlineAwait::Panicked(message) => ShutdownOutcome::with_detail(
                                component,
                                ShutdownOutcomeStatus::Panicked,
                                format!(
                                    "singleton '{type_name}' deadline-controlled disposal panicked: {message}"
                                ),
                            ),
                        },
                        None => match disposal.await {
                            Ok(()) => ShutdownOutcome::completed(component),
                            Err(InjectionError::DisposalPanicked { message, .. }) => {
                                ShutdownOutcome::with_detail(
                                    component,
                                    ShutdownOutcomeStatus::Panicked,
                                    message,
                                )
                            }
                            Err(error) => ShutdownOutcome::with_detail(
                                component,
                                ShutdownOutcomeStatus::Failed,
                                error.to_string(),
                            ),
                        },
                    };
                    outcomes.push(outcome);
                }
                RootManagedInstance::Transient {
                    type_name,
                    value,
                    dispose_fn,
                } => {
                    let component = format!("root-transient:{type_name}");
                    let Some(dispose_fn) = dispose_fn else {
                        outcomes.push(ShutdownOutcome::completed(component));
                        continue;
                    };
                    let disposal = AssertUnwindSafe(dispose_fn(value)).catch_unwind();
                    let outcome = match deadline {
                        Some(deadline) => match await_before_deadline(deadline, disposal).await {
                            DeadlineAwait::Completed(Ok(Ok(()))) => {
                                ShutdownOutcome::completed(component)
                            }
                            DeadlineAwait::Completed(Ok(Err(error))) => {
                                ShutdownOutcome::with_detail(
                                    component,
                                    ShutdownOutcomeStatus::Failed,
                                    error.to_string(),
                                )
                            }
                            DeadlineAwait::Completed(Err(_)) => ShutdownOutcome::with_detail(
                                component,
                                ShutdownOutcomeStatus::Panicked,
                                format!("root transient '{type_name}' disposal panicked"),
                            ),
                            DeadlineAwait::TimedOut => ShutdownOutcome::with_detail(
                                component,
                                ShutdownOutcomeStatus::TimedOut,
                                format!("root transient '{type_name}' disposal timed out"),
                            ),
                            DeadlineAwait::Panicked(message) => ShutdownOutcome::with_detail(
                                component,
                                ShutdownOutcomeStatus::Panicked,
                                format!(
                                    "root transient '{type_name}' deadline-controlled disposal panicked: {message}"
                                ),
                            ),
                        },
                        None => match disposal.await {
                            Ok(Ok(())) => ShutdownOutcome::completed(component),
                            Ok(Err(error)) => ShutdownOutcome::with_detail(
                                component,
                                ShutdownOutcomeStatus::Failed,
                                error.to_string(),
                            ),
                            Err(_) => ShutdownOutcome::with_detail(
                                component,
                                ShutdownOutcomeStatus::Panicked,
                                format!("root transient '{type_name}' disposal panicked"),
                            ),
                        },
                    };
                    outcomes.push(outcome);
                }
            }
        }
        outcomes
    }
}

/// Builder for one immutable application registration plan.
///
/// Registrations and routes are derive/link-time only. Build-time singleton
/// seeds may replace construction for an existing singleton, but cannot add or
/// mutate the graph.
pub(crate) struct ExtensionsBuilder {
    seeded_singletons: Vec<SeededSingleton>,
}

/// One caller-polled eager singleton initialization.
///
/// Keeping this future as an explicit field lets the application-container
/// build handle synchronously drop it before a detached rollback task can
/// inspect the lifecycle ledger.
pub(crate) struct EagerSingletonResolution {
    type_id: TypeId,
    type_name: &'static str,
    future: futures::future::BoxFuture<'static, Result<(), InjectionError>>,
}

impl EagerSingletonResolution {
    pub(crate) fn type_id(&self) -> TypeId {
        self.type_id
    }

    pub(crate) fn type_name(&self) -> &'static str {
        self.type_name
    }
}

impl Future for EagerSingletonResolution {
    type Output = Result<(), InjectionError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(context)
    }
}

/// Prepared provider plus its deterministic eager-singleton cursor.
///
/// Preparation is synchronous and resource-free until the provider is
/// created. The application-container build state machine owns this value from
/// provider creation through either commit or rollback handoff.
pub(crate) struct ExtensionsBuildTransaction {
    provider: Arc<Extensions>,
    singleton_order: Vec<TypeId>,
    next_singleton: usize,
    total_services: usize,
}

impl ExtensionsBuildTransaction {
    pub(crate) fn next_resolution(
        &self,
    ) -> Result<Option<EagerSingletonResolution>, InjectionError> {
        let Some(&type_id) = self.singleton_order.get(self.next_singleton) else {
            return Ok(None);
        };
        let descriptor = self
            .provider
            .service_descriptors
            .get(&type_id)
            .ok_or_else(|| {
                InjectionError::InvalidRegistrationPlan(format!(
                    "singleton startup order contains missing descriptor {type_id:?}"
                ))
            })?;
        let type_name = descriptor.type_name();
        let provider = Arc::clone(&self.provider);
        let future = async move { provider.resolve_eager_singleton(type_id).await }.boxed();
        Ok(Some(EagerSingletonResolution {
            type_id,
            type_name,
            future,
        }))
    }

    pub(crate) fn record_initialized(
        &mut self,
        resolution: &EagerSingletonResolution,
    ) -> Result<(), InjectionError> {
        let expected = self
            .singleton_order
            .get(self.next_singleton)
            .copied()
            .ok_or_else(|| {
                InjectionError::InvalidRegistrationPlan(
                    "eager singleton completion exceeded the startup plan".to_string(),
                )
            })?;
        if expected != resolution.type_id() {
            return Err(InjectionError::InvalidRegistrationPlan(format!(
                "eager singleton completion was out of order: expected {expected:?}, got {:?}",
                resolution.type_id()
            )));
        }
        self.provider
            .track_root_singleton(resolution.type_id(), resolution.type_name());
        self.next_singleton += 1;
        Ok(())
    }

    pub(crate) fn startup_error(&self, type_id: TypeId, source: InjectionError) -> InjectionError {
        match source {
            error @ InjectionError::ServiceInitializationFailed { .. } => error,
            source => InjectionError::ServiceInitializationFailed {
                service: self
                    .provider
                    .routes
                    .get(&type_id)
                    .map(|route| route.implementation_type_name)
                    .unwrap_or("unknown service")
                    .to_string(),
                source: Box::new(source),
            },
        }
    }

    pub(crate) fn mark_failed(&self) {
        self.provider.mark_failed();
    }

    pub(crate) fn provider(&self) -> Arc<Extensions> {
        Arc::clone(&self.provider)
    }

    pub(crate) fn commit(self) -> Arc<Extensions> {
        debug_assert_eq!(self.next_singleton, self.singleton_order.len());
        self.provider
            .state
            .store(PROVIDER_RUNNING, Ordering::Release);

        let total_services = self.total_services;
        let _ = global::meter("lily_injection")
            .u64_observable_gauge("di.services.registered")
            .with_description("Total number of registered services")
            .with_callback(move |observer| observer.observe(total_services as u64, &[]))
            .build();

        self.provider
    }
}

impl ExtensionsBuilder {
    pub(crate) fn new(seeded_singletons: Vec<SeededSingleton>) -> Self {
        Self { seeded_singletons }
    }

    pub(crate) fn prepare(self) -> Result<ExtensionsBuildTransaction, InjectionError> {
        let plan = RegistrationPlan::discover()?;
        let total_services = plan.registrations().len();
        let singleton_order = plan.singleton_initialization_order().to_vec();

        let mut seeded_singletons = HashMap::with_capacity(self.seeded_singletons.len());
        for seed in self.seeded_singletons {
            if seeded_singletons.contains_key(&seed.type_id) {
                return Err(InjectionError::InvalidRegistrationPlan(format!(
                    "singleton '{}' was seeded more than once",
                    seed.type_name
                )));
            }
            seeded_singletons.insert(seed.type_id, seed);
        }

        let mut descriptors = HashMap::with_capacity(total_services);
        for (type_id, planned) in plan.registrations() {
            let descriptor = if let Some(seed) = seeded_singletons.remove(type_id) {
                if planned.metadata.lifetime != lily_injection_registry::ServiceLifetime::Singleton
                {
                    return Err(InjectionError::InvalidRegistrationPlan(format!(
                        "service '{}' cannot be seeded because it is not a singleton",
                        seed.type_name
                    )));
                }
                let factory = seed.factory;
                ServiceDescriptor::from_closure(
                    planned.metadata.type_name,
                    planned.metadata.lifetime,
                    move |extensions| factory(extensions),
                    Some(seed.dispose_fn),
                )
            } else {
                let factory_fn = planned.metadata.factory_fn;
                ServiceDescriptor::from_closure(
                    planned.metadata.type_name,
                    planned.metadata.lifetime,
                    move |extensions: Arc<Extensions>| {
                        Box::pin(async move {
                            let provider: Arc<dyn Any + Send + Sync> = extensions;
                            factory_fn(provider).await
                        })
                    },
                    planned.dispose_fn,
                )
            };
            descriptors.insert(*type_id, descriptor);
        }

        if let Some(seed) = seeded_singletons.into_values().next() {
            return Err(InjectionError::InvalidRegistrationPlan(format!(
                "singleton seed '{}' has no link-time service registration",
                seed.type_name
            )));
        }

        let meter = global::meter("lily_injection");
        let descriptors = Arc::new(descriptors);
        let routes = Arc::new(plan.routes().clone());
        let provider = Arc::new_cyclic(|self_weak| Extensions {
            service_descriptors: Arc::clone(&descriptors),
            routes: Arc::clone(&routes),
            framework_singletons: RwLock::new(HashMap::new()),
            self_weak: self_weak.clone(),
            scope_manager: Arc::new(crate::storage::ScopeManager::new()),
            root_instances: Mutex::new(Vec::new()),
            state: AtomicU8::new(PROVIDER_BUILDING),
            in_flight_resolutions: AtomicUsize::new(0),
            resolutions_changed: Notify::new(),
            resolution_counter: meter
                .u64_counter("di.service.resolutions.total")
                .with_description("Total number of service resolutions")
                .build(),
            cache_hit_counter: meter
                .u64_counter("di.service.cache_hits.total")
                .with_description("Number of singleton cache hits")
                .build(),
            resolution_duration: meter
                .f64_histogram("di.service.resolution.duration")
                .with_description("Service resolution duration in seconds")
                .build(),
        });

        Ok(ExtensionsBuildTransaction {
            provider,
            singleton_order,
            next_singleton: 0,
            total_services,
        })
    }
}

/// Rollback guard for one application-adapter framework handle.
#[doc(hidden)]
pub struct FrameworkSingletonAttachment<T>
where
    T: Send + Sync + 'static,
{
    provider: Weak<Extensions>,
    committed: bool,
    marker: PhantomData<fn() -> T>,
}

impl<T> FrameworkSingletonAttachment<T>
where
    T: Send + Sync + 'static,
{
    /// Retain the attached handle for the provider's remaining lifetime.
    #[doc(hidden)]
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl<T> Drop for FrameworkSingletonAttachment<T>
where
    T: Send + Sync + 'static,
{
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Some(provider) = self.provider.upgrade() {
            provider.detach_framework_singleton(TypeId::of::<T>());
        }
    }
}

impl std::fmt::Debug for Extensions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Extensions")
            .field("service_count", &self.service_descriptors.len())
            .field("route_count", &self.routes.len())
            .field("active_scopes", &self.active_scope_count())
            .finish_non_exhaustive()
    }
}
