//! Application-defined queue delivery middleware and guard contracts.

use std::{
    any::{Any, TypeId},
    collections::{HashMap, HashSet},
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::FutureExt;
use lily_error::application::{
    MessageBrokerError, QueueHandlerError, message_broker::RabbitMQError,
};
use lily_injection::{Extensions, InjectionError};
use lily_queue_registry::{
    ErasedQueuePipelineComponent, QueueDeliveryOutcome, QueueGuardRegistration,
    QueueMiddlewareRegistration, QueuePipelineComponentInitError, QueuePipelineHookFuture,
    QueuePipelineInitializationFuture,
};

use crate::{
    DeliveryCancellation, DeliveryContext, DeliveryDeadline, DeliveryHeaders, DeliveryInvocation,
    DeliveryProperties, QueueDeliveryTerminationContext,
    delivery_lifecycle::{DeliveryMiddlewareLedger, TerminationState},
    delivery_termination::{
        DeliveryCleanupCancellation, DeliveryTerminationReason, TerminationInvocation,
    },
    shutdown_budget::QueueShutdownBudget,
};

/// Maximum middleware entries in one effective global/service/handler plan.
pub const MAX_QUEUE_MIDDLEWARES: usize = 64;

/// Maximum guards in one effective global/service/handler plan.
pub const MAX_QUEUE_GUARDS: usize = 64;

const MAX_QUEUE_PIPELINE_TYPE_NAME_BYTES: usize = 1_024;
const MAX_QUEUE_PIPELINE_PLAN_BYTES: usize = 256 * 1_024;

/// Read-only delivery authority shared by queue middleware and guards.
///
/// The exchange exists only inside one active delivery DI scope. It exposes
/// bounded metadata and immutable payload inspection, but never exposes a
/// RabbitMQ delivery tag, channel, ACK or NACK capability. Middleware and
/// guards may publish typed delivery-local values for later pipeline stages.
pub struct QueueDeliveryExchange<'delivery> {
    invocation: &'delivery mut DeliveryInvocation,
}

impl<'delivery> QueueDeliveryExchange<'delivery> {
    fn new(invocation: &'delivery mut DeliveryInvocation) -> Self {
        Self { invocation }
    }

    /// Immutable canonical delivery metadata.
    #[must_use]
    pub fn context(&self) -> &DeliveryContext {
        self.invocation.context()
    }

    /// Immutable bounded application headers.
    #[must_use]
    pub fn headers(&self) -> &DeliveryHeaders {
        self.invocation.headers()
    }

    /// Immutable bounded allowlisted AMQP properties.
    #[must_use]
    pub fn properties(&self) -> &DeliveryProperties {
        self.invocation.properties()
    }

    /// Immutable bounded delivery body. Inspecting it does not consume the
    /// sole typed payload authority reserved for the terminal extractor.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        self.invocation.body()
    }

    /// Cooperative delivery lifecycle signal.
    #[must_use]
    pub fn cancellation(&self) -> &DeliveryCancellation {
        self.invocation.cancellation()
    }

    /// Common absolute deadline for the complete normal delivery pipeline.
    #[must_use]
    pub fn deadline(&self) -> DeliveryDeadline {
        self.invocation.deadline()
    }

    /// Resolves a concrete or derive-declared trait service from this
    /// delivery's active DI scope.
    pub async fn service<T>(&self) -> Result<Arc<T>, InjectionError>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.invocation.extensions().get_service::<T>(None).await
    }

    /// Returns an owned clone of one delivery-local value.
    #[must_use]
    pub fn local<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.invocation.local::<T>()
    }

    /// Publishes one typed value for downstream middleware, guards,
    /// extractors and the handler.
    pub fn insert_local<T>(&mut self, value: T) -> Result<Option<T>, QueueHandlerError>
    where
        T: Send + Sync + 'static,
    {
        self.invocation.insert_local(value)
    }

    /// Removes and returns one typed delivery-local value.
    pub fn remove_local<T>(&mut self) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.invocation.remove_local::<T>()
    }
}

impl fmt::Debug for QueueDeliveryExchange<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueueDeliveryExchange")
            .field("queue", &self.context().queue())
            .field("schema_version", &self.context().schema_version())
            .field("content_kind", &self.context().content_kind())
            .field("body_bytes", &self.body().len())
            .field("deadline", &self.deadline())
            .finish_non_exhaustive()
    }
}

/// Per-delivery middleware with framework-owned forward and reverse execution.
///
/// Effective plans enter in `global -> service -> handler` order. Lily then
/// unwinds only the successfully entered prefix in reverse order before DI
/// scope cleanup and broker settlement. Returning a [`QueueHandlerError`]
/// explicitly selects retryable or permanent failure; middleware cannot ACK,
/// NACK or silently drop a delivery.
#[async_trait]
pub trait QueueMiddleware: Send + Sync + 'static {
    /// Constructs the one Consumer-owned instance for this concrete type.
    ///
    /// Application-lifetime dependencies may be retained here. Resolve
    /// scoped/transient dependencies from [`QueueDeliveryExchange::service`]
    /// inside a hook so their delivery lifetime is preserved. Retaining a
    /// transient resolved during `new` intentionally turns it into component-
    /// lifetime state and is an invalid application lifetime declaration; Lily
    /// cannot infer that intent from an erased application type.
    async fn new(extensions: Arc<Extensions>) -> Result<Self, QueuePipelineComponentInitError>
    where
        Self: Sized;

    /// Runs before guards, typed extraction and the queue handler.
    async fn before_delivery(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError> {
        Ok(())
    }

    /// Normal reverse exit for a successfully entered middleware.
    ///
    /// An error can turn a successful inner outcome into a retryable or
    /// permanent failure. It cannot erase or replace an existing primary
    /// failure. It shares the execution cancellation signal and common pipeline
    /// deadline. Forced cleanup uses [`Self::on_delivery_termination`] instead.
    async fn after_delivery(
        &self,
        _exchange: &mut QueueDeliveryExchange<'_>,
        _outcome: QueueDeliveryOutcome,
    ) -> Result<(), QueueHandlerError> {
        Ok(())
    }

    /// Best-effort abnormal cleanup for a successfully entered middleware.
    ///
    /// Called in reverse order after execution stops, only when normal exit
    /// never ran, was interrupted, or panicked. Returning `Err` records cleanup
    /// failure; it cannot rewrite the pipeline result or settle the delivery.
    /// The context supplies a separate read-only cleanup token and deadline.
    /// Completion is best effort within that deadline. Normal exits that
    /// returned `Ok` or `Err` are terminal and are never invoked here again.
    /// Partially executed normal exits must leave cancellation-safe state for
    /// this callback; interrupted termination callbacks are not retried.
    async fn on_delivery_termination(
        &self,
        _context: &mut QueueDeliveryTerminationContext<'_>,
    ) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

/// Application-defined queue delivery admission guard.
///
/// Guards run in `global -> service -> handler` order after every effective
/// middleware has entered and before typed extraction. `Ok(())` continues;
/// an explicit retryable or permanent [`QueueHandlerError`] rejects the
/// delivery through Lily's sole settlement authority.
#[async_trait]
pub trait QueueGuard: Send + Sync + 'static {
    /// Constructs the one Consumer-owned instance for this concrete type.
    ///
    /// Retain only application-lifetime dependencies. Delivery-scoped and
    /// transient work must be resolved through [`QueueDeliveryExchange::service`]
    /// from [`Self::can_activate`].
    async fn new(extensions: Arc<Extensions>) -> Result<Self, QueuePipelineComponentInitError>
    where
        Self: Sized;

    /// Allows the delivery or returns one typed rejection.
    async fn can_activate(
        &self,
        exchange: &mut QueueDeliveryExchange<'_>,
    ) -> Result<(), QueueHandlerError>;
}

#[doc(hidden)]
pub fn queue_middleware_registration<M>() -> QueueMiddlewareRegistration
where
    M: QueueMiddleware,
{
    QueueMiddlewareRegistration::new(
        TypeId::of::<M>(),
        std::any::type_name::<M>(),
        initialize_middleware::<M>,
        middleware_before::<M>,
        middleware_after::<M>,
    )
    .with_termination(middleware_termination::<M>)
}

fn middleware_termination<'a, M: QueueMiddleware>(
    component: &'a (dyn Any + Send + Sync),
    invocation: &'a mut (dyn Any + Send),
    termination: &'a (dyn Any + Send + Sync),
) -> QueuePipelineHookFuture<'a> {
    Box::pin(async move {
        let middleware = component.downcast_ref::<M>().ok_or_else(|| {
            QueueHandlerError::retryable("QUEUE_MIDDLEWARE_INSTANCE_TYPE_MISMATCH")
        })?;
        let invocation = invocation
            .downcast_mut::<DeliveryInvocation>()
            .ok_or_else(|| {
                QueueHandlerError::retryable("QUEUE_MIDDLEWARE_INVOCATION_TYPE_MISMATCH")
            })?;
        let termination = termination
            .downcast_ref::<TerminationInvocation>()
            .ok_or_else(|| {
                QueueHandlerError::retryable("QUEUE_MIDDLEWARE_TERMINATION_TYPE_MISMATCH")
            })?;
        middleware
            .on_delivery_termination(&mut QueueDeliveryTerminationContext::new(
                invocation,
                termination,
            ))
            .await
    })
}

#[doc(hidden)]
pub fn queue_guard_registration<G>() -> QueueGuardRegistration
where
    G: QueueGuard,
{
    QueueGuardRegistration::new(
        TypeId::of::<G>(),
        std::any::type_name::<G>(),
        initialize_guard::<G>,
        guard_can_activate::<G>,
    )
}

fn initialize_middleware<M>(extensions: Arc<Extensions>) -> QueuePipelineInitializationFuture
where
    M: QueueMiddleware,
{
    Box::pin(async move {
        M::new(extensions)
            .await
            .map(|middleware| Arc::new(middleware) as ErasedQueuePipelineComponent)
    })
}

fn initialize_guard<G>(extensions: Arc<Extensions>) -> QueuePipelineInitializationFuture
where
    G: QueueGuard,
{
    Box::pin(async move {
        G::new(extensions)
            .await
            .map(|guard| Arc::new(guard) as ErasedQueuePipelineComponent)
    })
}

fn middleware_before<'a, M>(
    component: &'a (dyn Any + Send + Sync),
    invocation: &'a mut (dyn Any + Send),
) -> QueuePipelineHookFuture<'a>
where
    M: QueueMiddleware,
{
    Box::pin(async move {
        let middleware = component.downcast_ref::<M>().ok_or_else(|| {
            QueueHandlerError::retryable("QUEUE_MIDDLEWARE_INSTANCE_TYPE_MISMATCH")
        })?;
        let invocation = invocation
            .downcast_mut::<DeliveryInvocation>()
            .ok_or_else(|| {
                QueueHandlerError::retryable("QUEUE_MIDDLEWARE_INVOCATION_TYPE_MISMATCH")
            })?;
        middleware
            .before_delivery(&mut QueueDeliveryExchange::new(invocation))
            .await
    })
}

fn middleware_after<'a, M>(
    component: &'a (dyn Any + Send + Sync),
    invocation: &'a mut (dyn Any + Send),
    outcome: QueueDeliveryOutcome,
) -> QueuePipelineHookFuture<'a>
where
    M: QueueMiddleware,
{
    Box::pin(async move {
        let middleware = component.downcast_ref::<M>().ok_or_else(|| {
            QueueHandlerError::retryable("QUEUE_MIDDLEWARE_INSTANCE_TYPE_MISMATCH")
        })?;
        let invocation = invocation
            .downcast_mut::<DeliveryInvocation>()
            .ok_or_else(|| {
                QueueHandlerError::retryable("QUEUE_MIDDLEWARE_INVOCATION_TYPE_MISMATCH")
            })?;
        middleware
            .after_delivery(&mut QueueDeliveryExchange::new(invocation), outcome)
            .await
    })
}

fn guard_can_activate<'a, G>(
    component: &'a (dyn Any + Send + Sync),
    invocation: &'a mut (dyn Any + Send),
) -> QueuePipelineHookFuture<'a>
where
    G: QueueGuard,
{
    Box::pin(async move {
        let guard = component
            .downcast_ref::<G>()
            .ok_or_else(|| QueueHandlerError::retryable("QUEUE_GUARD_INSTANCE_TYPE_MISMATCH"))?;
        let invocation = invocation
            .downcast_mut::<DeliveryInvocation>()
            .ok_or_else(|| QueueHandlerError::retryable("QUEUE_GUARD_INVOCATION_TYPE_MISMATCH"))?;
        guard
            .can_activate(&mut QueueDeliveryExchange::new(invocation))
            .await
    })
}

#[derive(Clone)]
struct CompiledQueueMiddleware {
    registration: QueueMiddlewareRegistration,
    component: ErasedQueuePipelineComponent,
}

#[derive(Clone)]
struct CompiledQueueGuard {
    registration: QueueGuardRegistration,
    component: ErasedQueuePipelineComponent,
}

#[derive(Clone, Default)]
pub(crate) struct CompiledQueuePipeline {
    middlewares: Arc<[CompiledQueueMiddleware]>,
    guards: Arc<[CompiledQueueGuard]>,
}

impl CompiledQueuePipeline {
    fn new(middlewares: Vec<CompiledQueueMiddleware>, guards: Vec<CompiledQueueGuard>) -> Self {
        Self {
            middlewares: middlewares.into(),
            guards: guards.into(),
        }
    }

    pub(crate) async fn enter_recorded(
        &self,
        invocation: &mut DeliveryInvocation,
        ledger: &mut DeliveryMiddlewareLedger,
        deadline: tokio::time::Instant,
    ) -> Result<(), QueueHandlerError> {
        invocation.set_deadline(deadline);
        for (index, middleware) in self.middlewares.iter().enumerate() {
            let result = run_normal_hook(|| {
                middleware
                    .registration
                    .before(middleware.component.as_ref(), invocation)
            })
            .await
            .unwrap_or_else(|()| Err(QueueHandlerError::retryable("QUEUE_MIDDLEWARE_PANICKED")));
            result?;
            // Commit before another await. Cancellation does not erase a
            // successful enter or short-circuit the cooperative pipeline.
            ledger.record_enter(index);
        }
        Ok(())
    }

    pub(crate) async fn evaluate_guards(
        &self,
        invocation: &mut DeliveryInvocation,
        deadline: tokio::time::Instant,
    ) -> Result<(), QueueHandlerError> {
        invocation.set_deadline(deadline);
        for guard in self.guards.iter() {
            run_normal_hook(|| {
                guard
                    .registration
                    .can_activate(guard.component.as_ref(), invocation)
            })
            .await
            .unwrap_or_else(|()| Err(QueueHandlerError::retryable("QUEUE_GUARD_PANICKED")))?;
        }
        Ok(())
    }

    pub(crate) async fn unwind_recorded(
        &self,
        invocation: &mut DeliveryInvocation,
        ledger: &mut DeliveryMiddlewareLedger,
        mut result: Result<(), QueueHandlerError>,
        deadline: tokio::time::Instant,
    ) -> Result<(), QueueHandlerError> {
        invocation.set_deadline(deadline);
        for index in (0..ledger.entered()).rev() {
            if !ledger.begin_exit(index) {
                continue;
            }
            let outcome = delivery_outcome(&result);
            let middleware = &self.middlewares[index];
            let after = run_normal_hook(|| {
                middleware
                    .registration
                    .after(middleware.component.as_ref(), invocation, outcome)
            })
            .await;
            let after = match after {
                Ok(result) => {
                    ledger.finish_exit(index, result.is_ok());
                    result
                }
                Err(()) => {
                    ledger.panicked_exit(index);
                    // Terminate this inner scope before any outer exit. The
                    // owner will unwind the unfinished prefix in reverse.
                    return result.and(Err(QueueHandlerError::retryable(
                        "QUEUE_MIDDLEWARE_PANICKED",
                    )));
                }
            };
            if let Err(after_error) = after {
                if result.is_ok() {
                    result = Err(after_error);
                } else {
                    tracing::warn!(
                        middleware = middleware.registration.type_name(),
                        error_code = after_error.code(),
                        "Queue middleware unwind failure did not replace the primary delivery failure"
                    );
                }
            }
        }
        result
    }

    pub(crate) async fn terminate_recorded(
        &self,
        invocation: &mut DeliveryInvocation,
        ledger: &mut DeliveryMiddlewareLedger,
        reason: DeliveryTerminationReason,
        deadline: tokio::time::Instant,
        cleanup: &tokio_util::sync::CancellationToken,
        budget: &QueueShutdownBudget,
    ) {
        for index in (0..ledger.entered()).rev() {
            let Some(normal_exit) = ledger.eligible_termination(index) else {
                continue;
            };
            let now = tokio::time::Instant::now();
            let hard = budget.cap(deadline);
            if now >= hard {
                ledger.termination_state(index, TerminationState::BudgetExhausted);
                tracing::warn!(middleware = self.middlewares[index].registration.type_name(),
                    normal_exit = ?normal_exit, reason = ?reason,
                    "Queue termination hook was not started because its cleanup budget expired");
                continue;
            }
            let remaining_hooks = (0..=index)
                .filter(|&entry| ledger.eligible_termination(entry).is_some())
                .count();
            let hook_deadline = now
                + hard.saturating_duration_since(now)
                    / u32::try_from(remaining_hooks).unwrap_or(u32::MAX);
            let metadata = TerminationInvocation {
                reason,
                normal_exit,
                cancellation: DeliveryCleanupCancellation::new(
                    cleanup,
                    hook_deadline,
                    budget.clone(),
                ),
            };
            ledger.termination_state(index, TerminationState::Running);
            let middleware = &self.middlewares[index];
            let mut hook = Box::pin(run_normal_hook(|| {
                middleware.registration.terminate(
                    middleware.component.as_ref(),
                    invocation,
                    &metadata,
                )
            }));
            let result = tokio::select! {
                biased;
                result = hook.as_mut() => match result {
                    Ok(Ok(())) => TerminationState::Completed,
                    Ok(Err(error)) => {
                        tracing::warn!(middleware = middleware.registration.type_name(), error_code = error.code(), "Queue termination hook failed");
                        TerminationState::Failed
                    },
                    Err(()) => TerminationState::Panicked,
                },
                _ = metadata.cancellation.cancelled() => TerminationState::TimedOut,
            };
            metadata.cancellation.stop();
            // Drop before starting an outer hook. No detached callback and no
            // speculative extra poll after the invocation budget expires.
            let result = if catch_unwind(AssertUnwindSafe(|| drop(hook))).is_err() {
                TerminationState::Panicked
            } else {
                result
            };
            ledger.termination_state(index, result);
            if result != TerminationState::Completed {
                tracing::warn!(middleware = middleware.registration.type_name(),
                    normal_exit = ?normal_exit, reason = ?reason, status = ?result,
                    "Queue termination hook did not complete successfully");
            }
        }
    }

    #[cfg(all(test, feature = "test-support"))]
    async fn enter(
        &self,
        invocation: &mut DeliveryInvocation,
        deadline: tokio::time::Instant,
    ) -> (usize, Result<(), QueueHandlerError>) {
        let mut ledger = DeliveryMiddlewareLedger::default();
        let result = self.enter_recorded(invocation, &mut ledger, deadline).await;
        (ledger.entered(), result)
    }

    #[cfg(all(test, feature = "test-support"))]
    async fn unwind(
        &self,
        invocation: &mut DeliveryInvocation,
        entered: usize,
        result: Result<(), QueueHandlerError>,
        deadline: tokio::time::Instant,
    ) -> Result<(), QueueHandlerError> {
        let mut ledger = DeliveryMiddlewareLedger::default();
        for index in 0..entered {
            ledger.record_enter(index);
        }
        self.unwind_recorded(invocation, &mut ledger, result, deadline)
            .await
    }
}

fn delivery_outcome(result: &Result<(), QueueHandlerError>) -> QueueDeliveryOutcome {
    match result {
        Ok(()) => QueueDeliveryOutcome::Succeeded,
        Err(error) if is_panic_code(error.code()) => QueueDeliveryOutcome::Panicked,
        // Actual timeout/force interruption runs termination, not normal
        // after. These strings in a returned error are application outcomes.
        Err(error) => QueueDeliveryOutcome::Failed {
            class: error.class(),
            code: error.code(),
        },
    }
}

fn is_panic_code(code: &str) -> bool {
    matches!(
        code,
        "QUEUE_HANDLER_PANICKED"
            | "QUEUE_HANDLER_SERVICE_RESOLUTION_PANICKED"
            | "QUEUE_MIDDLEWARE_PANICKED"
            | "QUEUE_GUARD_PANICKED"
    )
}

async fn run_normal_hook<'a>(
    create: impl FnOnce() -> QueuePipelineHookFuture<'a>,
) -> Result<Result<(), QueueHandlerError>, ()> {
    let future = catch_unwind(AssertUnwindSafe(create)).map_err(|_| ())?;
    AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .map_err(|_| ())
}

/// One exact handler/component input consumed by the atomic queue compiler.
#[doc(hidden)]
#[derive(Clone)]
pub struct QueueHandlerCompilationInput {
    pub(crate) metadata: &'static lily_queue_registry::QueueHandlerMetadata,
    pub(crate) component: Option<Arc<lily_trace::ComponentIdentity>>,
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    pub(crate) transactional_runtime: Option<crate::transactional::PreparedTransactionalRuntime>,
}

impl QueueHandlerCompilationInput {
    /// Freeze one generated handler and its optional telemetry identity.
    #[must_use]
    pub fn new(
        metadata: &'static lily_queue_registry::QueueHandlerMetadata,
        component: Option<Arc<lily_trace::ComponentIdentity>>,
    ) -> Self {
        Self {
            metadata,
            component,
            #[cfg(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            ))]
            transactional_runtime: None,
        }
    }

    /// Attaches the already prepared backend authority for this exact handler.
    #[cfg(any(
        feature = "transactional-inbox-postgresql",
        feature = "transactional-inbox-postgresql-factory",
        feature = "transactional-inbox-mongodb",
        feature = "transactional-inbox-mongodb-factory"
    ))]
    #[must_use]
    pub fn with_transactional_runtime(
        mut self,
        runtime: crate::transactional::PreparedTransactionalRuntime,
    ) -> Self {
        self.transactional_runtime = Some(runtime);
        self
    }
}

#[derive(Clone, Copy)]
enum PendingComponentRegistration {
    Middleware(QueueMiddlewareRegistration),
    Guard(QueueGuardRegistration),
}

impl PendingComponentRegistration {
    fn type_id(self) -> TypeId {
        match self {
            Self::Middleware(registration) => registration.type_id(),
            Self::Guard(registration) => registration.type_id(),
        }
    }

    fn type_name(self) -> &'static str {
        match self {
            Self::Middleware(registration) => registration.type_name(),
            Self::Guard(registration) => registration.type_name(),
        }
    }

    fn kind(self) -> &'static str {
        match self {
            Self::Middleware(_) => "middleware",
            Self::Guard(_) => "guard",
        }
    }

    fn initialize(self, extensions: Arc<Extensions>) -> QueuePipelineInitializationFuture {
        match self {
            Self::Middleware(registration) => registration.initialize(extensions),
            Self::Guard(registration) => registration.initialize(extensions),
        }
    }
}

struct InitializedComponent {
    registration: PendingComponentRegistration,
    component: ErasedQueuePipelineComponent,
}

/// Owns initialization references in exact constructor order. Popping in
/// `Drop` makes failed-build RAII rollback deterministic and reverse ordered.
struct InitializedComponents(Vec<InitializedComponent>);

impl Drop for InitializedComponents {
    fn drop(&mut self) {
        while self.0.pop().is_some() {}
    }
}

pub(crate) struct CompiledPipelineSet {
    pub(crate) pipelines: Vec<CompiledQueuePipeline>,
}

pub(crate) async fn compile_pipeline_set(
    inputs: &[QueueHandlerCompilationInput],
    global_middlewares: &[QueueMiddlewareRegistration],
    global_guards: &[QueueGuardRegistration],
    extensions: Arc<Extensions>,
    initialization_timeout: Duration,
) -> Result<CompiledPipelineSet, MessageBrokerError> {
    validate_pipeline_inputs(inputs, global_middlewares, global_guards)?;

    let ordered = ordered_unique_components(inputs, global_middlewares, global_guards);
    let deadline = tokio::time::Instant::now()
        .checked_add(initialization_timeout)
        .ok_or_else(|| {
            pipeline_configuration("queue pipeline initialization timeout overflowed")
        })?;
    let mut initialized = InitializedComponents(Vec::with_capacity(ordered.len()));
    let mut indices = HashMap::with_capacity(ordered.len());

    for registration in ordered {
        let component =
            initialize_component_before(registration, Arc::clone(&extensions), deadline).await?;
        let index = initialized.0.len();
        indices.insert((registration.kind(), registration.type_id()), index);
        initialized.0.push(InitializedComponent {
            registration,
            component,
        });
    }

    let mut pipelines = Vec::with_capacity(inputs.len());
    for input in inputs {
        let effective_middlewares = global_middlewares
            .iter()
            .chain(&input.metadata.service_middlewares)
            .chain(&input.metadata.handler_middlewares);
        let mut middlewares = Vec::new();
        for registration in effective_middlewares {
            let index = *indices
                .get(&("middleware", (*registration).type_id()))
                .ok_or_else(|| pipeline_configuration("compiled middleware instance is missing"))?;
            let owner = &initialized.0[index];
            debug_assert!(matches!(
                owner.registration,
                PendingComponentRegistration::Middleware(_)
            ));
            middlewares.push(CompiledQueueMiddleware {
                registration: *registration,
                component: Arc::clone(&owner.component),
            });
        }

        let effective_guards = global_guards
            .iter()
            .chain(&input.metadata.service_guards)
            .chain(&input.metadata.handler_guards);
        let mut guards = Vec::new();
        for registration in effective_guards {
            let index = *indices
                .get(&("guard", (*registration).type_id()))
                .ok_or_else(|| pipeline_configuration("compiled guard instance is missing"))?;
            let owner = &initialized.0[index];
            debug_assert!(matches!(
                owner.registration,
                PendingComponentRegistration::Guard(_)
            ));
            guards.push(CompiledQueueGuard {
                registration: *registration,
                component: Arc::clone(&owner.component),
            });
        }
        pipelines.push(CompiledQueuePipeline::new(middlewares, guards));
    }

    Ok(CompiledPipelineSet { pipelines })
}

fn validate_pipeline_inputs(
    inputs: &[QueueHandlerCompilationInput],
    global_middlewares: &[QueueMiddlewareRegistration],
    global_guards: &[QueueGuardRegistration],
) -> Result<(), MessageBrokerError> {
    let mut retained_bytes = 0usize;
    let mut roles = HashMap::<TypeId, &'static str>::new();

    for input in inputs {
        let middlewares = global_middlewares
            .iter()
            .chain(&input.metadata.service_middlewares)
            .chain(&input.metadata.handler_middlewares)
            .copied()
            .collect::<Vec<_>>();
        let guards = global_guards
            .iter()
            .chain(&input.metadata.service_guards)
            .chain(&input.metadata.handler_guards)
            .copied()
            .collect::<Vec<_>>();
        if middlewares.len() > MAX_QUEUE_MIDDLEWARES {
            return Err(pipeline_configuration(
                "effective queue middleware plan exceeds 64 entries",
            ));
        }
        if guards.len() > MAX_QUEUE_GUARDS {
            return Err(pipeline_configuration(
                "effective queue guard plan exceeds 64 entries",
            ));
        }

        let mut middleware_types = HashSet::with_capacity(middlewares.len());
        for registration in middlewares {
            validate_component_descriptor(registration.type_name())?;
            retained_bytes = retained_bytes.saturating_add(registration.type_name().len());
            if !middleware_types.insert(registration.type_id()) {
                return Err(pipeline_configuration(format!(
                    "duplicate queue middleware in effective handler plan: {}",
                    registration.type_name(),
                )));
            }
            validate_component_role(&mut roles, registration.type_id(), "middleware")?;
        }

        let mut guard_types = HashSet::with_capacity(guards.len());
        for registration in guards {
            validate_component_descriptor(registration.type_name())?;
            retained_bytes = retained_bytes.saturating_add(registration.type_name().len());
            if !guard_types.insert(registration.type_id()) {
                return Err(pipeline_configuration(format!(
                    "duplicate queue guard in effective handler plan: {}",
                    registration.type_name(),
                )));
            }
            validate_component_role(&mut roles, registration.type_id(), "guard")?;
        }
    }

    if retained_bytes > MAX_QUEUE_PIPELINE_PLAN_BYTES {
        return Err(pipeline_configuration(
            "queue pipeline retained descriptor bytes exceed the plan bound",
        ));
    }
    Ok(())
}

fn validate_component_descriptor(type_name: &'static str) -> Result<(), MessageBrokerError> {
    if type_name.is_empty()
        || type_name.len() > MAX_QUEUE_PIPELINE_TYPE_NAME_BYTES
        || type_name.chars().any(char::is_control)
    {
        return Err(pipeline_configuration(
            "queue pipeline component type name is invalid",
        ));
    }
    Ok(())
}

fn validate_component_role(
    roles: &mut HashMap<TypeId, &'static str>,
    type_id: TypeId,
    role: &'static str,
) -> Result<(), MessageBrokerError> {
    if roles
        .get(&type_id)
        .is_some_and(|existing| *existing != role)
    {
        return Err(pipeline_configuration(
            "one concrete queue pipeline type cannot be both middleware and guard",
        ));
    }
    roles.insert(type_id, role);
    Ok(())
}

fn ordered_unique_components(
    inputs: &[QueueHandlerCompilationInput],
    global_middlewares: &[QueueMiddlewareRegistration],
    global_guards: &[QueueGuardRegistration],
) -> Vec<PendingComponentRegistration> {
    let mut ordered = Vec::new();
    let mut seen = HashSet::new();

    for input in inputs {
        for registration in global_middlewares
            .iter()
            .chain(&input.metadata.service_middlewares)
            .chain(&input.metadata.handler_middlewares)
        {
            let pending = PendingComponentRegistration::Middleware(*registration);
            if seen.insert((pending.kind(), pending.type_id())) {
                ordered.push(pending);
            }
        }
        for registration in global_guards
            .iter()
            .chain(&input.metadata.service_guards)
            .chain(&input.metadata.handler_guards)
        {
            let pending = PendingComponentRegistration::Guard(*registration);
            if seen.insert((pending.kind(), pending.type_id())) {
                ordered.push(pending);
            }
        }
    }
    ordered
}

async fn initialize_component_before(
    registration: PendingComponentRegistration,
    extensions: Arc<Extensions>,
    deadline: tokio::time::Instant,
) -> Result<ErasedQueuePipelineComponent, MessageBrokerError> {
    let future =
        catch_unwind(AssertUnwindSafe(|| registration.initialize(extensions))).map_err(|_| {
            pipeline_configuration(format!(
                "{} initialization panicked before returning a future: {}",
                registration.kind(),
                registration.type_name(),
            ))
        })?;
    // Poll the application constructor in this composition task rather than a
    // detached child task. Dropping or aborting the outer build now drops the
    // pending constructor synchronously before `InitializedComponents` rolls
    // its completed prefix back. `timeout_at` retains the single aggregate
    // deadline and `catch_unwind` retains the typed panic boundary.
    let result = tokio::time::timeout_at(deadline, AssertUnwindSafe(future).catch_unwind())
        .await
        .map_err(|_| {
            pipeline_configuration(format!(
                "aggregate queue pipeline initialization timed out while constructing {}: {}",
                registration.kind(),
                registration.type_name(),
            ))
        })?
        .map_err(|_| {
            pipeline_configuration(format!(
                "{} initialization panicked: {}",
                registration.kind(),
                registration.type_name(),
            ))
        })?;
    result.map_err(|error| {
        pipeline_configuration(format!(
            "{} initialization failed with {}: {}",
            registration.kind(),
            error.diagnostic_code(),
            registration.type_name(),
        ))
    })
}

fn pipeline_configuration(detail: impl Into<String>) -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::Configuration(detail.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_delivery_outcomes_preserve_primary_failure_class() {
        let retryable = Err(QueueHandlerError::retryable("RETRY"));
        let permanent = Err(QueueHandlerError::permanent("PERMANENT"));

        assert_eq!(
            delivery_outcome(&retryable),
            QueueDeliveryOutcome::Failed {
                class: lily_error::application::QueueHandlerFailureClass::Retryable,
                code: "RETRY",
            }
        );
        assert_eq!(
            delivery_outcome(&permanent),
            QueueDeliveryOutcome::Failed {
                class: lily_error::application::QueueHandlerFailureClass::Permanent,
                code: "PERMANENT",
            }
        );
    }

    #[test]
    fn normal_exit_preserves_returned_errors_even_when_codes_resemble_interruption() {
        assert_eq!(
            delivery_outcome(&Err(
                QueueHandlerError::retryable("QUEUE_HANDLER_PANICKED",)
            )),
            QueueDeliveryOutcome::Panicked
        );
        assert_eq!(
            delivery_outcome(&Err(QueueHandlerError::retryable(
                "QUEUE_HANDLER_SERVICE_RESOLUTION_PANICKED",
            ))),
            QueueDeliveryOutcome::Panicked
        );
        assert_eq!(
            delivery_outcome(&Err(QueueHandlerError::retryable(
                "QUEUE_DELIVERY_EXECUTION_TIMED_OUT",
            ))),
            QueueDeliveryOutcome::Failed {
                class: lily_error::application::QueueHandlerFailureClass::Retryable,
                code: "QUEUE_DELIVERY_EXECUTION_TIMED_OUT",
            }
        );
        assert_eq!(
            delivery_outcome(&Err(QueueHandlerError::retryable(
                "QUEUE_HANDLER_CANCELLED",
            ))),
            QueueDeliveryOutcome::Failed {
                class: lily_error::application::QueueHandlerFailureClass::Retryable,
                code: "QUEUE_HANDLER_CANCELLED",
            }
        );
        assert_eq!(
            delivery_outcome(&Err(QueueHandlerError::permanent("APPLICATION_FAILURE"))),
            QueueDeliveryOutcome::Failed {
                class: lily_error::application::QueueHandlerFailureClass::Permanent,
                code: "APPLICATION_FAILURE",
            }
        );
    }
}

#[cfg(all(test, feature = "test-support"))]
#[path = "pipeline_qualification_tests.rs"]
mod qualification_tests;
