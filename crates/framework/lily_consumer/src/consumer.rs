#[cfg(feature = "asyncapi")]
use lily_asyncapi::AsyncApiConfig;
use lily_config::{ConfigService, QueueDefinition, SecretResolver};
#[cfg(test)]
use lily_error::application::consumer::ConsumerSignalFailureStage;
#[cfg(feature = "asyncapi")]
use lily_error::application::consumer::{ConsumerAsyncApiFailureStage, ConsumerPlanFailureKind};
use lily_error::application::consumer::{
    ConsumerConfigurationFailure, ConsumerError, ConsumerShutdownFailureEvidence,
    ConsumerShutdownFailureKind, ConsumerTracingFailureStage,
};
use lily_injection::{ApplicationContainer, ApplicationContainerBuilder, DEFAULT_SHUTDOWN_TIMEOUT};
#[cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
use lily_queue::__private::register_mongodb_transactional_runtime;
#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory"
))]
use lily_queue::__private::register_postgresql_transactional_runtime;
#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
use lily_queue::__private::PreparedTransactionalRuntime;
use lily_queue::{
    __private::{
        get_all_queue_handlers, queue_guard_registration, queue_middleware_registration,
        queue_runtime, register_compiled_dispatch, QueueGuardRegistration,
        QueueMiddlewareRegistration, QueueRuntimeHandle,
    },
    QueueGuard, QueueMiddleware, QueueService,
};
#[cfg(test)]
use lily_shutdown::ShutdownError;
use lily_shutdown::{
    FrameworkComponentStatus, FrameworkShutdownReport, ShutdownSignal, ShutdownState,
};
use lily_trace::prelude::*;
use std::{path::Path, sync::Arc, time::Duration};
use tokio::sync::{oneshot, Mutex};
#[cfg(test)]
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[cfg(any(
    feature = "transactional-inbox-postgresql",
    feature = "transactional-inbox-postgresql-factory",
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]
use crate::plan::invalid_materialized_plan;

#[cfg(feature = "asyncapi")]
use crate::asyncapi::{ConsumerDocument, PreparedConsumerAsyncApi};
use crate::{
    operational::{
        aggregate_readiness, classify_admission, classify_runtime, ConsumerOperationalState,
    },
    plan::{
        ConsumerExecutionPlan, ConsumerExecutionPlanBinding, ConsumerExecutionPlanError,
        ValidatedTraceCells,
    },
    ConsumerOperationalSnapshot, ConsumerShutdownSnapshot,
};

#[path = "consumer_build_transaction.rs"]
mod build_transaction;
#[path = "consumer_dependencies.rs"]
mod dependencies;
#[path = "consumer_owned_tasks.rs"]
mod owned_tasks;
#[path = "consumer_runtime_owner.rs"]
mod runtime_owner;
use owned_tasks::OwnedTask;
#[cfg(test)]
#[path = "consumer_runtime_qualification_tests.rs"]
mod runtime_qualification_tests;

use build_transaction::{poll_startup_activity, ConsumerBuildTransaction, StartupActivityOutcome};
#[cfg(feature = "fuzzing")]
pub(crate) use runtime_owner::exercise_runtime_ownership_for_fuzz;
use runtime_owner::{ConsumerRuntimeWaiterGuard, ManagedStartupWaiterGuard};

fn application_container_builder() -> ApplicationContainerBuilder {
    #[cfg(test)]
    {
        crate::test_application_container_builder()
    }
    #[cfg(not(test))]
    {
        ApplicationContainer::builder()
    }
}

/// Explicit Consumer composition-root builder.
///
/// Tracing is disabled by default and never discovered from the current
/// directory. Select an owned strict configuration, or `tracing_external`
/// when the process-level composition root owns telemetry for several
/// adapters.
pub struct ConsumerBuilder {
    container: Option<Arc<ApplicationContainer>>,
    #[cfg(test)]
    owned_container_builder: Option<ApplicationContainerBuilder>,
    secret_resolver: Option<Arc<dyn SecretResolver>>,
    tracing_mode: TracingMode,
    trace_cells: Option<Vec<lily_trace::runtime::TraceCellConfig>>,
    middleware: Vec<QueueMiddlewareRegistration>,
    guards: Vec<QueueGuardRegistration>,
    pipeline_initialization_timeout: Duration,
    #[cfg(feature = "asyncapi")]
    asyncapi: ConsumerAsyncApiConfiguration,
    #[cfg(test)]
    managed_startup_handoff_barrier: Option<ManagedStartupHandoffBarrier>,
    #[cfg(test)]
    config_resolution_barrier: Option<ConfigResolutionBarrier>,
}

#[cfg(feature = "asyncapi")]
#[derive(Default)]
struct ConsumerAsyncApiConfiguration {
    config: Option<AsyncApiConfig>,
    duplicate: bool,
}

/// Container-local ownership token for the single Consumer AsyncAPI snapshot.
///
/// The token is attached before the shared queue runtime is adopted, then
/// retained only when the complete Consumer build commits. This prevents a
/// second Consumer build from mutating or stopping an already-running shared
/// queue runtime merely to discover the public snapshot conflict at the final
/// attachment boundary.
#[cfg(feature = "asyncapi")]
struct ConsumerAsyncApiBuildReservation;

#[cfg(test)]
#[derive(Clone)]
struct ManagedStartupHandoffBarrier {
    entered: CancellationToken,
    release: CancellationToken,
}

#[cfg(test)]
impl ManagedStartupHandoffBarrier {
    fn new() -> Self {
        Self {
            entered: CancellationToken::new(),
            release: CancellationToken::new(),
        }
    }

    async fn wait_until_entered(&self) {
        self.entered.cancelled().await;
    }

    fn release(&self) {
        self.release.cancel();
    }
}

#[cfg(test)]
#[derive(Clone)]
struct ConfigResolutionBarrier {
    entered: CancellationToken,
    release: CancellationToken,
    activity_dropped: Arc<std::sync::atomic::AtomicBool>,
    changed: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl ConfigResolutionBarrier {
    fn new() -> Self {
        Self {
            entered: CancellationToken::new(),
            release: CancellationToken::new(),
            activity_dropped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            changed: Arc::new(tokio::sync::Notify::new()),
        }
    }

    async fn enter(&self) {
        let _drop = ConfigResolutionActivityDrop(self.clone());
        self.entered.cancel();
        self.release.cancelled().await;
    }

    async fn wait_until_entered(&self) {
        self.entered.cancelled().await;
    }

    fn release(&self) {
        self.release.cancel();
    }

    async fn wait_until_activity_dropped(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .activity_dropped
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return;
            }
            changed.as_mut().await;
        }
    }
}

#[cfg(test)]
struct ConfigResolutionActivityDrop(ConfigResolutionBarrier);

#[cfg(test)]
impl Drop for ConfigResolutionActivityDrop {
    fn drop(&mut self) {
        self.0
            .activity_dropped
            .store(true, std::sync::atomic::Ordering::Release);
        self.0.changed.notify_waiters();
    }
}

const DEFAULT_PIPELINE_INITIALIZATION_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_PIPELINE_INITIALIZATION_TIMEOUT: Duration = Duration::from_secs(300);

struct ConsumerPipelineConfiguration {
    middleware: Vec<QueueMiddlewareRegistration>,
    guards: Vec<QueueGuardRegistration>,
    initialization_timeout: Duration,
}

impl ConsumerPipelineConfiguration {
    fn new(
        middleware: Vec<QueueMiddlewareRegistration>,
        guards: Vec<QueueGuardRegistration>,
        initialization_timeout: Duration,
    ) -> Self {
        Self {
            middleware,
            guards,
            initialization_timeout,
        }
    }
}

impl Default for ConsumerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsumerBuilder {
    /// Create a builder with an owned DI container and tracing disabled.
    pub fn new() -> Self {
        Self {
            container: None,
            #[cfg(test)]
            owned_container_builder: None,
            secret_resolver: None,
            tracing_mode: TracingMode::Disabled,
            trace_cells: None,
            middleware: Vec::new(),
            guards: Vec::new(),
            pipeline_initialization_timeout: DEFAULT_PIPELINE_INITIALIZATION_TIMEOUT,
            #[cfg(feature = "asyncapi")]
            asyncapi: ConsumerAsyncApiConfiguration::default(),
            #[cfg(test)]
            managed_startup_handoff_barrier: None,
            #[cfg(test)]
            config_resolution_barrier: None,
        }
    }

    #[cfg(test)]
    fn managed_startup_handoff_barrier(mut self, barrier: ManagedStartupHandoffBarrier) -> Self {
        self.managed_startup_handoff_barrier = Some(barrier);
        self
    }

    #[cfg(test)]
    fn config_resolution_barrier(mut self, barrier: ConfigResolutionBarrier) -> Self {
        self.config_resolution_barrier = Some(barrier);
        self
    }

    /// Use a caller-owned DI container.
    ///
    /// The consumer resolves its configuration, queue service and handlers
    /// from this container but never closes it. The consumer still shuts down
    /// the queue runtime used by this Consumer and any tracing runtime selected
    /// on this builder.
    pub fn container(mut self, container: Arc<ApplicationContainer>) -> Self {
        self.container = Some(container);
        #[cfg(test)]
        {
            self.owned_container_builder = None;
        }
        self
    }

    #[cfg(test)]
    fn framework_owned_container_builder_for_test(
        mut self,
        builder: ApplicationContainerBuilder,
    ) -> Self {
        self.container = None;
        self.owned_container_builder = Some(builder);
        self
    }

    /// Supplies this application's secret provider before configuration is
    /// loaded. Lily does not provide concrete secret-provider integrations.
    ///
    /// This cannot be combined with [`Self::container`]. Seed `ConfigService`
    /// while constructing a caller-owned container instead.
    pub fn secret_resolver<R>(mut self, resolver: R) -> Self
    where
        R: SecretResolver + 'static,
    {
        self.secret_resolver = Some(Arc::new(resolver));
        self
    }

    /// Explicitly keep tracing disabled.
    pub fn tracing_disabled(mut self) -> Self {
        self.tracing_mode = TracingMode::Disabled;
        self
    }

    /// Install and own the supplied tracing configuration.
    pub fn tracing_config(mut self, config: TraceConfig) -> Self {
        self.tracing_mode = TracingMode::owned_config(config);
        self
    }

    /// Load, validate and own tracing configuration from an explicit path.
    pub fn tracing_config_path(mut self, path: impl AsRef<Path>) -> Self {
        self.tracing_mode = TracingMode::owned_path(path.as_ref().to_path_buf());
        self
    }

    /// Reuse telemetry installed by a process-level composition root.
    pub fn tracing_external(mut self) -> Self {
        self.tracing_mode = TracingMode::External;
        self
    }

    /// Replace the complete worker/component identity list independently of
    /// tracing ownership.
    ///
    /// This is required when an external process owner installs tracing but
    /// the consumer still needs typed component metadata.
    pub fn trace_cells(mut self, cells: Vec<lily_trace::runtime::TraceCellConfig>) -> Self {
        self.trace_cells = Some(cells);
        self
    }

    /// Append one global queue middleware to the consumer execution pipeline.
    ///
    /// Middleware types are initialized once during startup through the
    /// application DI container. Declaration order is preserved among global
    /// middleware. All effective middleware runs before every guard.
    pub fn middleware<M>(mut self) -> Self
    where
        M: QueueMiddleware,
    {
        self.middleware.push(queue_middleware_registration::<M>());
        self
    }

    /// Append one global queue guard to the consumer execution pipeline.
    ///
    /// Guard types are initialized once during startup through the application
    /// DI container. Declaration order is preserved among global guards. All
    /// effective middleware runs before guards regardless of builder call
    /// interleaving.
    pub fn guard<G>(mut self) -> Self
    where
        G: QueueGuard,
    {
        self.guards.push(queue_guard_registration::<G>());
        self
    }

    /// Bound aggregate asynchronous construction of all queue middleware and
    /// guards.
    ///
    /// Zero and values above 300 seconds fail before tracing, DI or broker admission.
    pub fn pipeline_initialization_timeout(mut self, timeout: Duration) -> Self {
        self.pipeline_initialization_timeout = timeout;
        self
    }

    /// Enables one immutable AsyncAPI 3.1 document for this Consumer.
    ///
    /// Lily projects the document only from the accepted execution plan and
    /// effective RabbitMQ topology. The completed snapshot is published as
    /// [`crate::ConsumerAsyncApiService`] after every queue registration and
    /// the final startup-cancellation fence have succeeded. Lily does not add
    /// an HTTP endpoint, file exporter, UI or broker topology side effect.
    /// Registering this method more than once fails before tracing, DI or
    /// RabbitMQ startup begins.
    #[cfg(feature = "asyncapi")]
    #[must_use]
    pub fn asyncapi(mut self, config: AsyncApiConfig) -> Self {
        if self.asyncapi.config.replace(config).is_some() {
            self.asyncapi.duplicate = true;
        }
        self
    }

    /// Run until a supported operating-system shutdown signal or supervised
    /// queue-runtime termination.
    ///
    /// This profile installs signal handlers and performs bounded admission,
    /// drain, dependency and tracing shutdown in framework order.
    pub async fn run(self) -> Result<(), ConsumerError> {
        #[cfg(test)]
        let owned_container_builder = self.owned_container_builder;
        Consumer::run_internal(
            self.container,
            #[cfg(not(test))]
            None,
            #[cfg(test)]
            owned_container_builder,
            self.secret_resolver,
            self.tracing_mode,
            self.trace_cells,
            ConsumerPipelineConfiguration::new(
                self.middleware,
                self.guards,
                self.pipeline_initialization_timeout,
            ),
            #[cfg(feature = "asyncapi")]
            self.asyncapi,
            ConsumerLifecycleTrigger::Signals,
            #[cfg(test)]
            self.config_resolution_barrier,
        )
        .await
    }

    /// Run until a caller-owned cancellation token requests shutdown or the
    /// supervised queue runtime terminates.
    ///
    /// This profile never installs platform signal handlers. A token which is
    /// already cancelled returns before tracing, DI or queue admission starts.
    pub async fn run_with_cancellation(
        self,
        cancellation: CancellationToken,
    ) -> Result<(), ConsumerError> {
        if cancellation.is_cancelled() {
            return Ok(());
        }

        #[cfg(test)]
        let owned_container_builder = self.owned_container_builder;
        Consumer::run_internal(
            self.container,
            #[cfg(not(test))]
            None,
            #[cfg(test)]
            owned_container_builder,
            self.secret_resolver,
            self.tracing_mode,
            self.trace_cells,
            ConsumerPipelineConfiguration::new(
                self.middleware,
                self.guards,
                self.pipeline_initialization_timeout,
            ),
            #[cfg(feature = "asyncapi")]
            self.asyncapi,
            ConsumerLifecycleTrigger::Cancellation(cancellation),
            #[cfg(test)]
            self.config_resolution_barrier,
        )
        .await
    }

    /// Start the Consumer and return after every configured queue has been
    /// registered successfully.
    ///
    /// The returned handle owns programmatic shutdown and terminal-result
    /// observation. This profile never installs platform signal handlers.
    pub async fn start_managed(self) -> Result<ManagedConsumer, ConsumerError> {
        tokio::runtime::Handle::try_current().map_err(|_| {
            ConsumerError::dependency_initialization(
                lily_error::injection::InjectionError::RuntimeUnavailable {
                    operation: "ConsumerBuilder::start_managed".to_owned(),
                },
            )
        })?;
        #[cfg(test)]
        let handoff_barrier = self.managed_startup_handoff_barrier;
        #[cfg(test)]
        let config_resolution_barrier = self.config_resolution_barrier;
        #[cfg(test)]
        let owned_container_builder = self.owned_container_builder;
        #[cfg(not(test))]
        let owned_container_builder = None;
        let (startup_tx, startup_rx) = oneshot::channel();
        let waiter_cancellation = CancellationToken::new();
        let mut waiter = ManagedStartupWaiterGuard::new(waiter_cancellation.clone());
        let operational = Arc::new(ConsumerOperationalState::default());
        let runtime_task = OwnedTask::from(tokio::spawn(Consumer::run_internal(
            self.container,
            owned_container_builder,
            self.secret_resolver,
            self.tracing_mode,
            self.trace_cells,
            ConsumerPipelineConfiguration::new(
                self.middleware,
                self.guards,
                self.pipeline_initialization_timeout,
            ),
            #[cfg(feature = "asyncapi")]
            self.asyncapi,
            ConsumerLifecycleTrigger::Managed {
                startup_tx,
                waiter_cancellation,
            },
            #[cfg(test)]
            config_resolution_barrier,
        )));
        let task = OwnedTask::from(tokio::spawn(observe_managed_runtime(
            runtime_task,
            Arc::clone(&operational),
        )));

        match startup_rx.await {
            Ok(startup) => {
                #[cfg(test)]
                if let Some(barrier) = handoff_barrier {
                    barrier.entered.cancel();
                    barrier.release.cancelled().await;
                }
                let consumer = ManagedConsumer::new(startup, operational, task);
                waiter.disarm();
                Ok(consumer)
            }
            Err(_) => match task.await {
                Ok(Err(error)) => Err(error),
                Ok(Ok(())) => Err(ConsumerError::ManagedStartupIncomplete),
                Err(error) => Err(crate::consumer::owned_tasks::join_failure(error)),
            },
        }
    }
}

async fn observe_managed_runtime(
    runtime_task: impl Into<OwnedTask<Result<(), ConsumerError>>>,
    operational: Arc<ConsumerOperationalState>,
) -> Result<(), ConsumerError> {
    let result = match runtime_task.into().await {
        Ok(result) => result,
        Err(error) => Err(crate::consumer::owned_tasks::join_failure(error)),
    };
    operational.record_terminal(&result);
    result
}

struct ManagedConsumerStartup {
    shutdown_report: Arc<crate::shutdown_report::ConsumerShutdownObservation>,
    shutdown_state: Arc<ShutdownState>,
    provider: QueueRuntimeHandle,
    configured_queues: usize,
    registered_handlers: usize,
}

enum ConsumerLifecycleTrigger {
    Signals,
    Cancellation(CancellationToken),
    Managed {
        startup_tx: oneshot::Sender<ManagedConsumerStartup>,
        waiter_cancellation: CancellationToken,
    },
}

impl ConsumerLifecycleTrigger {
    fn startup_cancellation(&self) -> Option<CancellationToken> {
        match self {
            Self::Cancellation(cancellation) => Some(cancellation.clone()),
            Self::Managed {
                waiter_cancellation,
                ..
            } => Some(waiter_cancellation.clone()),
            Self::Signals => None,
        }
    }
}

/// Cloneable programmatic lifecycle handle for one running Consumer.
///
/// Concurrent [`Self::wait`] and [`Self::shutdown`] callers serialize around
/// the same retained runtime task and replay one immutable terminal result.
/// Dropping the last handle requests the same graceful, bounded shutdown used
/// by [`ManagedConsumer::shutdown`]. It never aborts the Consumer runtime task.
#[must_use = "dropping the handle requests Consumer shutdown; call wait or shutdown to observe the terminal result"]
#[derive(Clone)]
pub struct ManagedConsumer {
    inner: Arc<ManagedConsumerInner>,
}

struct ManagedConsumerInner {
    shutdown_report: Arc<crate::shutdown_report::ConsumerShutdownObservation>,
    shutdown_state: Arc<ShutdownState>,
    provider: QueueRuntimeHandle,
    configured_queues: usize,
    registered_handlers: usize,
    operational: Arc<ConsumerOperationalState>,
    state: Mutex<ManagedConsumerState>,
}

struct ManagedConsumerState {
    task: Option<OwnedTask<Result<(), ConsumerError>>>,
    terminal_result: Option<Result<(), ConsumerError>>,
}

impl ManagedConsumer {
    fn new(
        startup: ManagedConsumerStartup,
        operational: Arc<ConsumerOperationalState>,
        task: impl Into<OwnedTask<Result<(), ConsumerError>>>,
    ) -> Self {
        Self {
            inner: Arc::new(ManagedConsumerInner {
                shutdown_report: startup.shutdown_report,
                shutdown_state: startup.shutdown_state,
                provider: startup.provider,
                configured_queues: startup.configured_queues,
                registered_handlers: startup.registered_handlers,
                operational,
                state: Mutex::new(ManagedConsumerState {
                    task: Some(task.into()),
                    terminal_result: None,
                }),
            }),
        }
    }

    /// Return the immutable shutdown evidence after the actual runtime join.
    ///
    /// Returns `None` while the runtime is running, or when it failed before
    /// producing coordinator evidence. Absence is never successful cleanup.
    /// Available after both successful and failed `wait()`/`shutdown()` calls,
    /// and also after natural completion without an active waiter. The report
    /// never contains message bodies, exception text or broker credentials.
    pub fn shutdown_report(&self) -> Option<crate::ConsumerShutdownReport> {
        let (joined, failure_code) = self.inner.operational.terminal();
        if !joined {
            return None;
        }
        self.inner
            .shutdown_report
            .get()
            .cloned()
            .map(|report| report.after_runtime_join(failure_code))
    }

    /// Return one immutable, payload-free operational snapshot.
    ///
    /// Broker recovery keeps `live` true but lowers `ready`. Lily does not
    /// create a health endpoint; applications may expose this value through
    /// their own HTTP, metrics or orchestration adapter.
    pub fn snapshot(&self) -> ConsumerOperationalSnapshot {
        let deliveries = self.inner.provider.delivery_terminal_snapshot();
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        let transactional_outbox = self.inner.provider.transactional_outbox_snapshot();
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        let transactional_inbox = self.inner.provider.transactional_inbox_snapshot();
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        let transactional_outbox_ready = self.inner.provider.transactional_outbox_ready();
        #[cfg(not(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        )))]
        let transactional_outbox_ready = true;
        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        let transactional_failure_code = transactional_inbox
            .last_failure_code
            .or(transactional_outbox.last_failure_code);
        #[cfg(not(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        )))]
        let transactional_failure_code = None;
        let (completed, terminal_failure_code) = self.inner.operational.terminal();
        let failed = terminal_failure_code.is_some();
        let last_failure_code = terminal_failure_code
            .or(deliveries.last_operational_failure_code)
            .or(transactional_failure_code);
        let effective_runtime_state = if matches!(
            deliveries.runtime_state,
            lily_queue::ConsumerRuntimeState::Ready
        ) && !deliveries.is_ready()
        {
            let expected_consumers = deliveries
                .expected_consumers
                .max(self.inner.configured_queues as u64);
            if deliveries.registered_consumers < expected_consumers {
                lily_queue::ConsumerRuntimeState::Starting
            } else {
                lily_queue::ConsumerRuntimeState::Recovering
            }
        } else {
            deliveries.runtime_state
        };
        let (mut lifecycle, mut broker, mut topology) =
            classify_runtime(effective_runtime_state, completed, failed);
        let shutdown_ready = self.inner.shutdown_state.is_ready();
        let accepting = self.inner.shutdown_state.is_accepting_connections();
        let shutdown_requested = self.inner.shutdown_state.is_shutdown_initiated();
        if shutdown_requested
            && !completed
            && !matches!(
                effective_runtime_state,
                lily_queue::ConsumerRuntimeState::Failed
            )
        {
            lifecycle = crate::ConsumerLifecycleState::Draining;
            broker = crate::ConsumerBrokerState::Closing;
            topology = crate::ConsumerTopologyState::Draining;
        }
        let admission = if completed
            || matches!(
                effective_runtime_state,
                lily_queue::ConsumerRuntimeState::Failed
            ) {
            crate::ConsumerAdmissionState::Closed
        } else if shutdown_requested {
            crate::ConsumerAdmissionState::Draining
        } else {
            classify_admission(
                effective_runtime_state,
                deliveries.ready_consumers,
                deliveries.registered_consumers,
            )
        };
        let ready = aggregate_readiness(
            deliveries.is_ready(),
            transactional_outbox_ready,
            shutdown_ready,
            accepting,
            shutdown_requested,
            completed,
        );
        let live = !matches!(
            lifecycle,
            crate::ConsumerLifecycleState::Stopped | crate::ConsumerLifecycleState::Failed
        );

        ConsumerOperationalSnapshot {
            lifecycle,
            live,
            ready,
            broker,
            topology,
            admission,
            registered_queues: deliveries.registered_consumers,
            registered_handlers: self.inner.registered_handlers,
            ready_queues: deliveries.ready_consumers,
            deliveries,
            #[cfg(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            ))]
            transactional_outbox,
            #[cfg(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            ))]
            transactional_inbox,
            shutdown: ConsumerShutdownSnapshot {
                requested: shutdown_requested,
                forced: self.inner.shutdown_state.is_force_requested()
                    || self.inner.shutdown_report.get().is_some_and(|r| r.forced),
                completed,
                drain_reconciled: self.inner.provider.drain_reconciled(),
                // Use the same terminal sample as lifecycle/completed. A join
                // racing this snapshot must not publish a final report inside
                // a snapshot which still says the runtime is running.
                report: if completed {
                    self.inner
                        .shutdown_report
                        .get()
                        .cloned()
                        .map(|report| report.after_runtime_join(terminal_failure_code))
                } else {
                    None
                },
            },
            last_failure_code,
        }
    }

    /// Wait for natural runtime termination without initiating shutdown.
    /// Repeated calls replay the same terminal result.
    pub async fn wait(&self) -> Result<(), ConsumerError> {
        // Keep the JoinHandle inside the mutex while awaiting it. If this
        // observer future is cancelled, ownership remains in the shared state
        // and the next caller can continue observing the same runtime task.
        let mut state = self.inner.state.lock().await;
        if let Some(result) = &state.terminal_result {
            return result.clone();
        }

        let result = match state.task.as_mut() {
            Some(task) => match task.await {
                Ok(result) => result,
                Err(error) => Err(crate::consumer::owned_tasks::join_failure(error)),
            },
            None => Err(ConsumerError::ManagedRuntimeStateUnavailable),
        };
        self.inner.operational.record_terminal(&result);
        state.task.take();
        state.terminal_result = Some(result.clone());
        result
    }

    /// Request graceful shutdown and wait for its bounded terminal result.
    /// Repeated calls replay the same result and never rerun cleanup.
    pub async fn shutdown(&self) -> Result<(), ConsumerError> {
        let initiation_result = self
            .inner
            .shutdown_state
            .initiate_shutdown(ShutdownSignal::Manual)
            .map(|_| ())
            .map_err(ConsumerError::shutdown_initiation);
        let runtime_result = self.wait().await;
        match initiation_result {
            Ok(()) => runtime_result,
            Err(initiation_error) => {
                Consumer::merge_primary_then_secondary(runtime_result, Err(initiation_error))
            }
        }
    }
}

impl Drop for ManagedConsumerInner {
    fn drop(&mut self) {
        let has_terminal_result = self
            .state
            .try_lock()
            .map(|state| state.terminal_result.is_some())
            .unwrap_or(false);
        if !has_terminal_result {
            let _ = self
                .shutdown_state
                .initiate_shutdown(ShutdownSignal::Manual);
        }
    }
}

/// Convenience facade for starting a consumer application.
///
/// Use [`Consumer::builder`] when the process needs explicit DI, secret,
/// tracing or shutdown ownership.
pub struct Consumer {
    _private: (),
}

struct PreparedConsumerStartup {
    execution_plan: ConsumerExecutionPlan,
    configured_queues: usize,
    registered_handlers: usize,
    #[cfg(feature = "asyncapi")]
    asyncapi_document: Option<PreparedConsumerAsyncApi>,
}

struct ConsumerStartup {
    configured_queues: usize,
    registered_handlers: usize,
}

const fn shutdown_report_reconciled(is_success: bool, reconciles: bool) -> bool {
    is_success && reconciles
}

fn shutdown_report_has_primary_failure(report: &FrameworkShutdownReport) -> bool {
    report
        .phases
        .iter()
        .flat_map(|phase| &phase.components)
        .any(|component| shutdown_status_is_primary_failure(&component.status))
}

/// Returns whether the coordinator report contains failure evidence which is
/// not already represented by a typed component error.
///
/// A returned queue/DI/tracing error is retained separately with its original
/// source. Panics are contained by the coordinator before those component
/// evidence handles can observe them, so primary report observations must be
/// compared with the number of retained typed failures instead of suppressing
/// the aggregate whenever any typed error exists.
fn shutdown_report_requires_aggregate(
    report: &FrameworkShutdownReport,
    reconciled: bool,
    represented_primary_failures: usize,
) -> bool {
    let represented_component_failures = report
        .metrics
        .failed
        .saturating_add(report.metrics.forced_cleanup_failed);
    let untyped_component_panics = report
        .metrics
        .panicked
        .saturating_add(report.metrics.forced_cleanup_panicked);
    !reconciled
        || untyped_component_panics > 0
        || represented_component_failures > represented_primary_failures
}

fn shutdown_failure(report: &FrameworkShutdownReport, primary_failure: bool) -> ConsumerError {
    let metrics = report.metrics;
    ConsumerError::Shutdown {
        evidence: ConsumerShutdownFailureEvidence {
            kind: if primary_failure {
                ConsumerShutdownFailureKind::PrimaryFailure
            } else {
                ConsumerShutdownFailureKind::Incomplete
            },
            failed: metrics.failed,
            panicked: metrics.panicked,
            timed_out: metrics.timed_out,
            cancelled_by_force: metrics.cancelled_by_force,
            forced_cleanup_failed: metrics.forced_cleanup_failed,
            forced_cleanup_panicked: metrics.forced_cleanup_panicked,
            forced_cleanup_timed_out: metrics.forced_cleanup_timed_out,
            forced_cleanup_unavailable: metrics.forced_cleanup_unavailable,
        },
    }
}

const fn shutdown_status_is_primary_failure(status: &FrameworkComponentStatus) -> bool {
    matches!(
        status,
        FrameworkComponentStatus::Failed { .. } | FrameworkComponentStatus::Panicked { .. }
    )
}

#[cfg(test)]
fn ensure_queue_drain_reconciled(reconciled: bool) -> Result<(), ShutdownError> {
    if reconciled {
        Ok(())
    } else {
        Err(ShutdownError::Component(
            "queue delivery tasks were not joined; application DI disposal was withheld".into(),
        ))
    }
}

fn validate_bootstrap_ownership(
    has_container: bool,
    has_secret_resolver: bool,
) -> Result<(), ConsumerError> {
    if has_container && has_secret_resolver {
        return Err(ConsumerError::Configuration(
            ConsumerConfigurationFailure::BootstrapOwnershipConflict,
        ));
    }
    Ok(())
}

fn validate_pipeline_initialization_timeout(timeout: Duration) -> Result<(), ConsumerError> {
    if timeout.is_zero() || timeout > MAX_PIPELINE_INITIALIZATION_TIMEOUT {
        return Err(ConsumerError::Configuration(
            ConsumerConfigurationFailure::PipelineInitializationTimeoutInvalid,
        ));
    }
    Ok(())
}

/// Accepted queue configuration used by plan materialization.
struct ConsumerQueueConfiguration {
    definitions: Vec<QueueDefinition>,
    #[cfg(feature = "asyncapi")]
    virtual_host: Option<String>,
}

// Read queue definitions from the canonical `rabbitmq.topology.queues`
// section. Canonical URI parsing is opt-in with document production so merely
// compiling the feature cannot change a Consumer whose builder disabled it.
async fn get_queue_definitions(
    config_service: &ConfigService,
    #[cfg(feature = "asyncapi")] include_virtual_host: bool,
) -> Result<ConsumerQueueConfiguration, ConsumerError> {
    let lily_config = config_service.get_lily_config().await;

    // A consumer still requires connection settings even though topology has a
    // separate shared authority.
    let _consumer = lily_config
        .rabbitmq
        .consumer
        .as_ref()
        .ok_or(ConsumerError::Configuration(
            ConsumerConfigurationFailure::RabbitMqConsumerMissing,
        ))?;

    #[cfg(feature = "asyncapi")]
    let virtual_host = include_virtual_host
        .then(|| lily_queue::__private::rabbitmq_consumer_virtual_host(_consumer))
        .transpose()
        .map_err(|source| ConsumerError::PipelineInitialization { source })?;

    Ok(ConsumerQueueConfiguration {
        definitions: lily_config.rabbitmq.topology.queues,
        #[cfg(feature = "asyncapi")]
        virtual_host,
    })
}

fn execution_plan_error(error: ConsumerExecutionPlanError) -> ConsumerError {
    match error {
        ConsumerExecutionPlanError::Selection(error) => ConsumerError::Configuration(
            ConsumerConfigurationFailure::ExecutionPlanInvalid(error.into()),
        ),
        ConsumerExecutionPlanError::Compilation(source) => {
            ConsumerError::PipelineInitialization { source }
        }
    }
}

impl Consumer {
    /// Create an explicit consumer composition builder.
    pub fn builder() -> ConsumerBuilder {
        ConsumerBuilder::new()
    }

    /// Build a dedicated container and run the consumer application.
    pub async fn run() -> Result<(), ConsumerError> {
        ConsumerBuilder::new().run().await
    }

    /// Run the consumer with a caller-owned container.
    ///
    /// This allows HTTP, WebSocket and Consumer adapters in the same process
    /// to share one explicit composition root without global state. The
    /// container remains open, but this run still shuts down the queue runtime
    /// used by the Consumer.
    pub async fn run_with_container(
        container: Arc<ApplicationContainer>,
    ) -> Result<(), ConsumerError> {
        ConsumerBuilder::new().container(container).run().await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the private composition root keeps ownership, tracing, pipeline, optional AsyncAPI and lifecycle authorities explicit"
    )]
    async fn run_internal(
        container: Option<Arc<ApplicationContainer>>,
        owned_container_builder: Option<ApplicationContainerBuilder>,
        secret_resolver: Option<Arc<dyn SecretResolver>>,
        tracing_mode: TracingMode,
        trace_cells_override: Option<Vec<lily_trace::runtime::TraceCellConfig>>,
        pipeline: ConsumerPipelineConfiguration,
        #[cfg(feature = "asyncapi")] asyncapi: ConsumerAsyncApiConfiguration,
        lifecycle_trigger: ConsumerLifecycleTrigger,
        #[cfg(test)] config_resolution_barrier: Option<ConfigResolutionBarrier>,
    ) -> Result<(), ConsumerError> {
        validate_bootstrap_ownership(container.is_some(), secret_resolver.is_some())?;
        validate_pipeline_initialization_timeout(pipeline.initialization_timeout)?;
        #[cfg(feature = "asyncapi")]
        if asyncapi.duplicate {
            return Err(ConsumerError::Configuration(
                ConsumerConfigurationFailure::DuplicateAsyncApiConfiguration,
            ));
        }
        let trace_config = tracing_mode.resolve_owned_config().map_err(|error| {
            ConsumerError::tracing(ConsumerTracingFailureStage::Configuration, error)
        })?;
        let trace_cells = trace_cells_override.unwrap_or_else(|| {
            trace_config
                .as_ref()
                .map(|config| config.cells.clone())
                .unwrap_or_default()
        });
        let trace_cells = ValidatedTraceCells::try_new(trace_cells).map_err(|error| {
            ConsumerError::Configuration(ConsumerConfigurationFailure::TraceDescriptorInvalid(
                error.into(),
            ))
        })?;

        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            ConsumerError::dependency_initialization(
                lily_error::injection::InjectionError::RuntimeUnavailable {
                    operation: "Consumer::run_internal".to_owned(),
                },
            )
        })?;
        let tracing_owner = match trace_config.as_ref() {
            Some(config) => match TracingRuntimeOwner::install(config) {
                Ok(TraceInstallOutcome::Disabled) => None,
                Ok(TraceInstallOutcome::Owned(owner)) => Some(owner),
                Err(error) => {
                    return Err(ConsumerError::tracing(
                        ConsumerTracingFailureStage::Initialization,
                        error,
                    ));
                }
            },
            None => None,
        };
        let startup_cancellation = lifecycle_trigger.startup_cancellation();
        let mut build_transaction =
            ConsumerBuildTransaction::new(runtime, tracing_owner, DEFAULT_SHUTDOWN_TIMEOUT);

        let container = match container {
            Some(container) => container,
            None => {
                let builder = match owned_container_builder {
                    Some(builder) => builder,
                    None => match secret_resolver {
                        Some(resolver) => application_container_builder()
                            .seed_singleton(ConfigService::with_shared_secret_resolver(resolver)),
                        None => application_container_builder(),
                    },
                };
                let build = build_transaction.build_owned_container(builder);
                match poll_startup_activity(build, startup_cancellation.as_ref()).await {
                    StartupActivityOutcome::Completed(Ok(container)) => container,
                    StartupActivityOutcome::Completed(Err(error)) => {
                        return build_transaction.rollback(Some(error)).await;
                    }
                    StartupActivityOutcome::Cancelled => {
                        return build_transaction.rollback(None).await;
                    }
                }
            }
        };

        #[cfg(feature = "asyncapi")]
        let asyncapi_reservation = if asyncapi.config.is_some() {
            match lily_injection::__private::attach_framework_singleton(
                &container.services(),
                Arc::new(ConsumerAsyncApiBuildReservation),
            ) {
                Ok(reservation) => Some(reservation),
                Err(error) => {
                    return build_transaction
                        .rollback(Some(ConsumerError::asyncapi(
                            ConsumerAsyncApiFailureStage::Attachment,
                            error,
                        )))
                        .await;
                }
            }
        } else {
            None
        };

        let startup_span = tracing::info_span!(
            "consumer.startup",
            queues_registered = tracing::field::Empty,
            handlers_discovered = tracing::field::Empty,
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );

        // Resolve and adopt the already initialized QueueService runtime before
        // any other cancellable composition work. A caller-owned container
        // remains caller-owned, but the runtime selected for this Consumer is
        // still stopped by the Consumer on every later startup exit.
        let queue_resolution = build_transaction.track(
            async {
                let queue_service = container
                    .resolve::<QueueService>(None)
                    .await
                    .map_err(ConsumerError::dependency_initialization)?;
                let provider = queue_runtime(&queue_service)
                    .map_err(|source| ConsumerError::ReceiverStartup { source })?;
                Ok::<_, ConsumerError>((queue_service, provider))
            }
            .instrument(startup_span.clone()),
        );
        let (queue_service, provider) =
            match poll_startup_activity(queue_resolution, startup_cancellation.as_ref()).await {
                StartupActivityOutcome::Completed(Ok(resolved)) => resolved,
                StartupActivityOutcome::Completed(Err(error)) => {
                    startup_span.record("lily.outcome", "error");
                    startup_span.record("lily.error_code", error.error_code());
                    startup_span.record("otel.status_code", "ERROR");
                    return build_transaction.rollback(Some(error)).await;
                }
                StartupActivityOutcome::Cancelled => {
                    return build_transaction.rollback(None).await;
                }
            };
        build_transaction.retain_queue_runtime(provider.clone());

        let config_resolution = build_transaction.track(
            async {
                #[cfg(test)]
                if let Some(barrier) = config_resolution_barrier.as_ref() {
                    barrier.enter().await;
                }
                let config_service = container
                    .resolve::<ConfigService>(None)
                    .await
                    .map_err(ConsumerError::dependency_initialization)?;
                let shutdown_timeout = Duration::from_secs(
                    config_service
                        .get_lily_config()
                        .await
                        .lifecycle
                        .shutdown_timeout_secs,
                );
                Ok::<_, ConsumerError>((config_service, shutdown_timeout))
            }
            .instrument(startup_span.clone()),
        );
        let (config_service, shutdown_timeout) =
            match poll_startup_activity(config_resolution, startup_cancellation.as_ref()).await {
                StartupActivityOutcome::Completed(Ok(resolved)) => resolved,
                StartupActivityOutcome::Completed(Err(error)) => {
                    startup_span.record("lily.outcome", "error");
                    startup_span.record("lily.error_code", error.error_code());
                    startup_span.record("otel.status_code", "ERROR");
                    return build_transaction.rollback(Some(error)).await;
                }
                StartupActivityOutcome::Cancelled => {
                    return build_transaction.rollback(None).await;
                }
            };
        build_transaction.set_shutdown_timeout(shutdown_timeout);

        let plan_materialization = build_transaction.track(
            async {
                let all_handlers = get_all_queue_handlers();
                tracing::Span::current().record("handlers_discovered", all_handlers.len());
                info!(
                    handler_count = all_handlers.len(),
                    "Discovered queue handlers from linked metadata"
                );
                let queue_configuration = get_queue_definitions(
                    &config_service,
                    #[cfg(feature = "asyncapi")]
                    asyncapi.config.is_some(),
                )
                .await?;
                info!(
                    configured_queue_count = queue_configuration.definitions.len(),
                    "Loaded queue topology configuration"
                );
                let execution_plan = ConsumerExecutionPlan::build(
                    &queue_configuration.definitions,
                    &all_handlers,
                    &trace_cells,
                    Arc::clone(&container),
                    &pipeline.middleware,
                    &pipeline.guards,
                    pipeline.initialization_timeout,
                )
                .await
                .map_err(execution_plan_error)?;
                tracing::Span::current().record("queues_registered", execution_plan.len());
                info!(
                    active_queue_count = execution_plan.len(),
                    handler_count = execution_plan.handler_count(),
                    "Materialized immutable Consumer execution plan"
                );
                #[cfg(feature = "asyncapi")]
                let asyncapi_document = match asyncapi.config.as_ref() {
                    Some(config) => {
                        let virtual_host = queue_configuration.virtual_host.as_deref().ok_or(
                            ConsumerError::Configuration(
                                ConsumerConfigurationFailure::ExecutionPlanInvalid(
                                    ConsumerPlanFailureKind::QueueConfigurationInvalid,
                                ),
                            ),
                        )?;
                        Some(
                            execution_plan
                                .prepare_asyncapi(config, virtual_host)
                                .map_err(|error| {
                                    ConsumerError::asyncapi(
                                        ConsumerAsyncApiFailureStage::DocumentBuild,
                                        error,
                                    )
                                })?,
                        )
                    }
                    None => None,
                };
                Ok::<_, ConsumerError>(PreparedConsumerStartup {
                    configured_queues: execution_plan.len(),
                    registered_handlers: execution_plan.handler_count(),
                    execution_plan,
                    #[cfg(feature = "asyncapi")]
                    asyncapi_document,
                })
            }
            .instrument(startup_span.clone()),
        );
        let prepared = match poll_startup_activity(
            plan_materialization,
            startup_cancellation.as_ref(),
        )
        .await
        {
            StartupActivityOutcome::Completed(Ok(prepared)) => prepared,
            StartupActivityOutcome::Completed(Err(error)) => {
                startup_span.record("lily.outcome", "error");
                startup_span.record("lily.error_code", error.error_code());
                startup_span.record("otel.status_code", "ERROR");
                return build_transaction.rollback(Some(error)).await;
            }
            StartupActivityOutcome::Cancelled => {
                return build_transaction.rollback(None).await;
            }
        };

        let PreparedConsumerStartup {
            execution_plan,
            configured_queues,
            registered_handlers,
            #[cfg(feature = "asyncapi")]
            asyncapi_document,
        } = prepared;

        let registration = build_transaction.track(
            async {
                for binding in execution_plan.into_bindings() {
                    Self::register_queue_traced(&queue_service, binding).await?;
                }
                Ok::<_, ConsumerError>(())
            }
            .instrument(startup_span.clone()),
        );
        match poll_startup_activity(registration, startup_cancellation.as_ref()).await {
            StartupActivityOutcome::Completed(Ok(())) => {}
            StartupActivityOutcome::Completed(Err(error)) => {
                startup_span.record("lily.outcome", "error");
                startup_span.record("lily.error_code", error.error_code());
                startup_span.record("otel.status_code", "ERROR");
                return build_transaction.rollback(Some(error)).await;
            }
            StartupActivityOutcome::Cancelled => {
                return build_transaction.rollback(None).await;
            }
        }
        if startup_cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return build_transaction.rollback(None).await;
        }

        // AsyncAPI publication is the final Consumer build transaction. The
        // document was prepared from the accepted execution plan, but neither
        // its service nor an uninitialized placeholder was observable while
        // queue registration could still fail or startup could be cancelled.
        // Keep this success path synchronous through both commit boundaries so
        // cancellation after the fence belongs to the retained runtime owner.
        #[cfg(feature = "asyncapi")]
        let asyncapi_attachment = if let Some(document) = asyncapi_document {
            let service = Arc::new(lily_asyncapi::__private::new_service::<ConsumerDocument>());
            if let Err(error) = lily_asyncapi::__private::attach_document(&service, document) {
                return build_transaction
                    .rollback(Some(ConsumerError::asyncapi(
                        ConsumerAsyncApiFailureStage::Attachment,
                        error,
                    )))
                    .await;
            }
            match lily_injection::__private::attach_framework_singleton(
                &container.services(),
                service,
            ) {
                Ok(attachment) => Some(attachment),
                Err(error) => {
                    return build_transaction
                        .rollback(Some(ConsumerError::asyncapi(
                            ConsumerAsyncApiFailureStage::Attachment,
                            error,
                        )))
                        .await;
                }
            }
        } else {
            None
        };
        startup_span.record("lily.outcome", "success");

        let startup = ConsumerStartup {
            configured_queues,
            registered_handlers,
        };
        // The committed owner exists before readiness or runtime waiting can
        // observe cancellation. There is deliberately no await between this
        // synchronous transfer and construction of the runtime future.
        let committed_build = build_transaction.commit();
        #[cfg(feature = "asyncapi")]
        if let Some(attachment) = asyncapi_attachment {
            attachment.commit();
        }
        #[cfg(feature = "asyncapi")]
        if let Some(reservation) = asyncapi_reservation {
            reservation.commit();
        }
        let shutdown_state = Arc::new(ShutdownState::new());
        let runtime_owner = committed_build.into_runtime_owner(Arc::clone(&shutdown_state));
        let supervisor = runtime_owner.spawn(startup, lifecycle_trigger);
        let mut waiter = ConsumerRuntimeWaiterGuard::new(shutdown_state);
        match supervisor.await {
            Ok(result) => {
                waiter.disarm();
                result
            }
            Err(error) => Err(crate::consumer::owned_tasks::join_failure(error)),
        }
    }

    #[cfg(test)]
    fn merge_lifecycle_results<const N: usize>(
        results: [Result<(), ConsumerError>; N],
    ) -> Result<(), ConsumerError> {
        Self::merge_lifecycle_vec(results.into_iter().collect())
    }

    fn merge_lifecycle_vec(results: Vec<Result<(), ConsumerError>>) -> Result<(), ConsumerError> {
        let failures = results
            .into_iter()
            .filter_map(Result::err)
            .collect::<Vec<_>>();
        let mut failures = failures.into_iter();
        match failures.next() {
            None => Ok(()),
            Some(primary) => Err(ConsumerError::aggregate(primary, failures.collect())),
        }
    }

    fn merge_primary_then_secondary(
        primary: Result<(), ConsumerError>,
        secondary: Result<(), ConsumerError>,
    ) -> Result<(), ConsumerError> {
        match (primary, secondary) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(primary), Err(secondary)) => {
                Err(ConsumerError::aggregate(primary, vec![secondary]))
            }
        }
    }

    fn queue_runtime_completed(
        result: Result<(), lily_error::application::MessageBrokerError>,
        shutdown_state: &ShutdownState,
    ) -> (Result<(), ConsumerError>, ShutdownSignal) {
        info!("All background tasks completed");
        let initiation = shutdown_state.initiate_shutdown(ShutdownSignal::Manual);
        let runtime_result = result.map_err(|source| ConsumerError::RuntimeSupervision { source });
        let initiation_result = initiation
            .map(|_| ())
            .map_err(ConsumerError::shutdown_initiation);
        let runtime_result = Self::merge_primary_then_secondary(runtime_result, initiation_result);
        (runtime_result, ShutdownSignal::Manual)
    }

    fn programmatic_shutdown_requested(
        shutdown_state: &ShutdownState,
    ) -> (Result<(), ConsumerError>, ShutdownSignal) {
        let result = shutdown_state
            .initiate_shutdown(ShutdownSignal::Manual)
            .map(|_| ())
            .map_err(ConsumerError::shutdown_initiation);
        (result, ShutdownSignal::Manual)
    }

    #[instrument(
        name = "consumer.register_queue",
        skip(queue_service, binding),
        fields(
            queue_name = %binding.definition.name,
            handler_count = binding.handler_count
        )
    )]
    async fn register_queue_traced(
        queue_service: &QueueService,
        binding: ConsumerExecutionPlanBinding,
    ) -> Result<(), ConsumerError> {
        // This remains a real use even when Cargo feature unification exposes
        // only a dependency backend variant which this lily_consumer build
        // must reject below.
        let _ = queue_service;
        let ConsumerExecutionPlanBinding {
            definition,
            dispatch,
            handler_count,
            #[cfg(any(
                feature = "transactional-inbox-postgresql",
                feature = "transactional-inbox-postgresql-factory",
                feature = "transactional-inbox-mongodb",
                feature = "transactional-inbox-mongodb-factory"
            ))]
            transactional_runtime,
        } = binding;
        info!(
            "Registering queue: {} with {} version/content handlers",
            definition.name, handler_count
        );

        #[cfg(any(
            feature = "transactional-inbox-postgresql",
            feature = "transactional-inbox-postgresql-factory",
            feature = "transactional-inbox-mongodb",
            feature = "transactional-inbox-mongodb-factory"
        ))]
        if let Some(runtime) = transactional_runtime {
            let result = match runtime {
                #[cfg(any(
                    feature = "transactional-inbox-postgresql",
                    feature = "transactional-inbox-postgresql-factory"
                ))]
                PreparedTransactionalRuntime::PostgreSql(runtime) => {
                    register_postgresql_transactional_runtime(queue_service, runtime).await
                }
                #[cfg(any(
                    feature = "transactional-inbox-mongodb",
                    feature = "transactional-inbox-mongodb-factory"
                ))]
                PreparedTransactionalRuntime::MongoDb(runtime) => {
                    register_mongodb_transactional_runtime(queue_service, runtime).await
                }
                #[allow(
                    unreachable_patterns,
                    reason = "Cargo feature unification can add a lily_queue runtime variant whose adapter is not enabled on lily_consumer"
                )]
                _ => Err(invalid_materialized_plan(
                    "prepared transactional inbox backend adapter is not enabled in lily_consumer",
                )),
            };
            result.map_err(|source| ConsumerError::TopologyBootstrap {
                source,
                lifecycle: None,
            })?;
        }

        register_compiled_dispatch(queue_service, &definition.exchange_name, dispatch)
            .await
            .map_err(|source| ConsumerError::TopologyBootstrap {
                source,
                lifecycle: None,
            })?;

        info!(
            queue = %definition.name,
            concurrency = definition.concurrency,
            prefetch = definition.prefetch_count,
            retry_attempts = definition.retry_attempts,
            handler_count,
            "Queue registration completed"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::task::noop_waker_ref;
    use lily_trace::runtime::TraceCellConfig;
    use std::{future::Future, task::Context};

    #[cfg(feature = "asyncapi")]
    use {
        crate::schemars::{self, JsonSchema},
        lily_queue::Json,
        std::{
            borrow::Cow,
            sync::atomic::{AtomicUsize, Ordering},
        },
    };

    #[cfg(feature = "asyncapi")]
    static ASYNCAPI_SCHEMA_FACTORY_CALLS: AtomicUsize = AtomicUsize::new(0);
    #[cfg(feature = "asyncapi")]
    static ASYNCAPI_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[cfg(feature = "asyncapi")]
    #[derive(JsonSchema)]
    struct ConsumerAsyncApiPayloadShape {
        #[allow(dead_code)]
        event_id: String,
    }

    #[cfg(feature = "asyncapi")]
    #[derive(serde::Deserialize)]
    struct ConsumerAsyncApiPayloadProbe {
        #[allow(dead_code)]
        event_id: String,
    }

    #[cfg(feature = "asyncapi")]
    impl JsonSchema for ConsumerAsyncApiPayloadProbe {
        fn schema_name() -> Cow<'static, str> {
            Cow::Borrowed("ConsumerAsyncApiPayloadProbe")
        }

        fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
            ASYNCAPI_SCHEMA_FACTORY_CALLS.fetch_add(1, Ordering::SeqCst);
            ConsumerAsyncApiPayloadShape::json_schema(generator)
        }
    }

    #[cfg(feature = "asyncapi")]
    #[derive(Default, lily_injection::Injectable)]
    #[service(lifetime = "Singleton")]
    struct ConsumerAsyncApiHandler;

    #[cfg(feature = "asyncapi")]
    impl lily_injection::ServiceTrait for ConsumerAsyncApiHandler {}

    #[cfg(feature = "asyncapi")]
    #[lily_queue::queue_service]
    impl ConsumerAsyncApiHandler {
        #[lily_queue::queue("capasync02b.consumer.documented", version = 1, content = "json")]
        #[lily_queue::asyncapi(documented, operation_id = "consumer.documented.v1")]
        async fn documented(
            &self,
            Json(_payload): Json<ConsumerAsyncApiPayloadProbe>,
        ) -> Result<(), lily_queue::QueueHandlerError> {
            Ok(())
        }
    }

    #[cfg(feature = "asyncapi")]
    const ASYNCAPI_TRANSPORT_FREE_CONFIG: &str = r#"
[rabbitmq.consumer]
username = "guest"
password = "guest"
hostname = "localhost"
port = 5672
vhost = "/"
use_tls = false
pool_size = 1
connection_timeout_secs = 1
confirm_timeout_secs = 1
heartbeat_secs = 30
max_reconnect_attempts = 1
reconnect_backoff_millis = 50
persistence_enabled = true

[[rabbitmq.topology.queues]]
name = "capasync02b.consumer.documented"
exchange_name = "capq02b"
routing_key = "capasync02b.consumer.documented"
retention = { main_max_messages = 1000, main_max_bytes = 1048576, retry_bucket_max_messages = 100, retry_bucket_max_bytes = 262144, dead_letter_max_messages = 100, dead_letter_max_bytes = 262144 }
concurrency = 1
prefetch_count = 1
retry_attempts = 1
retry_backoff_millis = 250
max_retry_backoff_millis = 5000
durable = true
"#;

    #[cfg(feature = "asyncapi")]
    fn consumer_asyncapi_config() -> AsyncApiConfig {
        let mut config =
            AsyncApiConfig::new("Consumer contract", "1.0.0").expect("bounded AsyncAPI metadata");
        let server = crate::AsyncApiServer::new(
            "rabbitmq",
            "localhost:5672",
            crate::AsyncApiServerProtocol::Amqp,
        )
        .expect("bounded AsyncAPI server")
        .with_pathname("/")
        .expect("RabbitMQ virtual host pathname");
        config.add_server(server).expect("one AMQP server");
        config
    }

    #[cfg(feature = "asyncapi")]
    async fn asyncapi_transport_free_fixture() -> (
        tempfile::NamedTempFile,
        Arc<ApplicationContainer>,
        lily_queue::__private::QueueServiceTestProbe,
    ) {
        let config_file = tempfile::NamedTempFile::new().expect("temporary Consumer config");
        std::fs::write(config_file.path(), ASYNCAPI_TRANSPORT_FREE_CONFIG)
            .expect("write transport-free Consumer config");
        let (queue_service, probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
            ConfigService::development(config_file.path()),
        ));
        let container = Arc::new(
            crate::test_application_container_builder()
                .seed_singleton(ConfigService::development(config_file.path()))
                .seed_singleton(queue_service)
                .build()
                .await
                .expect("transport-free Consumer container"),
        );
        (config_file, container, probe)
    }

    #[test]
    fn managed_start_without_tokio_runtime_returns_a_typed_error() {
        let mut future = Box::pin(ConsumerBuilder::new().start_managed());
        let mut context = Context::from_waker(noop_waker_ref());

        let std::task::Poll::Ready(result) = future.as_mut().poll(&mut context) else {
            panic!("runtime validation must complete before spawning any managed task");
        };
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("an active Tokio runtime is required"),
        };
        let ConsumerError::DependencyInitialization {
            source: lily_error::injection::InjectionError::RuntimeUnavailable { operation },
        } = error
        else {
            panic!("missing Tokio runtime must retain typed DI evidence");
        };
        assert_eq!(operation, "ConsumerBuilder::start_managed");
    }

    fn managed_test_consumer(
        shutdown_state: Arc<ShutdownState>,
        task: JoinHandle<Result<(), ConsumerError>>,
    ) -> ManagedConsumer {
        managed_test_consumer_with_probe(shutdown_state, task, 0, 0).0
    }

    fn managed_test_consumer_with_probe(
        shutdown_state: Arc<ShutdownState>,
        task: JoinHandle<Result<(), ConsumerError>>,
        configured_queues: usize,
        registered_handlers: usize,
    ) -> (
        ManagedConsumer,
        lily_queue::__private::QueueServiceTestProbe,
    ) {
        let (queue_service, probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
            ConfigService::development("/tmp/lily-capq05-managed-consumer-test.toml"),
        ));
        let provider = queue_runtime(&queue_service).expect("test queue runtime");
        (
            ManagedConsumer::new(
                ManagedConsumerStartup {
                    shutdown_report: Arc::default(),
                    shutdown_state,
                    provider,
                    configured_queues,
                    registered_handlers,
                },
                Arc::new(ConsumerOperationalState::default()),
                task,
            ),
            probe,
        )
    }

    const fn test_runtime_failure() -> ConsumerError {
        ConsumerError::Configuration(ConsumerConfigurationFailure::RabbitMqConsumerMissing)
    }

    struct TestSecretResolver;

    #[async_trait::async_trait]
    impl SecretResolver for TestSecretResolver {
        async fn resolve(&self, _key: &str) -> Result<String, lily_config::ConfigError> {
            unreachable!("container/bootstrap conflict must be rejected before config loading")
        }
    }

    #[test]
    fn caller_owned_container_cannot_be_combined_with_secret_resolver() {
        let builder = ConsumerBuilder::new().secret_resolver(TestSecretResolver);
        let error = validate_bootstrap_ownership(true, builder.secret_resolver.is_some())
            .expect_err("ambiguous bootstrap ownership must fail");
        assert!(matches!(
            error,
            ConsumerError::Configuration(ConsumerConfigurationFailure::BootstrapOwnershipConflict)
        ));
    }

    #[cfg(feature = "asyncapi")]
    #[tokio::test]
    async fn duplicate_asyncapi_configuration_fails_before_composition() {
        let error = ConsumerBuilder::new()
            .asyncapi(consumer_asyncapi_config())
            .asyncapi(consumer_asyncapi_config())
            .run_with_cancellation(CancellationToken::new())
            .await
            .expect_err("duplicate AsyncAPI ownership must fail");

        assert_eq!(
            error.error_code(),
            "CONSUMER_ASYNCAPI_CONFIGURATION_DUPLICATE"
        );
    }

    #[cfg(feature = "asyncapi")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn asyncapi_is_absent_when_disabled_and_attached_only_after_success() {
        let _serial = ASYNCAPI_TEST_LOCK.lock().await;
        let schema_calls_before_disabled = ASYNCAPI_SCHEMA_FACTORY_CALLS.load(Ordering::SeqCst);
        let (_config_file, disabled_container, disabled_probe) =
            asyncapi_transport_free_fixture().await;
        disabled_probe.pause_runtime_completion();
        let disabled = ConsumerBuilder::new()
            .container(Arc::clone(&disabled_container))
            .start_managed()
            .await
            .expect("Consumer without AsyncAPI becomes ready");
        assert!(matches!(
            disabled_container
                .resolve::<crate::ConsumerAsyncApiService>(None)
                .await,
            Err(lily_error::injection::InjectionError::ServiceNotFound(_))
        ));
        disabled
            .shutdown()
            .await
            .expect("disabled Consumer shutdown");
        disabled_container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("close disabled caller container");
        assert_eq!(
            ASYNCAPI_SCHEMA_FACTORY_CALLS.load(Ordering::SeqCst),
            schema_calls_before_disabled,
            "omitting ConsumerBuilder::asyncapi must not invoke a compiled schema factory"
        );

        let (_config_file, enabled_container, enabled_probe) =
            asyncapi_transport_free_fixture().await;
        enabled_probe.pause_runtime_completion();
        let enabled = ConsumerBuilder::new()
            .container(Arc::clone(&enabled_container))
            .asyncapi(consumer_asyncapi_config())
            .start_managed()
            .await
            .expect("Consumer with AsyncAPI becomes ready");
        let service = enabled_container
            .resolve::<crate::ConsumerAsyncApiService>(None)
            .await
            .expect("committed AsyncAPI service");
        let snapshot = service.snapshot().expect("complete AsyncAPI document");
        assert_eq!(snapshot.document().specification_version(), "3.1.0");
        assert_eq!(snapshot.document().title(), "Consumer contract");
        assert_eq!(snapshot.document().channel_count(), 1);
        assert_eq!(snapshot.document().operation_count(), 1);
        assert!(
            ASYNCAPI_SCHEMA_FACTORY_CALLS.load(Ordering::SeqCst) > schema_calls_before_disabled,
            "enabling AsyncAPI must materialize the accepted generated payload schema"
        );
        enabled.shutdown().await.expect("enabled Consumer shutdown");
        enabled_container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("close enabled caller container");
    }

    #[cfg(feature = "asyncapi")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn asyncapi_projection_failure_leaves_no_framework_service_or_registration() {
        let _serial = ASYNCAPI_TEST_LOCK.lock().await;
        let (_config_file, container, probe) = asyncapi_transport_free_fixture().await;
        let config = AsyncApiConfig::new("Consumer contract", "1.0.0")
            .expect("metadata without an advertised AMQP server");

        let result = ConsumerBuilder::new()
            .container(Arc::clone(&container))
            .asyncapi(config)
            .start_managed()
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("missing AMQP server authority must fail projection"),
        };

        assert_eq!(error.error_code(), "CONSUMER_ASYNCAPI_DOCUMENT_BUILD");
        assert_eq!(probe.registration_count(), 0);
        assert!(matches!(
            container
                .resolve::<crate::ConsumerAsyncApiService>(None)
                .await,
            Err(lily_error::injection::InjectionError::ServiceNotFound(_))
        ));
        container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("caller container remains closeable after projection rollback");
    }

    #[cfg(feature = "asyncapi")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn second_asyncapi_consumer_fails_before_touching_the_running_shared_runtime() {
        let _serial = ASYNCAPI_TEST_LOCK.lock().await;
        let (_config_file, container, probe) = asyncapi_transport_free_fixture().await;
        probe.pause_runtime_completion();
        let first = ConsumerBuilder::new()
            .container(Arc::clone(&container))
            .asyncapi(consumer_asyncapi_config())
            .start_managed()
            .await
            .expect("first Consumer becomes ready");
        let existing = container
            .resolve::<crate::ConsumerAsyncApiService>(None)
            .await
            .expect("first Consumer owns the snapshot");

        let result = ConsumerBuilder::new()
            .container(Arc::clone(&container))
            .asyncapi(consumer_asyncapi_config())
            .start_managed()
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("duplicate framework service authority must fail"),
        };

        assert_eq!(error.error_code(), "CONSUMER_ASYNCAPI_ATTACHMENT");
        let retained = container
            .resolve::<crate::ConsumerAsyncApiService>(None)
            .await
            .expect("pre-existing service remains attached");
        assert!(Arc::ptr_eq(&retained, &existing));
        assert_eq!(
            retained
                .snapshot()
                .expect("first snapshot remains initialized")
                .document()
                .operation_count(),
            1
        );
        // The second build is rejected by the private reservation before it
        // adopts, registers with or can roll back the first Consumer's shared
        // queue runtime.
        assert_eq!(probe.registration_count(), 1);
        first
            .shutdown()
            .await
            .expect("first Consumer remains healthy and shuts down normally");
        container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("caller container remains closeable after attachment rollback");
    }

    #[cfg(feature = "asyncapi")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independent_composition_roots_own_independent_asyncapi_snapshots() {
        let _serial = ASYNCAPI_TEST_LOCK.lock().await;
        let (_first_config, first_container, first_probe) = asyncapi_transport_free_fixture().await;
        let (_second_config, second_container, second_probe) =
            asyncapi_transport_free_fixture().await;
        first_probe.pause_runtime_completion();
        second_probe.pause_runtime_completion();

        let first = ConsumerBuilder::new()
            .container(Arc::clone(&first_container))
            .asyncapi(consumer_asyncapi_config())
            .start_managed()
            .await
            .expect("first composition root becomes ready");
        let second = ConsumerBuilder::new()
            .container(Arc::clone(&second_container))
            .asyncapi(consumer_asyncapi_config())
            .start_managed()
            .await
            .expect("second composition root becomes ready");

        let first_service = first_container
            .resolve::<crate::ConsumerAsyncApiService>(None)
            .await
            .expect("first snapshot");
        let second_service = second_container
            .resolve::<crate::ConsumerAsyncApiService>(None)
            .await
            .expect("second snapshot");
        assert!(!Arc::ptr_eq(&first_service, &second_service));
        assert_eq!(
            first_service
                .snapshot()
                .expect("first document")
                .document()
                .operation_count(),
            1
        );
        assert_eq!(
            second_service
                .snapshot()
                .expect("second document")
                .document()
                .operation_count(),
            1
        );
        assert_eq!(first_probe.registration_count(), 1);
        assert_eq!(second_probe.registration_count(), 1);

        first.shutdown().await.expect("first shutdown");
        second.shutdown().await.expect("second shutdown");
        first_container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("close first container");
        second_container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("close second container");
    }

    #[cfg(feature = "asyncapi")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn asyncapi_mid_registration_cancellation_never_publishes_a_service() {
        let _serial = ASYNCAPI_TEST_LOCK.lock().await;
        let (_config_file, container, probe) = asyncapi_transport_free_fixture().await;
        probe.pause_registration();
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(
            ConsumerBuilder::new()
                .container(Arc::clone(&container))
                .asyncapi(consumer_asyncapi_config())
                .run_with_cancellation(cancellation.clone()),
        );
        tokio::time::timeout(
            Duration::from_secs(1),
            probe.wait_for_registration_attempt(1),
        )
        .await
        .expect("registration reaches the paused provider");

        assert!(matches!(
            container
                .resolve::<crate::ConsumerAsyncApiService>(None)
                .await,
            Err(lily_error::injection::InjectionError::ServiceNotFound(_))
        ));
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("cancelled build remains bounded")
            .expect("Consumer task does not panic")
            .expect("startup cancellation cleanup succeeds");
        assert_eq!(probe.registration_count(), 0);
        assert_eq!(probe.cancelled_registration_count(), 1);
        assert!(matches!(
            container
                .resolve::<crate::ConsumerAsyncApiService>(None)
                .await,
            Err(lily_error::injection::InjectionError::ServiceNotFound(_))
        ));
        container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("caller container remains closeable after cancellation");
    }

    #[test]
    fn shutdown_reconciliation_requires_success_and_resource_reconciliation() {
        assert!(shutdown_report_reconciled(true, true));
        assert!(!shutdown_report_reconciled(true, false));
        assert!(!shutdown_report_reconciled(false, true));
        assert!(!shutdown_report_reconciled(false, false));
    }

    #[test]
    fn forced_cleanup_does_not_erase_a_primary_operation_failure() {
        assert!(shutdown_status_is_primary_failure(
            &FrameworkComponentStatus::Failed {
                error: "broker task failed".into(),
            }
        ));
        assert!(shutdown_status_is_primary_failure(
            &FrameworkComponentStatus::Panicked {
                message: "broker task panicked".into(),
            }
        ));
        assert!(!shutdown_status_is_primary_failure(
            &FrameworkComponentStatus::TimedOut
        ));
        assert!(!shutdown_status_is_primary_failure(
            &FrameworkComponentStatus::CancelledByForce
        ));
    }

    #[test]
    fn application_di_disposal_requires_proven_queue_task_join() {
        ensure_queue_drain_reconciled(true).expect("joined delivery tasks permit DI disposal");
        let error = ensure_queue_drain_reconciled(false)
            .expect_err("unreconciled delivery tasks must withhold DI disposal");
        assert!(error.to_string().contains("DI disposal was withheld"));
    }

    #[test]
    fn pipeline_initialization_timeout_is_bounded() {
        assert!(validate_pipeline_initialization_timeout(Duration::from_millis(1)).is_ok());
        assert!(validate_pipeline_initialization_timeout(Duration::from_secs(300)).is_ok());

        for timeout in [Duration::ZERO, Duration::from_secs(301)] {
            assert!(matches!(
                validate_pipeline_initialization_timeout(timeout),
                Err(ConsumerError::Configuration(
                    ConsumerConfigurationFailure::PipelineInitializationTimeoutInvalid
                ))
            ));
        }
    }

    #[tokio::test]
    async fn invalid_pipeline_initialization_timeout_fails_before_composition() {
        let error = Consumer::run_internal(
            None,
            None,
            None,
            TracingMode::Disabled,
            None,
            ConsumerPipelineConfiguration::new(Vec::new(), Vec::new(), Duration::ZERO),
            #[cfg(feature = "asyncapi")]
            ConsumerAsyncApiConfiguration::default(),
            ConsumerLifecycleTrigger::Signals,
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(error.error_code(), "CONSUMER_PIPELINE_TIMEOUT_INVALID");
    }

    #[test]
    fn lifecycle_results_preserve_every_failure_in_order() {
        assert!(Consumer::merge_lifecycle_results([Ok(()), Ok(())]).is_ok());

        let error = Consumer::merge_lifecycle_results([
            Err(ConsumerError::ManagedStartupIncomplete),
            Ok(()),
            Err(ConsumerError::ManagedRuntimeStateUnavailable),
        ])
        .unwrap_err();
        let ConsumerError::LifecycleFailures(failures) = error else {
            panic!("two failures must produce an ordered aggregate");
        };
        assert_eq!(
            failures.primary().error_code(),
            "CONSUMER_MANAGED_STARTUP_INCOMPLETE"
        );
        assert_eq!(
            failures.secondary_failures()[0].error_code(),
            "CONSUMER_MANAGED_STATE_UNAVAILABLE"
        );
    }

    #[test]
    fn lifecycle_merge_preserves_a_single_typed_broker_failure() {
        use lily_error::application::message_broker::{
            RabbitMQError, RabbitMqTopologyError, RabbitMqTopologyErrorKind,
            RabbitMqTopologyOperation, RabbitMqTopologyResourceKind,
        };

        let broker = lily_error::application::MessageBrokerError::RabbitMQError(
            RabbitMQError::Topology(RabbitMqTopologyError {
                kind: RabbitMqTopologyErrorKind::PermissionDenied,
                operation: RabbitMqTopologyOperation::Declare,
                resource_kind: RabbitMqTopologyResourceKind::Exchange,
                resource_name: "orders".into(),
            }),
        );

        let error = Consumer::merge_lifecycle_results([
            Err(ConsumerError::RuntimeSupervision { source: broker }),
            Ok(()),
        ])
        .expect_err("broker failure must remain terminal");
        let ConsumerError::RuntimeSupervision { source: broker } = error else {
            panic!("a successful cleanup must not erase the typed primary failure");
        };
        assert_eq!(broker.error_code(), "BROKER_TOPOLOGY_PERMISSION");
    }

    #[tokio::test]
    async fn run_internal_rejects_invalid_trace_cells_before_composition() {
        let duplicate = TraceCellConfig {
            id: "worker-id".into(),
            worker_type: "Worker".into(),
            worker_name: "worker".into(),
            kind: "consumer".into(),
        };
        let error = Consumer::run_internal(
            None,
            None,
            None,
            TracingMode::Disabled,
            Some(vec![duplicate.clone(), duplicate]),
            ConsumerPipelineConfiguration::new(
                Vec::new(),
                Vec::new(),
                DEFAULT_PIPELINE_INITIALIZATION_TIMEOUT,
            ),
            #[cfg(feature = "asyncapi")]
            ConsumerAsyncApiConfiguration::default(),
            ConsumerLifecycleTrigger::Signals,
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(error.error_code(), "CONSUMER_TRACE_CONFIG_INVALID");
    }

    #[tokio::test]
    async fn pre_cancelled_run_returns_before_validation_or_composition() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let duplicate = TraceCellConfig {
            id: "worker-id".into(),
            worker_type: "Worker".into(),
            worker_name: "worker".into(),
            kind: "consumer".into(),
        };

        ConsumerBuilder::new()
            .trace_cells(vec![duplicate.clone(), duplicate])
            .run_with_cancellation(cancellation)
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mid_registration_cancellation_runs_canonical_startup_rollback() {
        use lily_queue::__private::QueueServiceTestLifecycleCall as Call;

        let config_file = tempfile::NamedTempFile::new().expect("temporary Consumer config");
        std::fs::write(
            config_file.path(),
            r#"
[rabbitmq.consumer]
username = "guest"
password = "guest"
hostname = "localhost"
port = 5672
vhost = "/"
use_tls = false
pool_size = 1
connection_timeout_secs = 1
confirm_timeout_secs = 1
heartbeat_secs = 30
max_reconnect_attempts = 1
reconnect_backoff_millis = 50
persistence_enabled = true

[[rabbitmq.topology.queues]]
name = "capq01d.consumer-plan.alpha"
exchange_name = "capq06e"
routing_key = "capq01d.consumer-plan.alpha"
retention = { main_max_messages = 1000, main_max_bytes = 1048576, retry_bucket_max_messages = 100, retry_bucket_max_bytes = 262144, dead_letter_max_messages = 100, dead_letter_max_bytes = 262144 }
concurrency = 1
prefetch_count = 1
retry_attempts = 1
retry_backoff_millis = 250
max_retry_backoff_millis = 5000
durable = true
"#,
        )
        .expect("write Consumer config");

        let config = ConfigService::development(config_file.path());
        let (queue_service, probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
            ConfigService::development(config_file.path()),
        ));
        probe.pause_registration();
        let container = Arc::new(
            crate::test_application_container_builder()
                .seed_singleton(config)
                .seed_singleton(queue_service)
                .build()
                .await
                .expect("transport-free Consumer container"),
        );
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(
            ConsumerBuilder::new()
                .container(Arc::clone(&container))
                .run_with_cancellation(cancellation.clone()),
        );
        tokio::time::timeout(
            Duration::from_secs(1),
            probe.wait_for_registration_attempt(1),
        )
        .await
        .expect("registration must reach the paused provider");

        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("cancelled Consumer startup must terminate")
            .expect("Consumer task must not panic")
            .expect("successful cancellation cleanup");

        assert_eq!(probe.registration_count(), 0);
        assert_eq!(probe.cancelled_registration_count(), 1);
        assert_eq!(
            probe.lifecycle_snapshot().calls(),
            &[
                Call::StartAsync,
                Call::StopAdmissionAsync,
                Call::DrainAsync,
                Call::CloseAsync,
            ]
        );
        container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("caller-owned container remains independently closeable");
        assert_eq!(probe.lifecycle_snapshot().count(Call::StopAsync), 1);
    }

    #[tokio::test]
    async fn managed_startup_returns_pre_runtime_validation_failure() {
        let duplicate = TraceCellConfig {
            id: "worker-id".into(),
            worker_type: "Worker".into(),
            worker_name: "worker".into(),
            kind: "consumer".into(),
        };

        let error = ConsumerBuilder::new()
            .trace_cells(vec![duplicate.clone(), duplicate])
            .start_managed()
            .await
            .err()
            .expect("managed startup should return the validation failure");

        assert_eq!(error.error_code(), "CONSUMER_TRACE_CONFIG_INVALID");
    }

    #[tokio::test]
    async fn managed_shutdown_requests_manual_termination_and_replays_result() {
        let shutdown_state = Arc::new(ShutdownState::new());
        let mut receiver = shutdown_state.subscribe();
        let task = tokio::spawn(async move {
            let signal = receiver.recv().await.map_err(|error| {
                ConsumerError::signal(ConsumerSignalFailureStage::ManagedReceive, error)
            })?;
            assert_eq!(signal, ShutdownSignal::Manual);
            Err(test_runtime_failure())
        });
        let consumer = managed_test_consumer(Arc::clone(&shutdown_state), task);

        let first = consumer.shutdown().await.unwrap_err();
        let second = consumer.shutdown().await.unwrap_err();
        let waited = consumer.wait().await.unwrap_err();

        assert_eq!(first.error_code(), "CONSUMER_RABBITMQ_CONFIG_MISSING");
        assert_eq!(second.error_code(), first.error_code());
        assert_eq!(waited.error_code(), first.error_code());
        assert_eq!(
            shutdown_state.initial_signal(),
            Some(ShutdownSignal::Manual)
        );
    }

    #[tokio::test]
    async fn managed_snapshot_is_ready_during_runtime_and_terminal_after_shutdown() {
        let shutdown_state = Arc::new(ShutdownState::new());
        let mut receiver = shutdown_state.subscribe();
        let task = tokio::spawn(async move {
            receiver.recv().await.map(|_| ()).map_err(|error| {
                ConsumerError::signal(ConsumerSignalFailureStage::ManagedReceive, error)
            })
        });
        let (consumer, probe) =
            managed_test_consumer_with_probe(Arc::clone(&shutdown_state), task, 2, 3);
        probe.set_delivery_terminal_snapshot(lily_queue::DeliveryTerminalSnapshot {
            registered_consumers: 2,
            expected_consumers: 2,
            ready_consumers: 2,
            in_flight: 1,
            retry_confirmed: 4,
            dead_letter_confirmed: 1,
            runtime_state: lily_queue::ConsumerRuntimeState::Ready,
            ..lily_queue::DeliveryTerminalSnapshot::default()
        });

        let ready = consumer.snapshot();
        assert!(ready.live);
        assert!(ready.ready);
        assert_eq!(ready.registered_queues, 2);
        assert_eq!(ready.registered_handlers, 3);
        assert_eq!(ready.deliveries.in_flight, 1);
        assert_eq!(ready.deliveries.retry_confirmed, 4);
        assert_eq!(ready.deliveries.dead_letter_confirmed, 1);
        assert_eq!(ready.admission, crate::ConsumerAdmissionState::Open);
        let encoded = serde_json::to_value(&ready).expect("snapshot must be serializable");
        assert_eq!(encoded["lifecycle"], "ready");
        assert!(encoded.get("event_id").is_none());

        consumer.shutdown().await.expect("managed shutdown");
        let terminal = consumer.snapshot();
        assert!(!terminal.live);
        assert!(!terminal.ready);
        assert!(terminal.shutdown.requested);
        assert!(terminal.shutdown.completed);
        assert_eq!(terminal.lifecycle, crate::ConsumerLifecycleState::Stopped);
        assert_eq!(terminal.last_failure_code, None);
    }

    #[tokio::test]
    async fn managed_snapshot_tracks_recovery_and_readiness_restoration() {
        let shutdown_state = Arc::new(ShutdownState::new());
        let (done_tx, done_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            done_rx
                .await
                .map_err(|_| ConsumerError::ManagedRuntimeStateUnavailable)
        });
        let (consumer, probe) = managed_test_consumer_with_probe(shutdown_state, task, 2, 3);
        probe.set_delivery_terminal_snapshot(lily_queue::DeliveryTerminalSnapshot {
            registered_consumers: 2,
            expected_consumers: 2,
            ready_consumers: 1,
            last_operational_failure_code: Some("BROKER_IO"),
            // Exercise the counter-before-state publication race: readiness
            // must fail closed even while the coarse state is still Ready.
            runtime_state: lily_queue::ConsumerRuntimeState::Ready,
            ..lily_queue::DeliveryTerminalSnapshot::default()
        });

        let recovering = consumer.snapshot();
        assert!(recovering.live);
        assert!(!recovering.ready);
        assert_eq!(
            recovering.lifecycle,
            crate::ConsumerLifecycleState::Recovering
        );
        assert_eq!(
            recovering.admission,
            crate::ConsumerAdmissionState::PartiallyOpen
        );
        assert_eq!(recovering.last_failure_code, Some("BROKER_IO"));

        probe.set_delivery_terminal_snapshot(lily_queue::DeliveryTerminalSnapshot {
            registered_consumers: 2,
            expected_consumers: 2,
            ready_consumers: 2,
            last_operational_failure_code: Some("BROKER_IO"),
            runtime_state: lily_queue::ConsumerRuntimeState::Ready,
            ..lily_queue::DeliveryTerminalSnapshot::default()
        });
        let restored = consumer.snapshot();
        assert!(restored.live);
        assert!(restored.ready);
        assert_eq!(restored.admission, crate::ConsumerAdmissionState::Open);
        assert_eq!(restored.last_failure_code, Some("BROKER_IO"));

        done_tx.send(()).expect("release managed task");
        consumer.wait().await.expect("managed task completion");
    }

    #[tokio::test]
    async fn shutdown_request_overrides_stale_ready_state_but_not_provider_failure() {
        let shutdown_state = Arc::new(ShutdownState::new());
        let (done_tx, done_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            done_rx
                .await
                .map_err(|_| ConsumerError::ManagedRuntimeStateUnavailable)
        });
        let (consumer, probe) =
            managed_test_consumer_with_probe(Arc::clone(&shutdown_state), task, 1, 1);
        probe.set_delivery_terminal_snapshot(lily_queue::DeliveryTerminalSnapshot {
            registered_consumers: 1,
            expected_consumers: 1,
            ready_consumers: 1,
            runtime_state: lily_queue::ConsumerRuntimeState::Ready,
            ..lily_queue::DeliveryTerminalSnapshot::default()
        });
        shutdown_state
            .initiate_shutdown(ShutdownSignal::Manual)
            .expect("initiate shutdown");
        let draining = consumer.snapshot();
        assert_eq!(draining.lifecycle, crate::ConsumerLifecycleState::Draining);
        assert_eq!(draining.broker, crate::ConsumerBrokerState::Closing);
        assert_eq!(draining.topology, crate::ConsumerTopologyState::Draining);
        assert_eq!(draining.admission, crate::ConsumerAdmissionState::Draining);
        assert!(draining.live);
        assert!(!draining.ready);

        probe.set_delivery_terminal_snapshot(lily_queue::DeliveryTerminalSnapshot {
            registered_consumers: 1,
            expected_consumers: 1,
            runtime_state: lily_queue::ConsumerRuntimeState::Failed,
            last_operational_failure_code: Some("BROKER_CONSUMER_TASK_FAILED"),
            ..lily_queue::DeliveryTerminalSnapshot::default()
        });
        let failed = consumer.snapshot();
        assert_eq!(failed.lifecycle, crate::ConsumerLifecycleState::Failed);
        assert_eq!(failed.broker, crate::ConsumerBrokerState::Failed);
        assert_eq!(failed.admission, crate::ConsumerAdmissionState::Closed);
        assert!(!failed.live);

        done_tx.send(()).expect("release managed task");
        consumer.wait().await.expect("managed task completion");
    }

    #[tokio::test]
    async fn managed_supervisor_records_inner_panic_before_handle_observation() {
        let operational = Arc::new(ConsumerOperationalState::default());
        let runtime_task = tokio::spawn(async {
            panic!("sensitive panic payload must remain redacted");
            #[allow(unreachable_code)]
            Ok(())
        });
        let observer = tokio::spawn(observe_managed_runtime(
            runtime_task,
            Arc::clone(&operational),
        ));

        let (completed, failure_code) = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let terminal = operational.terminal();
                if terminal.0 {
                    break terminal;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("managed supervisor must publish terminal state");
        assert!(completed);
        assert_eq!(failure_code, Some("CONSUMER_RUNTIME_TASK_PANICKED"));
        let error = observer
            .await
            .expect("observer task")
            .expect_err("inner panic must be typed");
        assert_eq!(error.error_code(), "CONSUMER_RUNTIME_TASK_PANICKED");
        assert!(!error.to_string().contains("sensitive"));
        assert!(!format!("{error:?}").contains("sensitive"));
    }

    #[tokio::test]
    async fn concurrent_managed_shutdown_callers_replay_one_terminal_result() {
        let shutdown_state = Arc::new(ShutdownState::new());
        let mut receiver = shutdown_state.subscribe();
        let task = tokio::spawn(async move {
            receiver.recv().await.map_err(|error| {
                ConsumerError::signal(ConsumerSignalFailureStage::ManagedReceive, error)
            })?;
            Err(test_runtime_failure())
        });
        let consumer = managed_test_consumer(Arc::clone(&shutdown_state), task);
        let first = consumer.clone();
        let second = consumer.clone();

        let (first_result, second_result) = tokio::join!(first.shutdown(), second.shutdown());

        assert_eq!(
            first_result.unwrap_err().error_code(),
            second_result.unwrap_err().error_code()
        );
        assert_eq!(
            consumer.wait().await.unwrap_err().error_code(),
            "CONSUMER_RABBITMQ_CONFIG_MISSING"
        );
        assert_eq!(
            shutdown_state.initial_signal(),
            Some(ShutdownSignal::Manual)
        );
    }

    #[tokio::test]
    async fn managed_wait_does_not_initiate_shutdown_and_replays_natural_result() {
        let shutdown_state = Arc::new(ShutdownState::new());
        let task = tokio::spawn(async { Err(test_runtime_failure()) });
        let consumer = managed_test_consumer(Arc::clone(&shutdown_state), task);

        let first = consumer.wait().await.unwrap_err();
        let second = consumer.wait().await.unwrap_err();

        assert_eq!(second.error_code(), first.error_code());
        assert!(!shutdown_state.is_shutdown_initiated());
    }

    #[tokio::test]
    async fn dropping_managed_handle_requests_shutdown_without_aborting_runtime() {
        let shutdown_state = Arc::new(ShutdownState::new());
        let mut receiver = shutdown_state.subscribe();
        let (completed_tx, completed_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let result = receiver.recv().await.map(|_| ()).map_err(|error| {
                ConsumerError::signal(ConsumerSignalFailureStage::ManagedReceive, error)
            });
            let _ = completed_tx.send(());
            result
        });
        let consumer = managed_test_consumer(Arc::clone(&shutdown_state), task);

        drop(consumer);

        tokio::time::timeout(Duration::from_secs(1), completed_rx)
            .await
            .expect("managed runtime should observe the drop shutdown request")
            .expect("managed runtime should retain its detached task");
        assert_eq!(
            shutdown_state.initial_signal(),
            Some(ShutdownSignal::Manual)
        );
    }
}
