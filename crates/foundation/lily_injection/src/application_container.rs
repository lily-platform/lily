use crate::Extensions;
use crate::ServiceTrait;
use crate::private::InitializationGuard;
use crate::storage::{EagerSingletonResolution, ExtensionsBuildTransaction, ExtensionsBuilder};
use futures::FutureExt;
use futures::future::{BoxFuture, Shared, poll_fn};
use lily_error::injection::{
    InjectionError, ShutdownOutcome, ShutdownOutcomeStatus, ShutdownRemainingWork,
};
use lily_process::ProcessContext;
use std::any::{Any, TypeId};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::Instrument;

/// Default deadline used when an application does not provide a narrower
/// lifecycle-specific timeout.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Process environment variable that overrides the application-container
/// build rollback budget, in whole seconds.
///
/// This bootstrap setting is read directly by `lily_injection`; it does not
/// require `lily_config` or a running DI service. It affects only rollback of
/// a failed or cancelled container build. Normal container shutdown continues
/// to use the timeout supplied to [`ApplicationContainer::close_with_timeout`].
pub const BUILD_ROLLBACK_TIMEOUT_ENV: &str = "LILY_INJECTION_BUILD_ROLLBACK_TIMEOUT_SECS";

/// Inclusive upper bound accepted by [`BUILD_ROLLBACK_TIMEOUT_ENV`].
///
/// The bound keeps absolute deadline arithmetic in the cross-platform range
/// supported by `std::time::Instant` and matches Lily's lifecycle timeout
/// ceiling without depending on `lily_config`.
pub const MAX_BUILD_ROLLBACK_TIMEOUT_SECS: u64 = 100 * 365 * 24 * 60 * 60;

pub(crate) fn configured_build_rollback_timeout() -> Result<Duration, InjectionError> {
    parse_build_rollback_timeout(std::env::var_os(BUILD_ROLLBACK_TIMEOUT_ENV))
}

fn parse_build_rollback_timeout(
    value: Option<std::ffi::OsString>,
) -> Result<Duration, InjectionError> {
    let Some(value) = value else {
        return Ok(DEFAULT_SHUTDOWN_TIMEOUT);
    };
    let value = value
        .into_string()
        .map_err(|_| invalid_build_rollback_timeout())?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_build_rollback_timeout());
    }
    let seconds = value
        .parse::<u64>()
        .map_err(|_| invalid_build_rollback_timeout())?;
    if !(1..=MAX_BUILD_ROLLBACK_TIMEOUT_SECS).contains(&seconds) {
        return Err(invalid_build_rollback_timeout());
    }
    Ok(Duration::from_secs(seconds))
}

fn invalid_build_rollback_timeout() -> InjectionError {
    InjectionError::InitError(format!(
        "environment variable {BUILD_ROLLBACK_TIMEOUT_ENV} must be an integer between 1 and {MAX_BUILD_ROLLBACK_TIMEOUT_SECS} seconds"
    ))
}

/// Result of polling framework-owned work before one aggregate deadline.
///
/// The initial poll deliberately happens before a Tokio timer is constructed.
/// This preserves the existing ability to complete an entirely synchronous
/// rollback on a runtime without the time driver enabled. Pending asynchronous
/// work still requires the time driver; a missing driver is contained as typed
/// panic evidence instead of unwinding the lifecycle owner.
pub(crate) enum DeadlineAwait<T> {
    Completed(T),
    TimedOut,
    Panicked(String),
}

pub(crate) async fn await_before_deadline<F>(
    deadline: tokio::time::Instant,
    future: F,
) -> DeadlineAwait<F::Output>
where
    F: Future,
{
    if tokio::time::Instant::now() >= deadline {
        return DeadlineAwait::TimedOut;
    }

    tokio::pin!(future);
    let immediate = poll_fn(|context| {
        Poll::Ready(match future.as_mut().poll(context) {
            Poll::Ready(output) => Some(output),
            Poll::Pending => None,
        })
    })
    .await;
    if let Some(output) = immediate {
        return DeadlineAwait::Completed(output);
    }
    if tokio::time::Instant::now() >= deadline {
        return DeadlineAwait::TimedOut;
    }

    let waited = AssertUnwindSafe(async {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => None,
            output = &mut future => Some(output),
        }
    })
    .catch_unwind()
    .await;
    match waited {
        Ok(Some(output)) => DeadlineAwait::Completed(output),
        Ok(None) => DeadlineAwait::TimedOut,
        Err(payload) => DeadlineAwait::Panicked(panic_payload_message(payload)),
    }
}

/// Internal options for the application-wide DI drain and disposal sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContainerShutdownOptions {
    pub(crate) timeout: Duration,
    pub(crate) deadline: tokio::time::Instant,
}

impl ContainerShutdownOptions {
    fn from_timeout(timeout: Duration) -> Self {
        let started = tokio::time::Instant::now();
        Self {
            timeout,
            deadline: started.checked_add(timeout).unwrap_or(started),
        }
    }

    fn before(deadline: tokio::time::Instant) -> Self {
        Self {
            timeout: deadline.saturating_duration_since(tokio::time::Instant::now()),
            deadline,
        }
    }
}

impl Default for ContainerShutdownOptions {
    fn default() -> Self {
        Self::from_timeout(DEFAULT_SHUTDOWN_TIMEOUT)
    }
}

/// Successful terminal result of a container shutdown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerShutdownReport {
    /// Total wall-clock time spent in the shared shutdown operation.
    pub elapsed: Duration,
    /// Number of scopes drained before their cleanup began.
    pub scopes_drained: usize,
    /// Number of owned asynchronous cleanup tasks successfully joined.
    pub cleanup_tasks_joined: usize,
    /// Number of scopes forced into cleanup after the graceful drain deadline.
    pub forced_scopes: usize,
    /// Number of cleanup tasks aborted after the shutdown deadline.
    pub cancelled_cleanup_tasks: usize,
    /// Active scopes still recorded when shutdown produced this report.
    pub active_scopes_remaining: usize,
    /// Cleanup tasks still recorded when shutdown produced this report.
    pub cleanup_tasks_remaining: usize,
    /// Service resolutions still in flight when shutdown produced this report.
    pub active_resolutions_remaining: usize,
    /// Root-owned singleton/transient lifecycle entries still remaining.
    pub root_lifecycle_entries_remaining: usize,
    /// Typed per-resource shutdown outcomes.
    pub outcomes: Vec<ShutdownOutcome>,
}

type SharedShutdown = Shared<BoxFuture<'static, Result<ContainerShutdownReport, InjectionError>>>;

type SeedFactory = Box<
    dyn Fn(
            Arc<Extensions>,
        ) -> BoxFuture<'static, Result<Box<dyn Any + Send + Sync>, InjectionError>>
        + Send
        + Sync,
>;

pub(crate) struct SeededSingleton {
    pub(crate) type_id: TypeId,
    pub(crate) type_name: &'static str,
    pub(crate) factory: SeedFactory,
    pub(crate) dispose_fn: lily_injection_registry::ServiceDisposeFn,
}

/// Build-time overrides for link-time registered singleton services.
///
/// A seed replaces only the construction of an already registered singleton.
/// It does not add registrations, alter dependency routes, mutate a running
/// provider or publish process-global state. The seeded value still runs the
/// normal service initialization and disposal lifecycle.
#[must_use]
pub struct ApplicationContainerBuilder {
    seeded_singletons: Vec<SeededSingleton>,
}

impl ApplicationContainerBuilder {
    fn new() -> Self {
        Self {
            seeded_singletons: Vec::new(),
        }
    }

    /// Override one already registered singleton before any service is started.
    ///
    /// `T` must have a link-time registration whose lifetime is `Singleton`.
    /// Unknown, non-singleton and duplicate seeds fail during [`Self::build`]
    /// before any factory runs.
    pub fn seed_singleton<T>(mut self, service: T) -> Self
    where
        T: ServiceTrait + Send + Sync + 'static,
    {
        let type_name = std::any::type_name::<T>();
        let service = Arc::new(Mutex::new(Some(service)));
        self.seeded_singletons.push(SeededSingleton {
            type_id: TypeId::of::<T>(),
            type_name,
            factory: Box::new(move |extensions| {
                let service = Arc::clone(&service);
                async move {
                    let service = service
                        .lock()
                        .map_err(|_| {
                            InjectionError::InvalidRegistrationPlan(format!(
                                "singleton seed for '{type_name}' is unavailable"
                            ))
                        })
                        .and_then(|mut service| {
                            service.take().ok_or_else(|| {
                                InjectionError::InvalidRegistrationPlan(format!(
                                    "singleton seed for '{type_name}' was consumed more than once"
                                ))
                            })
                        })?;
                    initialize_seeded_singleton(service, extensions, type_name).await
                }
                .boxed()
            }),
            dispose_fn: dispose_seeded_singleton::<T>,
        });
        self
    }

    /// Validate the immutable graph and eagerly start its singleton services.
    pub async fn build(self) -> Result<ApplicationContainer, InjectionError> {
        let mut build = ApplicationContainerBuild::new(self);
        match (&mut build).await {
            ApplicationContainerBuildOutcome::Built(container) => Ok(container),
            ApplicationContainerBuildOutcome::Failed(error) => Err(error),
            ApplicationContainerBuildOutcome::Cancelled(rollback) => match rollback {
                Ok(()) => Err(InjectionError::General(
                    "application container build was cancelled internally".to_string(),
                )),
                Err(error) => Err(error),
            },
        }
    }
}

impl Default for ApplicationContainerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Terminal result produced by the framework-only application-container build
/// transaction.
#[doc(hidden)]
#[derive(Debug)]
pub enum ApplicationContainerBuildOutcome {
    /// The provider committed and ownership moved into a running container.
    Built(ApplicationContainer),
    /// Validation, initialization, or startup rollback failed.
    Failed(InjectionError),
    /// Cancellation won and detached rollback reached a terminal result.
    Cancelled(Result<(), InjectionError>),
}

enum ApplicationContainerBuildState {
    Unprepared(ExtensionsBuilder),
    Initializing {
        transaction: ExtensionsBuildTransaction,
        current: Option<EagerSingletonResolution>,
    },
    RollingBack {
        task: BuildRollbackTask,
        terminal: BuildRollbackTerminal,
    },
    Ready(Option<ApplicationContainerBuildOutcome>),
    Finished,
}

enum BuildRollbackTerminal {
    StartupFailure(InjectionError),
    Cancelled,
}

#[derive(Debug)]
struct BuildRollbackReport {
    errors: Vec<String>,
    outcomes: Vec<ShutdownOutcome>,
    remaining: ShutdownRemainingWork,
}

struct BuildRollbackTask {
    join: JoinHandle<BuildRollbackReport>,
    services: Arc<Extensions>,
}

impl BuildRollbackTask {
    fn poll(&mut self, context: &mut Context<'_>) -> Poll<BuildRollbackReport> {
        match Pin::new(&mut self.join).poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(report)) => Poll::Ready(report),
            Poll::Ready(Err(error)) => {
                let status = if error.is_panic() {
                    ShutdownOutcomeStatus::Panicked
                } else {
                    ShutdownOutcomeStatus::Cancelled
                };
                let detail = format!("container build rollback task failed: {error}");
                Poll::Ready(BuildRollbackReport {
                    errors: vec![detail.clone()],
                    outcomes: vec![ShutdownOutcome::with_detail(
                        "container-build-rollback-task",
                        status,
                        detail,
                    )],
                    remaining: remaining_work(&self.services),
                })
            }
        }
    }
}

/// Caller-polled application-container build transaction used by Lily
/// application adapters.
///
/// Normal eager initialization remains on the task that polls this future, so
/// task-local process, tracing, and user context retain their existing
/// semantics. Calling [`Self::cancel`] or dropping this value synchronously
/// closes provider admission and drops the active initializer before a
/// detached rollback task can inspect the lifecycle ledger. Resolution drain
/// and reverse root disposal then share one aggregate deadline measured from
/// that ownership handoff. [`BUILD_ROLLBACK_TIMEOUT_ENV`] selects the budget;
/// when it is absent, [`DEFAULT_SHUTDOWN_TIMEOUT`] is used.
#[doc(hidden)]
#[must_use = "the build transaction must be awaited, cancelled, or dropped"]
pub struct ApplicationContainerBuild {
    state: ApplicationContainerBuildState,
    runtime: Option<Handle>,
    rollback_context: Option<ProcessContext>,
    rollback_span: tracing::Span,
    rollback_timeout: Duration,
    rollback_started: OnceLock<tokio::time::Instant>,
    rollback_tail_percent: u32,
    rollback_services: OnceLock<std::sync::Weak<Extensions>>,
    rollback_joined: bool,
}

impl ApplicationContainerBuild {
    pub(crate) fn new(builder: ApplicationContainerBuilder) -> Self {
        let (state, rollback_timeout) = match configured_build_rollback_timeout() {
            Ok(timeout) => (
                ApplicationContainerBuildState::Unprepared(ExtensionsBuilder::new(
                    builder.seeded_singletons,
                )),
                timeout,
            ),
            Err(error) => (
                ApplicationContainerBuildState::Ready(Some(
                    ApplicationContainerBuildOutcome::Failed(error),
                )),
                DEFAULT_SHUTDOWN_TIMEOUT,
            ),
        };
        Self {
            state,
            runtime: None,
            rollback_context: None,
            rollback_span: tracing::Span::none(),
            rollback_timeout,
            rollback_started: OnceLock::new(),
            rollback_tail_percent: 0,
            rollback_services: OnceLock::new(),
            rollback_joined: false,
        }
    }

    /// Shorten the integration's rollback budget before rollback starts.
    /// No timer starts during successful application construction.
    #[doc(hidden)]
    pub fn limit_rollback_timeout(&mut self, timeout: Duration) {
        assert!(self.rollback_started.get().is_none());
        self.rollback_timeout = self.rollback_timeout.min(timeout);
    }

    /// Return the effective rollback budget, including integration caps.
    #[doc(hidden)]
    pub fn rollback_timeout(&self) -> Duration {
        self.rollback_timeout
    }

    /// Reserve part of the existing build rollback budget for an outer owner
    /// (for example telemetry). Does not start a timer during normal startup.
    #[doc(hidden)]
    pub fn reserve_rollback_tail(&mut self, percent: u32) {
        assert!(percent <= 100 && self.rollback_started.get().is_none());
        self.rollback_tail_percent = percent;
    }

    #[doc(hidden)]
    pub fn rollback_started_at(&self) -> Option<tokio::time::Instant> {
        self.rollback_started.get().copied()
    }

    /// Only a joined rollback with no active dependency users permits the
    /// outer owner to close telemetry. A returned error alone is insufficient.
    #[doc(hidden)]
    pub fn rollback_quiescent(&self) -> bool {
        self.rollback_services.get().is_none_or(|services| {
            self.rollback_joined
                && services.upgrade().is_none_or(|services| {
                    let remaining = remaining_work(&services);
                    remaining.active_scopes == 0
                        && remaining.cleanup_tasks == 0
                        && remaining.active_resolutions == 0
                })
        })
    }

    /// Begin cancellation synchronously.
    ///
    /// If a singleton initializer is pending, this call first rejects new
    /// provider work and drops that exact future. Its generated
    /// `InitializationGuard` can therefore publish the partial service before
    /// rollback starts. Continue polling the build to await the terminal
    /// rollback result; dropping it merely detaches that already-owned work.
    #[doc(hidden)]
    pub fn cancel(&mut self) {
        let state = std::mem::replace(&mut self.state, ApplicationContainerBuildState::Finished);
        self.state = match state {
            ApplicationContainerBuildState::Unprepared(_) => ApplicationContainerBuildState::Ready(
                Some(ApplicationContainerBuildOutcome::Cancelled(Ok(()))),
            ),
            ApplicationContainerBuildState::Initializing {
                transaction,
                current,
            } => {
                // Admission must close before the future is dropped. The drop
                // itself publishes any partially initialized service into the
                // root lifecycle ledger synchronously.
                transaction.mark_failed();
                drop(current);
                let task = self.start_rollback(transaction.provider());
                ApplicationContainerBuildState::RollingBack {
                    task,
                    terminal: BuildRollbackTerminal::Cancelled,
                }
            }
            state @ ApplicationContainerBuildState::RollingBack { .. }
            | state @ ApplicationContainerBuildState::Ready(_)
            | state @ ApplicationContainerBuildState::Finished => state,
        };
    }

    /// Poll this build through commit, failure, or a previously requested
    /// cancellation rollback.
    ///
    /// Dropping only this borrowed wait future does not cancel its build.
    /// Adapter cancellation branches must call [`Self::cancel`] explicitly;
    /// dropping the owning [`ApplicationContainerBuild`] also cancels it.
    #[doc(hidden)]
    pub async fn wait(&mut self) -> ApplicationContainerBuildOutcome {
        self.await
    }

    fn start_rollback(&self, services: Arc<Extensions>) -> BuildRollbackTask {
        // Begin the aggregate budget at the synchronous ownership handoff so
        // scheduler delay before the detached task starts also consumes it.
        let started = *self.rollback_started.get_or_init(tokio::time::Instant::now);
        let _ = self.rollback_services.set(Arc::downgrade(&services));
        let duration =
            self.rollback_timeout - (self.rollback_timeout / 100) * self.rollback_tail_percent;
        let deadline = started
            .checked_add(duration)
            .expect("the validated container build rollback timeout must fit Tokio Instant");
        let runtime = self
            .runtime
            .as_ref()
            .expect("a prepared container build must retain its Tokio runtime")
            .clone();
        let observed_services = Arc::clone(&services);
        let panic_observer = Arc::clone(&services);
        let process_context = self.rollback_context.clone();
        let span = self.rollback_span.clone();
        let rollback = async move {
            let work = AssertUnwindSafe(perform_build_rollback(services, deadline)).catch_unwind();
            let result = if let Some(process_context) = process_context {
                ProcessContext::scope(process_context, work).await
            } else {
                work.await
            };
            let report = match result {
                Ok(report) => report,
                Err(payload) => {
                    let detail = format!(
                        "container build rollback panicked: {}",
                        panic_payload_message(payload)
                    );
                    BuildRollbackReport {
                        errors: vec![detail.clone()],
                        outcomes: vec![ShutdownOutcome::with_detail(
                            "container-build-rollback-task",
                            ShutdownOutcomeStatus::Panicked,
                            detail,
                        )],
                        remaining: remaining_work(&panic_observer),
                    }
                }
            };
            if !report.errors.is_empty() {
                tracing::warn!(
                    errors = ?report.errors,
                    "application container build rollback completed with errors"
                );
            }
            report
        }
        .instrument(span);
        BuildRollbackTask {
            join: runtime.spawn(rollback),
            services: observed_services,
        }
    }

    fn finish_rollback(
        terminal: BuildRollbackTerminal,
        report: BuildRollbackReport,
    ) -> ApplicationContainerBuildOutcome {
        match terminal {
            BuildRollbackTerminal::StartupFailure(startup) => {
                if report.errors.is_empty() {
                    ApplicationContainerBuildOutcome::Failed(startup)
                } else {
                    ApplicationContainerBuildOutcome::Failed(
                        InjectionError::StartupRollbackFailed {
                            startup: Box::new(startup),
                            rollback_errors: report.errors,
                            rollback_outcomes: report.outcomes,
                            rollback_remaining: Some(report.remaining),
                        },
                    )
                }
            }
            BuildRollbackTerminal::Cancelled => {
                if report.errors.is_empty() {
                    ApplicationContainerBuildOutcome::Cancelled(Ok(()))
                } else {
                    ApplicationContainerBuildOutcome::Cancelled(Err(
                        InjectionError::ShutdownFailed {
                            errors: report.errors,
                            outcomes: report.outcomes,
                            remaining: Some(report.remaining),
                        },
                    ))
                }
            }
        }
    }
}

impl Future for ApplicationContainerBuild {
    type Output = ApplicationContainerBuildOutcome;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            let state =
                std::mem::replace(&mut this.state, ApplicationContainerBuildState::Finished);
            match state {
                ApplicationContainerBuildState::Unprepared(builder) => {
                    let runtime = match Handle::try_current() {
                        Ok(runtime) => runtime,
                        Err(_) => {
                            return Poll::Ready(ApplicationContainerBuildOutcome::Failed(
                                InjectionError::RuntimeUnavailable {
                                    operation: "ApplicationContainer::build".to_string(),
                                },
                            ));
                        }
                    };
                    this.runtime = Some(runtime);
                    this.rollback_context = ProcessContext::current();
                    this.rollback_span = tracing::Span::current();
                    match builder.prepare() {
                        Ok(transaction) => {
                            this.state = ApplicationContainerBuildState::Initializing {
                                transaction,
                                current: None,
                            };
                        }
                        Err(error) => {
                            return Poll::Ready(ApplicationContainerBuildOutcome::Failed(error));
                        }
                    }
                }
                ApplicationContainerBuildState::Initializing {
                    mut transaction,
                    mut current,
                } => {
                    if current.is_none() {
                        match transaction.next_resolution() {
                            Ok(Some(resolution)) => current = Some(resolution),
                            Ok(None) => {
                                let services = transaction.commit();
                                return Poll::Ready(ApplicationContainerBuildOutcome::Built(
                                    ApplicationContainer {
                                        services,
                                        shutdown: OnceLock::new(),
                                    },
                                ));
                            }
                            Err(source) => {
                                transaction.mark_failed();
                                let task = this.start_rollback(transaction.provider());
                                this.state = ApplicationContainerBuildState::RollingBack {
                                    task,
                                    terminal: BuildRollbackTerminal::StartupFailure(source),
                                };
                                continue;
                            }
                        }
                    }

                    let mut resolution = current
                        .take()
                        .expect("an eager singleton resolution must be present");
                    let resolution_poll = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        Pin::new(&mut resolution).poll(context)
                    }));
                    match resolution_poll {
                        Err(payload) => {
                            let startup = InjectionError::ServiceInitializationFailed {
                                service: resolution.type_name().to_string(),
                                source: Box::new(InjectionError::InitError(format!(
                                    "service factory panicked: {}",
                                    panic_payload_message(payload)
                                ))),
                            };
                            transaction.mark_failed();
                            drop(resolution);
                            let task = this.start_rollback(transaction.provider());
                            this.state = ApplicationContainerBuildState::RollingBack {
                                task,
                                terminal: BuildRollbackTerminal::StartupFailure(startup),
                            };
                        }
                        Ok(Poll::Pending) => {
                            this.state = ApplicationContainerBuildState::Initializing {
                                transaction,
                                current: Some(resolution),
                            };
                            return Poll::Pending;
                        }
                        Ok(Poll::Ready(Ok(()))) => {
                            if let Err(source) = transaction.record_initialized(&resolution) {
                                transaction.mark_failed();
                                drop(resolution);
                                let task = this.start_rollback(transaction.provider());
                                this.state = ApplicationContainerBuildState::RollingBack {
                                    task,
                                    terminal: BuildRollbackTerminal::StartupFailure(source),
                                };
                            } else {
                                drop(resolution);
                                this.state = ApplicationContainerBuildState::Initializing {
                                    transaction,
                                    current: None,
                                };
                            }
                        }
                        Ok(Poll::Ready(Err(source))) => {
                            let startup = transaction.startup_error(resolution.type_id(), source);
                            transaction.mark_failed();
                            drop(resolution);
                            let task = this.start_rollback(transaction.provider());
                            this.state = ApplicationContainerBuildState::RollingBack {
                                task,
                                terminal: BuildRollbackTerminal::StartupFailure(startup),
                            };
                        }
                    }
                }
                ApplicationContainerBuildState::RollingBack { mut task, terminal } => match task
                    .poll(context)
                {
                    Poll::Pending => {
                        this.state = ApplicationContainerBuildState::RollingBack { task, terminal };
                        return Poll::Pending;
                    }
                    Poll::Ready(report) => {
                        this.rollback_joined = true;
                        return Poll::Ready(Self::finish_rollback(terminal, report));
                    }
                },
                ApplicationContainerBuildState::Ready(mut outcome) => {
                    return Poll::Ready(
                        outcome
                            .take()
                            .expect("container build terminal outcome was already consumed"),
                    );
                }
                ApplicationContainerBuildState::Finished => {
                    panic!("ApplicationContainerBuild was polled after completion");
                }
            }
        }
    }
}

impl Drop for ApplicationContainerBuild {
    fn drop(&mut self) {
        self.cancel();
    }
}

async fn initialize_seeded_singleton<T>(
    service: T,
    extensions: Arc<Extensions>,
    type_name: &'static str,
) -> Result<Box<dyn Any + Send + Sync>, InjectionError>
where
    T: ServiceTrait + Send + Sync + 'static,
{
    let mut guard = InitializationGuard::new(
        service,
        Arc::clone(&extensions),
        type_name,
        dispose_seeded_singleton::<T>,
    );
    let initialization = AssertUnwindSafe(ServiceTrait::initialize(guard.service_mut()))
        .catch_unwind()
        .await;
    let initialization = match initialization {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(payload) => {
            let message = if let Some(message) = payload.downcast_ref::<&str>() {
                (*message).to_string()
            } else if let Some(message) = payload.downcast_ref::<String>() {
                message.clone()
            } else {
                "non-string panic payload".to_string()
            };
            Some(InjectionError::InitError(format!(
                "service initialization panicked: {message}"
            )))
        }
    };

    if let Some(initialization) = initialization {
        let cleanup = AssertUnwindSafe(ServiceTrait::dispose(guard.service()))
            .catch_unwind()
            .await;
        let cleanup = match cleanup {
            Ok(result) => result,
            Err(_) => Err(InjectionError::DisposeError(format!(
                "Service '{type_name}' cleanup panicked after initialization failure"
            ))),
        };
        let source = match cleanup {
            Ok(()) => initialization,
            Err(cleanup) => InjectionError::InitializationCleanupFailed {
                service: type_name.to_string(),
                initialization: Box::new(initialization),
                cleanup: Box::new(cleanup),
            },
        };
        let _ = guard.into_service();
        return Err(InjectionError::ServiceInitializationFailed {
            service: type_name.to_string(),
            source: Box::new(source),
        });
    }

    Ok(Box::new(guard.into_service()))
}

fn dispose_seeded_singleton<T>(
    instance: Arc<dyn Any + Send + Sync>,
) -> BoxFuture<'static, Result<(), InjectionError>>
where
    T: ServiceTrait + Send + Sync + 'static,
{
    async move {
        let instance = instance.downcast::<T>().map_err(|_| {
            InjectionError::DisposeError(format!(
                "Failed to downcast seeded service '{}' during disposal",
                std::any::type_name::<T>()
            ))
        })?;
        ServiceTrait::dispose(instance.as_ref())
            .await
            .map_err(|source| {
                InjectionError::DisposeError(format!(
                    "Service '{}' disposal failed: {source}",
                    std::any::type_name::<T>()
                ))
            })
    }
    .boxed()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootLifecycleDisposition {
    Quiescent,
    UnsafeInFlight,
}

struct RootLifecycleShutdown {
    errors: Vec<String>,
    outcomes: Vec<ShutdownOutcome>,
    disposition: RootLifecycleDisposition,
}

async fn shutdown_root_lifecycle_before(
    services: &Extensions,
    deadline: tokio::time::Instant,
) -> RootLifecycleShutdown {
    let mut errors = Vec::new();
    let mut outcomes = Vec::new();

    let resolution_drain = if services.in_flight_resolution_count() == 0 {
        DeadlineAwait::Completed(())
    } else {
        await_before_deadline(deadline, services.wait_for_no_in_flight_resolutions()).await
    };
    match resolution_drain {
        DeadlineAwait::Completed(()) => outcomes.push(ShutdownOutcome::with_detail(
            "service-resolution-drain",
            ShutdownOutcomeStatus::Completed,
            "all admitted DI resolutions completed or were cancelled",
        )),
        DeadlineAwait::TimedOut => {
            let remaining = services.in_flight_resolution_count();
            let detail = format!(
                "the aggregate deadline won before admitted DI resolution quiescence was proven; \
                 {remaining} resolution(s) were observed afterward"
            );
            errors.push(detail.clone());
            outcomes.push(ShutdownOutcome::with_detail(
                "service-resolution-drain",
                ShutdownOutcomeStatus::TimedOut,
                detail,
            ));
            outcomes.push(ShutdownOutcome::with_detail(
                "root-lifecycle",
                ShutdownOutcomeStatus::Cancelled,
                "root disposal was skipped to avoid racing an in-flight service factory",
            ));
            return RootLifecycleShutdown {
                errors,
                outcomes,
                disposition: RootLifecycleDisposition::UnsafeInFlight,
            };
        }
        DeadlineAwait::Panicked(message) => {
            let detail = format!(
                "service resolution deadline enforcement panicked before quiescence: {message}"
            );
            errors.push(detail.clone());
            outcomes.push(ShutdownOutcome::with_detail(
                "service-resolution-drain",
                ShutdownOutcomeStatus::Panicked,
                detail,
            ));
            outcomes.push(ShutdownOutcome::with_detail(
                "root-lifecycle",
                ShutdownOutcomeStatus::Cancelled,
                "root disposal was skipped because resolution ownership could not be proven quiescent",
            ));
            return RootLifecycleShutdown {
                errors,
                outcomes,
                disposition: RootLifecycleDisposition::UnsafeInFlight,
            };
        }
    }

    let root_outcomes = services.dispose_root_instances(Some(deadline)).await;
    for outcome in &root_outcomes {
        if outcome.status != ShutdownOutcomeStatus::Completed {
            errors.push(
                outcome.detail.clone().unwrap_or_else(|| {
                    format!("{} ended as {:?}", outcome.component, outcome.status)
                }),
            );
        }
    }
    outcomes.extend(root_outcomes);

    RootLifecycleShutdown {
        errors,
        outcomes,
        disposition: RootLifecycleDisposition::Quiescent,
    }
}

async fn perform_build_rollback(
    services: Arc<Extensions>,
    deadline: tokio::time::Instant,
) -> BuildRollbackReport {
    // The synchronous cancellation transition normally closes admission
    // before spawning this task. Reasserting FAILED also covers startup-error
    // handoff and keeps the worker independently safe.
    services.mark_failed();
    let shutdown = shutdown_root_lifecycle_before(&services, deadline).await;
    match shutdown.disposition {
        RootLifecycleDisposition::Quiescent => services.mark_closed(),
        RootLifecycleDisposition::UnsafeInFlight => services.mark_failed(),
    }

    BuildRollbackReport {
        errors: shutdown.errors,
        outcomes: shutdown.outcomes,
        remaining: remaining_work(&services),
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

fn remaining_work(services: &Extensions) -> ShutdownRemainingWork {
    let manager = services.scope_manager();
    ShutdownRemainingWork {
        active_scopes: manager
            .active_scope_count()
            .saturating_add(manager.closing_scope_count()),
        cleanup_tasks: manager.cleanup_task_count(),
        active_resolutions: services.in_flight_resolution_count(),
        root_lifecycle_entries: services.root_lifecycle_entry_count(),
    }
}

/// The composition root and explicit owner of one application's DI graph.
///
/// Building a container discovers registrations, validates the dependency
/// graph, creates an isolated service provider and eagerly starts singleton
/// services. The provider is not installed into process-global state.
pub struct ApplicationContainer {
    services: Arc<Extensions>,
    // Synchronously installed shared future ensures the first caller's
    // options win and every concurrent/subsequent close observes one result.
    // Its inner Tokio task continues even when the waiting caller is aborted.
    shutdown: OnceLock<SharedShutdown>,
}

impl ApplicationContainer {
    /// Start configuring build-time singleton seeds for one container.
    pub fn builder() -> ApplicationContainerBuilder {
        ApplicationContainerBuilder::new()
    }

    /// Build an application container from link-time service registrations.
    pub async fn build() -> Result<Self, InjectionError> {
        Self::builder().build().await
    }

    /// Resolve a concrete service or derive-declared interface.
    ///
    /// Passing `None` uses the task-local [`ProcessContext`] when present.
    /// Scoped services fail with [`InjectionError::ScopeRequired`] outside an
    /// active scope. Prefer constructor injection inside another service.
    pub async fn resolve<T: ?Sized + Send + Sync + 'static>(
        &self,
        context: Option<&ProcessContext>,
    ) -> Result<Arc<T>, InjectionError> {
        self.services.get_service(context).await
    }

    /// Framework ABI for resolving a service known only by runtime `TypeId`.
    ///
    /// Framework adapters such as queue consumers use this path for metadata-
    /// driven handlers while keeping resolution attached to the application
    /// that owns the container.
    #[doc(hidden)]
    pub async fn resolve_by_type_id(
        &self,
        type_id: TypeId,
        context: Option<&ProcessContext>,
    ) -> Result<Arc<dyn Any + Send + Sync>, InjectionError> {
        self.services.get_service_by_type_id(type_id, context).await
    }

    /// Obtain this container's read-only provider.
    ///
    /// Framework adapters pass this handle into controllers, middleware and
    /// extractors. Standalone application code can normally use
    /// [`Self::resolve`] instead.
    pub fn services(&self) -> Arc<Extensions> {
        Arc::clone(&self.services)
    }

    /// Framework diagnostic for the number of owned request/job scopes.
    #[doc(hidden)]
    pub fn active_scope_count(&self) -> usize {
        self.services.active_scope_count()
    }

    /// Create an owned request/job scope.
    ///
    /// Dropping the returned guard removes the scope immediately and schedules
    /// managed service disposal, including when its task panics or is aborted.
    pub fn create_scope(
        &self,
        context: ProcessContext,
    ) -> Result<ApplicationScope, InjectionError> {
        if !self.services.accepts_new_scopes() {
            return Err(InjectionError::ContainerClosing);
        }
        ApplicationScope::new(self.services.scope_manager_handle(), context)
    }

    /// Run a future with task-local context and deterministic scope cleanup.
    pub async fn run_scoped<F, T>(
        &self,
        context: ProcessContext,
        future: F,
    ) -> Result<T, InjectionError>
    where
        F: Future<Output = T>,
    {
        let mut scope = self.create_scope(context)?;
        let output = scope.run(future).await?;
        scope.close().await?;
        Ok(output)
    }

    /// Gracefully stop admission, drain active scopes and dispose all
    /// container-owned resources using the default 30-second deadline.
    pub async fn close(&self) -> Result<ContainerShutdownReport, InjectionError> {
        self.close_with_options(ContainerShutdownOptions::default())
            .await
    }

    /// Gracefully close with an explicit deadline.
    ///
    /// Composition roots should read
    /// `lifecycle.shutdown_timeout_secs` from `lily_config` and pass it here.
    /// If multiple callers race, the first call installs the shutdown task and
    /// its timeout; all callers await the same idempotent result.
    pub async fn close_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ContainerShutdownReport, InjectionError> {
        self.close_with_options(ContainerShutdownOptions::from_timeout(timeout))
            .await
    }

    /// Close using an absolute lifecycle deadline owned by a composition root.
    ///
    /// This is a framework integration seam. Application code should normally
    /// use [`Self::close_with_timeout`]. An absolute deadline prevents the
    /// container's retained shutdown task from starting a fresh relative
    /// budget after earlier framework shutdown phases have already run.
    #[doc(hidden)]
    pub async fn close_before(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<ContainerShutdownReport, InjectionError> {
        self.close_with_options(ContainerShutdownOptions::before(deadline))
            .await
    }

    async fn close_with_options(
        &self,
        options: ContainerShutdownOptions,
    ) -> Result<ContainerShutdownReport, InjectionError> {
        if let Some(shutdown) = self.shutdown.get() {
            return shutdown.clone().await;
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            InjectionError::RuntimeUnavailable {
                operation: "ApplicationContainer::close".to_string(),
            }
        })?;
        let shutdown = self
            .shutdown
            .get_or_init(move || {
                let services = Arc::clone(&self.services);
                let observed_services = Arc::clone(&services);
                async move {
                    runtime
                        .spawn(Self::perform_shutdown(services, options))
                        .await
                        .map_err(|error| InjectionError::ShutdownFailed {
                            errors: vec![format!("container shutdown task failed: {error}")],
                            outcomes: vec![ShutdownOutcome::with_detail(
                                "container-shutdown-task",
                                ShutdownOutcomeStatus::Panicked,
                                error.to_string(),
                            )],
                            remaining: Some(Self::remaining_work(&observed_services)),
                        })?
                }
                .boxed()
                .shared()
            })
            .clone();
        shutdown.await
    }

    pub(crate) fn shutdown_quiescent(&self) -> bool {
        let joined = self
            .shutdown
            .get()
            .is_some_and(|receipt| receipt.peek().is_some());
        let remaining = remaining_work(&self.services);
        joined
            && remaining.active_scopes == 0
            && remaining.cleanup_tasks == 0
            && remaining.active_resolutions == 0
    }

    pub(crate) fn shutdown_started(&self) -> bool {
        self.shutdown.get().is_some()
    }

    pub(crate) fn shutdown_succeeded(&self) -> Option<bool> {
        self.shutdown.get()?.peek().map(Result::is_ok)
    }

    async fn perform_shutdown(
        services: Arc<Extensions>,
        options: ContainerShutdownOptions,
    ) -> Result<ContainerShutdownReport, InjectionError> {
        let started = tokio::time::Instant::now();
        let deadline = options.deadline;
        services.begin_shutdown()?;

        let manager = services.scope_manager_handle();
        // Uses the same write-lock as create_scope, closing the admission race.
        manager.stop_accepting_scopes();

        let active_scopes_at_start = manager.active_scope_count();
        let cleanup_tasks_at_start = manager.cleanup_task_count();
        let completed_cleanup_tasks_at_start = manager.completed_cleanup_task_count();
        let mut forced_scopes = 0;
        let mut cancelled_cleanup_tasks = 0;
        let mut errors = Vec::new();
        let mut outcomes = Vec::new();

        if tokio::time::timeout_at(deadline, manager.wait_for_no_active_scopes())
            .await
            .is_err()
        {
            let remaining = manager.active_scope_count();
            let timeout = InjectionError::ShutdownTimedOut {
                timeout_ms: options.timeout.as_millis().min(u128::from(u64::MAX)) as u64,
                active_scopes: remaining,
            }
            .to_string();
            errors.push(timeout.clone());
            outcomes.push(ShutdownOutcome::with_detail(
                "active-scope-drain",
                ShutdownOutcomeStatus::TimedOut,
                timeout,
            ));
            forced_scopes = manager.begin_cleanup_all_scopes();
        } else {
            outcomes.push(ShutdownOutcome::with_detail(
                "active-scope-drain",
                ShutdownOutcomeStatus::Completed,
                format!("drained {active_scopes_at_start} active scope(s)"),
            ));
        }

        match tokio::time::timeout_at(deadline, manager.drain_cleanup_tasks()).await {
            Ok(Ok(())) => outcomes.push(ShutdownOutcome::with_detail(
                "scope-cleanup-tasks",
                ShutdownOutcomeStatus::Completed,
                format!("joined {cleanup_tasks_at_start} tracked cleanup task(s)"),
            )),
            Ok(Err(error)) => {
                let status = if matches!(&error, InjectionError::DisposalPanicked { .. }) {
                    ShutdownOutcomeStatus::Panicked
                } else {
                    ShutdownOutcomeStatus::Failed
                };
                let detail = error.to_string();
                errors.push(detail.clone());
                outcomes.push(ShutdownOutcome::with_detail(
                    "scope-cleanup-tasks",
                    status,
                    detail,
                ));
            }
            Err(_) => {
                cancelled_cleanup_tasks = manager.abort_cleanup_tasks();
                let retained_resolution_drains = manager.retained_resolution_drain_count();
                let detail = format!(
                    "scope cleanup deadline expired; claimed {cancelled_cleanup_tasks} task(s), \
                     retained {retained_resolution_drains} until admitted resolutions terminate"
                );
                errors.push(detail.clone());
                outcomes.push(ShutdownOutcome::with_detail(
                    "scope-cleanup-tasks",
                    if cancelled_cleanup_tasks == 0 || retained_resolution_drains > 0 {
                        ShutdownOutcomeStatus::TimedOut
                    } else {
                        ShutdownOutcomeStatus::Cancelled
                    },
                    detail,
                ));
                if retained_resolution_drains > 0 {
                    outcomes.extend(manager.take_cleanup_outcomes());
                    let remaining_resolutions = services.in_flight_resolution_count();
                    let detail = format!(
                        "{remaining_resolutions} admitted DI resolution(s) still own \
                         {retained_resolution_drains} closing scope cleanup task(s)"
                    );
                    errors.push(detail.clone());
                    outcomes.push(ShutdownOutcome::with_detail(
                        "service-resolution-drain",
                        ShutdownOutcomeStatus::TimedOut,
                        detail,
                    ));
                    outcomes.push(ShutdownOutcome::with_detail(
                        "root-lifecycle",
                        ShutdownOutcomeStatus::Cancelled,
                        "root disposal was skipped to avoid racing retained scoped factories",
                    ));
                    services.mark_failed();
                    return Err(InjectionError::ShutdownFailed {
                        errors,
                        outcomes,
                        remaining: Some(Self::remaining_work(&services)),
                    });
                }
                // Give abort-safe tasks one scheduler turn to drop their
                // futures. Never perform an unbounded join after the hard
                // deadline: if any task still owns cleanup, report retained
                // work and skip root disposal instead of racing it.
                tokio::task::yield_now().await;
                let cleanup_tasks_remaining = manager.cleanup_task_count();
                let closing_scopes_remaining = manager.closing_scope_count();
                if cleanup_tasks_remaining > 0 || closing_scopes_remaining > 0 {
                    outcomes.extend(manager.take_cleanup_outcomes());
                    let detail = format!(
                        "{cleanup_tasks_remaining} aborted scope cleanup task(s) and \
                         {closing_scopes_remaining} closing scope reservation(s) remained at the hard deadline"
                    );
                    errors.push(detail.clone());
                    outcomes.push(ShutdownOutcome::with_detail(
                        "scope-cleanup-join",
                        ShutdownOutcomeStatus::TimedOut,
                        detail,
                    ));
                    outcomes.push(ShutdownOutcome::with_detail(
                        "root-lifecycle",
                        ShutdownOutcomeStatus::Cancelled,
                        "root disposal was skipped to avoid racing retained scope cleanup",
                    ));
                    services.mark_failed();
                    return Err(InjectionError::ShutdownFailed {
                        errors,
                        outcomes,
                        remaining: Some(Self::remaining_work(&services)),
                    });
                }
                // The tracker is closed and empty here, so this only consumes
                // its aggregate failures and cannot await more task work.
                if let Err(error) = manager.drain_cleanup_tasks().await {
                    errors.push(error.to_string());
                }
            }
        }
        outcomes.extend(manager.take_cleanup_outcomes());

        let closing_scopes_remaining = manager.closing_scope_count();
        if closing_scopes_remaining > 0 {
            let detail = format!(
                "{closing_scopes_remaining} closing scope reservation(s) retained after cleanup task reconciliation"
            );
            errors.push(detail.clone());
            outcomes.push(ShutdownOutcome::with_detail(
                "scope-cleanup-ownership",
                ShutdownOutcomeStatus::Failed,
                detail,
            ));
            outcomes.push(ShutdownOutcome::with_detail(
                "root-lifecycle",
                ShutdownOutcomeStatus::Cancelled,
                "root disposal was skipped because scope cleanup ownership was not reconciled",
            ));
            services.mark_failed();
            return Err(InjectionError::ShutdownFailed {
                errors,
                outcomes,
                remaining: Some(Self::remaining_work(&services)),
            });
        }

        let root_shutdown = shutdown_root_lifecycle_before(&services, deadline).await;
        errors.extend(root_shutdown.errors);
        outcomes.extend(root_shutdown.outcomes);
        match root_shutdown.disposition {
            RootLifecycleDisposition::Quiescent => services.mark_closed(),
            RootLifecycleDisposition::UnsafeInFlight => {
                services.mark_failed();
                return Err(InjectionError::ShutdownFailed {
                    errors,
                    outcomes,
                    remaining: Some(Self::remaining_work(&services)),
                });
            }
        }

        if errors.is_empty() {
            Ok(ContainerShutdownReport {
                elapsed: started.elapsed(),
                scopes_drained: active_scopes_at_start,
                cleanup_tasks_joined: manager
                    .completed_cleanup_task_count()
                    .saturating_sub(completed_cleanup_tasks_at_start)
                    + cancelled_cleanup_tasks,
                forced_scopes,
                cancelled_cleanup_tasks,
                active_scopes_remaining: manager.active_scope_count(),
                cleanup_tasks_remaining: manager.cleanup_task_count(),
                active_resolutions_remaining: services.in_flight_resolution_count(),
                root_lifecycle_entries_remaining: services.root_lifecycle_entry_count(),
                outcomes,
            })
        } else {
            let remaining = Some(Self::remaining_work(&services));
            Err(InjectionError::ShutdownFailed {
                errors,
                outcomes,
                remaining,
            })
        }
    }

    fn remaining_work(services: &Extensions) -> ShutdownRemainingWork {
        remaining_work(services)
    }
}

impl std::fmt::Debug for ApplicationContainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApplicationContainer")
            .field("services", &self.services)
            .field("active_scopes", &self.active_scope_count())
            .finish()
    }
}

impl Drop for ApplicationContainer {
    fn drop(&mut self) {
        if self.shutdown.get().is_none() {
            tracing::warn!(
                active_scopes = self.active_scope_count(),
                "ApplicationContainer dropped without close().await; async DI cleanup was not requested"
            );
        }
    }
}

/// RAII owner for one application request/job scope.
#[must_use = "dropping the scope immediately begins cleanup"]
pub struct ApplicationScope {
    manager: Arc<crate::storage::ScopeManager>,
    observation: crate::storage::ScopeCleanupObservation,
    context: ProcessContext,
    closed: bool,
}

impl ApplicationScope {
    pub(crate) fn new(
        manager: Arc<crate::storage::ScopeManager>,
        context: ProcessContext,
    ) -> Result<Self, InjectionError> {
        let scope_id = context.process_id_string();
        let scope = manager.create_application_scope(context.clone())?;
        Ok(Self {
            manager,
            observation: crate::storage::ScopeCleanupObservation::new(scope_id, scope),
            context,
            closed: false,
        })
    }

    /// Borrow the request/job context that identifies this scope.
    pub fn context(&self) -> &ProcessContext {
        &self.context
    }

    pub(crate) fn cleanup_observation(&self) -> crate::__private::ScopeCleanupObservation {
        crate::__private::ScopeCleanupObservation {
            manager: self.manager.clone(),
            observation: self.observation.clone(),
        }
    }

    /// Run work inside this scope's Tokio task-local context.
    pub async fn run<F: Future>(&self, future: F) -> Result<F::Output, InjectionError> {
        if self.closed {
            return Err(InjectionError::ScopeClosed {
                scope_id: self.context.process_id_string(),
            });
        }
        Ok(ProcessContext::scope(self.context.clone(), future).await)
    }

    /// Close the scope and await all managed disposal hooks.
    ///
    /// Cleanup itself runs in an owned Tokio task. Cancelling the caller while
    /// this method waits therefore does not cancel disposal.
    pub async fn close(&mut self) -> Result<(), InjectionError> {
        let Some(cleanup) = self.begin_cleanup() else {
            return Ok(());
        };
        cleanup.wait().await
    }

    /// Close the scope before an adapter-owned absolute deadline.
    ///
    /// Framework request/job adapters use this boundary when normal scope
    /// disposal is part of their aggregate execution budget. If disposal
    /// exceeds the deadline, its container-owned task is stopped and joined
    /// before this method returns; no disposer can race the adapter's terminal
    /// response or broker settlement.
    #[doc(hidden)]
    pub async fn close_before(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), InjectionError> {
        self.close_before_observed(deadline).await.map(|_| ())
    }

    pub(crate) async fn close_before_observed(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<bool, InjectionError> {
        let Some(cleanup) = self.begin_cleanup() else {
            return Ok(false);
        };
        cleanup.wait_until(deadline).await.map(|()| true)
    }

    pub(crate) fn begin_cleanup(&mut self) -> Option<crate::storage::ScopeCleanupTicket> {
        if self.closed {
            return None;
        }
        self.closed = true;
        self.manager
            .begin_observed_scope_cleanup(&self.observation, Some(self.context.clone()))
    }
}

impl Drop for ApplicationScope {
    fn drop(&mut self) {
        // The manager owns the cleanup task. Dropping this observation ticket
        // cannot cancel disposal, including when the request task is aborted.
        let _ = self.begin_cleanup();
    }
}

impl std::fmt::Debug for ApplicationScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApplicationScope")
            .field("process_id", &self.context.process_id)
            .field("closed", &self.closed)
            .finish()
    }
}

#[cfg(test)]
mod scope_observation_tests {
    use super::*;

    #[tokio::test]
    async fn scope_handle_observation_and_close_stay_bound_to_the_created_generation() {
        let container = ApplicationContainer::build().await.unwrap();
        let context = ProcessContext::with_process_id(8_030_001);
        let mut first = container.create_scope(context.clone()).unwrap();
        let original = crate::__private::application_scope_cleanup_observation(&first);
        // Simulate cleanup initiated by another DI owner while this handle is
        // retained. Capturing its observation later must still name generation 1.
        first
            .manager
            .begin_scope_cleanup(&context.process_id_string(), None)
            .unwrap()
            .wait()
            .await
            .unwrap();
        crate::__private::wait_for_observed_scope_cleanup(original).await;
        let mut second = container.create_scope(context).unwrap();
        let late = crate::__private::application_scope_cleanup_observation(&first);
        crate::__private::wait_for_observed_scope_cleanup(late).await;
        assert!(
            !crate::__private::close_application_scope_before(
                &mut first,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap(),
            "another owner's disposal result must not be invented"
        );
        first.close().await.unwrap();
        assert_eq!(
            container.active_scope_count(),
            1,
            "stale close cannot dispose a successor"
        );
        second.close().await.unwrap();
        container.close().await.unwrap();
    }
}
