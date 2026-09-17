use super::owned_tasks::OwnedTask;
use futures_util::FutureExt;
use lily_error::application::consumer::{
    ConsumerError, ConsumerShutdownFailureEvidence, ConsumerShutdownFailureKind,
};
use lily_injection::{
    __private::{
        begin_application_container_build, ApplicationContainerBuild,
        ApplicationContainerBuildOutcome,
    },
    ApplicationContainer, ApplicationContainerBuilder, ProcessContext,
};
use lily_queue::__private::{register_queue_lifecycle, QueueRuntimeHandle, QueueShutdownDeadlines};
use lily_shutdown::{FrameworkShutdownCoordinator, ShutdownSignal, ShutdownState};
use lily_trace::TracingRuntimeOwner;
use std::{
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{runtime::Handle, sync::Notify};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::{
    runtime_owner::ConsumerRuntimeOwner, shutdown_failure, shutdown_report_has_primary_failure,
    shutdown_report_reconciled, shutdown_report_requires_aggregate, Consumer,
};

/// Result of polling one caller-owned startup activity against the optional
/// programmatic cancellation signal.
pub(super) enum StartupActivityOutcome<T> {
    Completed(T),
    Cancelled,
}

/// Poll startup work in the caller task so task-local DI and tracing context
/// are preserved.
///
/// The losing activity is dropped before this function returns. Callers must
/// only begin rollback after matching the returned outcome; starting rollback
/// inside the `select!` cancellation branch could make cleanup wait on the
/// still-live activity which it is trying to cancel.
pub(super) async fn poll_startup_activity<F>(
    future: F,
    cancellation: Option<&CancellationToken>,
) -> StartupActivityOutcome<F::Output>
where
    F: Future,
{
    let Some(cancellation) = cancellation else {
        return StartupActivityOutcome::Completed(future.await);
    };

    tokio::pin!(future);
    tokio::select! {
        biased;
        () = cancellation.cancelled() => StartupActivityOutcome::Cancelled,
        output = &mut future => StartupActivityOutcome::Completed(output),
    }
}

/// Owners transferred synchronously from startup into the ordinary Consumer
/// runtime path.
pub(super) struct ConsumerBuildCommit {
    runtime: Handle,
    owned_container: Option<Arc<ApplicationContainer>>,
    tracing_owner: Option<TracingRuntimeOwner>,
    queue_runtime: Option<QueueRuntimeHandle>,
    shutdown_timeout: Duration,
    process_context: Option<ProcessContext>,
    rollback_span: tracing::Span,
}

impl ConsumerBuildCommit {
    /// Transfer every post-readiness cleanup owner without yielding.
    pub(super) fn into_runtime_owner(
        mut self,
        shutdown_state: Arc<ShutdownState>,
    ) -> ConsumerRuntimeOwner {
        ConsumerRuntimeOwner::new(
            self.runtime.clone(),
            self.queue_runtime
                .take()
                .expect("committed Consumer queue runtime must be present"),
            self.owned_container.take(),
            self.tracing_owner.take(),
            shutdown_state,
            self.shutdown_timeout,
            self.process_context.take(),
            self.rollback_span.clone(),
        )
    }
}

impl Drop for ConsumerBuildCommit {
    fn drop(&mut self) {
        if self.owned_container.is_none()
            && self.tracing_owner.is_none()
            && self.queue_runtime.is_none()
        {
            return;
        }
        let mut cleanup = ConsumerBuildTransaction {
            runtime: self.runtime.clone(),
            di_build: None,
            di_build_finished: false,
            owned_container: self.owned_container.take(),
            tracing_owner: self.tracing_owner.take(),
            queue_runtime: self.queue_runtime.take(),
            active_activity: None,
            shutdown_timeout: self.shutdown_timeout,
            process_context: self.process_context.take(),
            rollback_span: self.rollback_span.clone(),
        };
        let _ = cleanup.start_cleanup();
    }
}

/// Cancellation-safe owner for Consumer composition resources.
///
/// All user/configuration/pipeline/registration futures remain caller-polled.
/// If that caller disappears, `Drop` first takes ownership of every Lily
/// resource and then registers one bounded rollback task on the runtime which
/// originally polled the build.
pub(super) struct ConsumerBuildTransaction {
    runtime: Handle,
    di_build: Option<ApplicationContainerBuild>,
    di_build_finished: bool,
    owned_container: Option<Arc<ApplicationContainer>>,
    tracing_owner: Option<TracingRuntimeOwner>,
    queue_runtime: Option<QueueRuntimeHandle>,
    active_activity: Option<Arc<BuildActivityState>>,
    shutdown_timeout: Duration,
    process_context: Option<ProcessContext>,
    rollback_span: tracing::Span,
}

impl ConsumerBuildTransaction {
    pub(super) fn new(
        runtime: Handle,
        tracing_owner: Option<TracingRuntimeOwner>,
        shutdown_timeout: Duration,
    ) -> Self {
        Self {
            runtime,
            di_build: None,
            di_build_finished: false,
            owned_container: None,
            tracing_owner,
            queue_runtime: None,
            active_activity: None,
            shutdown_timeout,
            process_context: ProcessContext::current(),
            rollback_span: tracing::Span::current(),
        }
    }

    /// Build and immediately adopt a framework-owned DI container.
    pub(super) async fn build_owned_container(
        &mut self,
        builder: ApplicationContainerBuilder,
    ) -> Result<Arc<ApplicationContainer>, ConsumerError> {
        debug_assert!(self.di_build.is_none());
        debug_assert!(self.owned_container.is_none());
        let mut build = begin_application_container_build(builder);
        build.limit_rollback_timeout(self.shutdown_timeout);
        build.reserve_rollback_tail(15);
        self.di_build = Some(build);

        let outcome = self
            .di_build
            .as_mut()
            .expect("Consumer DI build transaction must be present")
            .wait()
            .await;
        match outcome {
            ApplicationContainerBuildOutcome::Built(container) => {
                let container = Arc::new(container);
                self.owned_container = Some(Arc::clone(&container));
                self.di_build.take();
                Ok(container)
            }
            ApplicationContainerBuildOutcome::Failed(error) => {
                self.di_build_finished = true;
                Err(ConsumerError::dependency_initialization(error))
            }
            ApplicationContainerBuildOutcome::Cancelled(result) => {
                self.di_build_finished = true;
                match result {
                    Ok(()) => Err(ConsumerError::ManagedStartupIncomplete),
                    Err(error) => Err(ConsumerError::dependency_initialization(error)),
                }
            }
        }
    }

    pub(super) fn set_shutdown_timeout(&mut self, timeout: Duration) {
        self.shutdown_timeout = timeout;
    }

    /// Retain the queue runtime before the first physical registration begins.
    pub(super) fn retain_queue_runtime(&mut self, runtime: QueueRuntimeHandle) {
        debug_assert!(self.queue_runtime.is_none());
        self.queue_runtime = Some(runtime);
    }

    /// Register one caller-polled composition future with the rollback owner.
    pub(super) fn track<F>(&mut self, future: F) -> TrackedConsumerBuildActivity<F>
    where
        F: Future,
    {
        debug_assert!(
            self.active_activity
                .as_ref()
                .is_none_or(|activity| activity.is_complete()),
            "Consumer build activities must not overlap"
        );
        let state = Arc::new(BuildActivityState::default());
        self.active_activity = Some(Arc::clone(&state));
        TrackedConsumerBuildActivity {
            future: Some(Box::pin(future)),
            state,
        }
    }

    /// Transfer startup owners into the existing runtime path without an
    /// intervening await or a second cleanup authority.
    pub(super) fn commit(&mut self) -> ConsumerBuildCommit {
        debug_assert!(self.di_build.is_none());
        debug_assert!(self
            .active_activity
            .as_ref()
            .is_none_or(|activity| activity.is_complete()));
        self.active_activity.take();
        ConsumerBuildCommit {
            runtime: self.runtime.clone(),
            owned_container: self.owned_container.take(),
            tracing_owner: self.tracing_owner.take(),
            queue_runtime: Some(
                self.queue_runtime
                    .take()
                    .expect("Consumer startup must adopt its queue runtime before commit"),
            ),
            shutdown_timeout: self.shutdown_timeout,
            process_context: self.process_context.take(),
            rollback_span: self.rollback_span.clone(),
        }
    }

    /// Await explicit rollback while preserving the original startup error as
    /// the primary failure. With `None`, successful cancellation returns `Ok`.
    pub(super) async fn rollback(
        &mut self,
        original: Option<ConsumerError>,
    ) -> Result<(), ConsumerError> {
        let mut failures = match self.start_cleanup() {
            Some(task) => match task.await {
                Ok(failures) => failures,
                Err(error) => vec![rollback_task_failure(&error)],
            },
            None => Vec::new(),
        };

        if let Some(original) = original {
            return Err(ConsumerError::aggregate(original, failures));
        }
        Consumer::merge_lifecycle_vec(std::mem::take(&mut failures).into_iter().map(Err).collect())
    }

    fn start_cleanup(&mut self) -> Option<OwnedTask<Vec<ConsumerError>>> {
        let cleanup_started = self
            .di_build
            .as_ref()
            .and_then(|build| build.rollback_started_at())
            .unwrap_or_else(tokio::time::Instant::now);
        let mut di_build = self.di_build.take();
        if let Some(build) = di_build.as_mut() {
            // Close provider admission and synchronously drop the active eager
            // initializer before ownership is transferred from the caller.
            build.cancel();
        }

        let owned_container = self.owned_container.take();
        let tracing_owner = self.tracing_owner.take();
        let queue_runtime = self.queue_runtime.take();
        let active_activity = self.active_activity.take();
        if di_build.is_none()
            && owned_container.is_none()
            && tracing_owner.is_none()
            && queue_runtime.is_none()
        {
            return None;
        }

        let shutdown_timeout = self.shutdown_timeout;
        let deadlines = QueueShutdownDeadlines::starting_at(cleanup_started, shutdown_timeout);
        let cleanup_deadline = deadlines.hard();
        let process_context = self.process_context.take();
        let rollback_span = self.rollback_span.clone();
        let dependencies = super::dependencies::ConsumerDependencies::new(
            queue_runtime.clone(),
            owned_container,
            tracing_owner,
            None,
            deadlines,
        );
        // Keep the actual build/rollback owner if its bounded observer expires.
        let startup = StartupRemainder::new(active_activity, di_build, self.di_build_finished);
        let cleanup = async move {
            let mut failures = Vec::new();
            if let Some(activity) = &startup.activity {
                if super::dependencies::observe_before(cleanup_deadline, activity.wait())
                    .await
                    .is_none()
                {
                    failures.push(rollback_incomplete_failure());
                    return failures;
                }
            }
            let mut queue_reconciled = true;
            if let Some(runtime) = queue_runtime {
                queue_reconciled =
                    cleanup_queue_runtime(runtime, shutdown_timeout, deadlines, &mut failures)
                        .await;
            }
            let mut build_terminal = true;
            {
                let mut build = startup.build.lock().await;
                if let Some(build) = build.as_mut() {
                    if !startup.finished {
                        match super::dependencies::observe_before(
                            cleanup_deadline,
                            AssertUnwindSafe(build.wait()).catch_unwind(),
                        )
                        .await
                        {
                            Some(Ok(ApplicationContainerBuildOutcome::Cancelled(Ok(())))) => {}
                            Some(Ok(ApplicationContainerBuildOutcome::Cancelled(Err(error))))
                            | Some(Ok(ApplicationContainerBuildOutcome::Failed(error))) => {
                                failures.push(ConsumerError::dependency_initialization(error))
                            }
                            Some(Ok(ApplicationContainerBuildOutcome::Built(container))) => {
                                let child = super::dependencies::ConsumerDependencies::new(
                                    None,
                                    Some(Arc::new(container)),
                                    None,
                                    None,
                                    deadlines,
                                );
                                if !queue_reconciled || child.close_di().await.is_err() {
                                    failures.push(rollback_incomplete_failure());
                                }
                                build_terminal = child.reconcile().await;
                            }
                            Some(Err(_)) => {
                                build_terminal = false;
                                failures.push(rollback_panicked_failure());
                            }
                            None => {
                                build_terminal = false;
                                failures.push(rollback_incomplete_failure());
                            }
                        }
                    }
                    build_terminal &= build.rollback_quiescent();
                }
            }
            if build_terminal {
                startup
                    .retained
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take();
            }
            if queue_reconciled && build_terminal {
                if dependencies.close_di().await.is_err() {
                    failures.push(
                        dependencies
                            .di_error()
                            .map(ConsumerError::dependency_disposal)
                            .unwrap_or_else(rollback_incomplete_failure),
                    );
                }
                if dependencies.close_trace().await.is_err() {
                    failures.push(ConsumerError::tracing_shutdown_incomplete());
                }
            } else {
                failures.push(rollback_incomplete_failure());
            }
            if !dependencies.reconcile().await {
                failures.push(rollback_incomplete_failure());
            }
            if !dependencies.tracing_succeeded()
                && !failures
                    .iter()
                    .any(|error| matches!(error, ConsumerError::Tracing { .. }))
            {
                failures.push(ConsumerError::tracing_shutdown_incomplete());
            }
            failures
        };
        let cleanup = async move {
            if let Some(context) = process_context {
                ProcessContext::scope(context, cleanup).await
            } else {
                cleanup.await
            }
        }
        .instrument(rollback_span);
        Some(self.runtime.spawn(cleanup).into())
    }
}

impl Drop for ConsumerBuildTransaction {
    fn drop(&mut self) {
        // The fallback registry retains the real join after the waiter drops.
        // Cleanup uses the original attempt, never a replacement timeout.
        let _ = self.start_cleanup();
    }
}

async fn cleanup_queue_runtime(
    runtime: QueueRuntimeHandle,
    timeout: Duration,
    deadlines: QueueShutdownDeadlines,
    failures: &mut Vec<ConsumerError>,
) -> bool {
    let shutdown_state = Arc::new(ShutdownState::new());
    let _ = shutdown_state.initiate_shutdown(ShutdownSignal::Manual);
    let mut coordinator = FrameworkShutdownCoordinator::before(
        shutdown_state,
        timeout,
        deadlines.graceful(),
        deadlines.hard(),
    );
    let evidence = register_queue_lifecycle(&mut coordinator, runtime.clone(), timeout);
    let report = coordinator.execute_report(ShutdownSignal::Manual).await;
    let reconciled = shutdown_report_reconciled(report.is_terminal_complete(), report.reconciles());
    let primary_failure = shutdown_report_has_primary_failure(&report);
    let provider_failures = evidence
        .failures()
        .into_iter()
        .map(|source| ConsumerError::RuntimeSupervision { source })
        .collect::<Vec<_>>();
    let represented_primary_failures = provider_failures.len();
    failures.extend(provider_failures);
    if shutdown_report_requires_aggregate(&report, reconciled, represented_primary_failures) {
        failures.push(shutdown_failure(&report, primary_failure));
    }
    reconciled && runtime.close_reconciled()
}

fn rollback_incomplete_failure() -> ConsumerError {
    ConsumerError::Shutdown {
        evidence: ConsumerShutdownFailureEvidence {
            kind: ConsumerShutdownFailureKind::Incomplete,
            failed: 0,
            panicked: 0,
            timed_out: 0,
            cancelled_by_force: 0,
            forced_cleanup_failed: 0,
            forced_cleanup_panicked: 0,
            forced_cleanup_timed_out: 0,
            forced_cleanup_unavailable: 1,
        },
    }
}

struct StartupRemainder {
    activity: Option<Arc<BuildActivityState>>,
    build: tokio::sync::Mutex<Option<ApplicationContainerBuild>>,
    finished: bool,
    retained: std::sync::Mutex<Option<Arc<Self>>>,
}

impl StartupRemainder {
    fn new(
        activity: Option<Arc<BuildActivityState>>,
        build: Option<ApplicationContainerBuild>,
        finished: bool,
    ) -> Arc<Self> {
        let owner = Arc::new(Self {
            activity,
            build: tokio::sync::Mutex::new(build),
            finished,
            retained: std::sync::Mutex::new(None),
        });
        *owner.retained.lock().unwrap_or_else(|p| p.into_inner()) = Some(owner.clone());
        owner
    }
}

fn rollback_task_failure(error: &tokio::task::JoinError) -> ConsumerError {
    ConsumerError::Shutdown {
        evidence: ConsumerShutdownFailureEvidence {
            kind: ConsumerShutdownFailureKind::PrimaryFailure,
            failed: usize::from(!error.is_panic()),
            panicked: usize::from(error.is_panic()),
            timed_out: 0,
            cancelled_by_force: 0,
            forced_cleanup_failed: 0,
            forced_cleanup_panicked: 0,
            forced_cleanup_timed_out: 0,
            forced_cleanup_unavailable: 0,
        },
    }
}

fn rollback_panicked_failure() -> ConsumerError {
    ConsumerError::Shutdown {
        evidence: ConsumerShutdownFailureEvidence {
            kind: ConsumerShutdownFailureKind::PrimaryFailure,
            failed: 0,
            panicked: 1,
            timed_out: 0,
            cancelled_by_force: 0,
            forced_cleanup_failed: 0,
            forced_cleanup_panicked: 0,
            forced_cleanup_timed_out: 0,
            forced_cleanup_unavailable: 0,
        },
    }
}

#[derive(Default)]
struct BuildActivityState {
    complete: AtomicBool,
    notify: Notify,
}

impl BuildActivityState {
    fn complete(&self) {
        self.complete.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // `notify_waiters` does not retain a permit. Register this waiter
            // before re-reading the terminal atomic so completion cannot fall
            // into the subscribe/check gap and strand the retained rollback owner.
            notified.as_mut().enable();
            if self.is_complete() {
                return;
            }
            notified.as_mut().await;
        }
    }
}

pub(super) struct TrackedConsumerBuildActivity<F>
where
    F: Future,
{
    future: Option<Pin<Box<F>>>,
    state: Arc<BuildActivityState>,
}

impl<F> Future for TrackedConsumerBuildActivity<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = this
            .future
            .as_mut()
            .expect("completed Consumer build activity was polled again")
            .as_mut()
            .poll(context);
        match result {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => {
                let completion = BuildActivityCompletion(Arc::clone(&this.state));
                this.future.take();
                drop(completion);
                Poll::Ready(output)
            }
        }
    }
}

impl<F> Drop for TrackedConsumerBuildActivity<F>
where
    F: Future,
{
    fn drop(&mut self) {
        let completion = BuildActivityCompletion(Arc::clone(&self.state));
        self.future.take();
        drop(completion);
    }
}

struct BuildActivityCompletion(Arc<BuildActivityState>);

impl Drop for BuildActivityCompletion {
    fn drop(&mut self) {
        self.0.complete();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lily_config::ConfigService;
    use lily_injection::Injectable;
    use lily_injection::ServiceTrait;
    use lily_queue::__private::{
        queue_runtime, queue_service_test_seed, QueueServiceTestLifecycleCall,
    };
    use std::sync::{atomic::AtomicUsize, Mutex};

    #[derive(Default, Injectable)]
    #[service(lifetime = "Singleton")]
    struct PendingBuildService {
        state: Option<Arc<PendingBuildState>>,
    }

    #[derive(Default)]
    struct PendingBuildState {
        events: Mutex<Vec<&'static str>>,
        initialized: Notify,
        disposed: Notify,
        pending_dispose: AtomicBool,
    }

    struct DisposeDropGuard(Arc<PendingBuildState>);
    impl Drop for DisposeDropGuard {
        fn drop(&mut self) {
            self.0.events.lock().unwrap().push("dispose-drop");
        }
    }

    struct InitializeDropGuard(Arc<PendingBuildState>);

    impl Drop for InitializeDropGuard {
        fn drop(&mut self) {
            self.0
                .events
                .lock()
                .expect("pending build event ledger")
                .push("initialize-drop");
        }
    }

    #[async_trait::async_trait]
    impl ServiceTrait for PendingBuildService {
        async fn initialize(&mut self) -> Result<(), lily_error::injection::InjectionError> {
            let Some(state) = self.state.clone() else {
                return Ok(());
            };
            state
                .events
                .lock()
                .expect("pending build event ledger")
                .push("initialize-enter");
            state.initialized.notify_one();
            let _drop_guard = InitializeDropGuard(state);
            std::future::pending().await
        }

        async fn dispose(&self) -> Result<(), lily_error::injection::InjectionError> {
            let Some(state) = self.state.as_ref() else {
                return Ok(());
            };
            state
                .events
                .lock()
                .expect("pending build event ledger")
                .push("dispose");
            state.disposed.notify_one();
            if state.pending_dispose.load(Ordering::Acquire) {
                let _guard = DisposeDropGuard(state.clone());
                std::future::pending::<()>().await;
            }
            Ok(())
        }
    }

    struct PendingDropFuture {
        drops: Arc<AtomicUsize>,
        polled: Option<Arc<Notify>>,
    }

    impl Future for PendingDropFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            if let Some(polled) = &self.polled {
                polled.notify_one();
            }
            Poll::Pending
        }
    }

    impl Drop for PendingDropFuture {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::AcqRel);
        }
    }

    async fn owned_transaction_fixture() -> (
        ConsumerBuildTransaction,
        Arc<ApplicationContainer>,
        lily_queue::__private::QueueServiceTestProbe,
    ) {
        let config_path = format!("/tmp/lily-capq06e-owned-{}.toml", uuid::Uuid::new_v4());
        let config = ConfigService::development(&config_path);
        let (queue_service, probe) =
            queue_service_test_seed(Arc::new(ConfigService::development(config_path)));
        let mut transaction =
            ConsumerBuildTransaction::new(Handle::current(), None, Duration::from_secs(1));
        let container = transaction
            .build_owned_container(
                crate::test_application_container_builder()
                    .seed_singleton(config)
                    .seed_singleton(queue_service),
            )
            .await
            .expect("transport-free owned container");
        let queue_service = container
            .resolve::<lily_queue::QueueService>(None)
            .await
            .expect("seeded queue service");
        transaction.retain_queue_runtime(queue_runtime(&queue_service).expect("queue runtime"));
        (transaction, container, probe)
    }

    async fn caller_owned_transaction_fixture() -> (
        ConsumerBuildTransaction,
        Arc<ApplicationContainer>,
        lily_queue::__private::QueueServiceTestProbe,
    ) {
        let config_path = format!("/tmp/lily-capq06e-caller-{}.toml", uuid::Uuid::new_v4());
        let config = ConfigService::development(&config_path);
        let (queue_service, probe) =
            queue_service_test_seed(Arc::new(ConfigService::development(config_path)));
        let container = Arc::new(
            crate::test_application_container_builder()
                .seed_singleton(config)
                .seed_singleton(queue_service)
                .build()
                .await
                .expect("transport-free caller-owned container"),
        );
        let queue_service = container
            .resolve::<lily_queue::QueueService>(None)
            .await
            .expect("seeded queue service");
        let mut transaction =
            ConsumerBuildTransaction::new(Handle::current(), None, Duration::from_secs(1));
        transaction.retain_queue_runtime(queue_runtime(&queue_service).expect("queue runtime"));
        (transaction, container, probe)
    }

    async fn wait_for_lifecycle_count(
        probe: &lily_queue::__private::QueueServiceTestProbe,
        call: QueueServiceTestLifecycleCall,
        expected: usize,
    ) {
        tokio::time::timeout(
            Duration::from_secs(1),
            probe.wait_for_lifecycle_completion(call, expected),
        )
        .await
        .expect("lifecycle completion evidence must become visible");
    }

    #[tokio::test]
    async fn cancellation_drops_the_losing_activity_before_returning() {
        let drops = Arc::new(AtomicUsize::new(0));
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let outcome = poll_startup_activity(
            PendingDropFuture {
                drops: Arc::clone(&drops),
                polled: None,
            },
            Some(&cancellation),
        )
        .await;

        assert!(matches!(outcome, StartupActivityOutcome::Cancelled));
        assert_eq!(drops.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn activity_wait_handles_completion_on_both_sides_of_subscription() {
        let completed_before_wait = BuildActivityState::default();
        completed_before_wait.complete();
        tokio::time::timeout(Duration::from_secs(1), completed_before_wait.wait())
            .await
            .expect("an already completed activity must not require a notification permit");

        let completed_after_wait = Arc::new(BuildActivityState::default());
        let waiting = Arc::clone(&completed_after_wait);
        let waiter = tokio::spawn(async move { waiting.wait().await });
        tokio::task::yield_now().await;
        completed_after_wait.complete();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("a registered waiter must observe completion")
            .expect("activity waiter task must not panic");
    }

    #[tokio::test]
    async fn explicit_cancellation_orders_activity_queue_and_owned_di_cleanup() {
        let (mut transaction, _container, probe) = owned_transaction_fixture().await;
        let drops = Arc::new(AtomicUsize::new(0));
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let activity = transaction.track(PendingDropFuture {
            drops: Arc::clone(&drops),
            polled: None,
        });

        let outcome = poll_startup_activity(activity, Some(&cancellation)).await;
        assert!(matches!(outcome, StartupActivityOutcome::Cancelled));
        assert_eq!(drops.load(Ordering::Acquire), 1);
        transaction
            .rollback(None)
            .await
            .expect("successful startup cancellation rollback");

        use QueueServiceTestLifecycleCall as Call;
        assert_eq!(
            probe.lifecycle_snapshot().calls(),
            &[
                Call::StartAsync,
                Call::StopAdmissionAsync,
                Call::DrainAsync,
                Call::CloseAsync,
                Call::StopAsync,
            ]
        );
    }

    #[tokio::test]
    async fn caller_owned_container_is_not_disposed_but_its_queue_runtime_is_closed() {
        let (mut transaction, container, probe) = caller_owned_transaction_fixture().await;
        transaction
            .rollback(None)
            .await
            .expect("queue-only rollback must succeed");

        use QueueServiceTestLifecycleCall as Call;
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
            .expect("caller can still close its own container");
        assert_eq!(probe.lifecycle_snapshot().count(Call::StopAsync), 1);
    }

    #[tokio::test]
    async fn explicit_startup_error_remains_primary_after_successful_cleanup() {
        let (mut transaction, container, _probe) = caller_owned_transaction_fixture().await;
        let error = transaction
            .rollback(Some(ConsumerError::ManagedStartupIncomplete))
            .await
            .expect_err("startup error must remain visible");
        assert_eq!(error.error_code(), "CONSUMER_MANAGED_STARTUP_INCOMPLETE");
        container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("caller cleanup");
    }

    #[tokio::test]
    async fn startup_error_preserves_typed_queue_cleanup_failure_as_secondary() {
        let (mut transaction, container, probe) = caller_owned_transaction_fixture().await;
        probe.fail_next_lifecycle(QueueServiceTestLifecycleCall::CloseAsync);

        let error = transaction
            .rollback(Some(ConsumerError::ManagedStartupIncomplete))
            .await
            .expect_err("startup and cleanup failures must remain visible");
        let ConsumerError::LifecycleFailures(failures) = error else {
            panic!("cleanup failure must be attached to the startup primary");
        };
        assert_eq!(
            failures.primary().error_code(),
            "CONSUMER_MANAGED_STARTUP_INCOMPLETE"
        );
        assert_eq!(failures.secondary_failures().len(), 1);
        assert_eq!(
            failures.secondary_failures()[0].error_code(),
            "BROKER_GENERAL"
        );
        assert_eq!(
            probe
                .lifecycle_snapshot()
                .count(QueueServiceTestLifecycleCall::CloseAsync),
            2,
            "forced cleanup must retry the one-shot close failure"
        );
        container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("caller cleanup");
    }

    #[tokio::test]
    async fn startup_error_retains_typed_cleanup_failure_and_cleanup_panic_evidence() {
        let (mut transaction, container, probe) = caller_owned_transaction_fixture().await;
        probe.fail_next_lifecycle(QueueServiceTestLifecycleCall::DrainAsync);
        probe.panic_next_lifecycle(QueueServiceTestLifecycleCall::CloseAsync);

        let error = transaction
            .rollback(Some(ConsumerError::ManagedStartupIncomplete))
            .await
            .expect_err("startup, typed cleanup failure, and cleanup panic must remain visible");
        let ConsumerError::LifecycleFailures(failures) = error else {
            panic!("mixed startup rollback observations must form an ordered aggregate");
        };
        assert_eq!(
            failures.primary().error_code(),
            "CONSUMER_MANAGED_STARTUP_INCOMPLETE"
        );
        assert_eq!(failures.secondary_failures().len(), 2);
        assert_eq!(
            failures.secondary_failures()[0].error_code(),
            "BROKER_GENERAL"
        );
        let ConsumerError::Shutdown { evidence } = &failures.secondary_failures()[1] else {
            panic!("the contained close panic must remain as bounded aggregate evidence");
        };
        assert_eq!(evidence.kind, ConsumerShutdownFailureKind::PrimaryFailure);
        assert_eq!(evidence.failed, 1);
        assert_eq!(evidence.panicked, 1);
        assert_eq!(evidence.timed_out, 0);
        assert_eq!(evidence.forced_cleanup_failed, 0);
        assert_eq!(evidence.forced_cleanup_panicked, 0);

        use QueueServiceTestLifecycleCall as Call;
        assert_eq!(
            probe.lifecycle_snapshot().calls(),
            &[
                Call::StartAsync,
                Call::StopAdmissionAsync,
                Call::DrainAsync,
                Call::ForceDrainAsync,
                Call::CloseAsync,
                Call::CloseAsync,
            ]
        );
        container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("caller cleanup");
        assert_eq!(probe.lifecycle_snapshot().count(Call::StopAsync), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn outer_task_abort_detaches_one_owned_cleanup_task() {
        let (mut transaction, _container, probe) = owned_transaction_fixture().await;
        let drops = Arc::new(AtomicUsize::new(0));
        let activity_drops = Arc::clone(&drops);
        let polled = Arc::new(Notify::new());
        let activity_polled = Arc::clone(&polled);
        let task = tokio::spawn(async move {
            transaction
                .track(PendingDropFuture {
                    drops: activity_drops,
                    polled: Some(activity_polled),
                })
                .await;
        });
        tokio::time::timeout(Duration::from_secs(1), polled.notified())
            .await
            .expect("pending activity must be polled before abort");
        task.abort();
        let _ = task.await;

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if drops.load(Ordering::Acquire) == 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending activity must be dropped");
        wait_for_lifecycle_count(&probe, QueueServiceTestLifecycleCall::StopAsync, 1).await;

        use QueueServiceTestLifecycleCall as Call;
        let snapshot = probe.lifecycle_snapshot();
        assert_eq!(snapshot.count(Call::StopAdmissionAsync), 1);
        assert_eq!(snapshot.count(Call::DrainAsync), 1);
        assert_eq!(snapshot.count(Call::CloseAsync), 1);
        assert_eq!(snapshot.count(Call::StopAsync), 1);
    }

    #[tokio::test]
    async fn committed_owner_survives_transaction_drop_and_cleans_exactly_once() {
        let (mut transaction, _container, probe) = owned_transaction_fixture().await;
        let committed = transaction.commit();
        drop(transaction);

        use QueueServiceTestLifecycleCall as Call;
        assert_eq!(probe.lifecycle_snapshot().calls(), &[Call::StartAsync]);
        drop(committed);
        wait_for_lifecycle_count(&probe, Call::StopAsync, 1).await;
        let snapshot = probe.lifecycle_snapshot();
        assert_eq!(snapshot.count(Call::StopAdmissionAsync), 1);
        assert_eq!(snapshot.count(Call::DrainAsync), 1);
        assert_eq!(snapshot.count(Call::CloseAsync), 1);
        assert_eq!(snapshot.count(Call::StopAsync), 1);
    }

    #[tokio::test]
    async fn normal_commit_handoff_does_not_start_a_second_cleanup_authority() {
        let (mut transaction, _container, probe) = owned_transaction_fixture().await;
        let committed = transaction.commit();
        drop(transaction);
        let state = Arc::new(ShutdownState::new());
        let owner = committed.into_runtime_owner(state);

        use QueueServiceTestLifecycleCall as Call;
        assert_eq!(probe.lifecycle_snapshot().calls(), &[Call::StartAsync]);
        drop(owner);
        wait_for_lifecycle_count(&probe, Call::StopAsync, 1).await;

        let snapshot = probe.lifecycle_snapshot();
        assert_eq!(snapshot.count(Call::StopAdmissionAsync), 1);
        assert_eq!(snapshot.count(Call::DrainAsync), 1);
        assert_eq!(snapshot.count(Call::CloseAsync), 1);
        assert_eq!(snapshot.count(Call::StopAsync), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn outer_abort_cancels_pending_di_build_before_reverse_disposal() {
        let state = Arc::new(PendingBuildState::default());
        let config_path = format!("/tmp/lily-capq06e-di-build-{}.toml", uuid::Uuid::new_v4());
        let config = ConfigService::development(&config_path);
        let (queue_service, _probe) =
            queue_service_test_seed(Arc::new(ConfigService::development(config_path)));
        let service = PendingBuildService {
            state: Some(Arc::clone(&state)),
        };
        let task = tokio::spawn(async move {
            let mut transaction =
                ConsumerBuildTransaction::new(Handle::current(), None, Duration::from_secs(1));
            let _ = transaction
                .build_owned_container(
                    crate::test_application_container_builder()
                        .seed_singleton(config)
                        .seed_singleton(queue_service)
                        .seed_singleton(service),
                )
                .await;
        });
        tokio::time::timeout(Duration::from_secs(1), state.initialized.notified())
            .await
            .expect("pending DI initializer must be polled before abort");
        task.abort();
        let _ = task.await;
        tokio::time::timeout(Duration::from_secs(1), state.disposed.notified())
            .await
            .expect("retained DI rollback must dispose the partial service");

        assert_eq!(
            *state.events.lock().expect("pending build event ledger"),
            ["initialize-enter", "initialize-drop", "dispose"]
        );
    }
    #[tokio::test(start_paused = true)]
    async fn startup_rollback_and_pending_disposer_share_the_original_total_cutoff() {
        let state = Arc::new(PendingBuildState::default());
        state.pending_dispose.store(true, Ordering::Release);
        let config = ConfigService::development("/tmp/consumer-stage5-startup.toml");
        let (queue, _) = queue_service_test_seed(Arc::new(ConfigService::development(
            "/tmp/consumer-stage5-startup.toml",
        )));
        let mut transaction =
            ConsumerBuildTransaction::new(Handle::current(), None, Duration::from_millis(40));
        let builder = crate::test_application_container_builder()
            .seed_singleton(config)
            .seed_singleton(queue)
            .seed_singleton(PendingBuildService {
                state: Some(state.clone()),
            });
        {
            let build = transaction.build_owned_container(builder);
            tokio::pin!(build);
            tokio::select! {
                result = &mut build => panic!("initializer must be pending: {result:?}"),
                _ = state.initialized.notified() => {},
            }
        }
        let started = tokio::time::Instant::now();
        assert!(transaction.rollback(None).await.is_err());
        assert!(tokio::time::Instant::now() <= started + Duration::from_millis(40));
        assert_eq!(
            *state.events.lock().unwrap(),
            [
                "initialize-enter",
                "initialize-drop",
                "dispose",
                "dispose-drop"
            ]
        );
    }
}
