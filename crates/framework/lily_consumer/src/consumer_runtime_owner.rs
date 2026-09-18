use super::owned_tasks::OwnedTask;
use futures_util::FutureExt;
use lily_error::application::{
    MessageBrokerError,
    consumer::{ConsumerError, ConsumerSignalFailureStage},
    message_broker::{
        RabbitMQError, RabbitMqConsumerTaskFailure, RabbitMqConsumerTaskFailureKind,
        RabbitMqConsumerTaskRole,
    },
};
use lily_injection::{ApplicationContainer, ProcessContext};
use lily_queue::__private::{QueueRuntimeHandle, QueueShutdownDeadlines, register_queue_lifecycle};
use lily_shutdown::{
    FrameworkShutdownCoordinator, FrameworkShutdownPhase, ShutdownAction, ShutdownSignal,
    ShutdownState, SignalHandler, SignalMonitor,
};
use lily_trace::TracingRuntimeOwner;
use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, warn};

use super::{
    Consumer, ConsumerLifecycleTrigger, ConsumerStartup, ManagedConsumerStartup, shutdown_failure,
    shutdown_report_has_primary_failure, shutdown_report_reconciled,
    shutdown_report_requires_aggregate,
};

const CONSUMER_RUNTIME_SUPERVISOR: &str = "consumer-runtime";

#[cfg(test)]
static OWNED_TRACING_SHUTDOWN_REPORTS: Mutex<Vec<lily_trace::TraceShutdownReport>> =
    Mutex::new(Vec::new());

#[cfg(test)]
pub(super) fn owned_tracing_shutdown_reports_for_test() -> Vec<lily_trace::TraceShutdownReport> {
    OWNED_TRACING_SHUTDOWN_REPORTS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

struct RuntimeTriggerOutcome {
    runtime_result: Result<(), ConsumerError>,
    signal: ShutdownSignal,
}

type ConsumerCleanupFuture =
    Pin<Box<dyn Future<Output = Result<(), ConsumerError>> + Send + 'static>>;

/// Sole owner of every Consumer resource which must survive runtime waiting.
pub(super) struct ConsumerRuntimeOwner {
    shutdown_report: Arc<crate::shutdown_report::ConsumerShutdownObservation>,
    runtime: Handle,
    queue_runtime: Option<QueueRuntimeHandle>,
    owned_container: Option<Arc<ApplicationContainer>>,
    tracing_owner: Option<TracingRuntimeOwner>,
    shutdown_state: Arc<ShutdownState>,
    signal_monitor: Option<SignalMonitor>,
    shutdown_timeout: Duration,
    process_context: Option<ProcessContext>,
    lifecycle_span: tracing::Span,
    pending_cleanup: Option<ConsumerCleanupFuture>,
}

impl ConsumerRuntimeOwner {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        runtime: Handle,
        queue_runtime: QueueRuntimeHandle,
        owned_container: Option<Arc<ApplicationContainer>>,
        tracing_owner: Option<TracingRuntimeOwner>,
        shutdown_state: Arc<ShutdownState>,
        shutdown_timeout: Duration,
        process_context: Option<ProcessContext>,
        lifecycle_span: tracing::Span,
    ) -> Self {
        Self {
            shutdown_report: Arc::default(),
            runtime,
            queue_runtime: Some(queue_runtime),
            owned_container,
            tracing_owner,
            shutdown_state,
            signal_monitor: None,
            shutdown_timeout,
            process_context,
            lifecycle_span,
            pending_cleanup: None,
        }
    }

    pub(super) fn queue_runtime(&self) -> QueueRuntimeHandle {
        self.queue_runtime
            .as_ref()
            .expect("Consumer runtime owner queue runtime must be present")
            .clone()
    }

    /// Move this complete owner into one supervised task before waiting.
    pub(super) fn spawn(
        self,
        startup: ConsumerStartup,
        lifecycle_trigger: ConsumerLifecycleTrigger,
    ) -> OwnedTask<Result<(), ConsumerError>> {
        let runtime = self.runtime.clone();
        let process_context = self.process_context.clone();
        let lifecycle_span = self.lifecycle_span.clone();
        let supervisor = async move { self.supervise(startup, lifecycle_trigger).await };
        let supervisor = async move {
            if let Some(context) = process_context {
                ProcessContext::scope(context, supervisor).await
            } else {
                supervisor.await
            }
        }
        .instrument(lifecycle_span);
        runtime.spawn(supervisor).into()
    }

    async fn supervise(
        mut self,
        startup: ConsumerStartup,
        lifecycle_trigger: ConsumerLifecycleTrigger,
    ) -> Result<(), ConsumerError> {
        let runtime_span = tracing::info_span!(
            "consumer.runtime",
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let trigger = AssertUnwindSafe(
            self.wait_for_trigger(startup, lifecycle_trigger)
                .instrument(runtime_span.clone()),
        )
        .catch_unwind()
        .await;
        let outcome = match trigger {
            Ok(outcome) => outcome,
            Err(_) => {
                let panic_failure = Err(runtime_supervisor_panicked());
                let (shutdown_result, signal) =
                    Consumer::programmatic_shutdown_requested(&self.shutdown_state);
                RuntimeTriggerOutcome {
                    runtime_result: Consumer::merge_primary_then_secondary(
                        panic_failure,
                        shutdown_result,
                    ),
                    signal,
                }
            }
        };
        match &outcome.runtime_result {
            Ok(()) => {
                runtime_span.record("lily.outcome", "success");
            }
            Err(error) => {
                runtime_span.record("lily.outcome", "error");
                runtime_span.record("lily.error_code", error.error_code());
                runtime_span.record("otel.status_code", "ERROR");
            }
        }

        let runtime_primary = outcome.runtime_result.clone();
        let (cleanup, schedule_failure) =
            match self.start_cleanup(outcome.runtime_result, outcome.signal) {
                Ok(Some(cleanup)) => (cleanup, None),
                Ok(None) => return runtime_primary,
                Err(schedule_failure) => match self.schedule_pending_cleanup() {
                    Ok(Some(cleanup)) => (cleanup, Some(schedule_failure)),
                    Ok(None) => {
                        return Consumer::merge_primary_then_secondary(
                            runtime_primary,
                            Err(schedule_failure),
                        );
                    }
                    Err(retry_failure) => {
                        let Some(cleanup) = self.pending_cleanup.take() else {
                            return Consumer::merge_lifecycle_vec(vec![
                                runtime_primary,
                                Err(schedule_failure),
                                Err(retry_failure),
                            ]);
                        };
                        let cleanup_result = cleanup.await;
                        return merge_recovered_cleanup_schedule_failures(
                            &runtime_primary,
                            cleanup_result,
                            vec![schedule_failure, retry_failure],
                        );
                    }
                },
            };
        match cleanup.await {
            Ok(result) => match schedule_failure {
                Some(schedule_failure) => merge_recovered_cleanup_schedule_failures(
                    &runtime_primary,
                    result,
                    vec![schedule_failure],
                ),
                None => result,
            },
            Err(error) => {
                let mut failures = vec![runtime_primary];
                if let Some(schedule_failure) = schedule_failure {
                    failures.push(Err(schedule_failure));
                }
                failures.push(Err(crate::consumer::owned_tasks::join_failure(error)));
                Consumer::merge_lifecycle_vec(failures)
            }
        }
    }

    async fn wait_for_trigger(
        &mut self,
        startup: ConsumerStartup,
        lifecycle_trigger: ConsumerLifecycleTrigger,
    ) -> RuntimeTriggerOutcome {
        let ConsumerStartup {
            configured_queues,
            registered_handlers,
        } = startup;
        let provider = self.queue_runtime();

        let (runtime_result, signal) = match lifecycle_trigger {
            ConsumerLifecycleTrigger::Signals => {
                info!(shutdown_trigger = "os_signal", "Consumer runtime is ready");
                let signal_monitor =
                    match SignalHandler::install(Arc::clone(&self.shutdown_state)).await {
                        Ok(monitor) => monitor,
                        Err(error) => {
                            return RuntimeTriggerOutcome {
                                runtime_result: Err(ConsumerError::signal(
                                    ConsumerSignalFailureStage::Installation,
                                    error,
                                )),
                                signal: ShutdownSignal::Manual,
                            };
                        }
                    };
                self.signal_monitor = Some(signal_monitor);
                let signal_monitor = self
                    .signal_monitor
                    .as_mut()
                    .expect("installed Consumer signal monitor must be retained");
                tokio::select! {
                    biased;
                    result = provider.wait_for_shutdown() => {
                        Consumer::queue_runtime_completed(result, &self.shutdown_state)
                    }
                    signal_result = signal_monitor.wait_for_first() => {
                        match signal_result {
                            Ok(signal) => {
                                warn!("Shutdown signal received, stopping consumer...");
                                info!("Shutdown signal handled");
                                (Ok(()), signal)
                            }
                            Err(error) => (
                                Err(ConsumerError::signal(
                                    ConsumerSignalFailureStage::Receive,
                                    error,
                                )),
                                ShutdownSignal::Manual,
                            ),
                        }
                    }
                }
            }
            ConsumerLifecycleTrigger::Cancellation(cancellation) => {
                let mut shutdown_receiver = self.shutdown_state.subscribe();
                tokio::select! {
                    biased;
                    result = provider.wait_for_shutdown() => {
                        Consumer::queue_runtime_completed(result, &self.shutdown_state)
                    }
                    () = cancellation.cancelled() => {
                        Consumer::programmatic_shutdown_requested(&self.shutdown_state)
                    }
                    signal_result = shutdown_receiver.recv() => {
                        shutdown_notification(signal_result)
                    }
                }
            }
            ConsumerLifecycleTrigger::Managed {
                startup_tx,
                waiter_cancellation,
            } => {
                let mut shutdown_receiver = self.shutdown_state.subscribe();
                if startup_tx
                    .send(ManagedConsumerStartup {
                        shutdown_report: self.shutdown_report.clone(),
                        shutdown_state: Arc::clone(&self.shutdown_state),
                        provider: provider.clone(),
                        configured_queues,
                        registered_handlers,
                    })
                    .is_err()
                {
                    let _ = self
                        .shutdown_state
                        .initiate_shutdown(ShutdownSignal::Manual);
                }

                tokio::select! {
                    biased;
                    result = provider.wait_for_shutdown() => {
                        Consumer::queue_runtime_completed(result, &self.shutdown_state)
                    }
                    signal_result = shutdown_receiver.recv() => {
                        shutdown_notification(signal_result)
                    }
                    () = waiter_cancellation.cancelled() => {
                        Consumer::programmatic_shutdown_requested(&self.shutdown_state)
                    }
                }
            }
        };

        RuntimeTriggerOutcome {
            runtime_result,
            signal,
        }
    }

    /// Move all resources into the cleanup future before its handle is exposed.
    fn start_cleanup(
        &mut self,
        runtime_result: Result<(), ConsumerError>,
        signal: ShutdownSignal,
    ) -> Result<Option<OwnedTask<Result<(), ConsumerError>>>, ConsumerError> {
        if self.pending_cleanup.is_some() {
            return self.schedule_pending_cleanup();
        }
        let Some(provider) = self.queue_runtime.take() else {
            return Ok(None);
        };
        let owned_container = self.owned_container.take();
        let tracing_owner = self.tracing_owner.take();
        let signal_monitor = self.signal_monitor.take();
        let shutdown_state = Arc::clone(&self.shutdown_state);
        let shutdown_timeout = self.shutdown_timeout;
        let process_context = self.process_context.take();
        let lifecycle_span = self.lifecycle_span.clone();
        let shutdown_started = shutdown_state
            .initiation_started_at()
            .unwrap_or_else(tokio::time::Instant::now);
        let deadlines = QueueShutdownDeadlines::starting_at(shutdown_started, shutdown_timeout);
        let cleanup_deadline = deadlines.hard();
        let shutdown_observation = self.shutdown_report.clone();

        let owns_di = owned_container.is_some();
        let owns_trace = tracing_owner.is_some();
        let owns_signal = signal_monitor.is_some();
        let dependencies = super::dependencies::ConsumerDependencies::new(
            Some(provider.clone()),
            owned_container,
            tracing_owner,
            signal_monitor,
            deadlines,
        );
        let cleanup = async move {
            let mut coordinator = FrameworkShutdownCoordinator::before(
                Arc::clone(&shutdown_state),
                shutdown_timeout,
                deadlines.graceful(),
                cleanup_deadline,
            );
            if owns_di {
                let graceful = dependencies.clone();
                let forced = dependencies.clone();
                coordinator.register(
                    ShutdownAction::new(
                        "consumer-di-container",
                        FrameworkShutdownPhase::DisposeDependencies,
                        shutdown_timeout,
                        move || async move { graceful.close_di().await },
                    )
                    .with_force(move || async move { forced.close_di().await }),
                );
            }
            // Same-phase reverse order: confirmed queue close precedes DI.
            let queue_lifecycle_evidence =
                register_queue_lifecycle(&mut coordinator, provider.clone(), shutdown_timeout);
            if owns_trace || owns_signal {
                let graceful = dependencies.clone();
                let forced = dependencies.clone();
                coordinator.register(
                    ShutdownAction::new(
                        if owns_trace {
                            "tracing-runtime"
                        } else {
                            "consumer-signal-monitor"
                        },
                        FrameworkShutdownPhase::FlushTelemetry,
                        shutdown_timeout,
                        move || async move { graceful.close_trace().await },
                    )
                    .with_force(move || async move { forced.close_trace().await }),
                );
            }

            let shutdown_span = tracing::info_span!(
                "consumer.shutdown",
                lily.outcome = tracing::field::Empty,
                lily.shutdown_category = tracing::field::Empty,
                lily.error_code = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
            );
            let report = coordinator
                .execute_report(signal)
                .instrument(shutdown_span.clone())
                .await;
            let dependencies_reconciled = dependencies.reconcile().await;
            let shutdown_reconciled = dependencies_reconciled
                && shutdown_report_reconciled(report.is_terminal_complete(), report.reconciles());
            let primary_shutdown_failure = shutdown_report_has_primary_failure(&report);
            if shutdown_reconciled && !primary_shutdown_failure {
                shutdown_span.record("lily.outcome", "success");
                shutdown_span.record(
                    "lily.shutdown_category",
                    if report.is_graceful() {
                        "graceful"
                    } else {
                        "forced"
                    },
                );
            } else {
                shutdown_span.record("lily.outcome", "error");
                shutdown_span.record(
                    "lily.shutdown_category",
                    if primary_shutdown_failure {
                        "primary_failure"
                    } else {
                        "incomplete"
                    },
                );
                shutdown_span.record(
                    "lily.error_code",
                    if primary_shutdown_failure {
                        "CONSUMER_SHUTDOWN_PRIMARY_FAILURE"
                    } else {
                        "CONSUMER_SHUTDOWN_INCOMPLETE"
                    },
                );
                shutdown_span.record("otel.status_code", "ERROR");
            }

            let runtime_broker_failure = runtime_result
                .as_ref()
                .err()
                .and_then(ConsumerError::message_broker_error)
                .cloned();
            let queue_lifecycle_failures = queue_lifecycle_evidence.failures();
            let queue_lifecycle_evidence_count = queue_lifecycle_failures.len();
            let queue_lifecycle_results = queue_lifecycle_failures
                .into_iter()
                .filter(|source| runtime_broker_failure.as_ref() != Some(source))
                .map(|source| Err(ConsumerError::RuntimeSupervision { source }))
                .collect::<Vec<_>>();
            let dependency_disposal_result = dependencies
                .di_error()
                .map(|error| Err(ConsumerError::dependency_disposal(error)))
                .unwrap_or(Ok(()));
            #[cfg(test)]
            let tracing_shutdown_report = dependencies
                .trace_evidence()
                .and_then(|evidence| evidence.report());
            #[cfg(test)]
            if let Some(report) = &tracing_shutdown_report {
                OWNED_TRACING_SHUTDOWN_REPORTS
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(report.clone());
            }
            let tracing_shutdown_result = if dependencies.tracing_succeeded() {
                Ok(())
            } else {
                Err(ConsumerError::tracing_shutdown_incomplete())
            };
            let signal_result = dependencies.signal_error().map_or(Ok(()), Err);
            let represented_primary_failures = queue_lifecycle_evidence_count
                .saturating_add(usize::from(dependency_disposal_result.is_err()))
                .saturating_add(usize::from(tracing_shutdown_result.is_err()))
                .saturating_add(usize::from(signal_result.is_err()));
            let lifecycle_result = if shutdown_report_requires_aggregate(
                &report,
                shutdown_reconciled,
                represented_primary_failures,
            ) {
                Err(shutdown_failure(&report, primary_shutdown_failure))
            } else {
                Ok(())
            };

            let mut lifecycle_results = vec![runtime_result];
            lifecycle_results.extend(queue_lifecycle_results);
            lifecycle_results.extend([
                dependency_disposal_result,
                tracing_shutdown_result,
                lifecycle_result,
                signal_result,
            ]);
            let result = Consumer::merge_lifecycle_vec(lifecycle_results);
            let captured = crate::ConsumerShutdownReport::capture(
                &report,
                shutdown_started.elapsed(),
                provider.drain_reconciled(),
                provider.close_reconciled(),
                dependencies.report(),
            );
            // Publish before returning; public observation additionally requires
            // the actual runtime join. No task or payload history is retained.
            let _ = shutdown_observation.set(captured);
            result
        };
        let cleanup = async move {
            if let Some(context) = process_context {
                ProcessContext::scope(context, cleanup).await
            } else {
                cleanup.await
            }
        }
        .instrument(lifecycle_span);
        self.pending_cleanup = Some(Box::pin(cleanup));
        self.schedule_pending_cleanup()
    }

    fn schedule_pending_cleanup(
        &mut self,
    ) -> Result<Option<OwnedTask<Result<(), ConsumerError>>>, ConsumerError> {
        let Some(cleanup) = self.pending_cleanup.take() else {
            return Ok(None);
        };
        // Tokio never polls a spawned task synchronously. Retaining this
        // second envelope reference lets a synchronous scheduling panic put
        // the complete cleanup future back under owner authority.
        let envelope = Arc::new(Mutex::new(Some(cleanup)));
        let task_envelope = Arc::clone(&envelope);
        let cleanup = async move {
            let cleanup = task_envelope
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
                .expect("scheduled Consumer cleanup future must be present");
            cleanup.await
        };
        let runtime = self.runtime.clone();
        match catch_unwind(AssertUnwindSafe(|| runtime.spawn(cleanup))) {
            Ok(task) => Ok(Some(task.into())),
            Err(_) => {
                self.pending_cleanup = envelope
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take();
                Err(runtime_supervisor_cleanup_schedule_failed())
            }
        }
    }
}

impl Drop for ConsumerRuntimeOwner {
    fn drop(&mut self) {
        if self.queue_runtime.is_none() && self.pending_cleanup.is_none() {
            return;
        }
        let (shutdown_result, signal) =
            Consumer::programmatic_shutdown_requested(&self.shutdown_state);
        // The fallback registry retains the real join if this observer drops.
        // The captured runtime must still be alive to poll cleanup.
        let _ = self.start_cleanup(shutdown_result, signal);
    }
}

/// Requests durable shutdown if a direct `run*` waiter disappears.
pub(super) struct ConsumerRuntimeWaiterGuard {
    shutdown_state: Arc<ShutdownState>,
    armed: bool,
}

impl ConsumerRuntimeWaiterGuard {
    pub(super) fn new(shutdown_state: Arc<ShutdownState>) -> Self {
        Self {
            shutdown_state,
            armed: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ConsumerRuntimeWaiterGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self
                .shutdown_state
                .initiate_shutdown(ShutdownSignal::Manual);
        }
    }
}

/// Cancels startup/runtime observation when a managed readiness waiter drops.
pub(super) struct ManagedStartupWaiterGuard {
    cancellation: CancellationToken,
    armed: bool,
}

impl ManagedStartupWaiterGuard {
    pub(super) fn new(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            armed: true,
        }
    }

    pub(super) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ManagedStartupWaiterGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
        }
    }
}

fn shutdown_notification(
    result: Result<ShutdownSignal, lily_shutdown::ShutdownReceiveError>,
) -> (Result<(), ConsumerError>, ShutdownSignal) {
    match result {
        Ok(signal) => (Ok(()), signal),
        Err(error) => (
            Err(ConsumerError::signal(
                ConsumerSignalFailureStage::ManagedReceive,
                error,
            )),
            ShutdownSignal::Manual,
        ),
    }
}

fn runtime_supervisor_panicked() -> ConsumerError {
    runtime_supervisor_failure(RabbitMqConsumerTaskFailureKind::Panicked)
}

fn runtime_supervisor_cleanup_schedule_failed() -> ConsumerError {
    runtime_supervisor_failure(RabbitMqConsumerTaskFailureKind::OperationFailed)
}

fn runtime_supervisor_failure(kind: RabbitMqConsumerTaskFailureKind) -> ConsumerError {
    ConsumerError::RuntimeSupervision {
        source: MessageBrokerError::RabbitMQError(RabbitMQError::ConsumerTaskFailed(
            RabbitMqConsumerTaskFailure {
                queue: CONSUMER_RUNTIME_SUPERVISOR.into(),
                role: RabbitMqConsumerTaskRole::Supervisor,
                kind,
                operation_error_code: None,
            },
        )),
    }
}

fn merge_recovered_cleanup_schedule_failures(
    runtime_primary: &Result<(), ConsumerError>,
    cleanup_result: Result<(), ConsumerError>,
    schedule_failures: Vec<ConsumerError>,
) -> Result<(), ConsumerError> {
    debug_assert!(!schedule_failures.is_empty());
    if runtime_primary.is_err() {
        return match cleanup_result {
            Err(ConsumerError::LifecycleFailures(failures)) => {
                let mut secondary = schedule_failures;
                secondary.extend(failures.secondary_failures().iter().cloned());
                Err(ConsumerError::aggregate(
                    failures.primary().clone(),
                    secondary,
                ))
            }
            Err(primary) => Err(ConsumerError::aggregate(primary, schedule_failures)),
            Ok(()) => {
                let mut results = vec![runtime_primary.clone()];
                results.extend(schedule_failures.into_iter().map(Err));
                Consumer::merge_lifecycle_vec(results)
            }
        };
    }

    let mut schedule_failures = schedule_failures.into_iter();
    let Some(schedule_primary) = schedule_failures.next() else {
        return cleanup_result;
    };
    let mut secondary = schedule_failures.collect::<Vec<_>>();
    match cleanup_result {
        Ok(()) if secondary.is_empty() => Err(schedule_primary),
        Ok(()) => Err(ConsumerError::aggregate(schedule_primary, secondary)),
        Err(ConsumerError::LifecycleFailures(failures)) => {
            secondary.push(failures.primary().clone());
            secondary.extend(failures.secondary_failures().iter().cloned());
            Err(ConsumerError::aggregate(schedule_primary, secondary))
        }
        Err(cleanup_failure) => {
            secondary.push(cleanup_failure);
            Err(ConsumerError::aggregate(schedule_primary, secondary))
        }
    }
}

#[cfg(feature = "fuzzing")]
pub(crate) struct RuntimeOwnershipFuzzEvidence {
    pub(crate) shutdown_initiated: bool,
    pub(crate) supervisor_finished: bool,
    pub(crate) exactly_once: bool,
    pub(crate) no_late_calls: bool,
    pub(crate) drain_reconciled: bool,
    pub(crate) active_jobs: usize,
    pub(crate) lifecycle_calls: usize,
}

/// Drive the real private runtime owner with the transport-free queue seam.
///
/// Event interpretation exists only in the opt-in fuzz build. Ownership,
/// biased trigger selection, waiter-drop behavior, cleanup coordination and
/// queue lifecycle calls all remain the production implementations above.
#[cfg(feature = "fuzzing")]
pub(crate) async fn exercise_runtime_ownership_for_fuzz(
    profile: u8,
    events: &[u8],
) -> RuntimeOwnershipFuzzEvidence {
    use lily_config::ConfigService;
    use lily_queue::__private::{
        QueueServiceTestLifecycleCall as Call, queue_runtime, queue_service_test_seed,
    };

    let (queue_service, probe) = queue_service_test_seed(Arc::new(ConfigService::development(
        "/tmp/lily-consumer-runtime-ownership-fuzz.toml",
    )));
    probe.pause_runtime_completion();
    if events.contains(&6) {
        probe.fail_next_lifecycle(Call::WaitForShutdown);
    }
    if events.contains(&7) {
        probe.fail_next_lifecycle(Call::DrainAsync);
    }
    if events.contains(&8) {
        probe.panic_next_lifecycle(Call::DrainAsync);
    }
    let pause_drain = events.contains(&9);
    if pause_drain {
        probe.pause_lifecycle(Call::DrainAsync);
    }

    let provider = queue_runtime(&queue_service).expect("fuzz queue runtime must be present");
    let provider_evidence = provider.clone();
    let shutdown_state = Arc::new(ShutdownState::new());
    let owner = ConsumerRuntimeOwner::new(
        Handle::current(),
        provider,
        None,
        None,
        Arc::clone(&shutdown_state),
        Duration::from_millis(10),
        None,
        tracing::Span::none(),
    );
    let trigger_cancellation = CancellationToken::new();
    let managed_waiter_cancellation = CancellationToken::new();
    let mut managed_startup = None;
    let trigger = match profile % 3 {
        0 => ConsumerLifecycleTrigger::Cancellation(trigger_cancellation.clone()),
        managed_profile => {
            let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();
            if managed_profile == 1 {
                managed_startup = Some(startup_rx);
            } else {
                drop(startup_rx);
            }
            ConsumerLifecycleTrigger::Managed {
                startup_tx,
                waiter_cancellation: managed_waiter_cancellation.clone(),
            }
        }
    };
    let supervisor = owner.spawn(
        ConsumerStartup {
            configured_queues: 0,
            registered_handlers: 0,
        },
        trigger,
    );
    let supervisor_status = supervisor.abort_handle();
    let waiter_state = Arc::clone(&shutdown_state);
    let mut waiter = Some(tokio::spawn(async move {
        let mut guard = ConsumerRuntimeWaiterGuard::new(waiter_state);
        let result = match supervisor.await {
            Ok(result) => result,
            Err(error) => Err(crate::consumer::owned_tasks::join_failure(error)),
        };
        guard.disarm();
        result
    }));

    if let Some(startup) = managed_startup {
        let _ = startup.await;
    }
    probe.wait_for_runtime_wait(1).await;

    for event in events.iter().copied().take(32) {
        match event {
            0 => {
                trigger_cancellation.cancel();
                managed_waiter_cancellation.cancel();
            }
            1 => probe.release_runtime_completion(1),
            2 => {
                let _ = shutdown_state.initiate_shutdown(ShutdownSignal::Manual);
            }
            3 => {
                if let Some(task) = waiter.take() {
                    task.abort();
                    let _ = task.await;
                }
            }
            4 => {
                let _ = shutdown_state.request_force();
            }
            5 => tokio::task::yield_now().await,
            6..=9 => {}
            _ => tokio::task::yield_now().await,
        }
    }

    let _ = shutdown_state.initiate_shutdown(ShutdownSignal::Manual);
    trigger_cancellation.cancel();
    managed_waiter_cancellation.cancel();
    probe.release_runtime_completion(1);
    if let Some(task) = waiter.take() {
        let _ = task.await;
    }
    probe
        .wait_for_lifecycle_completion(Call::CloseAsync, 1)
        .await;
    while !supervisor_status.is_finished() {
        tokio::task::yield_now().await;
    }

    let terminal = probe.lifecycle_snapshot();
    if pause_drain {
        probe.release_lifecycle(Call::DrainAsync, 1);
    }
    probe.release_runtime_completion(1);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    let replay = probe.lifecycle_snapshot();
    let exactly_once = terminal.count(Call::StopAdmissionAsync) == 1
        && terminal.count(Call::DrainAsync) <= 1
        && terminal.count(Call::ForceDrainAsync) <= 1
        && (terminal.count(Call::DrainAsync) == 1 || terminal.count(Call::ForceDrainAsync) == 1)
        && terminal.count(Call::CloseAsync) == 1
        && terminal.completion_count(Call::CloseAsync) == 1;

    RuntimeOwnershipFuzzEvidence {
        shutdown_initiated: shutdown_state.is_shutdown_initiated(),
        supervisor_finished: supervisor_status.is_finished(),
        exactly_once,
        no_late_calls: terminal == replay,
        drain_reconciled: provider_evidence.drain_reconciled(),
        active_jobs: shutdown_state.active_job_count(),
        lifecycle_calls: terminal.calls().len(),
    }
}
