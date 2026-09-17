//! Framework shutdown adapters for a running queue provider.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use lily_error::application::MessageBrokerError;
use lily_shutdown::{
    FrameworkForceShutdownFuture, FrameworkShutdownComponent, FrameworkShutdownCoordinator,
    FrameworkShutdownPhase, ShutdownError, ShutdownState,
};
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
};
use tracing::Instrument;

use crate::{
    DeliveryCancellationReason, queue_trait::Queue, shutdown_budget::QueueShutdownDeadlines,
};

struct QueueLifecycleHandle {
    name: &'static str,
    phase: FrameworkShutdownPhase,
    provider: Arc<dyn Queue>,
    timeout: Duration,
    metrics: Arc<QueueLifecycleMetrics>,
    evidence: QueueLifecycleEvidence,
    state: Arc<ShutdownState>,
    deadlines: Option<QueueShutdownDeadlines>,
}

#[derive(Clone, Default)]
pub(crate) struct QueueLifecycleEvidence {
    failures: Arc<Mutex<Vec<MessageBrokerError>>>,
}

impl QueueLifecycleEvidence {
    fn record(&self, error: &MessageBrokerError) {
        let mut failures = self
            .failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if failures.len() < 3 && !failures.contains(error) {
            failures.push(error.clone());
        }
    }

    pub(crate) fn primary_error(&self) -> Option<MessageBrokerError> {
        self.failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .first()
            .cloned()
    }

    pub(crate) fn failures(&self) -> Vec<MessageBrokerError> {
        self.failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

struct QueueLifecycleMetrics {
    duration: Histogram<f64>,
    outcomes: Counter<u64>,
}

impl QueueLifecycleMetrics {
    fn new() -> Arc<Self> {
        let meter = global::meter("lily_queue");
        Arc::new(Self {
            duration: meter
                .f64_histogram("messaging.queue.shutdown.phase.duration")
                .with_unit("s")
                .build(),
            outcomes: meter
                .u64_counter("messaging.queue.shutdown.phase.outcomes")
                .build(),
        })
    }

    fn record(&self, phase: &'static str, outcome: &'static str, duration: Duration) {
        let attributes = [
            KeyValue::new("lily.shutdown_category", phase),
            KeyValue::new("lily.outcome", outcome),
        ];
        self.duration.record(duration.as_secs_f64(), &attributes);
        self.outcomes.add(1, &attributes);
    }
}

#[async_trait]
impl FrameworkShutdownComponent for QueueLifecycleHandle {
    fn set_shutdown_deadlines(
        &mut self,
        graceful: tokio::time::Instant,
        hard: tokio::time::Instant,
    ) {
        let deadlines = QueueShutdownDeadlines::before(graceful, hard);
        self.deadlines = Some(deadlines);
        self.provider.set_shutdown_deadlines(deadlines);
    }

    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        let category = match self.phase {
            FrameworkShutdownPhase::StopAdmission => "stop_admission",
            FrameworkShutdownPhase::DrainInFlight => "drain_in_flight",
            FrameworkShutdownPhase::DisposeDependencies => "dispose_dependencies",
            _ => unreachable!("queue lifecycle handle registered in an invalid phase"),
        };
        let span = tracing::info_span!(
            "queue.shutdown.phase",
            lily.shutdown_category = category,
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let started = std::time::Instant::now();
        let result = async {
            match self.phase {
                FrameworkShutdownPhase::StopAdmission => self.provider.stop_admission_async().await,
                FrameworkShutdownPhase::DrainInFlight => self.provider.drain_async().await,
                FrameworkShutdownPhase::DisposeDependencies => self.provider.close_async().await,
                _ => unreachable!("queue lifecycle handle registered in an invalid phase"),
            }
        }
        .instrument(span.clone())
        .await;
        let outcome = if result.is_ok() { "success" } else { "error" };
        span.record("lily.outcome", outcome);
        if result.is_err() {
            span.record("lily.error_code", "QUEUE_SHUTDOWN_ERROR");
            span.record("otel.status_code", "ERROR");
        }
        self.metrics.record(category, outcome, started.elapsed());
        result.map_err(|error| {
            self.evidence.record(&error);
            ShutdownError::Component(format!(
                "queue shutdown phase failed [{}]",
                error.error_code()
            ))
        })
    }

    fn name(&self) -> &str {
        self.name
    }

    fn phase(&self) -> FrameworkShutdownPhase {
        self.phase
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn force_shutdown(&mut self) -> Option<FrameworkForceShutdownFuture<'_>> {
        let reason = if self.state.is_force_requested() {
            DeliveryCancellationReason::ForcedShutdown
        } else if self
            .deadlines
            .is_some_and(|deadlines| tokio::time::Instant::now() >= deadlines.graceful())
        {
            DeliveryCancellationReason::ShutdownDeadline
        } else {
            DeliveryCancellationReason::RuntimeFailure
        };
        // Synchronous notification is deliberate: even with no time left to
        // poll the returned future, cancellation evidence must be published.
        self.provider.cancel_execution(reason);
        let provider = Arc::clone(&self.provider);
        let phase = self.phase;
        let evidence = self.evidence.clone();
        Some(Box::pin(async move {
            let result = match phase {
                FrameworkShutdownPhase::StopAdmission => provider.stop_admission_async().await,
                FrameworkShutdownPhase::DrainInFlight => provider.force_drain_async().await,
                FrameworkShutdownPhase::DisposeDependencies => provider.close_async().await,
                _ => unreachable!("queue lifecycle handle registered in an invalid phase"),
            };
            result.map_err(|error| {
                evidence.record(&error);
                ShutdownError::Component(format!(
                    "queue forced shutdown phase failed [{}]",
                    error.error_code()
                ))
            })
        }))
    }
}

/// Registers queue admission, in-flight drain, and connection disposal as
/// separate ordered handles in the application-wide lifecycle report.
pub(crate) fn register_queue_lifecycle(
    coordinator: &mut FrameworkShutdownCoordinator,
    provider: Arc<dyn Queue>,
    timeout: Duration,
) -> QueueLifecycleEvidence {
    let metrics = QueueLifecycleMetrics::new();
    let evidence = QueueLifecycleEvidence::default();
    for (name, phase) in [
        (
            "queue-delivery-admission",
            FrameworkShutdownPhase::StopAdmission,
        ),
        (
            "queue-in-flight-drain",
            FrameworkShutdownPhase::DrainInFlight,
        ),
        (
            "queue-connection-dispose",
            FrameworkShutdownPhase::DisposeDependencies,
        ),
    ] {
        coordinator.register(QueueLifecycleHandle {
            name,
            phase,
            provider: Arc::clone(&provider),
            timeout,
            metrics: Arc::clone(&metrics),
            evidence: evidence.clone(),
            state: Arc::clone(coordinator.state()),
            deadlines: None,
        });
    }
    evidence
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use lily_error::application::{MessageBrokerError, message_broker::RabbitMQError};
    use lily_shutdown::{
        FrameworkComponentStatus, FrameworkForcedCleanupStatus, FrameworkShutdownCompletion,
        ShutdownAction, ShutdownSignal, ShutdownState,
    };
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    use crate::{
        DeliveryTerminalObservationsSnapshot, DeliveryTerminalSnapshot,
        queue_service::RegisteredQueueHandler,
    };

    use super::*;

    type FakeProviderFixture = (Arc<dyn Queue>, Arc<Mutex<Vec<LifecycleEvent>>>, Arc<Notify>);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum LifecycleEvent {
        StopAdmission,
        DrainStarted,
        ForceRequested,
        DrainCancelled,
        ForceDrain,
        Close,
        DiGraceful,
        DiForce,
    }

    #[test]
    fn lifecycle_evidence_is_ordered_distinct_and_bounded() {
        let evidence = QueueLifecycleEvidence::default();
        let errors = [
            MessageBrokerError::RabbitMQError(RabbitMQError::General("first".into())),
            MessageBrokerError::RabbitMQError(RabbitMQError::General("second".into())),
            MessageBrokerError::RabbitMQError(RabbitMQError::General("third".into())),
            MessageBrokerError::RabbitMQError(RabbitMQError::General("fourth".into())),
        ];

        evidence.record(&errors[0]);
        evidence.record(&errors[0]);
        for error in &errors[1..] {
            evidence.record(error);
        }

        assert_eq!(evidence.failures(), errors[..3]);
        assert_eq!(evidence.primary_error(), Some(errors[0].clone()));
    }

    #[derive(Clone, Copy)]
    enum ForceBehavior {
        Complete,
        Fail,
        Panic,
        Pending,
    }

    struct RecordOnDrop {
        ledger: Arc<Mutex<Vec<LifecycleEvent>>>,
        event: LifecycleEvent,
    }

    impl Drop for RecordOnDrop {
        fn drop(&mut self) {
            record(&self.ledger, self.event);
        }
    }

    struct FakeQueue {
        ledger: Arc<Mutex<Vec<LifecycleEvent>>>,
        drain_entered: Arc<Notify>,
        force_behavior: ForceBehavior,
        execution: crate::cancellation::DeliveryCancellationSource,
        deadlines: crate::shutdown_budget::QueueShutdownBudget,
    }

    impl FakeQueue {
        fn new(
            ledger: Arc<Mutex<Vec<LifecycleEvent>>>,
            drain_entered: Arc<Notify>,
            force_behavior: ForceBehavior,
        ) -> Self {
            Self {
                ledger,
                drain_entered,
                force_behavior,
                execution: crate::cancellation::DeliveryCancellationSource::new(),
                deadlines: crate::shutdown_budget::QueueShutdownBudget::default(),
            }
        }
    }

    #[async_trait]
    impl Queue for FakeQueue {
        fn set_shutdown_deadlines(&self, deadlines: QueueShutdownDeadlines) {
            self.deadlines.install(deadlines);
        }

        fn cancel_execution(&self, reason: DeliveryCancellationReason) {
            self.execution.cancel(reason);
        }

        async fn create_queue(
            &self,
            _exchange_name: &str,
            _queue: &str,
            _handler: RegisteredQueueHandler,
        ) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn start_async(
            &self,
            _cancellation: CancellationToken,
        ) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn stop_async(&self) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn stop_admission_async(&self) -> Result<(), MessageBrokerError> {
            record(&self.ledger, LifecycleEvent::StopAdmission);
            Ok(())
        }

        async fn drain_async(&self) -> Result<(), MessageBrokerError> {
            record(&self.ledger, LifecycleEvent::DrainStarted);
            self.drain_entered.notify_one();
            let _cancelled = RecordOnDrop {
                ledger: Arc::clone(&self.ledger),
                event: LifecycleEvent::DrainCancelled,
            };
            std::future::pending().await
        }

        async fn force_drain_async(&self) -> Result<(), MessageBrokerError> {
            record(&self.ledger, LifecycleEvent::ForceDrain);
            match self.force_behavior {
                ForceBehavior::Complete => Ok(()),
                ForceBehavior::Fail => Err(MessageBrokerError::RabbitMQError(
                    RabbitMQError::General("forced queue drain failed".into()),
                )),
                ForceBehavior::Panic => panic!("forced queue drain panicked"),
                ForceBehavior::Pending => std::future::pending().await,
            }
        }

        async fn close_async(&self) -> Result<(), MessageBrokerError> {
            record(&self.ledger, LifecycleEvent::Close);
            Ok(())
        }

        async fn wait_for_shutdown(&self) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        fn drain_reconciled(&self) -> bool {
            true
        }

        fn delivery_terminal_snapshot(&self) -> DeliveryTerminalSnapshot {
            DeliveryTerminalSnapshot::default()
        }

        fn delivery_terminal_observations(&self) -> DeliveryTerminalObservationsSnapshot {
            DeliveryTerminalObservationsSnapshot::default()
        }
    }

    fn record(ledger: &Mutex<Vec<LifecycleEvent>>, event: LifecycleEvent) {
        ledger
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event);
    }

    fn events(ledger: &Mutex<Vec<LifecycleEvent>>) -> Vec<LifecycleEvent> {
        ledger
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn fake_provider(behavior: ForceBehavior) -> FakeProviderFixture {
        let ledger = Arc::new(Mutex::new(Vec::new()));
        let drain_entered = Arc::new(Notify::new());
        let provider: Arc<dyn Queue> = Arc::new(FakeQueue::new(
            Arc::clone(&ledger),
            Arc::clone(&drain_entered),
            behavior,
        ));
        (provider, ledger, drain_entered)
    }

    fn drain_handle(provider: Arc<dyn Queue>, timeout: Duration) -> QueueLifecycleHandle {
        QueueLifecycleHandle {
            name: "queue-in-flight-drain",
            phase: FrameworkShutdownPhase::DrainInFlight,
            provider,
            timeout,
            metrics: QueueLifecycleMetrics::new(),
            evidence: QueueLifecycleEvidence::default(),
            state: Arc::new(ShutdownState::new()),
            deadlines: None,
        }
    }

    #[tokio::test]
    async fn expired_root_notifies_execution_even_when_force_future_cannot_be_polled() {
        let provider = Arc::new(FakeQueue::new(
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Notify::new()),
            ForceBehavior::Complete,
        ));
        let started = tokio::time::Instant::now() - Duration::from_secs(10);
        let deadlines = QueueShutdownDeadlines::starting_at(started, Duration::from_secs(1));
        let mut coordinator = FrameworkShutdownCoordinator::before(
            Arc::new(ShutdownState::new()),
            Duration::from_secs(1),
            deadlines.graceful(),
            deadlines.hard(),
        );
        register_queue_lifecycle(&mut coordinator, provider.clone(), Duration::from_secs(1));
        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        assert_eq!(provider.deadlines.deadlines(), Some(deadlines));
        assert_eq!(
            provider.execution.reason(),
            Some(DeliveryCancellationReason::ShutdownDeadline)
        );
        assert!(
            provider.ledger.lock().expect("phase evidence").is_empty(),
            "expired budget must not invent completed operations"
        );
        assert!(
            !report.is_terminal_complete(),
            "notification is not termination proof"
        );
    }

    #[tokio::test]
    async fn explicit_force_and_graceful_deadline_publish_distinct_reasons() {
        for explicit_force in [false, true] {
            let provider = Arc::new(FakeQueue::new(
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Notify::new()),
                ForceBehavior::Complete,
            ));
            let state = Arc::new(ShutdownState::new());
            if explicit_force {
                state.request_force();
            }
            let now = tokio::time::Instant::now();
            let deadlines = QueueShutdownDeadlines::before(
                if explicit_force {
                    now + Duration::from_secs(1)
                } else {
                    now
                },
                now + Duration::from_secs(1),
            );
            let mut coordinator = FrameworkShutdownCoordinator::before(
                state,
                Duration::from_secs(1),
                deadlines.graceful(),
                deadlines.hard(),
            );
            register_queue_lifecycle(&mut coordinator, provider.clone(), Duration::from_secs(1));
            coordinator.execute_report(ShutdownSignal::Manual).await;
            let expected = if explicit_force {
                DeliveryCancellationReason::ForcedShutdown
            } else {
                DeliveryCancellationReason::ShutdownDeadline
            };
            assert_eq!(provider.execution.reason(), Some(expected));
            assert_eq!(provider.deadlines.deadlines(), Some(deadlines));
        }
    }

    #[tokio::test]
    async fn force_drain_precedes_connection_close_and_di_force_cleanup() {
        let (provider, ledger, drain_entered) = fake_provider(ForceBehavior::Complete);
        let state = Arc::new(ShutdownState::new());
        let mut coordinator =
            FrameworkShutdownCoordinator::new(Arc::clone(&state), Duration::from_secs(1));

        let graceful_ledger = Arc::clone(&ledger);
        let force_ledger = Arc::clone(&ledger);
        coordinator.register(
            ShutdownAction::new(
                "consumer-di-container",
                FrameworkShutdownPhase::DisposeDependencies,
                Duration::from_secs(1),
                move || async move {
                    record(&graceful_ledger, LifecycleEvent::DiGraceful);
                    Ok(())
                },
            )
            .with_force(move || async move {
                record(&force_ledger, LifecycleEvent::DiForce);
                Ok(())
            }),
        );
        register_queue_lifecycle(&mut coordinator, provider, Duration::from_secs(1));

        let force_state = Arc::clone(&state);
        let force_ledger = Arc::clone(&ledger);
        let force_request = tokio::spawn(async move {
            drain_entered.notified().await;
            record(&force_ledger, LifecycleEvent::ForceRequested);
            force_state.request_force();
        });

        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        force_request.await.expect("force request task must join");

        assert_eq!(
            events(&ledger),
            vec![
                LifecycleEvent::StopAdmission,
                LifecycleEvent::DrainStarted,
                LifecycleEvent::ForceRequested,
                LifecycleEvent::DrainCancelled,
                LifecycleEvent::ForceDrain,
                LifecycleEvent::Close,
                LifecycleEvent::DiForce,
            ]
        );
        assert_eq!(
            report.completion,
            FrameworkShutdownCompletion::ForcedCompleted
        );
        assert_eq!(report.metrics.cancelled_by_force, 3);
        assert_eq!(report.metrics.forced_cleanup_completed, 3);
        assert!(report.reconciles());
    }

    #[tokio::test]
    async fn preforced_queue_lifecycle_runs_each_phase_specific_cleanup_exactly_once() {
        let (provider, ledger, _) = fake_provider(ForceBehavior::Complete);
        let state = Arc::new(ShutdownState::new());
        assert!(state.request_force());
        let mut coordinator = FrameworkShutdownCoordinator::new(state, Duration::from_secs(1));
        register_queue_lifecycle(&mut coordinator, provider, Duration::from_secs(1));

        let report = coordinator.execute_report(ShutdownSignal::Manual).await;

        assert_eq!(
            events(&ledger),
            vec![
                LifecycleEvent::StopAdmission,
                LifecycleEvent::ForceDrain,
                LifecycleEvent::Close,
            ]
        );
        assert_eq!(
            report.completion,
            FrameworkShutdownCompletion::ForcedCompleted
        );
        assert_eq!(report.metrics.cancelled_by_force, 3);
        assert_eq!(report.metrics.forced_cleanup_completed, 3);
        assert!(report.reconciles());
    }

    #[tokio::test]
    async fn queue_force_failure_is_typed_in_the_shutdown_report() {
        let (provider, ledger, _) = fake_provider(ForceBehavior::Fail);
        let mut coordinator = FrameworkShutdownCoordinator::new(
            Arc::new(ShutdownState::new()),
            Duration::from_secs(1),
        );
        coordinator.register(drain_handle(provider, Duration::from_secs(1)));

        let report = coordinator.execute_report(ShutdownSignal::Quit).await;
        let component = &report
            .phases
            .iter()
            .find(|phase| phase.phase == FrameworkShutdownPhase::DrainInFlight)
            .expect("drain phase")
            .components[0];

        assert_eq!(events(&ledger), vec![LifecycleEvent::ForceDrain]);
        assert_eq!(component.status, FrameworkComponentStatus::CancelledByForce);
        assert!(matches!(
            &component.forced_cleanup,
            FrameworkForcedCleanupStatus::Failed { error }
                if error.contains("BROKER_GENERAL")
                    && !error.contains("forced queue drain failed")
        ));
        assert_eq!(report.metrics.forced_cleanup_failed, 1);
        assert_eq!(report.completion, FrameworkShutdownCompletion::Incomplete);
    }

    #[tokio::test]
    async fn queue_force_panic_is_typed_without_escaping_the_coordinator() {
        let (provider, ledger, _) = fake_provider(ForceBehavior::Panic);
        let mut coordinator = FrameworkShutdownCoordinator::new(
            Arc::new(ShutdownState::new()),
            Duration::from_secs(1),
        );
        coordinator.register(drain_handle(provider, Duration::from_secs(1)));

        let report = coordinator.execute_report(ShutdownSignal::Quit).await;
        let component = &report
            .phases
            .iter()
            .find(|phase| phase.phase == FrameworkShutdownPhase::DrainInFlight)
            .expect("drain phase")
            .components[0];

        assert_eq!(events(&ledger), vec![LifecycleEvent::ForceDrain]);
        assert_eq!(component.status, FrameworkComponentStatus::CancelledByForce);
        assert!(matches!(
            &component.forced_cleanup,
            FrameworkForcedCleanupStatus::Panicked { message }
                if message == "forced queue drain panicked"
        ));
        assert_eq!(report.metrics.forced_cleanup_panicked, 1);
        assert_eq!(report.completion, FrameworkShutdownCompletion::Incomplete);
    }

    #[tokio::test]
    async fn queue_force_timeout_is_bounded_and_typed() {
        let (provider, ledger, _) = fake_provider(ForceBehavior::Pending);
        let mut coordinator = FrameworkShutdownCoordinator::new(
            Arc::new(ShutdownState::new()),
            Duration::from_millis(4),
        );
        coordinator.register(drain_handle(provider, Duration::from_secs(1)));

        let report = coordinator.execute_report(ShutdownSignal::Quit).await;
        let component = &report
            .phases
            .iter()
            .find(|phase| phase.phase == FrameworkShutdownPhase::DrainInFlight)
            .expect("drain phase")
            .components[0];

        assert_eq!(events(&ledger), vec![LifecycleEvent::ForceDrain]);
        assert_eq!(component.status, FrameworkComponentStatus::CancelledByForce);
        assert_eq!(
            component.forced_cleanup,
            FrameworkForcedCleanupStatus::TimedOut
        );
        assert_eq!(report.metrics.forced_cleanup_timed_out, 1);
        assert_eq!(report.completion, FrameworkShutdownCompletion::Incomplete);
        assert!(report.elapsed <= Duration::from_secs(1));
    }
}
