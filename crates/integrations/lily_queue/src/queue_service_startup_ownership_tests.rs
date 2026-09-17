use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lily_config::ConfigService;
use lily_error::application::{MessageBrokerError, message_broker::RabbitMQError};
use lily_error::injection::InjectionError;
use lily_injection::ApplicationContainer;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{QueueService, RegisteredQueueHandler};
use crate::queue_trait::Queue;
use crate::{DeliveryTerminalObservationsSnapshot, DeliveryTerminalSnapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupBehavior {
    Pending,
    Succeed,
    Fail,
    Panic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopBehavior {
    Succeed,
    Fail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    StartEntered,
    StartSucceeded,
    StartFailed,
    StartFutureDropped,
    StopEntered,
    StopSucceeded,
    StopFailed,
}

struct StartupState {
    startup: StartupBehavior,
    stop: StopBehavior,
    events: Mutex<Vec<Event>>,
    start_calls: AtomicUsize,
    stop_calls: AtomicUsize,
    resource_live: AtomicBool,
    start_entered: CancellationToken,
    stop_completed: CancellationToken,
}

impl StartupState {
    fn new(startup: StartupBehavior, stop: StopBehavior) -> Arc<Self> {
        Arc::new(Self {
            startup,
            stop,
            events: Mutex::new(Vec::new()),
            start_calls: AtomicUsize::new(0),
            stop_calls: AtomicUsize::new(0),
            resource_live: AtomicBool::new(false),
            start_entered: CancellationToken::new(),
            stop_completed: CancellationToken::new(),
        })
    }

    fn push(&self, event: Event) {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event);
    }

    fn events(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

struct StartFutureDropGuard {
    state: Arc<StartupState>,
    armed: bool,
}

impl StartFutureDropGuard {
    fn new(state: Arc<StartupState>) -> Self {
        Self { state, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StartFutureDropGuard {
    fn drop(&mut self) {
        if self.armed {
            self.state.push(Event::StartFutureDropped);
        }
    }
}

struct PartialStartQueue {
    state: Arc<StartupState>,
}

#[async_trait]
impl Queue for PartialStartQueue {
    async fn create_queue(
        &self,
        _exchange_name: &str,
        _queue: &str,
        _handler: RegisteredQueueHandler,
    ) -> Result<(), MessageBrokerError> {
        Ok(())
    }

    async fn start_async(&self, _ct: CancellationToken) -> Result<(), MessageBrokerError> {
        assert_eq!(
            self.state.start_calls.fetch_add(1, Ordering::SeqCst),
            0,
            "a provider generation must be started exactly once"
        );
        self.state.resource_live.store(true, Ordering::SeqCst);
        self.state.push(Event::StartEntered);
        self.state.start_entered.cancel();
        let mut drop_guard = StartFutureDropGuard::new(Arc::clone(&self.state));

        match self.state.startup {
            StartupBehavior::Pending => std::future::pending().await,
            StartupBehavior::Succeed => {
                drop_guard.disarm();
                self.state.push(Event::StartSucceeded);
                Ok(())
            }
            StartupBehavior::Fail => {
                drop_guard.disarm();
                self.state.push(Event::StartFailed);
                Err(start_failure())
            }
            StartupBehavior::Panic => panic!("transport-free provider startup panic"),
        }
    }

    async fn stop_async(&self) -> Result<(), MessageBrokerError> {
        let call = self.state.stop_calls.fetch_add(1, Ordering::SeqCst);
        self.state.push(Event::StopEntered);
        assert_eq!(call, 0, "provider stop authority must run exactly once");
        let result = match self.state.stop {
            StopBehavior::Succeed => {
                self.state.resource_live.store(false, Ordering::SeqCst);
                self.state.push(Event::StopSucceeded);
                Ok(())
            }
            StopBehavior::Fail => {
                self.state.push(Event::StopFailed);
                Err(stop_failure())
            }
        };
        self.state.stop_completed.cancel();
        result
    }

    async fn stop_admission_async(&self) -> Result<(), MessageBrokerError> {
        Ok(())
    }

    async fn drain_async(&self) -> Result<(), MessageBrokerError> {
        Ok(())
    }

    async fn force_drain_async(&self) -> Result<(), MessageBrokerError> {
        Ok(())
    }

    async fn close_async(&self) -> Result<(), MessageBrokerError> {
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

fn start_failure() -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::General(
        "transport-free provider start failed".into(),
    ))
}

fn stop_failure() -> MessageBrokerError {
    MessageBrokerError::RabbitMQError(RabbitMQError::General(
        "transport-free provider stop failed".into(),
    ))
}

fn builder(state: Arc<StartupState>) -> lily_injection::ApplicationContainerBuilder {
    let config_path = format!("/tmp/lily-capq06b-{}.toml", Uuid::new_v4());
    let config = ConfigService::development(&config_path);
    let provider: Arc<dyn Queue> = Arc::new(PartialStartQueue { state });
    let queue_service = QueueService {
        provider: Some(provider),
        config_service: Arc::new(ConfigService::development(config_path)),
    };
    ApplicationContainer::builder()
        .seed_singleton(config)
        .seed_singleton(queue_service)
}

fn find_initialization_cleanup(
    error: &InjectionError,
) -> Option<(&InjectionError, &InjectionError)> {
    match error {
        InjectionError::InitializationCleanupFailed {
            initialization,
            cleanup,
            ..
        } => Some((initialization, cleanup)),
        InjectionError::DependencyResolutionFailed { source, .. }
        | InjectionError::ServiceInitializationFailed { source, .. } => {
            find_initialization_cleanup(source)
        }
        InjectionError::StartupRollbackFailed { startup, .. } => {
            find_initialization_cleanup(startup)
        }
        _ => None,
    }
}

#[tokio::test]
async fn pending_provider_start_is_owned_before_di_cancellation() {
    let state = StartupState::new(StartupBehavior::Pending, StopBehavior::Succeed);
    let mut build =
        lily_injection::__private::begin_application_container_build(builder(Arc::clone(&state)));

    tokio::select! {
        biased;
        outcome = build.wait() => panic!("build unexpectedly completed: {outcome:?}"),
        () = state.start_entered.cancelled() => {}
    }
    build.cancel();
    let outcome = build.wait().await;
    assert!(matches!(
        outcome,
        lily_injection::__private::ApplicationContainerBuildOutcome::Cancelled(Ok(()))
    ));

    assert_eq!(state.start_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.stop_calls.load(Ordering::SeqCst), 1);
    assert!(!state.resource_live.load(Ordering::SeqCst));
    assert_eq!(
        state.events(),
        vec![
            Event::StartEntered,
            Event::StartFutureDropped,
            Event::StopEntered,
            Event::StopSucceeded,
        ]
    );
}

#[tokio::test]
async fn successful_provider_start_commits_until_container_shutdown() {
    let state = StartupState::new(StartupBehavior::Succeed, StopBehavior::Succeed);
    let container = builder(Arc::clone(&state))
        .build()
        .await
        .expect("provider startup must commit");

    assert_eq!(state.start_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.stop_calls.load(Ordering::SeqCst), 0);
    assert!(state.resource_live.load(Ordering::SeqCst));

    container.close().await.expect("container shutdown");
    container.close().await.expect("shutdown replay");
    assert_eq!(state.stop_calls.load(Ordering::SeqCst), 1);
    assert!(!state.resource_live.load(Ordering::SeqCst));
    assert_eq!(
        state.events(),
        vec![
            Event::StartEntered,
            Event::StartSucceeded,
            Event::StopEntered,
            Event::StopSucceeded,
        ]
    );
}

#[tokio::test]
async fn provider_start_failure_preserves_primary_and_cleans_up_once() {
    let state = StartupState::new(StartupBehavior::Fail, StopBehavior::Succeed);
    let error = builder(Arc::clone(&state))
        .build()
        .await
        .expect_err("provider startup must fail");

    assert_eq!(
        error
            .message_broker_error()
            .expect("typed broker primary")
            .error_code(),
        start_failure().error_code()
    );
    assert_eq!(state.start_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.stop_calls.load(Ordering::SeqCst), 1);
    assert!(!state.resource_live.load(Ordering::SeqCst));
}

#[tokio::test]
async fn provider_start_panic_still_runs_owned_cleanup_once() {
    let state = StartupState::new(StartupBehavior::Panic, StopBehavior::Succeed);
    let error = builder(Arc::clone(&state))
        .build()
        .await
        .expect_err("provider startup panic must be contained");

    assert!(
        error
            .to_string()
            .contains("service initialization panicked")
    );
    assert_eq!(state.start_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.stop_calls.load(Ordering::SeqCst), 1);
    assert!(!state.resource_live.load(Ordering::SeqCst));
}

#[tokio::test]
async fn failed_start_and_failed_cleanup_keep_typed_primary_and_secondary() {
    let state = StartupState::new(StartupBehavior::Fail, StopBehavior::Fail);
    let error = builder(Arc::clone(&state))
        .build()
        .await
        .expect_err("startup and cleanup must fail");

    let (initialization, cleanup) =
        find_initialization_cleanup(&error).expect("typed initialization cleanup evidence");
    assert_eq!(
        initialization
            .message_broker_error()
            .expect("startup broker primary")
            .error_code(),
        start_failure().error_code()
    );
    assert_eq!(
        cleanup
            .message_broker_error()
            .expect("cleanup broker secondary")
            .error_code(),
        stop_failure().error_code()
    );
    assert_eq!(state.stop_calls.load(Ordering::SeqCst), 1);
    assert!(state.resource_live.load(Ordering::SeqCst));
}

#[tokio::test]
async fn cancelled_start_with_failed_cleanup_is_not_reported_as_success() {
    let state = StartupState::new(StartupBehavior::Pending, StopBehavior::Fail);
    let mut build =
        lily_injection::__private::begin_application_container_build(builder(Arc::clone(&state)));

    tokio::select! {
        biased;
        outcome = build.wait() => panic!("build unexpectedly completed: {outcome:?}"),
        () = state.start_entered.cancelled() => {}
    }
    build.cancel();
    let outcome = build.wait().await;
    let lily_injection::__private::ApplicationContainerBuildOutcome::Cancelled(Err(
        InjectionError::ShutdownFailed { outcomes, .. },
    )) = outcome
    else {
        panic!("failed cancellation cleanup must remain typed: {outcome:?}")
    };

    assert!(outcomes.iter().any(|outcome| {
        outcome.component.contains("QueueService")
            && outcome.status == lily_error::injection::ShutdownOutcomeStatus::Failed
    }));
    assert_eq!(state.stop_calls.load(Ordering::SeqCst), 1);
    assert!(state.resource_live.load(Ordering::SeqCst));
    assert_eq!(
        state.events(),
        vec![
            Event::StartEntered,
            Event::StartFutureDropped,
            Event::StopEntered,
            Event::StopFailed,
        ]
    );
}
