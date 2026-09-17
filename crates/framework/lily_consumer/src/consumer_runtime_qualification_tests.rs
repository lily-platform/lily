use super::runtime_owner::{
    owned_tracing_shutdown_reports_for_test, ConsumerRuntimeOwner, ConsumerRuntimeWaiterGuard,
};
use super::*;
use lily_error::application::{
    message_broker::{RabbitMQError, RabbitMqConsumerTaskFailureKind, RabbitMqConsumerTaskRole},
    MessageBrokerError,
};
use lily_queue::__private::{
    QueueRuntimeHandle, QueueServiceTestLifecycleCall as LifecycleCall, QueueServiceTestProbe,
};
use lily_trace::{tracing_runtime_status, TraceConfig, TracingRuntimeStatus};
use std::{
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex as StdMutex,
    },
};
use tempfile::NamedTempFile;
use tokio::runtime::Handle;

const QUALIFICATION_TIMEOUT: Duration = Duration::from_secs(5);
const OWNED_HANDOFF_CHILD_CASE: &str = "LILY_CONSUMER_CAPQ06F_OWNED_HANDOFF_CHILD";
const OWNED_HANDOFF_TEST_NAME: &str = concat!(
    "consumer::runtime_qualification_tests::",
    "managed_readiness_handoff_drop_closes_owned_di_and_tracing_once"
);
const OWNED_HANDOFF_COMPLETION_MARKER: &str = "CAP_Q_06F_OWNED_HANDOFF_COMPLETE";

const TRANSPORT_FREE_CONFIG: &str = r#"
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
reconnect_backoff_millis = 1
persistence_enabled = true

[[rabbitmq.topology.queues]]
name = "capq01d.consumer-plan.alpha"
exchange_name = "capq06f"
routing_key = "capq01d.consumer-plan.alpha"
retention = { main_max_messages = 1000, main_max_bytes = 1048576, retry_bucket_max_messages = 100, retry_bucket_max_bytes = 262144, dead_letter_max_messages = 100, dead_letter_max_bytes = 262144 }
concurrency = 1
prefetch_count = 1
retry_attempts = 1
retry_backoff_millis = 250
max_retry_backoff_millis = 5000
durable = true
"#;

const SECOND_QUEUE_CONFIG: &str = r#"
[[rabbitmq.topology.queues]]
name = "capq01d.consumer-plan.beta"
exchange_name = "capq06f"
routing_key = "capq01d.consumer-plan.beta"
retention = { main_max_messages = 1000, main_max_bytes = 1048576, retry_bucket_max_messages = 100, retry_bucket_max_bytes = 262144, dead_letter_max_messages = 100, dead_letter_max_bytes = 262144 }
concurrency = 1
prefetch_count = 1
retry_attempts = 1
retry_backoff_millis = 250
max_retry_backoff_millis = 5000
durable = true
"#;

const ONE_SECOND_SHUTDOWN_CONFIG: &str = r#"
[lifecycle]
shutdown_timeout_secs = 1
"#;

struct TransportFreeFixture {
    _config_file: NamedTempFile,
    container: Arc<ApplicationContainer>,
    provider: QueueRuntimeHandle,
    probe: QueueServiceTestProbe,
}

fn managed_consumer_for_test(
    shutdown_state: Arc<ShutdownState>,
    task: JoinHandle<Result<(), ConsumerError>>,
) -> ManagedConsumer {
    let (queue_service, _probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
        ConfigService::development("/tmp/lily-capq06f-managed-observer.toml"),
    ));
    ManagedConsumer::new(
        ManagedConsumerStartup {
            shutdown_report: Arc::default(),
            shutdown_state,
            provider: queue_runtime(&queue_service).expect("test queue runtime"),
            configured_queues: 0,
            registered_handlers: 0,
        },
        Arc::new(ConsumerOperationalState::default()),
        task,
    )
}

const fn managed_test_runtime_failure() -> ConsumerError {
    ConsumerError::Configuration(ConsumerConfigurationFailure::RabbitMqConsumerMissing)
}

async fn transport_free_fixture() -> TransportFreeFixture {
    transport_free_fixture_with_config(TRANSPORT_FREE_CONFIG).await
}

async fn transport_free_fixture_with_config(config: &str) -> TransportFreeFixture {
    let config_file = NamedTempFile::new().expect("temporary Consumer config");
    std::fs::write(config_file.path(), config).expect("write transport-free Consumer config");
    let (queue_service, probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
        ConfigService::development(config_file.path()),
    ));
    let provider = queue_runtime(&queue_service).expect("test queue runtime");
    let container = Arc::new(
        crate::test_application_container_builder()
            .seed_singleton(ConfigService::development(config_file.path()))
            .seed_singleton(queue_service)
            .build()
            .await
            .expect("build transport-free Consumer container"),
    );
    TransportFreeFixture {
        _config_file: config_file,
        container,
        provider,
        probe,
    }
}

#[derive(Default)]
struct PendingMiddlewareProbe {
    entered: AtomicBool,
    dropped: AtomicBool,
    changed: tokio::sync::Notify,
}

impl PendingMiddlewareProbe {
    fn mark_entered(&self) {
        self.entered.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    fn mark_dropped(&self) {
        self.dropped.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }

    async fn wait_for(&self, predicate: impl Fn(&Self) -> bool) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if predicate(self) {
                return;
            }
            changed.as_mut().await;
        }
    }
}

static PENDING_MIDDLEWARE_PROBE: StdMutex<Option<Arc<PendingMiddlewareProbe>>> =
    StdMutex::new(None);

struct PendingQualificationMiddleware;

struct PendingMiddlewareDrop(Arc<PendingMiddlewareProbe>);

impl Drop for PendingMiddlewareDrop {
    fn drop(&mut self) {
        self.0.mark_dropped();
    }
}

#[lily_queue::async_trait]
impl lily_queue::QueueMiddleware for PendingQualificationMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, lily_queue::QueuePipelineComponentInitError> {
        let probe = PENDING_MIDDLEWARE_PROBE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .expect("pending middleware qualification probe must be installed");
        let _drop = PendingMiddlewareDrop(Arc::clone(&probe));
        probe.mark_entered();
        std::future::pending().await
    }
}

#[derive(Default, lily_injection::Injectable)]
#[service(lifetime = "Singleton")]
struct PendingConsumerDiBuildService {
    state: Option<Arc<PendingConsumerDiBuildState>>,
}

#[derive(Default)]
struct PendingConsumerDiBuildState {
    events: StdMutex<Vec<&'static str>>,
    initialized: tokio::sync::Notify,
    disposed: tokio::sync::Notify,
}

impl PendingConsumerDiBuildState {
    async fn wait_for_event(&self, event: &'static str) {
        loop {
            let changed = if event == "dispose" {
                self.disposed.notified()
            } else {
                self.initialized.notified()
            };
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains(&event)
            {
                return;
            }
            changed.as_mut().await;
        }
    }
}

struct PendingConsumerDiInitializeDrop(Arc<PendingConsumerDiBuildState>);

impl Drop for PendingConsumerDiInitializeDrop {
    fn drop(&mut self) {
        self.0
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("initialize-drop");
    }
}

#[async_trait::async_trait]
impl lily_injection::ServiceTrait for PendingConsumerDiBuildService {
    async fn initialize(&mut self) -> Result<(), lily_error::injection::InjectionError> {
        let Some(state) = self.state.clone() else {
            return Ok(());
        };
        state
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("initialize-enter");
        state.initialized.notify_waiters();
        let _drop = PendingConsumerDiInitializeDrop(state);
        std::future::pending().await
    }

    async fn dispose(&self) -> Result<(), lily_error::injection::InjectionError> {
        let Some(state) = self.state.as_ref() else {
            return Ok(());
        };
        state
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("dispose");
        state.disposed.notify_waiters();
        Ok(())
    }
}

async fn wait_for_lifecycle(probe: &QueueServiceTestProbe, call: LifecycleCall, expected: usize) {
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        probe.wait_for_lifecycle_count(call, expected),
    )
    .await
    .expect("transport-free lifecycle call must arrive");
}

async fn wait_for_lifecycle_completion(
    probe: &QueueServiceTestProbe,
    call: LifecycleCall,
    expected: usize,
) {
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        probe.wait_for_lifecycle_completion(call, expected),
    )
    .await
    .expect("transport-free lifecycle call must complete");
}

async fn assert_aborted<T>(task: JoinHandle<T>) {
    let result = tokio::time::timeout(QUALIFICATION_TIMEOUT, task)
        .await
        .expect("aborted outer waiter must terminate");
    let join_error = match result {
        Ok(_) => panic!("outer waiter must report task cancellation"),
        Err(error) => error,
    };
    assert!(join_error.is_cancelled());
}

async fn close_caller_container(
    container: &ApplicationContainer,
    probe: &QueueServiceTestProbe,
    expected_close_count: usize,
) {
    container
        .close_with_timeout(QUALIFICATION_TIMEOUT)
        .await
        .expect("caller remains able to close its DI container");
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::StopAsync),
        1
    );
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::CloseAsync),
        expected_close_count,
        "caller DI disposal must not race a later duplicate Consumer cleanup"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_runtime_waiter_abort_detaches_one_canonical_cleanup_authority() {
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    let outer = tokio::spawn(
        ConsumerBuilder::new()
            .container(Arc::clone(&fixture.container))
            .run_with_cancellation(CancellationToken::new()),
    );
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        fixture.probe.wait_for_runtime_wait(1),
    )
    .await
    .expect("Consumer must reach runtime wait");

    outer.abort();
    assert_aborted(outer).await;
    wait_for_lifecycle_completion(&fixture.probe, LifecycleCall::CloseAsync, 1).await;

    assert_eq!(
        fixture.probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::WaitForShutdown,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::CloseAsync,
        ]
    );
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .count(LifecycleCall::StopAsync),
        0
    );
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::WaitForShutdown),
        0
    );
    fixture.probe.release_runtime_completion(1);
    tokio::task::yield_now().await;
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::WaitForShutdown),
        0,
        "aborted runtime wait must not remain detached"
    );
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
}

#[tokio::test(start_paused = true)]
async fn delayed_runtime_owner_cannot_restart_an_expired_shutdown_budget() {
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    let state = Arc::new(ShutdownState::new());
    state
        .initiate_shutdown(ShutdownSignal::Manual)
        .expect("durable shutdown request");
    tokio::time::advance(Duration::from_millis(11)).await;
    let owner = ConsumerRuntimeOwner::new(
        Handle::current(),
        fixture.provider.clone(),
        None,
        None,
        Arc::clone(&state),
        Duration::from_millis(10),
        None,
        tracing::Span::none(),
    );
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let result = owner
        .spawn(
            ConsumerStartup {
                configured_queues: 0,
                registered_handlers: 0,
            },
            ConsumerLifecycleTrigger::Cancellation(cancellation),
        )
        .await
        .expect("supervisor must join");
    assert!(
        result.is_err(),
        "an expired root must report incomplete cleanup, not start another budget"
    );
    let snapshot = fixture.probe.lifecycle_snapshot();
    for call in [
        LifecycleCall::StopAdmissionAsync,
        LifecycleCall::DrainAsync,
        LifecycleCall::ForceDrainAsync,
        LifecycleCall::CloseAsync,
    ] {
        assert!(
            !snapshot.calls().contains(&call),
            "an expired cleanup operation must not be polled: {call:?}"
        );
    }
    close_caller_container(&fixture.container, &fixture.probe, 0).await;
}

#[tokio::test(start_paused = true)]
async fn waiter_abort_and_cancellation_force_one_stalled_drain_to_reconciled_close() {
    const CLEANUP_TIMEOUT: Duration = Duration::from_millis(10);

    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    fixture.probe.pause_lifecycle(LifecycleCall::DrainAsync);
    let shutdown_state = Arc::new(ShutdownState::new());
    let cancellation = CancellationToken::new();
    let owner = ConsumerRuntimeOwner::new(
        Handle::current(),
        fixture.provider.clone(),
        None,
        None,
        Arc::clone(&shutdown_state),
        CLEANUP_TIMEOUT,
        None,
        tracing::Span::none(),
    );
    let supervisor = owner.spawn(
        ConsumerStartup {
            configured_queues: 0,
            registered_handlers: 0,
        },
        ConsumerLifecycleTrigger::Cancellation(cancellation.clone()),
    );
    let supervisor_status = supervisor.abort_handle();
    let waiter_state = Arc::clone(&shutdown_state);
    let (waiter_ready_tx, waiter_ready_rx) = tokio::sync::oneshot::channel();
    let outer = tokio::spawn(async move {
        let mut guard = ConsumerRuntimeWaiterGuard::new(waiter_state);
        let _ = waiter_ready_tx.send(());
        let result = match supervisor.await {
            Ok(result) => result,
            Err(error) => Err(crate::consumer::owned_tasks::join_failure(error)),
        };
        guard.disarm();
        result
    });
    tokio::time::timeout(QUALIFICATION_TIMEOUT, waiter_ready_rx)
        .await
        .expect("the direct runtime waiter must install its Drop guard")
        .expect("the direct runtime waiter must remain alive");
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        fixture.probe.wait_for_runtime_wait(1),
    )
    .await
    .expect("Consumer must reach runtime wait");

    let logical_cleanup_started = tokio::time::Instant::now();
    outer.abort();
    assert_aborted(outer).await;
    assert!(
        shutdown_state.is_shutdown_initiated(),
        "aborting the armed waiter must durably initiate shutdown"
    );
    cancellation.cancel();
    wait_for_lifecycle(&fixture.probe, LifecycleCall::DrainAsync, 1).await;
    wait_for_lifecycle_completion(&fixture.probe, LifecycleCall::CloseAsync, 1).await;
    tokio::time::timeout(QUALIFICATION_TIMEOUT, async {
        while !supervisor_status.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the ten-millisecond cleanup supervisor must finish in logical time");
    let logical_cleanup_elapsed = logical_cleanup_started.elapsed();
    assert!(
        !logical_cleanup_elapsed.is_zero() && logical_cleanup_elapsed <= CLEANUP_TIMEOUT,
        "the stalled graceful drain must consume only the production ten-millisecond logical budget"
    );

    let terminal = fixture.probe.lifecycle_snapshot();
    assert_eq!(
        terminal.calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::WaitForShutdown,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::ForceDrainAsync,
            LifecycleCall::CloseAsync,
        ]
    );
    for call in [
        LifecycleCall::StopAdmissionAsync,
        LifecycleCall::DrainAsync,
        LifecycleCall::ForceDrainAsync,
        LifecycleCall::CloseAsync,
    ] {
        assert_eq!(terminal.count(call), 1, "{call:?} must run exactly once");
    }
    assert_eq!(
        terminal.completion_count(LifecycleCall::DrainAsync),
        0,
        "the timed-out graceful drain must be cancelled"
    );
    assert_eq!(terminal.completion_count(LifecycleCall::ForceDrainAsync), 1);
    assert_eq!(terminal.completion_count(LifecycleCall::CloseAsync), 1);
    assert!(fixture.provider.drain_reconciled());

    fixture.probe.release_runtime_completion(1);
    fixture
        .probe
        .release_lifecycle(LifecycleCall::DrainAsync, 1);
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert_eq!(
        fixture.probe.lifecycle_snapshot(),
        terminal,
        "terminal gate releases must not reveal detached lifecycle work"
    );
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn builder_di_build_abort_reaps_pending_initializer_and_owned_rollback() {
    let config_file = NamedTempFile::new().expect("temporary Consumer config");
    std::fs::write(config_file.path(), TRANSPORT_FREE_CONFIG)
        .expect("write transport-free Consumer config");
    let (queue_service, _probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
        ConfigService::development(config_file.path()),
    ));
    let state = Arc::new(PendingConsumerDiBuildState::default());
    let container_builder = crate::test_application_container_builder()
        .seed_singleton(ConfigService::development(config_file.path()))
        .seed_singleton(queue_service)
        .seed_singleton(PendingConsumerDiBuildService {
            state: Some(Arc::clone(&state)),
        });
    let cancellation = CancellationToken::new();
    let outer = tokio::spawn(
        ConsumerBuilder::new()
            .framework_owned_container_builder_for_test(container_builder)
            .run_with_cancellation(cancellation.clone()),
    );
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        state.wait_for_event("initialize-enter"),
    )
    .await
    .expect("ConsumerBuilder must poll the pending DI initializer");

    cancellation.cancel();
    tokio::time::timeout(QUALIFICATION_TIMEOUT, outer)
        .await
        .expect("DI build cancellation and rollback must remain bounded")
        .expect("Consumer task must not panic")
        .expect("startup cancellation with successful DI rollback is successful");
    tokio::time::timeout(QUALIFICATION_TIMEOUT, state.wait_for_event("dispose"))
        .await
        .expect("owned DI rollback must dispose the cancelled partial service");
    assert_eq!(
        *state
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        ["initialize-enter", "initialize-drop", "dispose"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signals_profile_waiter_abort_closes_owned_queue_di_and_signal_monitor() {
    let config_file = NamedTempFile::new().expect("temporary Consumer config");
    std::fs::write(config_file.path(), TRANSPORT_FREE_CONFIG)
        .expect("write transport-free Consumer config");
    let (queue_service, probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
        ConfigService::development(config_file.path()),
    ));
    probe.pause_runtime_completion();
    let container_builder = crate::test_application_container_builder()
        .seed_singleton(ConfigService::development(config_file.path()))
        .seed_singleton(queue_service);
    let outer = tokio::spawn(
        ConsumerBuilder::new()
            .framework_owned_container_builder_for_test(container_builder)
            .run(),
    );
    tokio::time::timeout(QUALIFICATION_TIMEOUT, probe.wait_for_runtime_wait(1))
        .await
        .expect("Signals profile must install its monitor and reach runtime wait");

    outer.abort();
    assert_aborted(outer).await;
    wait_for_lifecycle_completion(&probe, LifecycleCall::CloseAsync, 1).await;
    wait_for_lifecycle_completion(&probe, LifecycleCall::StopAsync, 1).await;
    assert_eq!(
        probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::WaitForShutdown,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::CloseAsync,
            LifecycleCall::StopAsync,
        ]
    );
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::WaitForShutdown),
        0,
        "the losing provider wait must be cancelled when Manual state wakes the signal monitor"
    );
    probe.release_runtime_completion(1);
    tokio::task::yield_now().await;
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::WaitForShutdown),
        0,
        "releasing a cancelled wait must not reveal a detached provider observer"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_resolution_abort_drops_activity_before_canonical_queue_cleanup() {
    let fixture = transport_free_fixture().await;
    let barrier = ConfigResolutionBarrier::new();
    let cancellation = CancellationToken::new();
    let outer = tokio::spawn(
        ConsumerBuilder::new()
            .container(Arc::clone(&fixture.container))
            .config_resolution_barrier(barrier.clone())
            .run_with_cancellation(cancellation.clone()),
    );
    tokio::time::timeout(QUALIFICATION_TIMEOUT, barrier.wait_until_entered())
        .await
        .expect("Consumer must enter tracked config resolution");

    cancellation.cancel();
    tokio::time::timeout(QUALIFICATION_TIMEOUT, outer)
        .await
        .expect("config-resolution cancellation and rollback must remain bounded")
        .expect("Consumer task must not panic")
        .expect("startup cancellation with successful cleanup is successful");
    tokio::time::timeout(QUALIFICATION_TIMEOUT, barrier.wait_until_activity_dropped())
        .await
        .expect("tracked config activity must be dropped before rollback completes");
    let terminal_calls = fixture.probe.lifecycle_snapshot().calls().to_vec();
    barrier.release();
    tokio::task::yield_now().await;

    assert_eq!(
        terminal_calls.as_slice(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::CloseAsync,
        ],
        "the tracked config future must be dropped before the canonical queue lifecycle"
    );
    assert_eq!(fixture.probe.lifecycle_snapshot().calls(), terminal_calls);
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_pipeline_constructor_abort_drops_constructor_before_queue_cleanup() {
    let fixture = transport_free_fixture().await;
    let probe = Arc::new(PendingMiddlewareProbe::default());
    *PENDING_MIDDLEWARE_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&probe));
    let cancellation = CancellationToken::new();
    let outer = tokio::spawn(
        ConsumerBuilder::new()
            .container(Arc::clone(&fixture.container))
            .middleware::<PendingQualificationMiddleware>()
            .run_with_cancellation(cancellation.clone()),
    );
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        probe.wait_for(|probe| probe.entered.load(Ordering::Acquire)),
    )
    .await
    .expect("Consumer plan must enter the real pending middleware constructor");

    cancellation.cancel();
    tokio::time::timeout(QUALIFICATION_TIMEOUT, outer)
        .await
        .expect("pending constructor cancellation and rollback must remain bounded")
        .expect("Consumer task must not panic")
        .expect("startup cancellation with successful cleanup is successful");
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        probe.wait_for(|probe| probe.dropped.load(Ordering::Acquire)),
    )
    .await
    .expect("pending constructor future must be synchronously reaped");

    assert_eq!(
        fixture.probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::CloseAsync,
        ]
    );
    assert!(probe.dropped.load(Ordering::Acquire));
    tokio::task::yield_now().await;
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .count(LifecycleCall::CloseAsync),
        1,
        "constructor cancellation must not leave a detached rollback authority"
    );
    *PENDING_MIDDLEWARE_PROBE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_queue_registration_abort_rolls_back_one_admitted_prefix_exactly_once() {
    let config = format!("{TRANSPORT_FREE_CONFIG}\n{SECOND_QUEUE_CONFIG}");
    let fixture = transport_free_fixture_with_config(&config).await;
    fixture.probe.pause_registration();
    let cancellation = CancellationToken::new();
    let outer = tokio::spawn(
        ConsumerBuilder::new()
            .container(Arc::clone(&fixture.container))
            .run_with_cancellation(cancellation.clone()),
    );
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        fixture.probe.wait_for_registration_attempt(1),
    )
    .await
    .expect("first queue registration must reach the provider gate");
    fixture.probe.release_registration(1);
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        fixture.probe.wait_for_registration_attempt(2),
    )
    .await
    .expect("second queue registration must reach the provider gate");
    assert_eq!(fixture.probe.registration_count(), 1);

    cancellation.cancel();
    tokio::time::timeout(QUALIFICATION_TIMEOUT, outer)
        .await
        .expect("second registration cancellation must remain bounded")
        .expect("Consumer task must not panic")
        .expect("partial-registration rollback must succeed");

    assert_eq!(fixture.probe.registration_attempt_count(), 2);
    assert_eq!(fixture.probe.registration_count(), 1);
    assert_eq!(fixture.probe.cancelled_registration_count(), 1);
    assert_eq!(
        fixture.probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::CloseAsync,
        ]
    );
    fixture.probe.release_registration(1);
    tokio::task::yield_now().await;
    assert_eq!(fixture.probe.registration_count(), 1);
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_waiter_abort_retains_owned_di_until_queue_reconciliation() {
    let config_file = NamedTempFile::new().expect("temporary Consumer config");
    std::fs::write(config_file.path(), TRANSPORT_FREE_CONFIG)
        .expect("write transport-free Consumer config");
    let (queue_service, probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
        ConfigService::development(config_file.path()),
    ));
    probe.pause_runtime_completion();
    probe.pause_lifecycle(LifecycleCall::DrainAsync);
    let mut transaction =
        ConsumerBuildTransaction::new(Handle::current(), None, QUALIFICATION_TIMEOUT);
    let container = transaction
        .build_owned_container(
            crate::test_application_container_builder()
                .seed_singleton(ConfigService::development(config_file.path()))
                .seed_singleton(queue_service),
        )
        .await
        .expect("build framework-owned transport-free container");
    let queue_service = container
        .resolve::<QueueService>(None)
        .await
        .expect("resolve framework-owned queue service");
    transaction.retain_queue_runtime(
        queue_runtime(&queue_service).expect("retain framework-owned queue runtime"),
    );
    let shutdown_state = Arc::new(ShutdownState::new());
    let owner = transaction
        .commit()
        .into_runtime_owner(Arc::clone(&shutdown_state));
    let cancellation = CancellationToken::new();
    let supervisor = owner.spawn(
        ConsumerStartup {
            configured_queues: 1,
            registered_handlers: 1,
        },
        ConsumerLifecycleTrigger::Cancellation(cancellation.clone()),
    );
    let waiter = tokio::spawn(async move {
        let mut guard = ConsumerRuntimeWaiterGuard::new(shutdown_state);
        match supervisor.await {
            Ok(result) => {
                guard.disarm();
                result
            }
            Err(error) => Err(crate::consumer::owned_tasks::join_failure(error)),
        }
    });
    tokio::time::timeout(QUALIFICATION_TIMEOUT, probe.wait_for_runtime_wait(1))
        .await
        .expect("owned Consumer must reach runtime wait");

    cancellation.cancel();
    wait_for_lifecycle(&probe, LifecycleCall::DrainAsync, 1).await;
    waiter.abort();
    assert_aborted(waiter).await;
    probe.release_lifecycle(LifecycleCall::DrainAsync, 1);
    wait_for_lifecycle_completion(&probe, LifecycleCall::CloseAsync, 1).await;
    wait_for_lifecycle_completion(&probe, LifecycleCall::StopAsync, 1).await;

    assert_eq!(
        probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::WaitForShutdown,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::CloseAsync,
            LifecycleCall::StopAsync,
        ]
    );
    drop(container);
    drop(config_file);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreconciled_queue_withholds_framework_owned_di_disposal() {
    let config_file = NamedTempFile::new().expect("temporary Consumer config");
    std::fs::write(config_file.path(), TRANSPORT_FREE_CONFIG)
        .expect("write transport-free Consumer config");
    let (queue_service, probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
        ConfigService::development(config_file.path()),
    ));
    probe.pause_runtime_completion();
    probe.set_drain_reconciled(false);
    let mut transaction =
        ConsumerBuildTransaction::new(Handle::current(), None, QUALIFICATION_TIMEOUT);
    let container = transaction
        .build_owned_container(
            crate::test_application_container_builder()
                .seed_singleton(ConfigService::development(config_file.path()))
                .seed_singleton(queue_service),
        )
        .await
        .expect("build framework-owned transport-free container");
    let queue_service = container
        .resolve::<QueueService>(None)
        .await
        .expect("resolve framework-owned queue service");
    transaction.retain_queue_runtime(
        queue_runtime(&queue_service).expect("retain framework-owned queue runtime"),
    );
    let shutdown_state = Arc::new(ShutdownState::new());
    let owner = transaction
        .commit()
        .into_runtime_owner(Arc::clone(&shutdown_state));
    let cancellation = CancellationToken::new();
    let supervisor = owner.spawn(
        ConsumerStartup {
            configured_queues: 1,
            registered_handlers: 1,
        },
        ConsumerLifecycleTrigger::Cancellation(cancellation.clone()),
    );
    tokio::time::timeout(QUALIFICATION_TIMEOUT, probe.wait_for_runtime_wait(1))
        .await
        .expect("owned Consumer must reach runtime wait");

    cancellation.cancel();
    let error = tokio::time::timeout(QUALIFICATION_TIMEOUT, supervisor)
        .await
        .expect("unreconciled cleanup must remain bounded")
        .expect("runtime owner supervisor must not panic")
        .expect_err("unreconciled queue must fail closed");

    assert_eq!(error.error_code(), "CONSUMER_SHUTDOWN_PRIMARY_FAILURE");
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::CloseAsync),
        1
    );
    assert_eq!(
        probe.lifecycle_snapshot().count(LifecycleCall::StopAsync),
        0,
        "DI disposal must be withheld until queue reconciliation is proven"
    );
    probe.set_drain_reconciled(true);
    container
        .close_with_timeout(QUALIFICATION_TIMEOUT)
        .await
        .expect("test cleanup closes the deliberately withheld container");
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::StopAsync),
        1
    );
    drop(config_file);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_managed_startup_receiver_requests_durable_manual_cleanup() {
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    let queue_service = fixture
        .container
        .resolve::<QueueService>(None)
        .await
        .expect("resolve caller-owned queue service");
    let mut transaction =
        ConsumerBuildTransaction::new(Handle::current(), None, QUALIFICATION_TIMEOUT);
    transaction.retain_queue_runtime(
        queue_runtime(&queue_service).expect("retain caller-owned queue runtime"),
    );
    let shutdown_state = Arc::new(ShutdownState::new());
    let owner = transaction
        .commit()
        .into_runtime_owner(Arc::clone(&shutdown_state));
    let (startup_tx, startup_rx) = oneshot::channel();
    drop(startup_rx);
    let supervisor = owner.spawn(
        ConsumerStartup {
            configured_queues: 1,
            registered_handlers: 1,
        },
        ConsumerLifecycleTrigger::Managed {
            startup_tx,
            waiter_cancellation: CancellationToken::new(),
        },
    );

    tokio::time::timeout(QUALIFICATION_TIMEOUT, supervisor)
        .await
        .expect("closed readiness receiver cleanup must remain bounded")
        .expect("managed owner supervisor must not panic")
        .expect("closed readiness receiver uses successful Manual cleanup");

    assert_eq!(
        shutdown_state.initial_signal(),
        Some(ShutdownSignal::Manual)
    );
    assert_eq!(
        fixture.probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::WaitForShutdown,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::CloseAsync,
        ]
    );
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::CloseAsync),
        1
    );
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_waiter_drop_mid_registration_cancels_without_headless_runtime() {
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_registration();
    let outer = tokio::spawn(
        ConsumerBuilder::new()
            .container(Arc::clone(&fixture.container))
            .start_managed(),
    );
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        fixture.probe.wait_for_registration_attempt(1),
    )
    .await
    .expect("managed startup must reach registration");

    outer.abort();
    assert_aborted(outer).await;
    wait_for_lifecycle_completion(&fixture.probe, LifecycleCall::CloseAsync, 1).await;
    assert_eq!(fixture.probe.registration_count(), 0);
    assert_eq!(fixture.probe.cancelled_registration_count(), 1);
    assert_eq!(
        fixture.probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::CloseAsync,
        ]
    );

    fixture.probe.release_registration(1);
    tokio::task::yield_now().await;
    assert_eq!(fixture.probe.registration_count(), 0);
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .count(LifecycleCall::CloseAsync),
        1
    );
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .count(LifecycleCall::StopAsync),
        0
    );
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_readiness_handoff_drop_closes_owned_di_and_tracing_once() {
    if std::env::var_os(OWNED_HANDOFF_CHILD_CASE).is_none() {
        let output =
            Command::new(std::env::current_exe().expect("current Consumer test executable"))
                .arg("--exact")
                .arg(OWNED_HANDOFF_TEST_NAME)
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env(OWNED_HANDOFF_CHILD_CASE, "1")
                .output()
                .expect("spawn isolated Consumer tracing-owner qualification");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "isolated owned handoff failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert!(
            stdout.contains(OWNED_HANDOFF_COMPLETION_MARKER),
            "isolated owned handoff test did not execute its child body: {stdout}"
        );
        assert!(
            !stderr.contains("tracing runtime owner dropped without awaited shutdown"),
            "owned tracing must use its awaited lifecycle action: {stderr}"
        );
        return;
    }

    assert_eq!(
        tracing_runtime_status(),
        TracingRuntimeStatus::Uninitialized
    );
    assert!(owned_tracing_shutdown_reports_for_test().is_empty());
    let config_file = NamedTempFile::new().expect("temporary Consumer config");
    std::fs::write(config_file.path(), TRANSPORT_FREE_CONFIG)
        .expect("write transport-free Consumer config");
    let (queue_service, probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
        ConfigService::development(config_file.path()),
    ));
    probe.pause_runtime_completion();
    let container_builder = crate::test_application_container_builder()
        .seed_singleton(ConfigService::development(config_file.path()))
        .seed_singleton(queue_service);
    let barrier = ManagedStartupHandoffBarrier::new();
    let outer = tokio::spawn(
        ConsumerBuilder::new()
            .framework_owned_container_builder_for_test(container_builder)
            .tracing_config(TraceConfig {
                enabled: true,
                service_name: "cap-q-06f-owned-handoff".to_string(),
                ..TraceConfig::default()
            })
            .managed_startup_handoff_barrier(barrier.clone())
            .start_managed(),
    );
    tokio::time::timeout(QUALIFICATION_TIMEOUT, barrier.wait_until_entered())
        .await
        .expect("managed readiness sender must reach the handoff barrier");
    tokio::time::timeout(QUALIFICATION_TIMEOUT, probe.wait_for_runtime_wait(1))
        .await
        .expect("managed owner must retain the queue runtime after readiness");

    outer.abort();
    assert_aborted(outer).await;
    barrier.release();
    wait_for_lifecycle_completion(&probe, LifecycleCall::CloseAsync, 1).await;
    wait_for_lifecycle_completion(&probe, LifecycleCall::StopAsync, 1).await;
    let reports = tokio::time::timeout(QUALIFICATION_TIMEOUT, async {
        loop {
            let reports = owned_tracing_shutdown_reports_for_test();
            if reports.len() == 1 {
                break reports;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned tracing shutdown report must follow DI disposal");

    assert_eq!(
        probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::WaitForShutdown,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::CloseAsync,
            LifecycleCall::StopAsync,
        ]
    );
    for call in [
        LifecycleCall::StartAsync,
        LifecycleCall::StopAdmissionAsync,
        LifecycleCall::DrainAsync,
        LifecycleCall::CloseAsync,
        LifecycleCall::StopAsync,
    ] {
        assert_eq!(
            probe.lifecycle_snapshot().completion_count(call),
            1,
            "owned managed cleanup phase {call:?} must complete exactly once"
        );
    }
    assert!(reports[0].was_initialized);
    assert!(!reports[0].already_shutdown);
    assert!(reports[0].is_success(), "tracing report: {:?}", reports[0]);
    assert_eq!(tracing_runtime_status(), TracingRuntimeStatus::Shutdown);
    tokio::task::yield_now().await;
    assert_eq!(owned_tracing_shutdown_reports_for_test(), reports);
    assert_eq!(
        probe.lifecycle_snapshot().count(LifecycleCall::CloseAsync),
        1
    );
    assert_eq!(
        probe.lifecycle_snapshot().count(LifecycleCall::StopAsync),
        1
    );
    println!("{OWNED_HANDOFF_COMPLETION_MARKER}");
    drop(config_file);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_readiness_handoff_drop_reaches_runtime_with_external_tracing_unowned() {
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    let barrier = ManagedStartupHandoffBarrier::new();
    let outer_barrier = barrier.clone();
    let builder = ConsumerBuilder::new()
        .container(Arc::clone(&fixture.container))
        .tracing_external()
        .managed_startup_handoff_barrier(outer_barrier);
    assert!(
        builder
            .tracing_mode
            .resolve_owned_config()
            .expect("external tracing mode is a valid ownership policy")
            .is_none(),
        "external tracing must never create a Consumer-owned runtime slot"
    );
    let outer = tokio::spawn(builder.start_managed());
    tokio::time::timeout(QUALIFICATION_TIMEOUT, barrier.wait_until_entered())
        .await
        .expect("managed readiness handoff barrier must be reached");
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        fixture.probe.wait_for_runtime_wait(1),
    )
    .await
    .expect("managed owner must reach post-readiness runtime wait");

    outer.abort();
    assert_aborted(outer).await;
    barrier.release();
    wait_for_lifecycle_completion(&fixture.probe, LifecycleCall::CloseAsync, 1).await;

    assert_eq!(
        fixture.probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::WaitForShutdown,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::CloseAsync,
        ]
    );
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .count(LifecycleCall::StopAsync),
        0
    );
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
}

#[tokio::test]
async fn provider_terminal_failure_stays_primary_when_cancellation_and_cleanup_fail() {
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    fixture
        .probe
        .fail_next_lifecycle(LifecycleCall::WaitForShutdown);
    fixture.probe.fail_next_lifecycle(LifecycleCall::CloseAsync);
    let cancellation = CancellationToken::new();
    let outer = tokio::spawn(
        ConsumerBuilder::new()
            .container(Arc::clone(&fixture.container))
            .run_with_cancellation(cancellation.clone()),
    );
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        fixture.probe.wait_for_runtime_wait(1),
    )
    .await
    .expect("Consumer must reach runtime wait");

    // This current-thread test cannot poll the supervisor between these two
    // synchronous publications, so both biased select branches are ready on
    // its next scheduler turn.
    cancellation.cancel();
    fixture.probe.release_runtime_completion(1);
    let error = tokio::time::timeout(QUALIFICATION_TIMEOUT, outer)
        .await
        .expect("runtime and cleanup must terminate")
        .expect("outer Consumer task must not panic")
        .expect_err("injected runtime failure must remain terminal");
    let ConsumerError::LifecycleFailures(failures) = error else {
        panic!("runtime plus cleanup failure must remain an ordered aggregate");
    };
    assert_general_broker_failure(
        failures.primary(),
        "test-support queue runtime-wait lifecycle failure",
    );
    assert_eq!(failures.secondary_failures().len(), 1);
    assert_general_broker_failure(
        &failures.secondary_failures()[0],
        "test-support queue close lifecycle failure",
    );
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .count(LifecycleCall::WaitForShutdown),
        1
    );
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .count(LifecycleCall::CloseAsync),
        2,
        "a failed graceful close must use exactly one forced retry"
    );
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .count(LifecycleCall::StopAsync),
        0
    );
    close_caller_container(&fixture.container, &fixture.probe, 2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_cleanup_error_still_disposes_di_after_forced_queue_close() {
    let config_file = NamedTempFile::new().expect("temporary Consumer config");
    std::fs::write(config_file.path(), TRANSPORT_FREE_CONFIG)
        .expect("write transport-free Consumer config");
    let (queue_service, probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
        ConfigService::development(config_file.path()),
    ));
    probe.pause_runtime_completion();
    probe.fail_next_lifecycle(LifecycleCall::CloseAsync);
    let consumer = ConsumerBuilder::new()
        .framework_owned_container_builder_for_test(
            crate::test_application_container_builder()
                .seed_singleton(ConfigService::development(config_file.path()))
                .seed_singleton(queue_service),
        )
        .start_managed()
        .await
        .expect("owned Consumer must become ready");
    tokio::time::timeout(QUALIFICATION_TIMEOUT, probe.wait_for_runtime_wait(1))
        .await
        .expect("owned Consumer must reach runtime wait");

    let error = tokio::time::timeout(QUALIFICATION_TIMEOUT, consumer.shutdown())
        .await
        .expect("typed close failure and force reconciliation must remain bounded")
        .expect_err("the graceful provider failure remains observable");
    assert_general_broker_failure(&error, "test-support queue close lifecycle failure");
    assert_eq!(
        probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::WaitForShutdown,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::CloseAsync,
            LifecycleCall::CloseAsync,
            LifecycleCall::StopAsync,
        ],
        "owned DI disposal must follow the queue close retry"
    );
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::StopAsync),
        1
    );
}

fn assert_general_broker_failure(error: &ConsumerError, expected_message: &str) {
    let ConsumerError::RuntimeSupervision {
        source: MessageBrokerError::RabbitMQError(RabbitMQError::General(message)),
    } = error
    else {
        panic!("expected a typed general broker runtime failure");
    };
    assert_eq!(message, expected_message);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_wait_panic_is_redacted_and_cleanup_finishes_before_publication() {
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    fixture
        .probe
        .panic_next_lifecycle(LifecycleCall::WaitForShutdown);
    let outer = tokio::spawn(
        ConsumerBuilder::new()
            .container(Arc::clone(&fixture.container))
            .run_with_cancellation(CancellationToken::new()),
    );
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        fixture.probe.wait_for_runtime_wait(1),
    )
    .await
    .expect("Consumer must reach runtime wait");
    fixture.probe.release_runtime_completion(1);

    let error = tokio::time::timeout(QUALIFICATION_TIMEOUT, outer)
        .await
        .expect("panic path must finish bounded cleanup")
        .expect("supervisor catches runtime-wait panic")
        .expect_err("runtime-wait panic must become a typed terminal error");
    let ConsumerError::RuntimeSupervision {
        source: MessageBrokerError::RabbitMQError(RabbitMQError::ConsumerTaskFailed(failure)),
    } = &error
    else {
        panic!("runtime-wait panic must map to typed supervisor evidence");
    };
    assert_eq!(failure.queue, "consumer-runtime");
    assert_eq!(failure.role, RabbitMqConsumerTaskRole::Supervisor);
    assert_eq!(failure.kind, RabbitMqConsumerTaskFailureKind::Panicked);
    assert_eq!(failure.operation_error_code, None);
    assert!(!error.to_string().contains("test-support injected"));
    assert!(!format!("{error:?}").contains("test-support injected"));
    assert_eq!(
        fixture.probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::WaitForShutdown,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::CloseAsync,
        ],
        "terminal publication must follow canonical cleanup"
    );
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_panic_is_primary_and_forced_reconciliation_remains_exactly_once() {
    let config_file = NamedTempFile::new().expect("temporary Consumer config");
    std::fs::write(config_file.path(), TRANSPORT_FREE_CONFIG)
        .expect("write transport-free Consumer config");
    let (queue_service, probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
        ConfigService::development(config_file.path()),
    ));
    probe.pause_runtime_completion();
    probe.panic_next_lifecycle(LifecycleCall::DrainAsync);
    let cancellation = CancellationToken::new();
    let outer = tokio::spawn(
        ConsumerBuilder::new()
            .framework_owned_container_builder_for_test(
                crate::test_application_container_builder()
                    .seed_singleton(ConfigService::development(config_file.path()))
                    .seed_singleton(queue_service),
            )
            .run_with_cancellation(cancellation.clone()),
    );
    tokio::time::timeout(QUALIFICATION_TIMEOUT, probe.wait_for_runtime_wait(1))
        .await
        .expect("Consumer must reach runtime wait");

    cancellation.cancel();
    let error = tokio::time::timeout(QUALIFICATION_TIMEOUT, outer)
        .await
        .expect("panic cleanup and force reconciliation must remain bounded")
        .expect("runtime owner catches component cleanup panics")
        .expect_err("a graceful cleanup panic remains a primary failure");
    assert_eq!(error.error_code(), "CONSUMER_SHUTDOWN_PRIMARY_FAILURE");
    assert_eq!(
        probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::WaitForShutdown,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::ForceDrainAsync,
            LifecycleCall::CloseAsync,
            LifecycleCall::StopAsync,
        ]
    );
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::DrainAsync),
        0,
        "a panicking graceful drain must not be reported as completed"
    );
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::ForceDrainAsync),
        1
    );
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::StopAsync),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_timeout_uses_one_force_path_and_publishes_reconciled_success() {
    let config = format!("{TRANSPORT_FREE_CONFIG}\n{ONE_SECOND_SHUTDOWN_CONFIG}");
    let config_file = NamedTempFile::new().expect("temporary Consumer config");
    std::fs::write(config_file.path(), config).expect("write transport-free Consumer config");
    let (queue_service, probe) = lily_queue::__private::queue_service_test_seed(Arc::new(
        ConfigService::development(config_file.path()),
    ));
    probe.pause_runtime_completion();
    probe.pause_lifecycle(LifecycleCall::DrainAsync);
    let cancellation = CancellationToken::new();
    let outer = tokio::spawn(
        ConsumerBuilder::new()
            .framework_owned_container_builder_for_test(
                crate::test_application_container_builder()
                    .seed_singleton(ConfigService::development(config_file.path()))
                    .seed_singleton(queue_service),
            )
            .run_with_cancellation(cancellation.clone()),
    );
    tokio::time::timeout(QUALIFICATION_TIMEOUT, probe.wait_for_runtime_wait(1))
        .await
        .expect("Consumer must reach runtime wait");

    cancellation.cancel();
    tokio::time::timeout(QUALIFICATION_TIMEOUT, outer)
        .await
        .expect("timed-out graceful cleanup plus force reserve must remain bounded")
        .expect("runtime owner must not panic")
        .expect("successful force reconciliation produces terminal success");
    assert_eq!(
        probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::WaitForShutdown,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::ForceDrainAsync,
            LifecycleCall::CloseAsync,
            LifecycleCall::StopAsync,
        ]
    );
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::DrainAsync),
        0,
        "the timed-out graceful future must be dropped"
    );
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::ForceDrainAsync),
        1
    );
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::StopAsync),
        1
    );
    probe.release_lifecycle(LifecycleCall::DrainAsync, 1);
    tokio::task::yield_now().await;
    assert_eq!(
        probe
            .lifecycle_snapshot()
            .completion_count(LifecycleCall::DrainAsync),
        0,
        "releasing a cancelled graceful gate must not reveal detached work"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_managed_observer_retains_task_and_replays_terminal_result() {
    let shutdown_state = Arc::new(ShutdownState::new());
    let mut shutdown_receiver = shutdown_state.subscribe();
    let runtime_task = tokio::spawn(async move {
        shutdown_receiver.recv().await.map_err(|error| {
            ConsumerError::signal(ConsumerSignalFailureStage::ManagedReceive, error)
        })?;
        Err(managed_test_runtime_failure())
    });
    let consumer = managed_consumer_for_test(Arc::clone(&shutdown_state), runtime_task);
    let observer_consumer = consumer.clone();
    let observer = tokio::spawn(async move { observer_consumer.wait().await });
    tokio::time::timeout(QUALIFICATION_TIMEOUT, async {
        loop {
            if consumer.inner.state.try_lock().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first observer must retain the shared task while awaiting it");

    observer.abort();
    assert_aborted(observer).await;
    let shutdown_error = consumer
        .shutdown()
        .await
        .expect_err("retained runtime result must remain observable");
    let replayed = consumer
        .wait()
        .await
        .expect_err("terminal result must be immutable and replayable");

    assert_eq!(
        shutdown_error.error_code(),
        "CONSUMER_RABBITMQ_CONFIG_MISSING"
    );
    assert_eq!(replayed.error_code(), shutdown_error.error_code());
    assert_eq!(
        shutdown_state.initial_signal(),
        Some(ShutdownSignal::Manual)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_managed_observer_abort_replays_ordered_cleanup_aggregate() {
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    fixture.probe.fail_next_lifecycle(LifecycleCall::DrainAsync);
    fixture.probe.fail_next_lifecycle(LifecycleCall::CloseAsync);
    let consumer = ConsumerBuilder::new()
        .container(Arc::clone(&fixture.container))
        .start_managed()
        .await
        .expect("transport-free managed Consumer must become ready");
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        fixture.probe.wait_for_runtime_wait(1),
    )
    .await
    .expect("managed Consumer must retain its provider wait");
    let observer_consumer = consumer.clone();
    let observer = tokio::spawn(async move { observer_consumer.wait().await });
    tokio::time::timeout(QUALIFICATION_TIMEOUT, async {
        loop {
            if consumer.inner.state.try_lock().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("managed observer must retain the real runtime task");

    observer.abort();
    assert_aborted(observer).await;
    let shutdown_error = consumer
        .shutdown()
        .await
        .expect_err("injected cleanup failures must remain observable");
    let replayed = consumer
        .wait()
        .await
        .expect_err("cleanup aggregate must replay after observer cancellation");
    let ConsumerError::LifecycleFailures(shutdown_failures) = &shutdown_error else {
        panic!("two cleanup failures must form an ordered aggregate");
    };
    let ConsumerError::LifecycleFailures(replayed_failures) = &replayed else {
        panic!("replayed terminal result must retain its aggregate shape");
    };
    assert_general_broker_failure(
        shutdown_failures.primary(),
        "test-support queue drain lifecycle failure",
    );
    assert_general_broker_failure(
        &shutdown_failures.secondary_failures()[0],
        "test-support queue close lifecycle failure",
    );
    assert_general_broker_failure(
        replayed_failures.primary(),
        "test-support queue drain lifecycle failure",
    );
    assert_general_broker_failure(
        &replayed_failures.secondary_failures()[0],
        "test-support queue close lifecycle failure",
    );
    assert_eq!(shutdown_failures.secondary_failures().len(), 1);
    assert_eq!(replayed_failures.secondary_failures().len(), 1);
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .count(LifecycleCall::ForceDrainAsync),
        1
    );
    close_caller_container(&fixture.container, &fixture.probe, 2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cleanup_error_and_panic_retain_typed_and_aggregate_evidence() {
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    fixture.probe.fail_next_lifecycle(LifecycleCall::DrainAsync);
    fixture
        .probe
        .panic_next_lifecycle(LifecycleCall::CloseAsync);
    let consumer = ConsumerBuilder::new()
        .container(Arc::clone(&fixture.container))
        .start_managed()
        .await
        .expect("transport-free managed Consumer must become ready");
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        fixture.probe.wait_for_runtime_wait(1),
    )
    .await
    .expect("managed Consumer must retain its provider wait");

    let error = consumer
        .shutdown()
        .await
        .expect_err("typed drain failure and contained close panic must both remain visible");
    let ConsumerError::LifecycleFailures(failures) = error else {
        panic!("mixed cleanup observations must form an ordered aggregate");
    };
    assert_general_broker_failure(
        failures.primary(),
        "test-support queue drain lifecycle failure",
    );
    assert_eq!(failures.secondary_failures().len(), 1);
    let ConsumerError::Shutdown { evidence } = &failures.secondary_failures()[0] else {
        panic!("the contained close panic must remain as bounded aggregate evidence");
    };
    assert_eq!(evidence.kind, ConsumerShutdownFailureKind::PrimaryFailure);
    assert_eq!(evidence.failed, 1);
    assert_eq!(evidence.panicked, 1);
    assert_eq!(evidence.timed_out, 0);
    assert_eq!(evidence.forced_cleanup_failed, 0);
    assert_eq!(evidence.forced_cleanup_panicked, 0);
    assert_eq!(
        fixture.probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::WaitForShutdown,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::ForceDrainAsync,
            LifecycleCall::CloseAsync,
            LifecycleCall::CloseAsync,
        ]
    );
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forced_cleanup_panic_is_not_hidden_by_the_graceful_typed_failure() {
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    fixture.probe.fail_next_lifecycle(LifecycleCall::CloseAsync);
    fixture
        .probe
        .panic_next_lifecycle(LifecycleCall::CloseAsync);
    let consumer = ConsumerBuilder::new()
        .container(Arc::clone(&fixture.container))
        .start_managed()
        .await
        .expect("transport-free managed Consumer must become ready");
    tokio::time::timeout(
        QUALIFICATION_TIMEOUT,
        fixture.probe.wait_for_runtime_wait(1),
    )
    .await
    .expect("managed Consumer must retain its provider wait");

    let error = consumer
        .shutdown()
        .await
        .expect_err("graceful close failure and forced close panic must both remain visible");
    let ConsumerError::LifecycleFailures(failures) = error else {
        panic!("mixed graceful/forced cleanup observations must form an ordered aggregate");
    };
    assert_general_broker_failure(
        failures.primary(),
        "test-support queue close lifecycle failure",
    );
    assert_eq!(failures.secondary_failures().len(), 1);
    let ConsumerError::Shutdown { evidence } = &failures.secondary_failures()[0] else {
        panic!("the forced close panic must remain as bounded aggregate evidence");
    };
    assert_eq!(evidence.kind, ConsumerShutdownFailureKind::PrimaryFailure);
    assert_eq!(evidence.failed, 1);
    assert_eq!(evidence.panicked, 0);
    assert_eq!(evidence.forced_cleanup_panicked, 1);
    assert_eq!(evidence.forced_cleanup_failed, 0);
    assert_eq!(
        fixture.probe.lifecycle_snapshot().calls(),
        &[
            LifecycleCall::StartAsync,
            LifecycleCall::WaitForShutdown,
            LifecycleCall::StopAdmissionAsync,
            LifecycleCall::DrainAsync,
            LifecycleCall::CloseAsync,
            LifecycleCall::CloseAsync,
        ]
    );
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
}

#[tokio::test]
async fn stage5_failed_connection_close_withholds_di_even_when_delivery_drain_is_proven() {
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    fixture.probe.fail_next_lifecycle(LifecycleCall::CloseAsync);
    fixture.probe.fail_next_lifecycle(LifecycleCall::CloseAsync);
    let shutdown_state = Arc::new(ShutdownState::new());
    let cancellation = CancellationToken::new();
    let owner = ConsumerRuntimeOwner::new(
        Handle::current(),
        fixture.provider.clone(),
        Some(fixture.container.clone()),
        None,
        shutdown_state,
        QUALIFICATION_TIMEOUT,
        None,
        tracing::Span::none(),
    );
    let supervisor = owner.spawn(
        ConsumerStartup {
            configured_queues: 1,
            registered_handlers: 1,
        },
        ConsumerLifecycleTrigger::Cancellation(cancellation.clone()),
    );
    fixture.probe.wait_for_runtime_wait(1).await;
    cancellation.cancel();
    assert!(supervisor.await.unwrap().is_err());
    assert!(fixture.provider.drain_reconciled());
    assert!(!fixture.provider.close_reconciled());
    assert!(!lily_injection::__private::container_shutdown_started(
        &fixture.container
    ));
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .count(LifecycleCall::StopAsync),
        0
    );
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .count(LifecycleCall::CloseAsync),
        2
    );
    fixture
        .container
        .close_with_timeout(QUALIFICATION_TIMEOUT)
        .await
        .unwrap();
}

#[tokio::test]
async fn stage5_dependency_owner_replays_one_di_receipt_after_its_observer_is_cancelled() {
    let fixture = transport_free_fixture().await;
    let now = tokio::time::Instant::now();
    let deadlines =
        lily_queue::__private::QueueShutdownDeadlines::starting_at(now, QUALIFICATION_TIMEOUT);
    let dependencies = super::dependencies::ConsumerDependencies::new(
        None,
        Some(fixture.container.clone()),
        None,
        None,
        deadlines,
    );
    fixture.probe.pause_lifecycle(LifecycleCall::StopAsync);
    let child = dependencies.clone();
    let waiter = tokio::spawn(async move { child.close_di().await });
    wait_for_lifecycle(&fixture.probe, LifecycleCall::StopAsync, 1).await;
    waiter.abort();
    assert_aborted(waiter).await;
    assert!(lily_injection::__private::container_shutdown_started(
        &fixture.container
    ));
    assert!(!lily_injection::__private::container_shutdown_quiescent(
        &fixture.container
    ));
    fixture.probe.release_lifecycle(LifecycleCall::StopAsync, 1);
    dependencies.close_di().await.unwrap();
    assert!(dependencies.reconcile().await);
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .count(LifecycleCall::StopAsync),
        1
    );
    assert!(lily_injection::__private::container_shutdown_quiescent(
        &fixture.container
    ));
}

#[tokio::test]
async fn stage6_report_waits_for_real_close_and_survives_cancelled_public_observer() {
    use crate::{
        ConsumerResourceReport, ConsumerShutdownActionOutcome as Outcome,
        ConsumerShutdownCompletion,
    };
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    fixture.probe.pause_lifecycle(LifecycleCall::CloseAsync);
    let consumer = ConsumerBuilder::new()
        .container(fixture.container.clone())
        .start_managed()
        .await
        .unwrap();
    assert!(consumer.shutdown_report().is_none());
    let observer = consumer.clone();
    let waiter = tokio::spawn(async move { observer.shutdown().await });
    wait_for_lifecycle(&fixture.probe, LifecycleCall::CloseAsync, 1).await;
    assert!(fixture.provider.drain_reconciled());
    assert!(!fixture.provider.close_reconciled());
    assert!(!consumer.snapshot().shutdown.completed);
    assert!(consumer.shutdown_report().is_none());
    waiter.abort();
    assert_aborted(waiter).await;
    fixture
        .probe
        .release_lifecycle(LifecycleCall::CloseAsync, 1);
    tokio::time::timeout(QUALIFICATION_TIMEOUT, consumer.wait())
        .await
        .unwrap()
        .unwrap();
    let report = consumer
        .shutdown_report()
        .expect("joined runtime retains its evidence");
    assert_eq!(
        report.completion,
        ConsumerShutdownCompletion::GracefulCompleted
    );
    assert!(report.is_success());
    assert!(
        report.runtime_joined && report.queue_drain_reconciled && report.queue_close_reconciled
    );
    assert_eq!(
        report.dependencies.di,
        ConsumerResourceReport::default(),
        "caller owns DI"
    );
    assert_eq!(
        report.dependencies.telemetry,
        ConsumerResourceReport::default()
    );
    assert_eq!(
        report.dependencies.signal_monitor,
        ConsumerResourceReport::default()
    );
    assert_eq!(
        report.actions.len(),
        3,
        "only queue admission, drain and close were registered"
    );
    assert!(report
        .actions
        .iter()
        .all(|a| a.graceful == Outcome::Completed && a.forced == Outcome::NotAttempted));
    assert!(!lily_injection::__private::container_shutdown_started(
        &fixture.container
    ));
    let calls = fixture.probe.lifecycle_snapshot();
    for _ in 0..20 {
        consumer.shutdown().await.unwrap();
        assert_eq!(consumer.shutdown_report().as_ref(), Some(&report));
        assert_eq!(consumer.snapshot().shutdown.report.as_ref(), Some(&report));
    }
    assert_eq!(
        fixture.probe.lifecycle_snapshot(),
        calls,
        "report reads/repeated observers cannot restart cleanup"
    );
    let serialized = serde_json::to_string(&consumer.snapshot()).unwrap();
    for secret in [
        "guest",
        "localhost",
        "capq01d.consumer-plan.alpha",
        "amqp://",
    ] {
        assert!(!serialized.contains(secret));
    }
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
    assert_eq!(
        consumer.shutdown_report(),
        Some(report),
        "caller disposal cannot rewrite original evidence"
    );
}

#[tokio::test(start_paused = true)]
async fn stage6_deadline_force_is_public_and_retains_original_timeout_evidence() {
    use crate::{ConsumerShutdownActionOutcome as Outcome, ConsumerShutdownCompletion};
    let fixture = transport_free_fixture_with_config(&format!(
        "{TRANSPORT_FREE_CONFIG}\n{ONE_SECOND_SHUTDOWN_CONFIG}"
    ))
    .await;
    fixture.probe.pause_runtime_completion();
    fixture.probe.pause_lifecycle(LifecycleCall::DrainAsync);
    let consumer = ConsumerBuilder::new()
        .container(fixture.container.clone())
        .start_managed()
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    consumer.shutdown().await.unwrap();
    assert_eq!(
        started.elapsed(),
        Duration::from_millis(750),
        "force starts at the shared graceful cutoff"
    );
    let report = consumer.shutdown_report().unwrap();
    assert_eq!(report.budget, Duration::from_secs(1));
    assert_eq!(report.elapsed, Duration::from_millis(750));
    assert_eq!(
        report.completion,
        ConsumerShutdownCompletion::ForcedCompleted
    );
    assert!(report.forced && report.is_success());
    assert!(
        consumer.snapshot().shutdown.forced,
        "deadline escalation is force even without an explicit caller force request"
    );
    assert!(report.coordinator_accounted && report.coordinator_complete);
    let drain = report
        .actions
        .iter()
        .find(|a| a.phase == "in-flight work drained")
        .unwrap();
    assert_eq!(drain.graceful, Outcome::TimedOut);
    assert_eq!(drain.forced, Outcome::Completed);
    let calls = fixture.probe.lifecycle_snapshot();
    assert_eq!(calls.completion_count(LifecycleCall::DrainAsync), 0);
    assert_eq!(calls.completion_count(LifecycleCall::ForceDrainAsync), 1);
    fixture
        .probe
        .release_lifecycle(LifecycleCall::DrainAsync, 1);
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(
        fixture.probe.lifecycle_snapshot(),
        calls,
        "a late released graceful future must not still execute"
    );
    assert_eq!(consumer.shutdown_report(), Some(report));
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
}

#[tokio::test]
async fn stage6_joined_cleanup_panic_remains_failed_after_successful_force() {
    use crate::{ConsumerShutdownActionOutcome as Outcome, ConsumerShutdownCompletion};
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    fixture
        .probe
        .panic_next_lifecycle(LifecycleCall::DrainAsync);
    let consumer = ConsumerBuilder::new()
        .container(fixture.container.clone())
        .start_managed()
        .await
        .unwrap();
    let error = consumer.shutdown().await.unwrap_err();
    assert_eq!(error.error_code(), "CONSUMER_SHUTDOWN_PRIMARY_FAILURE");
    let report = consumer.shutdown_report().unwrap();
    assert_eq!(report.completion, ConsumerShutdownCompletion::Failed);
    assert!(!report.is_success());
    assert!(report.runtime_joined && report.queue_close_reconciled);
    assert_eq!(report.failure_code, Some(error.error_code()));
    let drain = report
        .actions
        .iter()
        .find(|a| a.phase == "in-flight work drained")
        .unwrap();
    assert_eq!(drain.graceful, Outcome::Panicked);
    assert_eq!(drain.forced, Outcome::Completed);
    assert!(!serde_json::to_string(&report)
        .unwrap()
        .contains("test-support injected"));
    close_caller_container(&fixture.container, &fixture.probe, 1).await;
}

#[tokio::test]
async fn stage6_failed_broker_close_reports_unstarted_owned_di_and_incomplete_shutdown() {
    use crate::ConsumerShutdownCompletion;
    let fixture = transport_free_fixture().await;
    fixture.probe.pause_runtime_completion();
    fixture.probe.fail_next_lifecycle(LifecycleCall::CloseAsync);
    fixture.probe.fail_next_lifecycle(LifecycleCall::CloseAsync);
    // Use the production runtime owner with an explicitly owned application
    // container so the withheld parent is observable independently of the report.
    let shutdown_state = Arc::new(ShutdownState::new());
    let owner = ConsumerRuntimeOwner::new(
        Handle::current(),
        fixture.provider.clone(),
        Some(fixture.container.clone()),
        None,
        shutdown_state,
        QUALIFICATION_TIMEOUT,
        None,
        tracing::Span::none(),
    );
    let (startup_tx, startup_rx) = oneshot::channel();
    let supervisor = owner.spawn(
        ConsumerStartup {
            configured_queues: 1,
            registered_handlers: 1,
        },
        ConsumerLifecycleTrigger::Managed {
            startup_tx,
            waiter_cancellation: CancellationToken::new(),
        },
    );
    let operational = Arc::new(ConsumerOperationalState::default());
    let observer = OwnedTask::from(tokio::spawn(observe_managed_runtime(
        supervisor,
        operational.clone(),
    )));
    let consumer = ManagedConsumer::new(startup_rx.await.unwrap(), operational, observer);
    assert!(consumer.shutdown().await.is_err());
    let report = consumer.shutdown_report().unwrap();
    assert_eq!(report.completion, ConsumerShutdownCompletion::Incomplete);
    assert!(report.runtime_joined && report.queue_drain_reconciled);
    assert!(!report.queue_close_reconciled);
    assert!(report.dependencies.di.owned);
    assert!(!report.dependencies.di.started);
    assert!(!report.dependencies.di.terminal);
    assert!(!report.dependencies.di.succeeded);
    assert!(!lily_injection::__private::container_shutdown_started(
        &fixture.container
    ));
    assert_eq!(
        fixture
            .probe
            .lifecycle_snapshot()
            .count(LifecycleCall::StopAsync),
        0
    );
    // Late external recovery must not turn the original incomplete report into
    // success or restart an automatic disposal attempt with a fresh deadline.
    fixture
        .container
        .close_with_timeout(QUALIFICATION_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(consumer.shutdown_report(), Some(report));
}
