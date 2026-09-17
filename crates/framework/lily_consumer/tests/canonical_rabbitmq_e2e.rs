//! Canonical CAP-Q-01E qualification against a disposable RabbitMQ broker.
//!
//! Unlike the low-level RabbitMQ engine fixture, this test starts the public
//! Consumer composition root and reaches the transport only through real
//! `#[queue_service]`/`#[queue]` registry metadata and generated adapters.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use lapin::{
    options::{
        BasicAckOptions, BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions,
        ExchangeDeleteOptions, QueueDeleteOptions,
    },
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties,
};
use lily_config::{
    ConfigService, LifecycleConfig, LilyConfig, QueueDefinition, QueueRetentionConfig,
    RabbitMqConfig, RabbitMqConsumerConfig, RabbitMqTlsConfig, RabbitMqTopologyConfig,
};
use lily_consumer::{Consumer, ManagedConsumer};
use lily_error::{
    application::{message_broker::RabbitMQError, MessageBrokerError},
    injection::InjectionError,
};
use lily_injection::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait};
use lily_queue::{
    queue, queue_service, BinaryPayload, DeliveryContext, DeliveryTerminalOutcome, Json,
    QueueHandlerError, QueueService, Service,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use uuid::Uuid;

const WORKER: &str = "capq01e.canonical.worker";
const SUCCESS_QUEUE: &str = "capq01e.canonical.success";
const RETRY_EVENTUAL_QUEUE: &str = "capq01e.canonical.retry-eventual";
const RETRY_EXHAUST_QUEUE: &str = "capq01e.canonical.retry-exhaust";
const PERMANENT_QUEUE: &str = "capq01e.canonical.permanent";
const PANIC_QUEUE: &str = "capq01e.canonical.panic";
const TIMEOUT_QUEUE: &str = "capq01e.canonical.timeout";
const POISON_QUEUE: &str = "capq01e.canonical.poison";
const SHUTDOWN_QUEUE: &str = "capq01e.canonical.shutdown";
const HANDOFF_FAILURE_QUEUE: &str = "capq01e.canonical.handoff-failure";
const VERSIONED_QUEUE: &str = "capq04.canonical.versioned";

const QUEUES: [&str; 10] = [
    SUCCESS_QUEUE,
    RETRY_EVENTUAL_QUEUE,
    RETRY_EXHAUST_QUEUE,
    PERMANENT_QUEUE,
    PANIC_QUEUE,
    TIMEOUT_QUEUE,
    POISON_QUEUE,
    SHUTDOWN_QUEUE,
    HANDOFF_FAILURE_QUEUE,
    VERSIONED_QUEUE,
];

static NEXT_WORKER_ID: AtomicUsize = AtomicUsize::new(0);
static NEXT_PROBE_ID: AtomicUsize = AtomicUsize::new(0);
static WORKER_STARTS: AtomicUsize = AtomicUsize::new(0);
static WORKER_DISPOSES: AtomicUsize = AtomicUsize::new(0);
static PROBE_STARTS: AtomicUsize = AtomicUsize::new(0);
static PROBE_DISPOSES: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone)]
struct HandlerObservation {
    queue: String,
    marker: String,
    event_id: String,
    schema_version: u16,
    content_kind: String,
    retry_count: u32,
    redelivered: bool,
    worker_id: usize,
    probe_id: usize,
}

fn observations() -> &'static Mutex<Vec<HandlerObservation>> {
    static OBSERVATIONS: OnceLock<Mutex<Vec<HandlerObservation>>> = OnceLock::new();
    OBSERVATIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn shutdown_entered() -> &'static Mutex<Option<Arc<Notify>>> {
    static SHUTDOWN_ENTERED: OnceLock<Mutex<Option<Arc<Notify>>>> = OnceLock::new();
    SHUTDOWN_ENTERED.get_or_init(|| Mutex::new(None))
}

fn qualification_lock() -> &'static AsyncMutex<()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(()))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QualificationMessage {
    marker: String,
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct DeliveryProbe {
    id: usize,
}

#[async_trait]
impl ServiceTrait for DeliveryProbe {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.id = NEXT_PROBE_ID.fetch_add(1, Ordering::SeqCst) + 1;
        PROBE_STARTS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        PROBE_DISPOSES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct CanonicalRabbitWorker {
    id: usize,
}

#[async_trait]
impl ServiceTrait for CanonicalRabbitWorker {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.id = NEXT_WORKER_ID.fetch_add(1, Ordering::SeqCst) + 1;
        WORKER_STARTS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        WORKER_DISPOSES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn record(
    worker: &CanonicalRabbitWorker,
    context: DeliveryContext,
    probe: Arc<DeliveryProbe>,
    message: QualificationMessage,
) {
    record_marker(worker, context, probe, message.marker);
}

fn record_marker(
    worker: &CanonicalRabbitWorker,
    context: DeliveryContext,
    probe: Arc<DeliveryProbe>,
    marker: String,
) {
    observations()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(HandlerObservation {
            queue: context.queue().to_string(),
            marker,
            event_id: context.event_id().into_inner().to_string(),
            schema_version: context.schema_version().into_inner(),
            content_kind: context.content_kind().as_str().to_string(),
            retry_count: context.retry_count().into_inner(),
            redelivered: context.redelivered().into_inner(),
            worker_id: worker.id,
            probe_id: probe.id,
        });
}

#[queue_service]
impl CanonicalRabbitWorker {
    #[queue("capq01e.canonical.success", version = 1, content = "json")]
    async fn success(
        &self,
        context: DeliveryContext,
        Service(probe): Service<DeliveryProbe>,
        Json(message): Json<QualificationMessage>,
    ) -> Result<(), QueueHandlerError> {
        record(self, context, probe, message);
        Ok(())
    }

    #[queue("capq01e.canonical.retry-eventual", version = 1, content = "json")]
    async fn retry_eventual(
        &self,
        context: DeliveryContext,
        Service(probe): Service<DeliveryProbe>,
        Json(message): Json<QualificationMessage>,
    ) -> Result<(), QueueHandlerError> {
        let retry_count = context.retry_count().into_inner();
        record(self, context, probe, message);
        if retry_count == 0 {
            Err(QueueHandlerError::retryable("CAPQ01E_RETRY_ONCE"))
        } else {
            Ok(())
        }
    }

    #[queue("capq01e.canonical.retry-exhaust", version = 1, content = "json")]
    async fn retry_exhaust(
        &self,
        context: DeliveryContext,
        Service(probe): Service<DeliveryProbe>,
        Json(message): Json<QualificationMessage>,
    ) -> Result<(), QueueHandlerError> {
        record(self, context, probe, message);
        Err(QueueHandlerError::retryable("CAPQ01E_RETRY_EXHAUST"))
    }

    #[queue("capq01e.canonical.permanent", version = 1, content = "json")]
    async fn permanent(
        &self,
        context: DeliveryContext,
        Service(probe): Service<DeliveryProbe>,
        Json(message): Json<QualificationMessage>,
    ) -> Result<(), QueueHandlerError> {
        record(self, context, probe, message);
        Err(QueueHandlerError::permanent("CAPQ01E_PERMANENT"))
    }

    #[queue("capq01e.canonical.panic", version = 1, content = "json")]
    async fn panic(
        &self,
        context: DeliveryContext,
        Service(probe): Service<DeliveryProbe>,
        Json(message): Json<QualificationMessage>,
    ) -> Result<(), QueueHandlerError> {
        record(self, context, probe, message);
        panic!("CAP-Q-01E panic payload must not escape the generated adapter")
    }

    #[queue("capq01e.canonical.timeout", version = 1, content = "json")]
    async fn timeout(
        &self,
        context: DeliveryContext,
        Service(probe): Service<DeliveryProbe>,
        Json(message): Json<QualificationMessage>,
    ) -> Result<(), QueueHandlerError> {
        record(self, context, probe, message);
        std::future::pending::<()>().await;
        Ok(())
    }

    #[queue("capq01e.canonical.poison", version = 1, content = "json")]
    async fn poison(
        &self,
        context: DeliveryContext,
        Service(probe): Service<DeliveryProbe>,
        Json(message): Json<QualificationMessage>,
    ) -> Result<(), QueueHandlerError> {
        record(self, context, probe, message);
        Ok(())
    }

    #[queue("capq01e.canonical.shutdown", version = 1, content = "json")]
    async fn shutdown(
        &self,
        context: DeliveryContext,
        Service(probe): Service<DeliveryProbe>,
        Json(message): Json<QualificationMessage>,
    ) -> Result<(), QueueHandlerError> {
        record(self, context, probe, message);
        let signal = shutdown_entered()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .expect("shutdown signal must be installed before publishing");
        signal.notify_one();
        std::future::pending::<()>().await;
        Ok(())
    }

    #[queue("capq01e.canonical.handoff-failure", version = 1, content = "json")]
    async fn handoff_failure(
        &self,
        context: DeliveryContext,
        Service(probe): Service<DeliveryProbe>,
        Json(message): Json<QualificationMessage>,
    ) -> Result<(), QueueHandlerError> {
        let redelivered = context.redelivered().into_inner();
        record(self, context, probe, message);
        if redelivered {
            Ok(())
        } else {
            Err(QueueHandlerError::permanent(
                "CAPQ01E_HANDOFF_FAILURE_REQUEUE",
            ))
        }
    }

    #[queue("capq04.canonical.versioned", version = 1, content = "json")]
    async fn versioned_json_v1(
        &self,
        context: DeliveryContext,
        Service(probe): Service<DeliveryProbe>,
        Json(message): Json<QualificationMessage>,
    ) -> Result<(), QueueHandlerError> {
        record(self, context, probe, message);
        Ok(())
    }

    #[queue("capq04.canonical.versioned", version = 2, content = "json")]
    async fn versioned_json_v2(
        &self,
        context: DeliveryContext,
        Service(probe): Service<DeliveryProbe>,
        Json(message): Json<QualificationMessage>,
    ) -> Result<(), QueueHandlerError> {
        record(self, context, probe, message);
        Ok(())
    }

    #[queue("capq04.canonical.versioned", version = 2, content = "binary")]
    async fn versioned_binary_v2(
        &self,
        context: DeliveryContext,
        Service(probe): Service<DeliveryProbe>,
        payload: BinaryPayload,
    ) -> Result<(), QueueHandlerError> {
        if payload.as_bytes() != b"capq04-v2-binary" {
            return Err(QueueHandlerError::permanent("CAPQ04_BINARY_INVALID"));
        }
        record_marker(self, context, probe, "versioned-v2-binary".to_string());
        Ok(())
    }
}

fn reset_evidence() {
    NEXT_WORKER_ID.store(0, Ordering::SeqCst);
    NEXT_PROBE_ID.store(0, Ordering::SeqCst);
    WORKER_STARTS.store(0, Ordering::SeqCst);
    WORKER_DISPOSES.store(0, Ordering::SeqCst);
    PROBE_STARTS.store(0, Ordering::SeqCst);
    PROBE_DISPOSES.store(0, Ordering::SeqCst);
    observations()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clear();
    *shutdown_entered()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
}

fn tls_config(uri: &str) -> RabbitMqTlsConfig {
    if !uri.starts_with("amqps://") {
        return RabbitMqTlsConfig::default();
    }
    RabbitMqTlsConfig {
        additional_ca_bundle: std::env::var_os("LILY_TEST_RABBITMQ_CA_BUNDLE").map(Into::into),
        client_certificate_chain: std::env::var_os("LILY_TEST_RABBITMQ_CLIENT_CERTIFICATE_CHAIN")
            .map(Into::into),
        client_private_key: std::env::var_os("LILY_TEST_RABBITMQ_CLIENT_PRIVATE_KEY")
            .map(Into::into),
    }
}

async fn connect_fixture(uri: &str) -> Connection {
    Connection::connect(uri, ConnectionProperties::default())
        .await
        .expect("connect disposable RabbitMQ qualification observer")
}

fn retention() -> QueueRetentionConfig {
    QueueRetentionConfig {
        main_max_messages: 1_000,
        main_max_bytes: 16 * 1024 * 1024,
        retry_bucket_max_messages: 1_000,
        retry_bucket_max_bytes: 16 * 1024 * 1024,
        dead_letter_max_messages: 1_000,
        dead_letter_max_bytes: 16 * 1024 * 1024,
    }
}

fn definition(name: &str, retry_attempts: u32, execution_timeout_ms: u64) -> QueueDefinition {
    QueueDefinition {
        name: name.to_string(),
        exchange_name: WORKER.to_string(),
        routing_key: name.to_string(),
        concurrency: 1,
        prefetch_count: 4,
        delivery_buffer_capacity: Some(4),
        retry_attempts,
        retry_backoff_millis: Some(100),
        max_retry_backoff_millis: Some(200),
        delivery_execution_timeout_millis: Some(execution_timeout_ms),
        settlement_timeout_millis: Some(2_000),
        durable: true,
        max_message_size_bytes: 64 * 1024,
        retention: Some(retention()),
        ..QueueDefinition::default()
    }
}

fn fixture_config(uri: &str) -> LilyConfig {
    let tls = tls_config(uri);
    let mut queues = vec![
        definition(SUCCESS_QUEUE, 0, 2_000),
        definition(RETRY_EVENTUAL_QUEUE, 1, 2_000),
        definition(RETRY_EXHAUST_QUEUE, 2, 2_000),
        definition(PERMANENT_QUEUE, 0, 2_000),
        definition(PANIC_QUEUE, 0, 2_000),
        definition(TIMEOUT_QUEUE, 0, 150),
        definition(POISON_QUEUE, 0, 2_000),
        definition(SHUTDOWN_QUEUE, 0, 10_000),
        definition(HANDOFF_FAILURE_QUEUE, 0, 2_000),
        definition(VERSIONED_QUEUE, 0, 2_000),
    ];
    for queue in &mut queues {
        if matches!(
            queue.name.as_str(),
            RETRY_EVENTUAL_QUEUE | RETRY_EXHAUST_QUEUE
        ) {
            queue.retry_jitter_ratio = 0.2;
        }
    }
    LilyConfig {
        lifecycle: LifecycleConfig {
            shutdown_timeout_secs: 2,
        },
        rabbitmq: RabbitMqConfig {
            consumer: Some(RabbitMqConsumerConfig {
                connection_string: Some(uri.to_string()),
                pool_size: 2,
                connection_timeout_secs: Some(10),
                confirm_timeout_secs: Some(5),
                heartbeat_secs: Some(30),
                max_reconnect_attempts: Some(2),
                reconnect_backoff_millis: Some(100),
                use_tls: Some(uri.starts_with("amqps://")),
                tls,
                persistence_enabled: true,
                ..RabbitMqConsumerConfig::default()
            }),
            topology: RabbitMqTopologyConfig { queues },
        },
        ..LilyConfig::default()
    }
}

struct RunningConsumerFixture {
    _config_directory: tempfile::TempDir,
    container: Arc<ApplicationContainer>,
    queue_service: Arc<QueueService>,
    managed: ManagedConsumer,
}

async fn start_consumer_fixture(uri: &str) -> RunningConsumerFixture {
    let config_directory = tempfile::tempdir().expect("create private qualification config dir");
    let config_path = config_directory.path().join("lily.toml");
    std::fs::write(
        &config_path,
        toml::to_string_pretty(&fixture_config(uri)).expect("serialize qualification config"),
    )
    .expect("write qualification config");
    let container = Arc::new(
        ApplicationContainer::builder()
            .seed_singleton(ConfigService::production(&config_path))
            .build()
            .await
            .expect("build canonical Consumer DI container"),
    );
    let queue_service = container
        .resolve::<QueueService>(None)
        .await
        .expect("resolve production QueueService");
    let managed = Consumer::builder()
        .container(Arc::clone(&container))
        .tracing_disabled()
        .start_managed()
        .await
        .expect("start canonical managed Consumer");
    wait_until("all canonical consumers to report readiness", || {
        queue_service
            .delivery_terminal_snapshot()
            .is_ok_and(|snapshot| snapshot.is_ready() && snapshot.registered_consumers == 10)
    })
    .await;
    RunningConsumerFixture {
        _config_directory: config_directory,
        container,
        queue_service,
        managed,
    }
}

async fn await_confirm(confirm: lapin::PublisherConfirm, description: &str) {
    let confirmation = tokio::time::timeout(Duration::from_secs(5), confirm)
        .await
        .unwrap_or_else(|_| panic!("{description} confirmation exceeded its bound"))
        .unwrap_or_else(|error| panic!("{description} confirmation failed: {error}"));
    assert_eq!(
        confirmation,
        Confirmation::Ack(None),
        "{description} must be routed and ACK-confirmed"
    );
}

async fn publish_json(channel: &Channel, queue: &str, event_id: Uuid, marker: &str) {
    let body = serde_json::to_vec(&QualificationMessage {
        marker: marker.to_string(),
    })
    .expect("serialize canonical qualification payload");
    let confirm = channel
        .basic_publish(
            WORKER.into(),
            queue.into(),
            BasicPublishOptions {
                mandatory: true,
                ..BasicPublishOptions::default()
            },
            &body,
            envelope_properties(event_id, None),
        )
        .await
        .expect("canonical publish admission");
    await_confirm(confirm, "canonical publish").await;
}

async fn publish_contract_frame(
    channel: &Channel,
    queue: &str,
    event_id: Uuid,
    schema_version: u16,
    content_kind: &str,
    content_type: &str,
    body: &[u8],
) {
    let confirm = channel
        .basic_publish(
            WORKER.into(),
            queue.into(),
            BasicPublishOptions {
                mandatory: true,
                ..BasicPublishOptions::default()
            },
            body,
            envelope_properties_for(event_id, schema_version, content_kind, content_type, None),
        )
        .await
        .expect("versioned contract publish admission");
    await_confirm(confirm, "versioned contract publish").await;
}

fn envelope_properties(event_id: Uuid, retry_count: Option<AMQPValue>) -> BasicProperties {
    envelope_properties_for(event_id, 1, "json", "application/json", retry_count)
}

fn envelope_properties_for(
    event_id: Uuid,
    schema_version: u16,
    content_kind: &str,
    content_type: &str,
    retry_count: Option<AMQPValue>,
) -> BasicProperties {
    let mut headers = FieldTable::default();
    headers.insert(
        "x-lily-event-id".into(),
        AMQPValue::LongString(event_id.to_string().into()),
    );
    headers.insert(
        "x-lily-schema-version".into(),
        AMQPValue::LongString(schema_version.to_string().into()),
    );
    headers.insert(
        "x-lily-content-kind".into(),
        AMQPValue::LongString(content_kind.into()),
    );
    if let Some(retry_count) = retry_count {
        headers.insert("x-retry-count".into(), retry_count);
    }
    BasicProperties::default()
        .with_content_type(content_type.into())
        .with_message_id(event_id.to_string().into())
        .with_delivery_mode(2)
        .with_headers(headers)
}

async fn publish_remote_frame(
    channel: &Channel,
    event_id: Uuid,
    body: &[u8],
    retry_count: Option<AMQPValue>,
) {
    let confirm = channel
        .basic_publish(
            WORKER.into(),
            POISON_QUEUE.into(),
            BasicPublishOptions {
                mandatory: true,
                ..BasicPublishOptions::default()
            },
            body,
            envelope_properties(event_id, retry_count),
        )
        .await
        .expect("remote-controlled publish admission");
    await_confirm(confirm, "remote-controlled publish").await;
}

async fn wait_until(description: &str, predicate: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if predicate() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("qualification timed out while waiting for {description}"));
}

async fn wait_for_scope_disposal(description: &str) {
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let worker_starts = WORKER_STARTS.load(Ordering::SeqCst);
            if worker_starts > 0
                && WORKER_DISPOSES.load(Ordering::SeqCst) == worker_starts
                && PROBE_STARTS.load(Ordering::SeqCst) == PROBE_DISPOSES.load(Ordering::SeqCst)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    if result.is_err() {
        panic!(
            "qualification timed out while waiting for {description}: worker starts={}, worker disposes={}, probe starts={}, probe disposes={}",
            WORKER_STARTS.load(Ordering::SeqCst),
            WORKER_DISPOSES.load(Ordering::SeqCst),
            PROBE_STARTS.load(Ordering::SeqCst),
            PROBE_DISPOSES.load(Ordering::SeqCst),
        );
    }
}

async fn take_one(channel: &Channel, queue: &str) -> lapin::message::BasicGetMessage {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(delivery) = channel
                .basic_get(queue.into(), BasicGetOptions::default())
                .await
                .expect("inspect qualification queue")
            {
                break delivery;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("qualification queue {queue:?} did not receive a delivery"))
}

fn dlq(queue: &str) -> String {
    format!("{queue}.dlq.v2")
}

fn long_string_header(headers: &FieldTable, name: &str) -> String {
    match headers.inner().get(name) {
        Some(AMQPValue::LongString(value)) => std::str::from_utf8(value.as_bytes())
            .unwrap_or_else(|_| panic!("header {name:?} must be UTF-8"))
            .to_string(),
        value => panic!("expected long-string header {name:?}, got {value:?}"),
    }
}

async fn delete_topology(channel: &Channel) {
    for queue in QUEUES {
        let _ = channel
            .queue_delete(queue.into(), QueueDeleteOptions::default())
            .await;
        let _ = channel
            .queue_delete(dlq(queue).into(), QueueDeleteOptions::default())
            .await;
    }
    for queue in [
        format!("{RETRY_EVENTUAL_QUEUE}.retry.v2.100ms"),
        format!("{RETRY_EVENTUAL_QUEUE}.retry.v2.120ms"),
        format!("{RETRY_EXHAUST_QUEUE}.retry.v2.100ms"),
        format!("{RETRY_EXHAUST_QUEUE}.retry.v2.120ms"),
        format!("{RETRY_EXHAUST_QUEUE}.retry.v2.160ms"),
        format!("{RETRY_EXHAUST_QUEUE}.retry.v2.200ms"),
    ] {
        let _ = channel
            .queue_delete(queue.into(), QueueDeleteOptions::default())
            .await;
    }
    for exchange in [
        format!("{WORKER}.retry.v2"),
        format!("{WORKER}.dlx.v2"),
        WORKER.to_string(),
    ] {
        let _ = channel
            .exchange_delete(exchange.into(), ExchangeDeleteOptions::default())
            .await;
    }
}

#[test]
fn canonical_fixture_config_and_registry_round_trip_exactly() {
    let config = fixture_config("amqp://guest:guest@127.0.0.1:5672/%2f");
    let encoded = toml::to_string_pretty(&config).expect("serialize qualification config");
    let decoded: LilyConfig = toml::from_str(&encoded).expect("parse qualification config");
    let configured = decoded
        .rabbitmq
        .topology
        .queues
        .into_iter()
        .map(|definition| definition.name)
        .collect::<HashSet<_>>();
    let registered = lily_queue::__private::get_all_queue_handlers()
        .into_iter()
        .filter(|metadata| {
            metadata.queue_name.starts_with("capq01e.canonical.")
                || metadata.queue_name == VERSIONED_QUEUE
        })
        .map(|metadata| metadata.queue_name.to_string())
        .collect::<HashSet<_>>();

    assert_eq!(configured.len(), QUEUES.len());
    assert_eq!(registered.len(), QUEUES.len());
    assert_eq!(configured, registered);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "environment-restricted: requires LILY_TEST_RABBITMQ_URL and a fresh disposable RabbitMQ v2 fixture"]
async fn canonical_consumer_registry_typed_di_and_settlement_e2e() {
    let _serial = qualification_lock().lock().await;
    reset_evidence();
    let uri = std::env::var("LILY_TEST_RABBITMQ_URL")
        .expect("LILY_TEST_RABBITMQ_URL must identify a disposable broker");
    let fixture_connection = connect_fixture(&uri).await;
    let fixture_channel = fixture_connection
        .create_channel()
        .await
        .expect("create fixture RabbitMQ channel");
    fixture_channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("enable fixture publisher confirms");
    let RunningConsumerFixture {
        _config_directory,
        container,
        queue_service,
        managed,
    } = start_consumer_fixture(&uri).await;
    let event_ids: HashMap<&str, Uuid> = [
        ("success", Uuid::new_v4()),
        ("retry-eventual", Uuid::new_v4()),
        ("retry-exhaust", Uuid::new_v4()),
        ("permanent", Uuid::new_v4()),
        ("panic", Uuid::new_v4()),
        ("timeout", Uuid::new_v4()),
        ("poison-json", Uuid::new_v4()),
        ("poison-metadata", Uuid::new_v4()),
        ("poison-recovery", Uuid::new_v4()),
        ("shutdown", Uuid::new_v4()),
    ]
    .into_iter()
    .collect();

    publish_json(
        &fixture_channel,
        SUCCESS_QUEUE,
        event_ids["success"],
        "success",
    )
    .await;
    publish_json(
        &fixture_channel,
        RETRY_EVENTUAL_QUEUE,
        event_ids["retry-eventual"],
        "retry-eventual",
    )
    .await;
    publish_json(
        &fixture_channel,
        RETRY_EXHAUST_QUEUE,
        event_ids["retry-exhaust"],
        "retry-exhaust",
    )
    .await;
    publish_json(
        &fixture_channel,
        PERMANENT_QUEUE,
        event_ids["permanent"],
        "permanent",
    )
    .await;
    publish_json(&fixture_channel, PANIC_QUEUE, event_ids["panic"], "panic").await;
    publish_json(
        &fixture_channel,
        TIMEOUT_QUEUE,
        event_ids["timeout"],
        "timeout",
    )
    .await;
    publish_remote_frame(
        &fixture_channel,
        event_ids["poison-json"],
        br#"{"marker": "truncated""#,
        None,
    )
    .await;
    publish_remote_frame(
        &fixture_channel,
        event_ids["poison-metadata"],
        br#"{"marker":"poison-metadata"}"#,
        Some(AMQPValue::LongString("-1".into())),
    )
    .await;
    publish_json(
        &fixture_channel,
        POISON_QUEUE,
        event_ids["poison-recovery"],
        "poison-recovery",
    )
    .await;

    wait_until("canonical ACK/retry/DLQ terminal matrix", || {
        queue_service
            .delivery_terminal_snapshot()
            .is_ok_and(|snapshot| {
                snapshot.deliveries == 12
                    && snapshot.acked_handler_success == 3
                    && snapshot.acked_confirmed_handoff == 9
                    && snapshot.retry_confirmed == 3
                    && snapshot.dead_letter_confirmed == 6
                    && snapshot.handler_panic == 1
                    && snapshot.unacked_or_in_flight() == 0
                    && snapshot.is_reconciled()
            })
    })
    .await;

    let records = observations()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let mut actual_handler_counts = HashMap::new();
    for record in &records {
        *actual_handler_counts
            .entry(record.marker.as_str())
            .or_insert(0_u32) += 1;
    }
    assert_eq!(
        actual_handler_counts,
        HashMap::from([
            ("success", 1),
            ("retry-eventual", 2),
            ("retry-exhaust", 3),
            ("permanent", 1),
            ("panic", 1),
            ("timeout", 1),
            ("poison-recovery", 1),
        ]),
        "actual handler invocations, unlike diagnostic details, are never best-effort"
    );
    let retry_eventual = records
        .iter()
        .filter(|record| record.marker == "retry-eventual")
        .collect::<Vec<_>>();
    assert_eq!(
        retry_eventual
            .iter()
            .map(|record| record.retry_count)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    let retry_exhaust = records
        .iter()
        .filter(|record| record.marker == "retry-exhaust")
        .collect::<Vec<_>>();
    assert_eq!(
        retry_exhaust
            .iter()
            .map(|record| record.retry_count)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert!(records.iter().all(|record| {
        event_ids
            .get(record.marker.as_str())
            .is_some_and(|event_id| record.event_id == event_id.to_string())
    }));
    assert!(records
        .iter()
        .all(|record| record.queue.starts_with("capq01e.canonical.")));
    assert_eq!(
        records
            .iter()
            .map(|record| record.worker_id)
            .collect::<HashSet<_>>()
            .len(),
        records.len(),
        "each delivery must resolve a fresh scoped handler owner"
    );
    assert_eq!(
        records
            .iter()
            .map(|record| record.probe_id)
            .collect::<HashSet<_>>()
            .len(),
        records.len(),
        "each delivery must resolve a fresh scoped dependency"
    );
    assert!(
        !records
            .iter()
            .any(|record| matches!(record.marker.as_str(), "poison-json" | "poison-metadata")),
        "poison payload/metadata must fail before application handler invocation"
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| record.marker == "poison-recovery")
            .count(),
        1,
        "the same poison-queue receiver must process a subsequent healthy delivery"
    );

    let terminal_observations = queue_service
        .delivery_terminal_observations()
        .expect("read canonical terminal observations");
    // Detail capture is best-effort under concurrent settlement. The matrix
    // above is authoritative; every omitted detail must still be counted.
    // Allow the last writer to finish its post-counter observation first.
    let terminal_observations =
        if terminal_observations.observations.len() as u64 + terminal_observations.dropped == 12 {
            terminal_observations
        } else {
            wait_until("all terminal detail attempts to be accounted", || {
                queue_service
                    .delivery_terminal_observations()
                    .is_ok_and(|details| {
                        assert!(details.observations.len() as u64 + details.dropped <= 12);
                        details.observations.len() as u64 + details.dropped == 12
                    })
            })
            .await;
            queue_service.delivery_terminal_observations().unwrap()
        };
    let expected_details = [
        ("success", 1, DeliveryTerminalOutcome::HandlerSuccess),
        ("retry-eventual", 1, DeliveryTerminalOutcome::RetryConfirmed),
        ("retry-eventual", 2, DeliveryTerminalOutcome::HandlerSuccess),
        ("retry-exhaust", 1, DeliveryTerminalOutcome::RetryConfirmed),
        ("retry-exhaust", 2, DeliveryTerminalOutcome::RetryConfirmed),
        (
            "retry-exhaust",
            3,
            DeliveryTerminalOutcome::DeadLetterConfirmed,
        ),
        ("permanent", 1, DeliveryTerminalOutcome::DeadLetterConfirmed),
        ("panic", 1, DeliveryTerminalOutcome::DeadLetterConfirmed),
        ("timeout", 1, DeliveryTerminalOutcome::DeadLetterConfirmed),
        (
            "poison-json",
            1,
            DeliveryTerminalOutcome::DeadLetterConfirmed,
        ),
        (
            "poison-metadata",
            1,
            DeliveryTerminalOutcome::DeadLetterConfirmed,
        ),
        (
            "poison-recovery",
            1,
            DeliveryTerminalOutcome::HandlerSuccess,
        ),
    ]
    .into_iter()
    .map(|(marker, attempt, outcome)| ((event_ids[marker].to_string(), attempt), outcome))
    .collect::<HashMap<_, _>>();
    let mut retained_attempts = HashSet::new();
    for observation in &terminal_observations.observations {
        let attempt = (
            observation
                .event_id
                .clone()
                .expect("canonical event identity"),
            observation.delivery_attempt,
        );
        assert_eq!(expected_details.get(&attempt), Some(&observation.outcome));
        assert!(
            retained_attempts.insert(attempt),
            "duplicate terminal detail for one attempt"
        );
    }
    assert_eq!(
        expected_details.len() as u64 - retained_attempts.len() as u64,
        terminal_observations.dropped,
        "every missing terminal detail must have exact loss accounting"
    );

    for (queue, marker) in [
        (RETRY_EXHAUST_QUEUE, "retry-exhaust"),
        (PERMANENT_QUEUE, "permanent"),
        (PANIC_QUEUE, "panic"),
        (TIMEOUT_QUEUE, "timeout"),
    ] {
        let delivery = take_one(&fixture_channel, &dlq(queue)).await;
        assert_eq!(
            delivery
                .properties
                .message_id()
                .as_ref()
                .map(|id| id.as_str()),
            Some(event_ids[marker].to_string().as_str()),
            "the actual broker DLQ must contain the exact failed logical event"
        );
        assert!(delivery.ack(BasicAckOptions::default()).await.unwrap());
        assert!(
            fixture_channel
                .basic_get(dlq(queue).into(), BasicGetOptions::default())
                .await
                .expect("inspect exact DLQ cardinality")
                .is_none(),
            "one logical terminal failure must produce one DLQ message"
        );
    }
    let mut poison_dlq_ids = HashSet::new();
    for _ in 0..2 {
        let delivery = take_one(&fixture_channel, &dlq(POISON_QUEUE)).await;
        // Invalid retry metadata is handed off with sanitized properties.
        // The canonical event header survives; optional AMQP message_id need
        // not. Verify the exact original identity AND bytes at the broker.
        let headers = delivery.properties.headers().as_ref().unwrap();
        let Some(AMQPValue::LongString(event_id)) = headers.inner().get("x-lily-event-id") else {
            panic!("poison DLQ must retain the canonical event identity");
        };
        let event_id = event_id.to_string();
        let expected_body: &[u8] = if event_id == event_ids["poison-json"].to_string() {
            br#"{"marker": "truncated""#
        } else {
            assert_eq!(event_id, event_ids["poison-metadata"].to_string());
            br#"{"marker":"poison-metadata"}"#
        };
        assert_eq!(
            delivery.data, expected_body,
            "poison handoff must preserve the exact original bytes"
        );
        assert!(poison_dlq_ids.insert(event_id));
        assert!(delivery.ack(BasicAckOptions::default()).await.unwrap());
    }
    assert_eq!(
        poison_dlq_ids,
        HashSet::from([
            event_ids["poison-json"].to_string(),
            event_ids["poison-metadata"].to_string(),
        ])
    );
    assert!(fixture_channel
        .basic_get(dlq(POISON_QUEUE).into(), BasicGetOptions::default())
        .await
        .expect("inspect poison DLQ cardinality")
        .is_none());
    for queue in [SUCCESS_QUEUE, RETRY_EVENTUAL_QUEUE, POISON_QUEUE] {
        assert!(
            fixture_channel
                .basic_get(queue.into(), BasicGetOptions::default())
                .await
                .expect("inspect acknowledged main queue")
                .is_none(),
            "ACK-confirmed messages must not be visible for redelivery"
        );
    }

    wait_for_scope_disposal("completed delivery-scope disposal").await;
    assert_eq!(WORKER_STARTS.load(Ordering::SeqCst), 11);
    assert_eq!(PROBE_STARTS.load(Ordering::SeqCst), 11);

    let entered = Arc::new(Notify::new());
    *shutdown_entered()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&entered));
    publish_json(
        &fixture_channel,
        SHUTDOWN_QUEUE,
        event_ids["shutdown"],
        "shutdown",
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("shutdown delivery did not enter its handler before the qualification deadline");
    let shutdown_started = Instant::now();
    tokio::time::timeout(Duration::from_secs(5), managed.shutdown())
        .await
        .expect("managed shutdown exceeded the qualification bound")
        .expect("managed Consumer shutdown must reconcile owned work");
    assert!(shutdown_started.elapsed() <= Duration::from_secs(5));

    let final_snapshot = queue_service
        .delivery_terminal_snapshot()
        .expect("read terminal snapshot after managed shutdown");
    assert!(final_snapshot.is_reconciled());
    assert_eq!(final_snapshot.unacked_or_in_flight(), 0);
    assert_eq!(final_snapshot.deliveries, 13);
    assert_eq!(
        final_snapshot.buffered_pending_redelivery, 1,
        "unexpected shutdown terminal classification: {final_snapshot:?}"
    );
    // Shutdown success is the barrier. A post-return polling grace period
    // would incorrectly qualify detached delivery cleanup as reconciled.
    assert_eq!(WORKER_STARTS.load(Ordering::SeqCst), 12);
    assert_eq!(WORKER_DISPOSES.load(Ordering::SeqCst), 12);
    assert_eq!(PROBE_STARTS.load(Ordering::SeqCst), 12);
    assert_eq!(PROBE_DISPOSES.load(Ordering::SeqCst), 12);

    let shutdown_event_id = event_ids["shutdown"].to_string();
    let shutdown_observations = queue_service
        .delivery_terminal_observations()
        .expect("read terminal observations after managed shutdown");
    assert_eq!(
        shutdown_observations.observations.len() as u64 + shutdown_observations.dropped,
        13
    );
    assert_eq!(
        shutdown_observations.dropped, terminal_observations.dropped,
        "the final single delivery has no competing detail reader/writer"
    );
    let shutdown_observations = shutdown_observations
        .observations
        .iter()
        .filter(|observation| observation.event_id.as_deref() == Some(shutdown_event_id.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        shutdown_observations.len(),
        1,
        "one real delivery has one terminal observation"
    );
    assert_eq!(
        shutdown_observations[0].outcome,
        DeliveryTerminalOutcome::BufferedPendingRedelivery
    );

    let requeued_shutdown = take_one(&fixture_channel, SHUTDOWN_QUEUE).await;
    assert_eq!(
        requeued_shutdown
            .properties
            .message_id()
            .as_ref()
            .map(|value| value.as_str()),
        Some(shutdown_event_id.as_str()),
        "an unacknowledged active delivery must remain available after bounded shutdown"
    );
    assert!(
        requeued_shutdown.redelivered,
        "the same previously delivered original must be released by channel close"
    );
    assert!(requeued_shutdown
        .ack(BasicAckOptions::default())
        .await
        .expect("ack shutdown recovery evidence"));

    container
        .close_with_timeout(Duration::from_secs(5))
        .await
        .expect("dispose caller-owned qualification container");
    delete_topology(&fixture_channel).await;
    fixture_connection
        .close(200, "CAP-Q-01E qualification cleanup".into())
        .await
        .expect("close fixture RabbitMQ connection");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "environment-restricted: requires LILY_TEST_RABBITMQ_URL and a fresh disposable RabbitMQ v2 fixture"]
async fn versioned_content_dispatch_uses_one_physical_receiver_and_fails_closed() {
    let _serial = qualification_lock().lock().await;
    reset_evidence();
    let uri = std::env::var("LILY_TEST_RABBITMQ_URL")
        .expect("LILY_TEST_RABBITMQ_URL must identify a disposable broker");
    let fixture_connection = connect_fixture(&uri).await;
    let fixture_channel = fixture_connection
        .create_channel()
        .await
        .expect("create fixture RabbitMQ channel");
    fixture_channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("enable fixture publisher confirms");
    let RunningConsumerFixture {
        _config_directory,
        container,
        queue_service,
        managed,
    } = start_consumer_fixture(&uri).await;

    let v1_id = Uuid::new_v4();
    let v2_json_id = Uuid::new_v4();
    let v2_binary_id = Uuid::new_v4();
    let unknown_version_id = Uuid::new_v4();
    let unknown_content_id = Uuid::new_v4();
    let v1_body = serde_json::to_vec(&QualificationMessage {
        marker: "versioned-v1-json".to_string(),
    })
    .unwrap();
    let v2_body = serde_json::to_vec(&QualificationMessage {
        marker: "versioned-v2-json".to_string(),
    })
    .unwrap();

    publish_contract_frame(
        &fixture_channel,
        VERSIONED_QUEUE,
        v1_id,
        1,
        "json",
        "application/json",
        &v1_body,
    )
    .await;
    publish_contract_frame(
        &fixture_channel,
        VERSIONED_QUEUE,
        v2_json_id,
        2,
        "json",
        "application/json",
        &v2_body,
    )
    .await;
    publish_contract_frame(
        &fixture_channel,
        VERSIONED_QUEUE,
        v2_binary_id,
        2,
        "binary",
        "application/octet-stream",
        b"capq04-v2-binary",
    )
    .await;
    publish_contract_frame(
        &fixture_channel,
        VERSIONED_QUEUE,
        unknown_version_id,
        3,
        "json",
        "application/json",
        br#"{"marker":"unsupported-version"}"#,
    )
    .await;
    publish_contract_frame(
        &fixture_channel,
        VERSIONED_QUEUE,
        unknown_content_id,
        2,
        "protobuf",
        "application/protobuf",
        &[0x0A, 0x00],
    )
    .await;

    wait_until("CAP-Q-04 exact dispatch terminal matrix", || {
        queue_service
            .delivery_terminal_snapshot()
            .is_ok_and(|snapshot| {
                snapshot.deliveries == 5
                    && snapshot.acked_handler_success == 3
                    && snapshot.acked_confirmed_handoff == 2
                    && snapshot.dead_letter_confirmed == 2
                    && snapshot.unacked_or_in_flight() == 0
                    && snapshot.is_reconciled()
            })
    })
    .await;

    let records = observations()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .filter(|record| record.queue == VERSIONED_QUEUE)
        .map(|record| {
            (
                record.marker.clone(),
                record.schema_version,
                record.content_kind.clone(),
                record.event_id.clone(),
            )
        })
        .collect::<HashSet<_>>();
    assert_eq!(
        records,
        HashSet::from([
            (
                "versioned-v1-json".to_string(),
                1,
                "json".to_string(),
                v1_id.to_string(),
            ),
            (
                "versioned-v2-json".to_string(),
                2,
                "json".to_string(),
                v2_json_id.to_string(),
            ),
            (
                "versioned-v2-binary".to_string(),
                2,
                "binary".to_string(),
                v2_binary_id.to_string(),
            ),
        ])
    );

    let mut dead_letter_identities = HashSet::new();
    for _ in 0..2 {
        let delivery = take_one(&fixture_channel, &dlq(VERSIONED_QUEUE)).await;
        let headers = delivery
            .properties
            .headers()
            .as_ref()
            .expect("version/content rejection must preserve canonical headers");
        dead_letter_identities.insert((
            long_string_header(headers, "x-lily-event-id"),
            long_string_header(headers, "x-lily-schema-version"),
            long_string_header(headers, "x-lily-content-kind"),
        ));
        assert!(delivery
            .ack(BasicAckOptions::default())
            .await
            .expect("ack inspected CAP-Q-04 DLQ delivery"));
    }
    assert_eq!(
        dead_letter_identities,
        HashSet::from([
            (
                unknown_version_id.to_string(),
                "3".to_string(),
                "json".to_string(),
            ),
            (
                unknown_content_id.to_string(),
                "2".to_string(),
                "protobuf".to_string(),
            ),
        ])
    );
    assert!(fixture_channel
        .basic_get(VERSIONED_QUEUE.into(), BasicGetOptions::default())
        .await
        .expect("inspect acknowledged versioned queue")
        .is_none());

    managed
        .shutdown()
        .await
        .expect("versioned Consumer shutdown must reconcile");
    wait_for_scope_disposal("versioned delivery-scope disposal").await;
    container
        .close_with_timeout(Duration::from_secs(5))
        .await
        .expect("dispose versioned qualification container");
    delete_topology(&fixture_channel).await;
    fixture_connection
        .close(200, "CAP-Q-04 qualification cleanup".into())
        .await
        .expect("close versioned fixture RabbitMQ connection");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "environment-restricted: requires LILY_TEST_RABBITMQ_URL and a fresh disposable RabbitMQ v2 fixture"]
async fn failed_handoff_requeues_for_a_fresh_canonical_consumer() {
    let _serial = qualification_lock().lock().await;
    reset_evidence();
    let uri = std::env::var("LILY_TEST_RABBITMQ_URL")
        .expect("LILY_TEST_RABBITMQ_URL must identify a disposable broker");
    let fixture_connection = connect_fixture(&uri).await;
    let fixture_channel = fixture_connection
        .create_channel()
        .await
        .expect("create fixture RabbitMQ channel");
    fixture_channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("enable fixture publisher confirms");

    let first = start_consumer_fixture(&uri).await;
    fixture_channel
        .queue_delete(
            dlq(HANDOFF_FAILURE_QUEUE).into(),
            QueueDeleteOptions::default(),
        )
        .await
        .expect("remove only the handoff-failure DLQ binding");
    let event_id = Uuid::new_v4();
    publish_json(
        &fixture_channel,
        HANDOFF_FAILURE_QUEUE,
        event_id,
        "handoff-failure",
    )
    .await;

    let runtime_error = tokio::time::timeout(Duration::from_secs(10), first.managed.wait())
        .await
        .expect("handoff failure runtime observation exceeded its bound")
        .expect_err("a provider handoff failure must remain visible to the composition root");
    assert_eq!(runtime_error.error_code(), "BROKER_CONSUMER_TASK_FAILED");
    let Some(MessageBrokerError::RabbitMQError(RabbitMQError::ConsumerTaskFailed(task_failure))) =
        runtime_error.message_broker_error()
    else {
        panic!("runtime failure must retain typed queue-task evidence");
    };
    assert_eq!(task_failure.queue, HANDOFF_FAILURE_QUEUE);
    assert!(
        !runtime_error.to_string().contains(HANDOFF_FAILURE_QUEUE),
        "secret-safe Consumer display must not expose provider-owned queue identity"
    );
    let first_snapshot = first
        .queue_service
        .delivery_terminal_snapshot()
        .expect("read first Consumer terminal snapshot");
    assert_eq!(first_snapshot.nacked_or_requeued, 1);
    assert_eq!(first_snapshot.acked_confirmed_handoff, 0);
    assert_eq!(first_snapshot.unacked_or_in_flight(), 0);
    assert!(first_snapshot.is_reconciled());
    let event_id_text = event_id.to_string();
    let first_observations = first
        .queue_service
        .delivery_terminal_observations()
        .expect("read failed-handoff terminal observation");
    assert_eq!(
        first_observations.dropped, 0,
        "one joined writer, no concurrent detail reader"
    );
    assert_eq!(first_observations.observations.len(), 1);
    assert_eq!(
        first_observations
            .observations
            .iter()
            .filter(|observation| {
                observation.event_id.as_deref() == Some(event_id_text.as_str())
                    && observation.outcome == DeliveryTerminalOutcome::NackRequeue
            })
            .count(),
        1,
        "handoff failure must produce exactly one requeue NACK for the event"
    );
    assert_eq!(WORKER_STARTS.load(Ordering::SeqCst), 1);
    assert_eq!(WORKER_DISPOSES.load(Ordering::SeqCst), 1);
    assert_eq!(PROBE_STARTS.load(Ordering::SeqCst), 1);
    assert_eq!(PROBE_DISPOSES.load(Ordering::SeqCst), 1);
    first
        .container
        .close_with_timeout(Duration::from_secs(5))
        .await
        .expect("dispose failed-handoff Consumer container");

    // A new canonical composition root recreates the removed DLQ binding and
    // receives the original unacknowledged delivery. The handler deliberately
    // succeeds only when RabbitMQ marks that same event as redelivered.
    let second = start_consumer_fixture(&uri).await;
    wait_until("requeued event recovery in the second Consumer", || {
        observations()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|record| record.marker == "handoff-failure")
            .count()
            == 2
            && second
                .queue_service
                .delivery_terminal_snapshot()
                .is_ok_and(|snapshot| {
                    snapshot.deliveries == 1
                        && snapshot.acked_handler_success == 1
                        && snapshot.unacked_or_in_flight() == 0
                        && snapshot.is_reconciled()
                })
    })
    .await;
    let handoff_records = observations()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .filter(|record| record.marker == "handoff-failure")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(handoff_records.len(), 2);
    assert_eq!(
        handoff_records
            .iter()
            .map(|record| (
                record.event_id.as_str(),
                record.retry_count,
                record.redelivered
            ))
            .collect::<Vec<_>>(),
        vec![
            (event_id_text.as_str(), 0, false),
            (event_id_text.as_str(), 0, true),
        ],
        "the new Consumer must ACK the unchanged, broker-redelivered event"
    );
    tokio::time::timeout(Duration::from_secs(5), second.managed.shutdown())
        .await
        .expect("second Consumer shutdown exceeded its bound")
        .expect("second Consumer shutdown must reconcile recovered delivery");
    let second_observations = second
        .queue_service
        .delivery_terminal_observations()
        .expect("read recovered delivery terminal observation after its actual owner joins");
    assert_eq!(
        second_observations.dropped, 0,
        "one joined writer, no concurrent detail reader"
    );
    assert_eq!(second_observations.observations.len(), 1);
    assert_eq!(
        second_observations.observations[0].event_id.as_deref(),
        Some(event_id_text.as_str())
    );
    assert_eq!(
        second_observations.observations[0].outcome,
        DeliveryTerminalOutcome::HandlerSuccess
    );
    assert_eq!(WORKER_STARTS.load(Ordering::SeqCst), 2);
    assert_eq!(WORKER_DISPOSES.load(Ordering::SeqCst), 2);
    assert_eq!(PROBE_STARTS.load(Ordering::SeqCst), 2);
    assert_eq!(PROBE_DISPOSES.load(Ordering::SeqCst), 2);
    second
        .container
        .close_with_timeout(Duration::from_secs(5))
        .await
        .expect("dispose recovered Consumer container");
    assert!(
        fixture_channel
            .basic_get(HANDOFF_FAILURE_QUEUE.into(), BasicGetOptions::default())
            .await
            .expect("inspect recovered main queue")
            .is_none(),
        "the recovered ACK must remove the redelivered message"
    );
    assert!(fixture_channel
        .basic_get(
            dlq(HANDOFF_FAILURE_QUEUE).into(),
            BasicGetOptions::default(),
        )
        .await
        .expect("inspect recreated handoff DLQ")
        .is_none());
    delete_topology(&fixture_channel).await;
    fixture_connection
        .close(200, "CAP-Q-01E handoff qualification cleanup".into())
        .await
        .expect("close fixture RabbitMQ connection");
}
