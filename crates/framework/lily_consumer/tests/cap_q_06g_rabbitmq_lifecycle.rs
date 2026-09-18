//! CAP-Q-06G live ownership qualification against an explicitly disposable broker.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex as StdMutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use lapin::{
    BasicProperties, Channel, Confirmation, Connection, ConnectionProperties,
    options::{
        BasicAckOptions, BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions,
        ExchangeDeleteOptions, QueueDeleteOptions,
    },
    types::{AMQPValue, FieldTable},
};
use lily_config::{
    ConfigService, LifecycleConfig, LilyConfig, QueueDefinition, QueueRetentionConfig,
    RabbitMqConfig, RabbitMqConsumerConfig, RabbitMqTopologyConfig,
};
use lily_consumer::{
    Consumer, ConsumerDependencyReport, ConsumerShutdownActionOutcome, ConsumerShutdownCompletion,
    ManagedConsumer,
};
use lily_injection::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait};
use lily_queue::{
    ConsumerRuntimeState, DeliveryCancellation, DeliveryCancellationReason, DeliveryContext, Json,
    QueueHandlerError, queue, queue_service,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

const WORKER: &str = "capq06g.lifecycle.worker";
const FIRST_QUEUE: &str = "capq06g.lifecycle.first";
const SECOND_QUEUE: &str = "capq06g.lifecycle.second";
const QUEUES: [&str; 2] = [FIRST_QUEUE, SECOND_QUEUE];

static FIRST_DELIVERIES: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy)]
enum ExecutionMode {
    CompleteAfterCancellation(DeliveryCancellationReason),
    IgnoreCancellation,
}

struct ExecutionProbe {
    event_id: Uuid,
    mode: ExecutionMode,
    entered: Notify,
    events: StdMutex<Vec<&'static str>>,
}

impl ExecutionProbe {
    fn record(&self, event: &'static str) {
        self.events.lock().unwrap().push(event);
    }
}

fn execution_probe() -> &'static StdMutex<Option<Arc<ExecutionProbe>>> {
    static PROBE: OnceLock<StdMutex<Option<Arc<ExecutionProbe>>>> = OnceLock::new();
    PROBE.get_or_init(|| StdMutex::new(None))
}

struct ExecutionDrop(Arc<ExecutionProbe>);

impl Drop for ExecutionDrop {
    fn drop(&mut self) {
        self.0.record("execution_dropped");
    }
}

fn qualification_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[derive(Debug, Serialize, Deserialize)]
struct QualificationMessage {
    marker: String,
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct LifecycleWorker {
    probe: StdMutex<Option<Arc<ExecutionProbe>>>,
}

#[async_trait]
impl ServiceTrait for LifecycleWorker {
    async fn dispose(&self) -> Result<(), lily_injection::InjectionError> {
        if let Some(probe) = self.probe.lock().unwrap().as_ref() {
            probe.record("scope_disposed");
        }
        Ok(())
    }
}

#[queue_service]
impl LifecycleWorker {
    #[queue("capq06g.lifecycle.first", version = 1, content = "json")]
    async fn first(
        &self,
        context: DeliveryContext,
        cancellation: DeliveryCancellation,
        Json(message): Json<QualificationMessage>,
    ) -> Result<(), QueueHandlerError> {
        assert_eq!(message.marker, "cap-q-06g");
        FIRST_DELIVERIES.fetch_add(1, Ordering::AcqRel);
        let probe = execution_probe()
            .lock()
            .unwrap()
            .as_ref()
            .filter(|probe| probe.event_id == context.event_id().into_inner())
            .cloned();
        if let Some(probe) = probe {
            *self.probe.lock().unwrap() = Some(Arc::clone(&probe));
            let _execution = ExecutionDrop(Arc::clone(&probe));
            probe.record("execution_entered");
            assert!(!cancellation.is_cancelled());
            probe.entered.notify_one();
            match probe.mode {
                ExecutionMode::CompleteAfterCancellation(reason) => {
                    cancellation.cancelled().await;
                    assert_eq!(cancellation.reason(), Some(reason));
                    probe.record("cancellation_observed");
                    // Require the owner to keep polling the accepted future
                    // after notification, not merely poll it once then drop.
                    tokio::task::yield_now().await;
                    probe.record("execution_completed");
                }
                ExecutionMode::IgnoreCancellation => std::future::pending::<()>().await,
            }
        }
        Ok(())
    }

    #[queue("capq06g.lifecycle.second", version = 1, content = "json")]
    async fn second(
        &self,
        _context: DeliveryContext,
        Json(_message): Json<QualificationMessage>,
    ) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

#[derive(Clone)]
struct ManagementApi {
    client: reqwest::Client,
    base: String,
    username: String,
    password: String,
    vhost: String,
}

#[derive(Clone, Debug, Default)]
struct BrokerSnapshot {
    connections: HashSet<String>,
    channels: HashSet<String>,
    queues: HashSet<String>,
    consumers_by_queue: HashMap<String, usize>,
    consumer_channels_by_queue: HashMap<String, String>,
}

impl BrokerSnapshot {
    fn queue_count(&self) -> usize {
        self.queues.len()
    }

    fn consumer_count(&self) -> usize {
        self.consumers_by_queue.values().sum()
    }
}

impl ManagementApi {
    fn from_environment() -> Self {
        assert_eq!(
            std::env::var("LILY_TEST_RABBITMQ_DISPOSABLE").as_deref(),
            Ok("1"),
            "live lifecycle qualification refuses a broker not marked disposable"
        );
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("build RabbitMQ management client"),
            base: std::env::var("LILY_TEST_RABBITMQ_MANAGEMENT_URL")
                .expect("LILY_TEST_RABBITMQ_MANAGEMENT_URL must identify the disposable broker")
                .trim_end_matches('/')
                .to_owned(),
            username: std::env::var("LILY_TEST_RABBITMQ_MANAGEMENT_USERNAME")
                .unwrap_or_else(|_| "guest".to_owned()),
            password: std::env::var("LILY_TEST_RABBITMQ_MANAGEMENT_PASSWORD")
                .unwrap_or_else(|_| "guest".to_owned()),
            vhost: std::env::var("LILY_TEST_RABBITMQ_VHOST").unwrap_or_else(|_| "/".to_owned()),
        }
    }

    async fn array(&self, path: &str) -> Vec<Value> {
        let response = self
            .client
            .get(format!("{}{path}", self.base))
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .unwrap_or_else(|error| panic!("RabbitMQ management GET {path} failed: {error}"));
        assert!(
            response.status().is_success(),
            "RabbitMQ management GET {path} returned {}",
            response.status()
        );
        response
            .json::<Vec<Value>>()
            .await
            .unwrap_or_else(|error| panic!("RabbitMQ management GET {path} JSON failed: {error}"))
    }

    async fn snapshot(&self) -> BrokerSnapshot {
        let encoded_vhost = urlencoding::encode(&self.vhost);
        let connections = self
            .array(&format!("/api/vhosts/{encoded_vhost}/connections"))
            .await;
        let channels = self
            .array(&format!("/api/vhosts/{encoded_vhost}/channels"))
            .await;
        let queues = self.array(&format!("/api/queues/{encoded_vhost}")).await;
        let consumers = self.array(&format!("/api/consumers/{encoded_vhost}")).await;
        BrokerSnapshot {
            connections: names(&connections),
            channels: names(&channels),
            queues: names(&queues),
            consumer_channels_by_queue: consumers
                .iter()
                .map(|consumer| {
                    let queue = consumer["queue"]["name"].as_str().expect("consumer queue");
                    let channel = consumer["channel_details"]["name"]
                        .as_str()
                        .expect("consumer channel identity");
                    (queue.to_owned(), channel.to_owned())
                })
                .collect(),
            consumers_by_queue: consumers.into_iter().fold(
                HashMap::<String, usize>::new(),
                |mut counts, consumer| {
                    let queue = consumer
                        .get("queue")
                        .and_then(|queue| queue.get("name"))
                        .and_then(Value::as_str)
                        .expect("management consumer entry must identify its queue");
                    *counts.entry(queue.to_owned()).or_default() += 1;
                    counts
                },
            ),
        }
    }

    async fn wait_for(
        &self,
        description: &str,
        predicate: impl Fn(&BrokerSnapshot) -> bool,
    ) -> BrokerSnapshot {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let snapshot = self.snapshot().await;
                if predicate(&snapshot) {
                    return snapshot;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for RabbitMQ {description}"))
    }

    async fn delete_connection(&self, name: &str) {
        let encoded = urlencoding::encode(name);
        let response = self
            .client
            .delete(format!("{}/api/connections/{encoded}", self.base))
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .unwrap_or_else(|error| panic!("RabbitMQ connection delete failed: {error}"));
        assert!(
            response.status().is_success(),
            "RabbitMQ connection delete returned {}",
            response.status()
        );
    }
}

fn names(values: &[Value]) -> HashSet<String> {
    values
        .iter()
        .map(|value| {
            value
                .get("name")
                .and_then(Value::as_str)
                .expect("RabbitMQ management entry must have a name")
                .to_owned()
        })
        .collect()
}

fn retention() -> QueueRetentionConfig {
    QueueRetentionConfig {
        main_max_messages: 1_000,
        main_max_bytes: 16 * 1024 * 1024,
        retry_bucket_max_messages: 100,
        retry_bucket_max_bytes: 1024 * 1024,
        dead_letter_max_messages: 100,
        dead_letter_max_bytes: 1024 * 1024,
    }
}

fn definition(name: &str) -> QueueDefinition {
    QueueDefinition {
        name: name.to_owned(),
        exchange_name: WORKER.to_owned(),
        routing_key: name.to_owned(),
        concurrency: 1,
        prefetch_count: 1,
        delivery_buffer_capacity: Some(1),
        retry_attempts: 0,
        retry_backoff_millis: Some(100),
        max_retry_backoff_millis: Some(200),
        delivery_execution_timeout_millis: Some(2_000),
        settlement_timeout_millis: Some(2_000),
        durable: true,
        max_message_size_bytes: 64 * 1024,
        retention: Some(retention()),
        ..QueueDefinition::default()
    }
}

fn config(uri: &str) -> LilyConfig {
    LilyConfig {
        lifecycle: LifecycleConfig {
            shutdown_timeout_secs: 3,
        },
        rabbitmq: RabbitMqConfig {
            consumer: Some(RabbitMqConsumerConfig {
                connection_string: Some(uri.to_owned()),
                pool_size: 1,
                connection_timeout_secs: Some(5),
                confirm_timeout_secs: Some(5),
                heartbeat_secs: Some(10),
                max_reconnect_attempts: Some(20),
                reconnect_backoff_millis: Some(750),
                use_tls: Some(uri.starts_with("amqps://")),
                persistence_enabled: true,
                ..RabbitMqConsumerConfig::default()
            }),
            topology: RabbitMqTopologyConfig {
                queues: QUEUES.into_iter().map(definition).collect(),
            },
        },
        ..LilyConfig::default()
    }
}

struct ConsumerContainer {
    _directory: tempfile::TempDir,
    container: Arc<ApplicationContainer>,
}

async fn build_container(uri: &str) -> ConsumerContainer {
    build_container_with_config(config(uri)).await
}

async fn build_container_with_config(config: LilyConfig) -> ConsumerContainer {
    let directory = tempfile::tempdir().expect("create CAP-Q-06G config directory");
    let path = directory.path().join("lily.toml");
    std::fs::write(
        &path,
        toml::to_string_pretty(&config).expect("serialize CAP-Q-06G config"),
    )
    .expect("write CAP-Q-06G config");
    let container = Arc::new(
        ApplicationContainer::builder()
            .seed_singleton(ConfigService::production(path))
            .build()
            .await
            .expect("build CAP-Q-06G Consumer container"),
    );
    ConsumerContainer {
        _directory: directory,
        container,
    }
}

async fn start_managed(container: &Arc<ApplicationContainer>) -> ManagedConsumer {
    Consumer::builder()
        .container(Arc::clone(container))
        .tracing_disabled()
        .start_managed()
        .await
        .expect("start CAP-Q-06G managed Consumer")
}

fn assert_successful_report(managed: &ManagedConsumer, forced: bool) {
    let report = managed
        .shutdown_report()
        .expect("joined runtime publishes its final report");
    assert_eq!(report, managed.snapshot().shutdown.report.unwrap());
    assert_eq!(report.forced, forced);
    assert_eq!(
        report.completion,
        if forced {
            ConsumerShutdownCompletion::ForcedCompleted
        } else {
            ConsumerShutdownCompletion::GracefulCompleted
        }
    );
    assert!(report.is_success());
    assert!(report.runtime_joined && report.coordinator_accounted && report.coordinator_complete);
    assert!(report.queue_drain_reconciled && report.queue_close_reconciled);
    assert_eq!(report.failure_code, None);
    // These fixtures supply their own DI container and disable tracing and
    // signal handling. Neither shutdown nor its report can claim that work.
    assert_eq!(report.dependencies, ConsumerDependencyReport::default());
    assert!(report.actions.iter().all(|action| {
        action.graceful == ConsumerShutdownActionOutcome::Completed
            || action.forced == ConsumerShutdownActionOutcome::Completed
    }));
    if forced {
        assert!(
            report.actions.iter().any(|action| {
                action.graceful == ConsumerShutdownActionOutcome::TimedOut
                    && action.forced == ConsumerShutdownActionOutcome::Completed
            }),
            "the original deadline expiry must remain visible after successful force cleanup"
        );
    }
}

async fn connect_fixture(uri: &str) -> (Connection, Channel) {
    let connection = Connection::connect(uri, ConnectionProperties::default())
        .await
        .expect("connect disposable CAP-Q-06G RabbitMQ fixture");
    let channel = connection
        .create_channel()
        .await
        .expect("create CAP-Q-06G fixture channel");
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("enable CAP-Q-06G publisher confirms");
    (connection, channel)
}

fn envelope_properties(event_id: Uuid) -> BasicProperties {
    let mut headers = FieldTable::default();
    headers.insert(
        "x-lily-event-id".into(),
        AMQPValue::LongString(event_id.to_string().into()),
    );
    headers.insert(
        "x-lily-schema-version".into(),
        AMQPValue::LongString("1".into()),
    );
    headers.insert(
        "x-lily-content-kind".into(),
        AMQPValue::LongString("json".into()),
    );
    BasicProperties::default()
        .with_content_type("application/json".into())
        .with_message_id(event_id.to_string().into())
        .with_delivery_mode(2)
        .with_headers(headers)
}

async fn publish(channel: &Channel) {
    publish_event(channel, Uuid::new_v4()).await;
}

async fn publish_event(channel: &Channel, event_id: Uuid) {
    let body = serde_json::to_vec(&QualificationMessage {
        marker: "cap-q-06g".to_owned(),
    })
    .expect("serialize CAP-Q-06G delivery");
    let confirmation = channel
        .basic_publish(
            WORKER.into(),
            FIRST_QUEUE.into(),
            BasicPublishOptions {
                mandatory: true,
                ..BasicPublishOptions::default()
            },
            &body,
            envelope_properties(event_id),
        )
        .await
        .expect("publish CAP-Q-06G delivery")
        .await
        .expect("await CAP-Q-06G publish confirm");
    assert_eq!(confirmation, Confirmation::Ack(None));
}

async fn wait_for_delivery(expected: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while FIRST_DELIVERIES.load(Ordering::Acquire) < expected {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("CAP-Q-06G handler delivery timed out");
}

fn exact_consumers(snapshot: &BrokerSnapshot) -> bool {
    snapshot.consumer_count() == 2
        && snapshot.consumers_by_queue.get(FIRST_QUEUE) == Some(&1)
        && snapshot.consumers_by_queue.get(SECOND_QUEUE) == Some(&1)
}

async fn delete_topology(channel: &Channel) {
    for queue in QUEUES {
        for name in [queue.to_owned(), format!("{queue}.dlq.v2")] {
            let _ = channel
                .queue_delete(name.into(), QueueDeleteOptions::default())
                .await;
        }
    }
    for exchange in [
        format!("{WORKER}.retry.v2"),
        format!("{WORKER}.dlx.v2"),
        WORKER.to_owned(),
    ] {
        let _ = channel
            .exchange_delete(exchange.into(), ExchangeDeleteOptions::default())
            .await;
    }
}

async fn assert_clean_disposable(api: &ManagementApi) {
    let snapshot = api.snapshot().await;
    assert!(snapshot.connections.is_empty(), "connections: {snapshot:?}");
    assert!(snapshot.channels.is_empty(), "channels: {snapshot:?}");
    assert_eq!(snapshot.queue_count(), 0, "queues: {snapshot:?}");
    assert_eq!(snapshot.consumer_count(), 0, "consumers: {snapshot:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "environment-restricted: requires disposable RabbitMQ + management API variables"]
async fn partial_basic_consume_abort_reconciles_and_same_queues_restart_cleanly() {
    let _serial = qualification_lock().lock().await;
    FIRST_DELIVERIES.store(0, Ordering::Release);
    let uri = std::env::var("LILY_TEST_RABBITMQ_URL")
        .expect("LILY_TEST_RABBITMQ_URL must identify the disposable broker");
    let api = ManagementApi::from_environment();
    assert_clean_disposable(&api).await;
    let (fixture_connection, fixture_channel) = connect_fixture(&uri).await;
    let baseline = api
        .wait_for("fixture connection/channel baseline", |snapshot| {
            snapshot.connections.len() == 1
                && snapshot.channels.len() == 1
                && snapshot.consumer_count() == 0
        })
        .await;

    let first = build_container(&uri).await;
    let probe = lily_queue::__private::install_rabbitmq_registration_handoff_probe(SECOND_QUEUE);
    let starting_container = Arc::clone(&first.container);
    let starting = tokio::spawn(async move { start_managed(&starting_container).await });
    tokio::time::timeout(
        Duration::from_secs(15),
        probe.wait_for_accepted(SECOND_QUEUE, 1),
    )
    .await
    .expect("second Basic.Consume must be broker-accepted before readiness publication");
    assert_eq!(probe.accepted_count(FIRST_QUEUE), 1);
    assert_eq!(probe.channel_count(), 2);
    api.wait_for("two partially-started consumers", exact_consumers)
        .await;

    starting.abort();
    let join_error = match starting.await {
        Ok(_) => panic!("startup waiter must be cancelled"),
        Err(error) => error,
    };
    assert!(join_error.is_cancelled());
    api.wait_for("partial startup cleanup baseline", |snapshot| {
        snapshot.connections == baseline.connections
            && snapshot.channels == baseline.channels
            && snapshot.consumer_count() == 0
    })
    .await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while probe.connected_channel_count() != 0 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("every broker-accepted partial-start channel must close");
    probe.release();
    drop(probe);
    first
        .container
        .close_with_timeout(Duration::from_secs(5))
        .await
        .expect("dispose first caller-owned container");

    let second = build_container(&uri).await;
    let managed = start_managed(&second.container).await;
    api.wait_for("same-queue restart readiness", exact_consumers)
        .await;
    publish(&fixture_channel).await;
    wait_for_delivery(1).await;
    managed
        .shutdown()
        .await
        .expect("restarted Consumer shutdown must reconcile");
    second
        .container
        .close_with_timeout(Duration::from_secs(5))
        .await
        .expect("dispose restarted caller-owned container");
    api.wait_for("post-restart shutdown baseline", |snapshot| {
        snapshot.connections == baseline.connections
            && snapshot.channels == baseline.channels
            && snapshot.consumer_count() == 0
    })
    .await;

    delete_topology(&fixture_channel).await;
    fixture_connection
        .close(200, "CAP-Q-06G partial-start cleanup".into())
        .await
        .expect("close CAP-Q-06G fixture connection");
    api.wait_for("zero final partial-start resources", |snapshot| {
        snapshot.connections.is_empty()
            && snapshot.channels.is_empty()
            && snapshot.queue_count() == 0
            && snapshot.consumer_count() == 0
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "environment-restricted: requires disposable RabbitMQ + management API variables"]
async fn broker_disconnect_recovers_exact_consumers_and_shutdown_returns_to_baseline() {
    let _serial = qualification_lock().lock().await;
    FIRST_DELIVERIES.store(0, Ordering::Release);
    let uri = std::env::var("LILY_TEST_RABBITMQ_URL")
        .expect("LILY_TEST_RABBITMQ_URL must identify the disposable broker");
    let api = ManagementApi::from_environment();
    assert_clean_disposable(&api).await;
    let (fixture_connection, fixture_channel) = connect_fixture(&uri).await;
    let baseline = api
        .wait_for("fixture connection/channel baseline", |snapshot| {
            snapshot.connections.len() == 1
                && snapshot.channels.len() == 1
                && snapshot.consumer_count() == 0
        })
        .await;

    let consumer = build_container(&uri).await;
    let managed = start_managed(&consumer.container).await;
    let ready = api
        .wait_for("initial exact consumers", |snapshot| {
            exact_consumers(snapshot)
                && snapshot.connections.len() == baseline.connections.len() + 1
        })
        .await;
    let owned_connections = ready
        .connections
        .difference(&baseline.connections)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(owned_connections.len(), 1, "ready snapshot: {ready:?}");

    api.delete_connection(&owned_connections[0]).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = managed.snapshot();
            if !snapshot.ready
                && matches!(
                    snapshot.deliveries.runtime_state,
                    ConsumerRuntimeState::Recovering
                )
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("managed Consumer must expose broker recovery after disconnect");
    api.wait_for("recovered exact consumers", |snapshot| {
        exact_consumers(snapshot) && snapshot.connections.len() == baseline.connections.len() + 1
    })
    .await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !managed.snapshot().ready {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("managed Consumer must return to ready after broker reconnect");
    publish(&fixture_channel).await;
    wait_for_delivery(1).await;

    managed
        .shutdown()
        .await
        .expect("recovered Consumer shutdown must reconcile");
    consumer
        .container
        .close_with_timeout(Duration::from_secs(5))
        .await
        .expect("dispose recovered caller-owned container");
    api.wait_for("recovered shutdown baseline", |snapshot| {
        snapshot.connections == baseline.connections
            && snapshot.channels == baseline.channels
            && snapshot.consumer_count() == 0
    })
    .await;

    delete_topology(&fixture_channel).await;
    fixture_connection
        .close(200, "CAP-Q-06G broker-disconnect cleanup".into())
        .await
        .expect("close CAP-Q-06G fixture connection");
    api.wait_for("zero final broker-disconnect resources", |snapshot| {
        snapshot.connections.is_empty()
            && snapshot.channels.is_empty()
            && snapshot.queue_count() == 0
            && snapshot.consumer_count() == 0
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "environment-restricted: requires disposable RabbitMQ + management API variables"]
async fn basic_cancel_retains_the_active_delivery_channel_until_settlement_finishes() {
    let _serial = qualification_lock().lock().await;
    FIRST_DELIVERIES.store(0, Ordering::Release);
    let uri = std::env::var("LILY_TEST_RABBITMQ_URL").expect("disposable RabbitMQ URI");
    let api = ManagementApi::from_environment();
    assert_clean_disposable(&api).await;
    let (fixture_connection, fixture_channel) = connect_fixture(&uri).await;
    let baseline = api
        .wait_for("fixture baseline", |state| {
            state.connections.len() == 1 && state.channels.len() == 1 && state.consumer_count() == 0
        })
        .await;

    // Broker management observations have their own latency. Leave time to
    // observe Basic.Cancel before deliberately releasing the paused ACK.
    let mut settings = config(&uri);
    settings.lifecycle.shutdown_timeout_secs = 30;
    let container = build_container_with_config(settings).await;
    let managed = Arc::new(start_managed(&container.container).await);
    assert!(managed.shutdown_report().is_none());
    let ready = api.wait_for("both ready consumers", exact_consumers).await;
    let active_channel = ready
        .consumer_channels_by_queue
        .get(FIRST_QUEUE)
        .unwrap()
        .clone();
    let event_id = Uuid::new_v4();
    let probe = lily_queue::__private::install_rabbitmq_post_handler_settlement_probe(event_id);
    publish_event(&fixture_channel, event_id).await;
    tokio::time::timeout(Duration::from_secs(10), probe.wait_until_paused())
        .await
        .expect("pipeline must finish before its ACK is paused");
    assert_eq!(managed.snapshot().deliveries.acked_handler_success, 0);

    let stopping = managed.clone();
    let shutdown = tokio::spawn(async move { stopping.shutdown().await });
    api.wait_for(
        "cancelled subscription retaining its active channel",
        |state| {
            state.consumer_count() == 0
                && state.channels.contains(&active_channel)
                && state.connections == ready.connections
        },
    )
    .await;
    assert!(
        !shutdown.is_finished(),
        "channel barrier must wait for the real delivery task"
    );
    assert_eq!(managed.snapshot().deliveries.acked_handler_success, 0);
    let late_event_id = Uuid::new_v4();
    publish_event(&fixture_channel, late_event_id).await;
    // Cancellation of the public observer cannot discard the original drain
    // operation, its accepted delivery, or its channel-close obligation.
    shutdown.abort();
    assert!(
        shutdown
            .await
            .expect_err("cancel observer only")
            .is_cancelled()
    );
    assert!(!managed.snapshot().shutdown.completed);
    assert!(managed.shutdown_report().is_none());
    probe.release();
    tokio::time::timeout(Duration::from_secs(5), managed.wait())
        .await
        .expect("settlement release must allow shutdown")
        .expect("graceful settlement and close");
    assert_eq!(FIRST_DELIVERIES.load(Ordering::Acquire), 1);
    let terminal = managed.snapshot();
    assert_eq!(terminal.deliveries.deliveries, 1);
    assert_eq!(terminal.deliveries.acked_handler_success, 1);
    assert_eq!(terminal.deliveries.unacked_or_in_flight(), 0);
    assert!(terminal.shutdown.drain_reconciled);
    assert_successful_report(&managed, false);
    api.wait_for("channel and connection returned to baseline", |state| {
        state.connections == baseline.connections
            && state.channels == baseline.channels
            && state.consumer_count() == 0
    })
    .await;
    let late = fixture_channel
        .basic_get(FIRST_QUEUE.into(), BasicGetOptions::default())
        .await
        .expect("inspect source queue after ACK and close")
        .expect("post-admission publish must remain available in the broker");
    assert_eq!(
        late.properties.message_id().as_ref().map(|id| id.as_str()),
        Some(late_event_id.to_string().as_str()),
        "the acknowledged original must not reappear and the late event must not enter execution"
    );
    assert!(
        !late.redelivered,
        "closed admission never received the late event"
    );
    assert!(late.ack(BasicAckOptions::default()).await.unwrap());
    assert!(
        fixture_channel
            .basic_get(FIRST_QUEUE.into(), BasicGetOptions::default())
            .await
            .unwrap()
            .is_none()
    );

    container
        .container
        .close_with_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    delete_topology(&fixture_channel).await;
    fixture_connection
        .close(200, "stage 4 settlement barrier qualification".into())
        .await
        .unwrap();
    api.wait_for("zero final settlement-barrier resources", |state| {
        state.connections.is_empty()
            && state.channels.is_empty()
            && state.queue_count() == 0
            && state.consumer_count() == 0
    })
    .await;
}

async fn qualify_execution_cancellation(mode: ExecutionMode, local_timeout: bool) {
    let _serial = qualification_lock().lock().await;
    FIRST_DELIVERIES.store(0, Ordering::Release);
    let uri = std::env::var("LILY_TEST_RABBITMQ_URL").expect("disposable RabbitMQ URI");
    let api = ManagementApi::from_environment();
    assert_clean_disposable(&api).await;
    let (fixture_connection, fixture_channel) = connect_fixture(&uri).await;
    let baseline = api
        .wait_for("cancellation fixture baseline", |state| {
            state.connections.len() == 1 && state.channels.len() == 1 && state.consumer_count() == 0
        })
        .await;
    let mut settings = config(&uri);
    if !local_timeout {
        // The delivery's own deadline cannot win this shutdown scenario.
        settings.rabbitmq.topology.queues[0].delivery_execution_timeout_millis = Some(60_000);
    }
    let container = build_container_with_config(settings).await;
    let managed = start_managed(&container.container).await;
    assert!(managed.shutdown_report().is_none());
    api.wait_for("cancellation test ready consumers", exact_consumers)
        .await;

    let event_id = Uuid::new_v4();
    let probe = Arc::new(ExecutionProbe {
        event_id,
        mode,
        entered: Notify::new(),
        events: StdMutex::new(Vec::new()),
    });
    *execution_probe().lock().unwrap() = Some(Arc::clone(&probe));
    publish_event(&fixture_channel, event_id).await;
    tokio::time::timeout(Duration::from_secs(5), probe.entered.notified())
        .await
        .expect("real generated adapter must enter the handler before shutdown");

    if local_timeout {
        tokio::time::timeout(Duration::from_secs(5), async {
            while managed.snapshot().deliveries.acked_handler_success != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("a cooperative timeout result must reach real AMQP ACK");
        assert!(
            managed.snapshot().ready,
            "local timeout cannot close admission"
        );
        assert!(!managed.snapshot().shutdown.requested);
        // Same subscription remains usable after local cancellation. A token
        // shared accidentally with a generation would prevent this execution.
        publish(&fixture_channel).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while managed.snapshot().deliveries.acked_handler_success != 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("next delivery on the same subscription must execute and ACK");
    }

    tokio::time::timeout(Duration::from_secs(4), managed.shutdown())
        .await
        .expect("shutdown must use its configured three-second root")
        .expect("bounded delivery termination, actual joins and cleanup must reconcile");
    let terminal = managed.snapshot();
    assert!(terminal.shutdown.completed);
    assert!(terminal.shutdown.drain_reconciled);
    assert_eq!(terminal.shutdown.forced, !local_timeout);
    assert_successful_report(&managed, !local_timeout);
    assert_eq!(
        terminal.deliveries.deliveries,
        if local_timeout { 2 } else { 1 }
    );
    assert_eq!(terminal.deliveries.in_flight, 0);
    assert_eq!(terminal.deliveries.unresolved, 0);
    assert_eq!(terminal.deliveries.acked_confirmed_handoff, 0);
    assert_eq!(terminal.deliveries.nacked_or_requeued, 0);
    assert_eq!(terminal.deliveries.handler_failure, 0);
    assert_eq!(terminal.deliveries.handler_panic, 0);
    assert_eq!(
        FIRST_DELIVERIES.load(Ordering::Acquire),
        if local_timeout { 2 } else { 1 }
    );

    let cooperative = matches!(mode, ExecutionMode::CompleteAfterCancellation(_));
    let expected_events = if cooperative {
        vec![
            "execution_entered",
            "cancellation_observed",
            "execution_completed",
            "execution_dropped",
            "scope_disposed",
        ]
    } else {
        vec!["execution_entered", "execution_dropped", "scope_disposed"]
    };
    assert_eq!(
        *probe.events.lock().unwrap(),
        expected_events,
        "scope disposal must already be complete when shutdown returns, exactly once and after execution termination"
    );
    assert_eq!(
        terminal.deliveries.acked_handler_success,
        if cooperative {
            if local_timeout { 2 } else { 1 }
        } else {
            0
        }
    );
    assert_eq!(
        terminal.deliveries.buffered_pending_redelivery,
        u64::from(!cooperative)
    );
    api.wait_for("cancellation shutdown transport baseline", |state| {
        state.connections == baseline.connections
            && state.channels == baseline.channels
            && state.consumer_count() == 0
    })
    .await;

    let recovered = fixture_channel
        .basic_get(FIRST_QUEUE.into(), BasicGetOptions::default())
        .await
        .expect("inspect actual broker state after child joins and channel close");
    if cooperative {
        assert!(
            recovered.is_none(),
            "successful cooperative work must not be redelivered"
        );
    } else {
        let recovered = recovered.expect("unacknowledged interrupted work must survive shutdown");
        assert_eq!(
            recovered
                .properties
                .message_id()
                .as_ref()
                .map(|id| id.as_str()),
            Some(event_id.to_string().as_str())
        );
        assert!(
            recovered.redelivered,
            "original must have reached the first consumer"
        );
        assert!(recovered.ack(BasicAckOptions::default()).await.unwrap());
        assert!(
            fixture_channel
                .basic_get(FIRST_QUEUE.into(), BasicGetOptions::default())
                .await
                .unwrap()
                .is_none(),
            "the interrupted delivery must not have been duplicated"
        );
    }
    assert!(
        fixture_channel
            .basic_get(
                format!("{FIRST_QUEUE}.dlq.v2").into(),
                BasicGetOptions::default()
            )
            .await
            .unwrap()
            .is_none(),
        "framework cancellation must not fabricate a handler-failure DLQ handoff"
    );

    container
        .container
        .close_with_timeout(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        *probe.events.lock().unwrap(),
        expected_events,
        "caller-owned container close cannot rerun a delivery disposer"
    );
    *execution_probe().lock().unwrap() = None;
    delete_topology(&fixture_channel).await;
    fixture_connection
        .close(200, "stage 6 cancellation qualification".into())
        .await
        .unwrap();
    api.wait_for("zero final cancellation resources", |state| {
        state.connections.is_empty()
            && state.channels.is_empty()
            && state.queue_count() == 0
            && state.consumer_count() == 0
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "environment-restricted: requires disposable RabbitMQ + management API variables"]
async fn local_timeout_preserves_cooperative_ack_and_same_subscription_remains_usable() {
    qualify_execution_cancellation(
        ExecutionMode::CompleteAfterCancellation(DeliveryCancellationReason::DeliveryTimeout),
        true,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "environment-restricted: requires disposable RabbitMQ + management API variables"]
async fn shutdown_deadline_preserves_cooperative_success_through_real_ack_and_scope_disposal() {
    qualify_execution_cancellation(
        ExecutionMode::CompleteAfterCancellation(DeliveryCancellationReason::ShutdownDeadline),
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "environment-restricted: requires disposable RabbitMQ + management API variables"]
async fn uncooperative_shutdown_drops_execution_before_scope_and_broker_redelivers_exact_original()
{
    qualify_execution_cancellation(ExecutionMode::IgnoreCancellation, false).await;
}
