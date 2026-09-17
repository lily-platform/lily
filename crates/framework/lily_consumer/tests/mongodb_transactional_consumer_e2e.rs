#![cfg(any(
    feature = "transactional-inbox-mongodb",
    feature = "transactional-inbox-mongodb-factory"
))]

//! Two-process CAP-08-1 qualification against disposable MongoDB and RabbitMQ.
//!
//! This ignored target exercises the public `Consumer` composition root. It
//! intentionally accepts only dedicated fixtures and never provisions MongoDB
//! schema from runtime startup; the parent applies the explicit migration
//! before either child listener is admitted.

use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Write as _},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use lapin::{
    message::BasicGetMessage,
    options::{
        BasicAckOptions, BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions,
        ExchangeDeclareOptions, ExchangeDeleteOptions, QueueBindOptions, QueueDeclareOptions,
        QueueDeleteOptions,
    },
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};
#[cfg(feature = "transactional-inbox-mongodb-factory")]
use lily_config::DatabaseCellConfig;
use lily_config::{
    ConfigOptions, ConfigService, DatabaseConfig, LifecycleConfig, LilyConfig,
    MongoTransactionalInboxConfig, QueueDefinition, QueueRetentionConfig, RabbitMqConfig,
    RabbitMqConsumerConfig, RabbitMqTlsConfig, RabbitMqTopologyConfig, TransactionalInboxBackend,
    TransactionalInboxConfig,
};
use lily_consumer::{Consumer, ManagedConsumer};
use lily_error::injection::InjectionError;
use lily_injection::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait};
use lily_mongodb::DatabaseService;
#[cfg(feature = "transactional-inbox-mongodb-factory")]
use lily_mongodb::MongoFactory;
use lily_queue::__private::{
    install_outbox_post_confirm_probe, install_rabbitmq_post_handler_settlement_probe,
};
use lily_queue::{
    queue, queue_service, DeliveryCancellation, DeliveryCancellationReason, Json,
    MongoInboxOutboxMigrator, MongoTransaction, PublishContentKind, QueueHandlerError,
    TransactionalOutboxMessage,
};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const INPUT_EXCHANGE: &str = "cap081.qualification.consumer";
const INPUT_QUEUE: &str = "cap081.qualification.consumer.orders";
const OUTPUT_EXCHANGE: &str = "cap081.qualification.events";
const OUTPUT_QUEUE: &str = "cap081.qualification.events.observer";
const OUTPUT_ROUTING_KEY: &str = "orders.applied";

const CHILD_ROLE: &str = "LILY_CAP081_CHILD_ROLE";
const CHILD_CONFIG: &str = "LILY_CAP081_CHILD_CONFIG";
const CHILD_STATE: &str = "LILY_CAP081_CHILD_STATE";
const CHILD_STOP: &str = "LILY_CAP081_CHILD_STOP";
const CHILD_POST_HANDLER_MARKER: &str = "LILY_CAP081_CHILD_POST_HANDLER_MARKER";
const CHILD_POST_CONFIRM_MARKER: &str = "LILY_CAP081_CHILD_POST_CONFIRM_MARKER";
const CHILD_HANDLER_INVOCATIONS: &str = "LILY_CAP081_CHILD_HANDLER_INVOCATIONS";
const CHILD_SHUTDOWN_HANDLER_MARKER: &str = "LILY_CAP081_CHILD_SHUTDOWN_HANDLER_MARKER";
const CHILD_SHUTDOWN_DISPOSED_MARKER: &str = "LILY_CAP081_CHILD_SHUTDOWN_DISPOSED_MARKER";

const POST_HANDLER_EVENT_ID: &str = "08100000-0000-4000-8000-000000000001";
const POST_HANDLER_OUTPUT_ID: &str = "08100000-0000-4000-8000-000000000002";
const POST_CONFIRM_EVENT_ID: &str = "08100000-0000-4000-8000-000000000003";
const POST_CONFIRM_OUTPUT_ID: &str = "08100000-0000-4000-8000-000000000004";
const DEADLINE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QualificationInput {
    marker: String,
    outgoing_event_id: String,
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct MongoTransactionalQualificationWorker {
    shutdown_active: AtomicBool,
}

struct ShutdownExecutionDrop(PathBuf);

impl Drop for ShutdownExecutionDrop {
    fn drop(&mut self) {
        assert!(own_marker(&self.0.with_extension("dropped")));
    }
}

#[async_trait]
impl ServiceTrait for MongoTransactionalQualificationWorker {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        if self.shutdown_active.swap(false, Ordering::AcqRel) {
            let handler_marker =
                PathBuf::from(std::env::var_os(CHILD_SHUTDOWN_HANDLER_MARKER).unwrap());
            let dropped_by: u32 = fs::read_to_string(handler_marker.with_extension("dropped"))
                .expect("execution must be dropped before its request scope disposer")
                .trim()
                .parse()
                .unwrap();
            assert_eq!(dropped_by, std::process::id());
            let marker = PathBuf::from(
                std::env::var_os(CHILD_SHUTDOWN_DISPOSED_MARKER)
                    .expect("shutdown disposer marker path"),
            );
            let _ = own_marker(&marker);
        }
        Ok(())
    }
}

#[queue_service]
impl MongoTransactionalQualificationWorker {
    #[queue(
        "cap081.qualification.consumer.orders",
        version = 1,
        content = "json",
        delivery_guarantee = "transactional_inbox"
    )]
    async fn apply(
        &self,
        transaction: MongoTransaction,
        cancellation: DeliveryCancellation,
        Json(input): Json<QualificationInput>,
    ) -> Result<(), QueueHandlerError> {
        let outgoing = Uuid::parse_str(&input.outgoing_event_id)
            .map_err(|_| QueueHandlerError::permanent("CAP081_OUTPUT_EVENT_ID_INVALID"))?;
        let evidence = PathBuf::from(
            std::env::var_os(CHILD_HANDLER_INVOCATIONS).expect("handler invocation evidence path"),
        );
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(evidence)
            .expect("open handler invocation evidence");
        writeln!(file, "{}:{}", std::process::id(), input.marker)
            .expect("append handler invocation evidence");
        file.sync_data().expect("flush handler invocation evidence");

        if input.marker == "shutdown-active" {
            self.shutdown_active.store(true, Ordering::Release);
            let marker = PathBuf::from(
                std::env::var_os(CHILD_SHUTDOWN_HANDLER_MARKER)
                    .expect("shutdown handler marker path"),
            );
            let _drop = ShutdownExecutionDrop(marker.clone());
            let _ = own_marker(&marker);
            cancellation.cancelled().await;
            assert_eq!(
                cancellation.reason(),
                Some(DeliveryCancellationReason::ShutdownDeadline)
            );
            assert!(own_marker(&marker.with_extension("cancelled")));
            // Exercise actual bounded execution stop. Returning a retryable
            // error here would be a completed application result, whose retry
            // or DLQ settlement must be preserved instead of redelivery.
            std::future::pending::<()>().await;
            unreachable!("pending execution must be dropped by its real owner");
        }

        transaction
            .enqueue(
                TransactionalOutboxMessage::try_new(
                    outgoing,
                    OUTPUT_EXCHANGE,
                    OUTPUT_ROUTING_KEY,
                    1,
                    PublishContentKind::Json,
                    br#"{"kind":"mongo-order-applied"}"#.to_vec(),
                )
                .map_err(|_| QueueHandlerError::permanent("CAP081_OUTBOX_INVALID"))?,
            )
            .await
            .map_err(|error| QueueHandlerError::retryable(error.code()))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChildEvidence {
    ready: bool,
    terminal: bool,
    drain_reconciled: bool,
    relay_ready: u64,
    relay_in_flight: u64,
    inbox_body_attempts: u64,
    inbox_lease_contentions: u64,
    inbox_post_commit_release_failures: u64,
}

fn publish_state(path: &Path, state: &ChildEvidence) {
    let temporary = path.with_extension("tmp");
    fs::write(
        &temporary,
        serde_json::to_vec(state).expect("serialize child state"),
    )
    .expect("write child state");
    fs::rename(temporary, path).expect("publish child state atomically");
}

fn state(managed: &ManagedConsumer) -> ChildEvidence {
    let snapshot = managed.snapshot();
    ChildEvidence {
        ready: snapshot.ready,
        terminal: snapshot.shutdown.completed,
        drain_reconciled: snapshot.shutdown.drain_reconciled,
        relay_ready: snapshot.transactional_outbox.ready_relays,
        relay_in_flight: snapshot.transactional_outbox.in_flight,
        inbox_body_attempts: snapshot.transactional_inbox.body_attempts_total,
        inbox_lease_contentions: snapshot.transactional_inbox.lease_contentions_total,
        inbox_post_commit_release_failures: snapshot
            .transactional_inbox
            .post_commit_release_failures_total,
    }
}

fn own_marker(path: &Path) -> bool {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            writeln!(file, "{}", std::process::id()).expect("write marker PID");
            file.sync_all().expect("flush marker PID");
            true
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => false,
        Err(error) => panic!("create process marker {}: {error}", path.display()),
    }
}

async fn build_container(config: &Path) -> Arc<ApplicationContainer> {
    Arc::new(
        ApplicationContainer::builder()
            .seed_singleton(ConfigService::new(ConfigOptions::test(config)))
            .build()
            .await
            .expect("build CAP-08-1 container"),
    )
}

async fn run_child() {
    let config = PathBuf::from(std::env::var_os(CHILD_CONFIG).expect("child config"));
    let state_path = PathBuf::from(std::env::var_os(CHILD_STATE).expect("child state"));
    let stop = PathBuf::from(std::env::var_os(CHILD_STOP).expect("child stop"));
    let handler_marker =
        PathBuf::from(std::env::var_os(CHILD_POST_HANDLER_MARKER).expect("post-handler marker"));
    let confirm_marker =
        PathBuf::from(std::env::var_os(CHILD_POST_CONFIRM_MARKER).expect("post-confirm marker"));
    let handler_probe = install_rabbitmq_post_handler_settlement_probe(
        Uuid::parse_str(POST_HANDLER_EVENT_ID).expect("post-handler ID"),
    );
    let confirm_probe = install_outbox_post_confirm_probe(
        Uuid::parse_str(POST_CONFIRM_OUTPUT_ID).expect("post-confirm output ID"),
    );
    let handler_monitor = tokio::spawn(async move {
        handler_probe.wait_until_paused().await;
        if !own_marker(&handler_marker) {
            handler_probe.release();
        }
    });
    let confirm_monitor = tokio::spawn(async move {
        confirm_probe.wait_until_paused().await;
        if !own_marker(&confirm_marker) {
            confirm_probe.release();
        }
    });

    let container = build_container(&config).await;
    let managed = Consumer::builder()
        .container(Arc::clone(&container))
        .tracing_disabled()
        .start_managed()
        .await
        .expect("start MongoDB transactional child");
    loop {
        let snapshot = state(&managed);
        publish_state(&state_path, &snapshot);
        if snapshot.ready && snapshot.relay_ready == 1 {
            break;
        }
        assert!(!snapshot.terminal, "child terminated before readiness");
        sleep(Duration::from_millis(25)).await;
    }
    while !stop.exists() {
        publish_state(&state_path, &state(&managed));
        sleep(Duration::from_millis(25)).await;
    }
    tokio::time::timeout(DEADLINE, managed.shutdown())
        .await
        .expect("child shutdown deadline")
        .expect("child shutdown");
    let terminal = state(&managed);
    assert!(terminal.terminal && terminal.drain_reconciled);
    assert_eq!(terminal.relay_in_flight, 0);
    let report = managed
        .shutdown_report()
        .expect("actual runtime join must publish final evidence");
    assert!(
        report.runtime_joined && report.queue_drain_reconciled && report.queue_close_reconciled
    );
    assert!(report.coordinator_accounted && report.coordinator_complete && report.is_success());
    assert_eq!(
        report.dependencies.di,
        lily_consumer::ConsumerResourceReport::default(),
        "caller-owned DI cannot be reported as disposed by Consumer"
    );
    assert_eq!(managed.snapshot().shutdown.report.as_ref(), Some(&report));
    publish_state(&state_path, &terminal);
    container.close().await.expect("child DI shutdown");
    // Fixture-owned task cleanup also requires join evidence; process exit
    // must not hide a failed or still-running probe.
    for monitor in [handler_monitor, confirm_monitor] {
        monitor.abort();
        if let Err(error) = monitor.await {
            assert!(error.is_cancelled(), "fixture monitor panicked: {error}");
        }
    }
}

struct ChildConsumer {
    process: Child,
    state: PathBuf,
    stop: PathBuf,
    stdout: PathBuf,
    stderr: PathBuf,
}

impl ChildConsumer {
    fn spawn(role: &str, config: &Path, evidence: &Path) -> Self {
        let state = evidence.join(format!("consumer-{role}.json"));
        let stop = evidence.join(format!("consumer-{role}.stop"));
        let stdout = evidence.join(format!("consumer-{role}.stdout.log"));
        let stderr = evidence.join(format!("consumer-{role}.stderr.log"));
        let process = Command::new(std::env::current_exe().expect("integration test binary"))
            .args([
                "--exact",
                "mongodb_transactional_consumer_two_process_e2e",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_ROLE, role)
            .env(CHILD_CONFIG, config)
            .env(CHILD_STATE, &state)
            .env(CHILD_STOP, &stop)
            .env(
                CHILD_POST_HANDLER_MARKER,
                evidence.join("post-handler.marker"),
            )
            .env(
                CHILD_POST_CONFIRM_MARKER,
                evidence.join("post-confirm.marker"),
            )
            .env(
                CHILD_HANDLER_INVOCATIONS,
                evidence.join("handler-invocations.log"),
            )
            .env(
                CHILD_SHUTDOWN_HANDLER_MARKER,
                evidence.join("shutdown-handler.marker"),
            )
            .env(
                CHILD_SHUTDOWN_DISPOSED_MARKER,
                evidence.join("shutdown-disposed.marker"),
            )
            .stdout(Stdio::from(File::create(&stdout).expect("child stdout")))
            .stderr(Stdio::from(File::create(&stderr).expect("child stderr")))
            .spawn()
            .expect("spawn child Consumer");
        Self {
            process,
            state,
            stop,
            stdout,
            stderr,
        }
    }

    fn logs(&self) -> String {
        format!(
            "stdout:\n{}\nstderr:\n{}",
            fs::read_to_string(&self.stdout).unwrap_or_default(),
            fs::read_to_string(&self.stderr).unwrap_or_default()
        )
    }

    async fn wait_ready(&mut self) {
        let deadline = Instant::now() + DEADLINE;
        loop {
            if let Ok(bytes) = fs::read(&self.state) {
                if let Ok(snapshot) = serde_json::from_slice::<ChildEvidence>(&bytes) {
                    if snapshot.ready && snapshot.relay_ready == 1 {
                        return;
                    }
                }
            }
            if let Some(status) = self.process.try_wait().expect("inspect child") {
                panic!("child exited before readiness ({status}); {}", self.logs());
            }
            assert!(
                Instant::now() < deadline,
                "child readiness timeout; {}",
                self.logs()
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    fn evidence(&self) -> ChildEvidence {
        serde_json::from_slice(&fs::read(&self.state).expect("read child evidence"))
            .expect("decode child evidence")
    }

    async fn stop(&mut self) {
        fs::write(&self.stop, b"stop").expect("signal child stop");
        let deadline = Instant::now() + DEADLINE;
        loop {
            if let Some(status) = self.process.try_wait().expect("inspect child shutdown") {
                assert!(status.success(), "child shutdown failed; {}", self.logs());
                let evidence = self.evidence();
                assert!(evidence.terminal && evidence.drain_reconciled);
                assert_eq!(evidence.relay_in_flight, 0);
                return;
            }
            assert!(
                Instant::now() < deadline,
                "child shutdown timeout; {}",
                self.logs()
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    fn kill(&mut self) {
        self.process.kill().expect("kill qualification child");
        let status = self.process.wait().expect("join killed child");
        assert!(!status.success());
    }
}

impl Drop for ChildConsumer {
    fn drop(&mut self) {
        if self.process.try_wait().ok().flatten().is_none() {
            let _ = self.process.kill();
            let _ = self.process.wait();
        }
    }
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

fn fixture_config(mongodb: &str, rabbitmq: &str, database_name: &str) -> LilyConfig {
    let queue = QueueDefinition {
        name: INPUT_QUEUE.into(),
        exchange_name: INPUT_EXCHANGE.into(),
        routing_key: INPUT_QUEUE.into(),
        concurrency: 2,
        prefetch_count: 8,
        delivery_buffer_capacity: Some(8),
        // Duplicate lease contention is deferred directly to RabbitMQ and
        // must not require or consume Lily's ordinary retry/DLQ budget.
        retry_attempts: 0,
        retry_backoff_millis: Some(100),
        max_retry_backoff_millis: Some(200),
        // The shutdown-active scenario must reach the ten-second application
        // root before its local pipeline deadline can notify cancellation.
        delivery_execution_timeout_millis: Some(30_000),
        settlement_timeout_millis: Some(2_000),
        durable: true,
        max_message_size_bytes: 64 * 1024,
        retention: Some(retention()),
        transactional_inbox: Some(TransactionalInboxConfig {
            backend: TransactionalInboxBackend::MongoDb,
            database_cell: qualification_database_cell(),
            inbox_lock_timeout_millis: 2_000,
            outbox_claim_lease_millis: 1_000,
            relay_publish_timeout_millis: 500,
            relay_poll_interval_millis: 25,
            shutdown_drain_timeout_millis: 3_000,
            mongodb: Some(MongoTransactionalInboxConfig::default()),
            ..TransactionalInboxConfig::default()
        }),
        ..QueueDefinition::default()
    };
    LilyConfig {
        lifecycle: LifecycleConfig {
            shutdown_timeout_secs: 10,
        },
        database: Some(qualification_database_config(mongodb, database_name)),
        rabbitmq: RabbitMqConfig {
            consumer: Some(RabbitMqConsumerConfig {
                connection_string: Some(rabbitmq.into()),
                pool_size: 2,
                connection_timeout_secs: Some(10),
                confirm_timeout_secs: Some(5),
                heartbeat_secs: Some(30),
                max_reconnect_attempts: Some(3),
                reconnect_backoff_millis: Some(50),
                use_tls: Some(false),
                tls: RabbitMqTlsConfig::default(),
                persistence_enabled: true,
                ..RabbitMqConsumerConfig::default()
            }),
            topology: RabbitMqTopologyConfig {
                queues: vec![queue],
            },
        },
        ..LilyConfig::default()
    }
}

#[cfg(feature = "transactional-inbox-mongodb")]
fn qualification_database_cell() -> Option<String> {
    None
}

#[cfg(feature = "transactional-inbox-mongodb-factory")]
fn qualification_database_cell() -> Option<String> {
    Some("primary".into())
}

#[cfg(feature = "transactional-inbox-mongodb")]
fn qualification_database_config(mongodb: &str, database_name: &str) -> DatabaseConfig {
    DatabaseConfig {
        mode: Some("single".into()),
        database_type: Some("mongodb".into()),
        connection_string: Some(mongodb.into()),
        database_name: Some(database_name.into()),
        pool_size: Some(8),
        pooling_enabled: Some(true),
        connection_timeout_secs: Some(10),
        query_timeout_secs: Some(30),
        app_name: Some("lily-cap081-consumer-e2e".into()),
        ..DatabaseConfig::default()
    }
}

#[cfg(feature = "transactional-inbox-mongodb-factory")]
fn qualification_database_config(mongodb: &str, database_name: &str) -> DatabaseConfig {
    DatabaseConfig {
        mode: Some("factory".into()),
        cells: Some(vec![DatabaseCellConfig {
            name: "primary".into(),
            database_type: "mongodb".into(),
            connection_string: Some(mongodb.into()),
            pool_size: Some(8),
            connection_timeout_secs: Some(10),
            query_timeout_secs: Some(30),
            pooling_enabled: Some(true),
            database_name: database_name.into(),
            host: None,
            port: None,
            username: None,
            password: None,
            auth_database: None,
            use_tls: None,
            app_name: Some("lily-cap081-consumer-e2e".into()),
        }]),
        ..DatabaseConfig::default()
    }
}

#[cfg(feature = "transactional-inbox-mongodb")]
async fn resolve_migration_database(container: &ApplicationContainer) -> Arc<DatabaseService> {
    container
        .resolve(None)
        .await
        .expect("resolve MongoDB service for deployment migration")
}

#[cfg(feature = "transactional-inbox-mongodb-factory")]
async fn resolve_migration_database(container: &ApplicationContainer) -> Arc<DatabaseService> {
    let factory: Arc<MongoFactory> = container
        .resolve(None)
        .await
        .expect("resolve MongoDB factory for deployment migration");
    factory
        .get("primary")
        .expect("resolve exact MongoDB transactional cell")
}

async fn rabbit(url: &str) -> (Connection, Channel) {
    let connection = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("connect RabbitMQ observer");
    let channel = connection.create_channel().await.expect("observer channel");
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("publisher confirms");
    (connection, channel)
}

async fn cleanup_rabbit(channel: &Channel) {
    let retry_100 = format!("{INPUT_QUEUE}.retry.v2.100ms");
    let retry_200 = format!("{INPUT_QUEUE}.retry.v2.200ms");
    let dead_letter = format!("{INPUT_QUEUE}.dlq.v2");
    for queue in [
        INPUT_QUEUE,
        OUTPUT_QUEUE,
        retry_100.as_str(),
        retry_200.as_str(),
        dead_letter.as_str(),
    ] {
        let _ = channel
            .queue_delete(queue.into(), QueueDeleteOptions::default())
            .await;
    }
    let retry_exchange = format!("{INPUT_EXCHANGE}.retry.v2");
    let dead_letter_exchange = format!("{INPUT_EXCHANGE}.dlx.v2");
    for exchange in [
        INPUT_EXCHANGE,
        OUTPUT_EXCHANGE,
        retry_exchange.as_str(),
        dead_letter_exchange.as_str(),
    ] {
        let _ = channel
            .exchange_delete(exchange.into(), ExchangeDeleteOptions::default())
            .await;
    }
}

async fn declare_output(channel: &Channel) {
    channel
        .exchange_declare(
            OUTPUT_EXCHANGE.into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("declare output exchange");
    channel
        .queue_declare(
            OUTPUT_QUEUE.into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("declare output queue");
    channel
        .queue_bind(
            OUTPUT_QUEUE.into(),
            OUTPUT_EXCHANGE.into(),
            OUTPUT_ROUTING_KEY.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("bind output queue");
}

fn envelope(event_id: Uuid) -> BasicProperties {
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
        .with_message_id(event_id.to_string().into())
        .with_content_type("application/json".into())
        .with_headers(headers)
        .with_delivery_mode(2)
}

async fn publish(channel: &Channel, event_id: Uuid, input: &QualificationInput) {
    let confirm = channel
        .basic_publish(
            INPUT_EXCHANGE.into(),
            INPUT_QUEUE.into(),
            BasicPublishOptions::default(),
            &serde_json::to_vec(input).expect("serialize input"),
            envelope(event_id),
        )
        .await
        .expect("publish input")
        .await
        .expect("confirm input");
    assert!(!confirm.is_nack());
}

async fn output(channel: &Channel) -> BasicGetMessage {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some(message) = channel
            .basic_get(OUTPUT_QUEUE.into(), BasicGetOptions::default())
            .await
            .expect("poll output")
        {
            return message;
        }
        assert!(Instant::now() < deadline, "output deadline elapsed");
        sleep(Duration::from_millis(25)).await;
    }
}

async fn process_marker(path: &Path) -> u32 {
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Ok(value) = fs::read_to_string(path) {
            if let Ok(pid) = value.trim().parse() {
                return pid;
            }
        }
        assert!(Instant::now() < deadline, "process marker deadline elapsed");
        sleep(Duration::from_millis(25)).await;
    }
}

fn invocation_count(path: &Path) -> usize {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count()
}

fn message_id(message: &BasicGetMessage) -> String {
    message
        .properties
        .message_id()
        .as_ref()
        .map(|value| value.as_str().to_owned())
        .expect("stable output message ID")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires LILY_CAP081_DISPOSABLE=1 plus dedicated LILY_CAP081_MONGODB_URL and LILY_CAP081_RABBITMQ_URL"]
async fn mongodb_transactional_consumer_two_process_e2e() {
    if std::env::var_os(CHILD_ROLE).is_some() {
        run_child().await;
        return;
    }
    assert_eq!(
        std::env::var("LILY_CAP081_DISPOSABLE").as_deref(),
        Ok("1"),
        "refusing destructive qualification without disposable-fixture opt-in"
    );
    let mongodb = std::env::var("LILY_CAP081_MONGODB_URL").expect("MongoDB fixture URI");
    let rabbitmq = std::env::var("LILY_CAP081_RABBITMQ_URL").expect("RabbitMQ fixture URI");
    let evidence = TempDir::new().expect("qualification evidence");
    let config = evidence.path().join("lily.toml");
    let database_name = format!("lily_cap081_{}", Uuid::new_v4().simple());
    fs::write(
        &config,
        toml::to_string_pretty(&fixture_config(&mongodb, &rabbitmq, &database_name))
            .expect("serialize config"),
    )
    .expect("write config");

    let migration_container = build_container(&config).await;
    let database = resolve_migration_database(&migration_container).await;
    let operation = database
        .operation_context(CancellationToken::new())
        .expect("migration operation context");
    MongoInboxOutboxMigrator::new(&database)
        .expect("framework migrator")
        .apply(&operation)
        .await
        .expect("explicit framework migration");
    migration_container
        .close()
        .await
        .expect("close migrator DI");

    let (_rabbit_connection, rabbit) = rabbit(&rabbitmq).await;
    cleanup_rabbit(&rabbit).await;
    declare_output(&rabbit).await;
    let mut child_a = ChildConsumer::spawn("a", &config, evidence.path());
    let mut child_b = ChildConsumer::spawn("b", &config, evidence.path());
    child_a.wait_ready().await;
    child_b.wait_ready().await;

    let invocations = evidence.path().join("handler-invocations.log");
    let duplicate_input = Uuid::new_v4();
    let duplicate_output = Uuid::new_v4();
    let input = QualificationInput {
        marker: "concurrent-duplicate".into(),
        outgoing_event_id: duplicate_output.to_string(),
    };
    tokio::join!(
        publish(&rabbit, duplicate_input, &input),
        publish(&rabbit, duplicate_input, &input)
    );
    let first = output(&rabbit).await;
    assert_eq!(message_id(&first), duplicate_output.to_string());
    first
        .ack(BasicAckOptions::default())
        .await
        .expect("ACK output");
    sleep(Duration::from_millis(500)).await;
    assert_eq!(invocation_count(&invocations), 1);
    assert!(rabbit
        .basic_get(OUTPUT_QUEUE.into(), BasicGetOptions::default())
        .await
        .expect("inspect duplicate output")
        .is_none());
    let evidence_a = child_a.evidence();
    let evidence_b = child_b.evidence();
    assert!(evidence_a.inbox_body_attempts + evidence_b.inbox_body_attempts >= 1);
    assert!(
        evidence_a.inbox_lease_contentions + evidence_b.inbox_lease_contentions >= 1,
        "the production operational snapshot must retain external contention evidence"
    );
    assert_eq!(
        evidence_a.inbox_post_commit_release_failures
            + evidence_b.inbox_post_commit_release_failures,
        0
    );
    assert!(rabbit
        .basic_get(INPUT_QUEUE.into(), BasicGetOptions::default())
        .await
        .expect("inspect deferred duplicate input")
        .is_none());
    assert!(rabbit
        .basic_get(
            format!("{INPUT_QUEUE}.dlq.v2").into(),
            BasicGetOptions::default(),
        )
        .await
        .expect("inspect duplicate dead-letter queue")
        .is_none());

    // Kill after MongoDB commit and before input settlement. Redelivery must
    // observe AlreadyCompleted and never invoke the handler again.
    let handler_event = Uuid::parse_str(POST_HANDLER_EVENT_ID).expect("handler event ID");
    let handler_output = Uuid::parse_str(POST_HANDLER_OUTPUT_ID).expect("handler output ID");
    publish(
        &rabbit,
        handler_event,
        &QualificationInput {
            marker: "post-handler".into(),
            outgoing_event_id: handler_output.to_string(),
        },
    )
    .await;
    let owner = process_marker(&evidence.path().join("post-handler.marker")).await;
    let count_before = invocation_count(&invocations);
    if child_a.process.id() == owner {
        child_a.kill();
        child_a = ChildConsumer::spawn("handler-replacement-a", &config, evidence.path());
        child_a.wait_ready().await;
    } else if child_b.process.id() == owner {
        child_b.kill();
        child_b = ChildConsumer::spawn("handler-replacement-b", &config, evidence.path());
        child_b.wait_ready().await;
    } else {
        panic!("post-handler marker names unknown process {owner}");
    }
    let committed = output(&rabbit).await;
    assert_eq!(message_id(&committed), handler_output.to_string());
    committed
        .ack(BasicAckOptions::default())
        .await
        .expect("ACK committed output");
    sleep(Duration::from_millis(500)).await;
    assert_eq!(invocation_count(&invocations), count_before);

    // Kill after publisher confirm but before delivered marking. The durable
    // row is replayed with the same stable event identity, which a downstream
    // transactional inbox can collapse.
    let confirm_event = Uuid::parse_str(POST_CONFIRM_EVENT_ID).expect("confirm event ID");
    let confirm_output = Uuid::parse_str(POST_CONFIRM_OUTPUT_ID).expect("confirm output ID");
    publish(
        &rabbit,
        confirm_event,
        &QualificationInput {
            marker: "post-confirm".into(),
            outgoing_event_id: confirm_output.to_string(),
        },
    )
    .await;
    let confirm_owner = process_marker(&evidence.path().join("post-confirm.marker")).await;
    let confirmed = output(&rabbit).await;
    let stable_id = message_id(&confirmed);
    assert_eq!(stable_id, confirm_output.to_string());
    confirmed
        .ack(BasicAckOptions::default())
        .await
        .expect("ACK first confirmed output");
    if child_a.process.id() == confirm_owner {
        child_a.kill();
        child_a = ChildConsumer::spawn("confirm-replacement-a", &config, evidence.path());
        child_a.wait_ready().await;
    } else if child_b.process.id() == confirm_owner {
        child_b.kill();
        child_b = ChildConsumer::spawn("confirm-replacement-b", &config, evidence.path());
        child_b.wait_ready().await;
    } else {
        panic!("post-confirm marker names unknown process {confirm_owner}");
    }
    let replayed = output(&rabbit).await;
    assert_eq!(message_id(&replayed), stable_id);
    replayed
        .ack(BasicAckOptions::default())
        .await
        .expect("ACK replayed output");

    // Shutdown must let an admitted MongoDB action unwind and close its
    // delivery scope before the transaction owner becomes terminal. The
    // framework-owned cancellation path leaves the original delivery
    // available for redelivery and bypasses ordinary retry/DLQ routing.
    let shutdown_event = Uuid::new_v4();
    publish(
        &rabbit,
        shutdown_event,
        &QualificationInput {
            marker: "shutdown-active".into(),
            outgoing_event_id: Uuid::new_v4().to_string(),
        },
    )
    .await;
    let shutdown_owner = process_marker(&evidence.path().join("shutdown-handler.marker")).await;
    tokio::join!(child_a.stop(), child_b.stop());
    for extension in ["cancelled", "dropped"] {
        let owner: u32 = fs::read_to_string(
            evidence
                .path()
                .join("shutdown-handler.marker")
                .with_extension(extension),
        )
        .expect("shutdown cancellation and execution-drop evidence must exist before child join")
        .trim()
        .parse()
        .unwrap();
        assert_eq!(owner, shutdown_owner);
    }
    let disposed_owner: u32 = fs::read_to_string(evidence.path().join("shutdown-disposed.marker"))
        .expect("scope disposal marker must exist before the child shutdown returns")
        .trim()
        .parse()
        .expect("scope disposal marker identifies its actual owner process");
    assert_eq!(disposed_owner, shutdown_owner);

    let requeued = rabbit
        .basic_get(INPUT_QUEUE.into(), BasicGetOptions::default())
        .await
        .expect("inspect shutdown-cancelled input")
        .expect("shutdown-cancelled input must remain available for redelivery");
    assert_eq!(message_id(&requeued), shutdown_event.to_string());
    assert!(
        requeued.redelivered,
        "shutdown must release the previously delivered original"
    );
    requeued
        .ack(BasicAckOptions::default())
        .await
        .expect("ACK shutdown qualification input");
    assert!(rabbit
        .basic_get(
            format!("{INPUT_QUEUE}.dlq.v2").into(),
            BasicGetOptions::default(),
        )
        .await
        .expect("inspect shutdown dead-letter queue")
        .is_none());
    assert!(rabbit
        .basic_get(OUTPUT_QUEUE.into(), BasicGetOptions::default())
        .await
        .expect("inspect shutdown output queue")
        .is_none());

    cleanup_rabbit(&rabbit).await;
}
