#![cfg(feature = "transactional-inbox-postgresql")]

//! Environment-qualified CAP-08 proof against disposable PostgreSQL and RabbitMQ.
//!
//! The parent process owns migration and transport assertions. Two independent
//! child OS processes run the public `Consumer` composition root against the
//! same physical queue and PostgreSQL inbox/outbox. The test is deliberately
//! ignored: callers must opt into destructive use of a dedicated database with
//! `LILY_CAP08_DISPOSABLE=1` and provide both qualification URLs.

use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Write as _},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{
        BasicAckOptions, BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions,
        ExchangeDeclareOptions, ExchangeDeleteOptions, QueueBindOptions, QueueDeclareOptions,
        QueueDeleteOptions,
    },
    types::{AMQPValue, FieldTable},
};
use lily_config::{
    ConfigOptions, ConfigService, LifecycleConfig, LilyConfig, PgConfig, PgTlsConfig, PgTlsMode,
    QueueDefinition, QueueRetentionConfig, RabbitMqConfig, RabbitMqConsumerConfig,
    RabbitMqTlsConfig, RabbitMqTopologyConfig, TransactionalInboxConfig,
};
use lily_consumer::{Consumer, ManagedConsumer};
use lily_error::injection::InjectionError;
use lily_injection::Injectable;
use lily_injection::{ApplicationContainer, ServiceTrait};
use lily_queue::__private::postgresql_reexports::{diesel, diesel_async};
use lily_queue::__private::{
    install_outbox_post_confirm_probe, install_rabbitmq_post_handler_settlement_probe,
    prepare_postgresql_transactional_runtime,
};
use lily_queue::{
    Json, PostgresInboxOutboxMigrator, PostgresReliabilityError, PostgresTransaction,
    PublishContentKind, QueueHandlerError, TransactionalOutboxMessage, queue, queue_service,
};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::time::sleep;
use tokio_postgres::{Client as PgObserver, NoTls};
use uuid::Uuid;

const INPUT_EXCHANGE: &str = "cap08.qualification.consumer";
const INPUT_QUEUE: &str = "cap08.qualification.consumer.orders";
const OUTPUT_EXCHANGE: &str = "cap08.qualification.events";
const OUTPUT_QUEUE: &str = "cap08.qualification.events.observer";
const OUTPUT_ROUTING_KEY: &str = "orders.applied";

const CHILD_ROLE: &str = "LILY_CAP08_CHILD_ROLE";
const CHILD_CONFIG: &str = "LILY_CAP08_CHILD_CONFIG";
const CHILD_STATE: &str = "LILY_CAP08_CHILD_STATE";
const CHILD_STOP: &str = "LILY_CAP08_CHILD_STOP";
const CHILD_PRECOMMIT_MARKER: &str = "LILY_CAP08_CHILD_PRECOMMIT_MARKER";
const CHILD_POST_HANDLER_MARKER: &str = "LILY_CAP08_CHILD_POST_HANDLER_MARKER";
const CHILD_POST_CONFIRM_MARKER: &str = "LILY_CAP08_CHILD_POST_CONFIRM_MARKER";
const CHILD_HANDLER_INVOCATIONS: &str = "LILY_CAP08_CHILD_HANDLER_INVOCATIONS";

const POST_HANDLER_EVENT_ID: &str = "08000000-0000-4000-8000-000000000001";
const POST_HANDLER_OUTPUT_EVENT_ID: &str = "08000000-0000-4000-8000-000000000002";
const POST_CONFIRM_EVENT_ID: &str = "08000000-0000-4000-8000-000000000003";
const POST_CONFIRM_OUTPUT_EVENT_ID: &str = "08000000-0000-4000-8000-000000000004";

const STARTUP_DEADLINE: Duration = Duration::from_secs(30);
const ASSERTION_DEADLINE: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QualificationInput {
    marker: String,
    outgoing_event_id: String,
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct TransactionalQualificationWorker;

#[async_trait]
impl ServiceTrait for TransactionalQualificationWorker {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Ok(())
    }
}

#[queue_service]
impl TransactionalQualificationWorker {
    #[queue(
        "cap08.qualification.consumer.orders",
        version = 1,
        content = "json",
        delivery_guarantee = "transactional_inbox"
    )]
    async fn apply_order(
        &self,
        transaction: PostgresTransaction,
        Json(input): Json<QualificationInput>,
    ) -> Result<(), QueueHandlerError> {
        let outgoing_event_id = Uuid::parse_str(&input.outgoing_event_id)
            .map_err(|_| QueueHandlerError::permanent("CAP08_OUTPUT_EVENT_ID_INVALID"))?;
        let marker = input.marker;
        if marker == "post-handler-pre-settlement" {
            let path = PathBuf::from(
                std::env::var_os(CHILD_HANDLER_INVOCATIONS)
                    .expect("post-handler invocation evidence path must be configured"),
            );
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .expect("open post-handler invocation evidence");
            let evidence = format!("{}\n", std::process::id());
            file.write_all(evidence.as_bytes())
                .expect("append post-handler invocation evidence");
            file.sync_data()
                .expect("flush post-handler invocation evidence");
        }
        let business_marker = marker.clone();
        let precommit_marker = std::env::var_os(CHILD_PRECOMMIT_MARKER).map(PathBuf::from);
        transaction
            .with_connection(move |connection| {
                Box::pin(async move {
                    use diesel_async::RunQueryDsl as _;

                    diesel::sql_query(
                        r#"INSERT INTO lily_queue.cap08_qualification_effects
                           (event_id, marker) VALUES ($1, $2)"#,
                    )
                    .bind::<diesel::sql_types::Uuid, _>(outgoing_event_id)
                    .bind::<diesel::sql_types::Text, _>(business_marker)
                    .execute(connection)
                    .await?;
                    if marker == "precommit-kill" {
                        let marker_path = precommit_marker
                            .expect("pre-commit crash qualification marker must be configured");
                        if let Ok(mut file) = OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(marker_path)
                        {
                            writeln!(file, "{}", std::process::id())
                                .expect("write pre-commit child PID");
                            file.sync_all().expect("flush pre-commit child PID");
                            sleep(Duration::from_secs(60)).await;
                        }
                    }
                    // Keep the transaction open briefly so duplicate deliveries
                    // genuinely overlap across the two child processes.
                    diesel::sql_query("SELECT pg_sleep(0.20)")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .await?;
        transaction
            .enqueue(TransactionalOutboxMessage::try_new(
                outgoing_event_id,
                OUTPUT_EXCHANGE,
                OUTPUT_ROUTING_KEY,
                1,
                PublishContentKind::Json,
                br#"{"kind":"order-applied"}"#.to_vec(),
            )?)
            .await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChildEvidence {
    role: String,
    ready: bool,
    terminal: bool,
    drain_reconciled: bool,
    relay_registered: u64,
    relay_ready: u64,
    relay_delivered: u64,
    relay_in_flight: u64,
    relay_publish_failures: u64,
    relay_uncertain: u64,
    relay_exhausted: u64,
    last_failure_code: Option<String>,
}

fn publish_evidence(path: &Path, evidence: &ChildEvidence) {
    let temporary = path.with_extension("tmp");
    fs::write(
        &temporary,
        serde_json::to_vec(evidence).expect("serialize child evidence"),
    )
    .expect("write child evidence");
    fs::rename(temporary, path).expect("publish child evidence atomically");
}

fn publish_process_owner(path: &Path) -> bool {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            writeln!(file, "{}", std::process::id()).expect("write probe owner PID");
            file.sync_all().expect("flush probe owner PID");
            true
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => false,
        Err(error) => panic!("create probe owner marker {}: {error}", path.display()),
    }
}

fn child_evidence(role: &str, managed: &ManagedConsumer) -> ChildEvidence {
    let snapshot = managed.snapshot();
    ChildEvidence {
        role: role.to_owned(),
        ready: snapshot.ready,
        terminal: snapshot.shutdown.completed,
        drain_reconciled: snapshot.shutdown.drain_reconciled,
        relay_registered: snapshot.transactional_outbox.registered_relays,
        relay_ready: snapshot.transactional_outbox.ready_relays,
        relay_delivered: snapshot.transactional_outbox.delivered,
        relay_in_flight: snapshot.transactional_outbox.in_flight,
        relay_publish_failures: snapshot.transactional_outbox.publish_failures,
        relay_uncertain: snapshot.transactional_outbox.uncertain_after_publish,
        relay_exhausted: snapshot.transactional_outbox.exhausted,
        last_failure_code: snapshot.last_failure_code.map(str::to_owned),
    }
}

async fn build_container(config_path: &Path) -> Arc<ApplicationContainer> {
    Arc::new(
        ApplicationContainer::builder()
            .seed_singleton(ConfigService::new(ConfigOptions::test(config_path)))
            .build()
            .await
            .expect("build CAP-08 qualification container"),
    )
}

async fn run_child() {
    let role = std::env::var(CHILD_ROLE).expect("child role");
    let config_path = PathBuf::from(std::env::var_os(CHILD_CONFIG).expect("child config path"));
    let state_path = PathBuf::from(std::env::var_os(CHILD_STATE).expect("child state path"));
    let stop_path = PathBuf::from(std::env::var_os(CHILD_STOP).expect("child stop path"));
    let post_handler_marker = PathBuf::from(
        std::env::var_os(CHILD_POST_HANDLER_MARKER).expect("post-handler marker path"),
    );
    let post_confirm_marker = PathBuf::from(
        std::env::var_os(CHILD_POST_CONFIRM_MARKER).expect("post-confirm marker path"),
    );
    let post_handler_probe = install_rabbitmq_post_handler_settlement_probe(
        Uuid::parse_str(POST_HANDLER_EVENT_ID).expect("post-handler event ID"),
    );
    let post_confirm_probe = install_outbox_post_confirm_probe(
        Uuid::parse_str(POST_CONFIRM_OUTPUT_EVENT_ID).expect("post-confirm output event ID"),
    );
    let post_handler_monitor = tokio::spawn(async move {
        post_handler_probe.wait_until_paused().await;
        if !publish_process_owner(&post_handler_marker) {
            post_handler_probe.release();
        }
    });
    let post_confirm_monitor = tokio::spawn(async move {
        post_confirm_probe.wait_until_paused().await;
        if !publish_process_owner(&post_confirm_marker) {
            post_confirm_probe.release();
        }
    });
    let container = build_container(&config_path).await;
    let managed = Consumer::builder()
        .container(Arc::clone(&container))
        .tracing_disabled()
        .start_managed()
        .await
        .expect("start child Consumer");

    loop {
        let evidence = child_evidence(&role, &managed);
        publish_evidence(&state_path, &evidence);
        if evidence.ready && evidence.relay_registered == 1 && evidence.relay_ready == 1 {
            break;
        }
        assert!(
            !evidence.terminal,
            "child terminated before readiness: {evidence:?}"
        );
        sleep(Duration::from_millis(25)).await;
    }

    loop {
        if stop_path.exists() {
            break;
        }
        let evidence = child_evidence(&role, &managed);
        assert!(
            !evidence.terminal,
            "child terminated unexpectedly: {evidence:?}"
        );
        publish_evidence(&state_path, &evidence);
        sleep(Duration::from_millis(25)).await;
    }

    tokio::time::timeout(STARTUP_DEADLINE, managed.shutdown())
        .await
        .expect("child Consumer shutdown deadline")
        .expect("child Consumer shutdown");
    let terminal = child_evidence(&role, &managed);
    assert!(terminal.terminal);
    assert!(terminal.drain_reconciled);
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
    publish_evidence(&state_path, &terminal);
    tokio::time::timeout(STARTUP_DEADLINE, container.close())
        .await
        .expect("child DI shutdown deadline")
        .expect("child DI shutdown");
    // These probes are fixture-owned tasks; finish their actual joins rather
    // than relying on process/runtime drop to make the harness look clean.
    for monitor in [post_handler_monitor, post_confirm_monitor] {
        monitor.abort();
        if let Err(error) = monitor.await {
            assert!(error.is_cancelled(), "fixture monitor panicked: {error}");
        }
    }
}

struct ChildConsumer {
    role: &'static str,
    process: Child,
    state: PathBuf,
    stop: PathBuf,
    stdout: PathBuf,
    stderr: PathBuf,
}

impl ChildConsumer {
    fn spawn(role: &'static str, config: &Path, evidence: &Path) -> Self {
        let state = evidence.join(format!("consumer-{role}.json"));
        let stop = evidence.join(format!("consumer-{role}.stop"));
        let stdout = evidence.join(format!("consumer-{role}.stdout.log"));
        let stderr = evidence.join(format!("consumer-{role}.stderr.log"));
        let precommit_marker = evidence.join("precommit-kill.marker");
        let post_handler_marker = evidence.join("post-handler-pre-settlement.marker");
        let post_confirm_marker = evidence.join("post-confirm-pre-mark.marker");
        let handler_invocations = evidence.join("post-handler-invocations.log");
        let process = Command::new(std::env::current_exe().expect("integration-test executable"))
            .args([
                "--exact",
                "postgresql_transactional_consumer_two_process_e2e",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_ROLE, role)
            .env(CHILD_CONFIG, config)
            .env(CHILD_STATE, &state)
            .env(CHILD_STOP, &stop)
            .env(CHILD_PRECOMMIT_MARKER, precommit_marker)
            .env(CHILD_POST_HANDLER_MARKER, post_handler_marker)
            .env(CHILD_POST_CONFIRM_MARKER, post_confirm_marker)
            .env(CHILD_HANDLER_INVOCATIONS, handler_invocations)
            .stdout(Stdio::from(File::create(&stdout).expect("child stdout")))
            .stderr(Stdio::from(File::create(&stderr).expect("child stderr")))
            .spawn()
            .expect("spawn child Consumer process");
        Self {
            role,
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
            fs::read_to_string(&self.stdout).unwrap_or_else(|_| "<unavailable>".into()),
            fs::read_to_string(&self.stderr).unwrap_or_else(|_| "<unavailable>".into())
        )
    }

    async fn wait_ready(&mut self) -> ChildEvidence {
        let deadline = Instant::now() + STARTUP_DEADLINE;
        loop {
            if let Ok(bytes) = fs::read(&self.state) {
                if let Ok(evidence) = serde_json::from_slice::<ChildEvidence>(&bytes) {
                    if evidence.ready && evidence.relay_registered == 1 && evidence.relay_ready == 1
                    {
                        return evidence;
                    }
                }
            }
            if let Some(status) = self.process.try_wait().expect("poll child Consumer") {
                panic!(
                    "child {} exited before readiness with {status}; {}",
                    self.role,
                    self.logs()
                );
            }
            assert!(
                Instant::now() < deadline,
                "child {} readiness timed out; {}",
                self.role,
                self.logs()
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn stop(&mut self) -> ChildEvidence {
        fs::write(&self.stop, b"stop").expect("signal child Consumer stop");
        let deadline = Instant::now() + STARTUP_DEADLINE;
        loop {
            if let Some(status) = self.process.try_wait().expect("poll child shutdown") {
                assert!(
                    status.success(),
                    "child {} shutdown failed with {status}; {}",
                    self.role,
                    self.logs()
                );
                let evidence: ChildEvidence = serde_json::from_slice(
                    &fs::read(&self.state).expect("read terminal child evidence"),
                )
                .expect("parse terminal child evidence");
                assert!(evidence.terminal);
                assert!(evidence.drain_reconciled);
                assert_eq!(evidence.relay_in_flight, 0);
                return evidence;
            }
            assert!(
                Instant::now() < deadline,
                "child {} shutdown timed out; {}",
                self.role,
                self.logs()
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    fn process_id(&self) -> u32 {
        self.process.id()
    }

    fn kill_abruptly(&mut self, boundary: &str) {
        self.process.kill().expect("kill child Consumer");
        let status = self.process.wait().expect("join killed child Consumer");
        assert!(
            !status.success(),
            "{boundary} crash fixture must not exit successfully"
        );
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

fn fixture_config(postgresql_url: &str, rabbitmq_url: &str) -> LilyConfig {
    let transactional = TransactionalInboxConfig {
        relay_poll_interval_millis: 25,
        relay_publish_timeout_millis: 2_000,
        outbox_claim_lease_millis: 5_000,
        shutdown_drain_timeout_millis: 5_000,
        ..TransactionalInboxConfig::default()
    };
    let queue = QueueDefinition {
        name: INPUT_QUEUE.to_owned(),
        exchange_name: INPUT_EXCHANGE.to_owned(),
        routing_key: INPUT_QUEUE.to_owned(),
        concurrency: 2,
        prefetch_count: 8,
        delivery_buffer_capacity: Some(8),
        retry_attempts: 3,
        retry_backoff_millis: Some(100),
        max_retry_backoff_millis: Some(200),
        delivery_execution_timeout_millis: Some(5_000),
        settlement_timeout_millis: Some(2_000),
        durable: true,
        max_message_size_bytes: 64 * 1024,
        retention: Some(retention()),
        transactional_inbox: Some(transactional),
        ..QueueDefinition::default()
    };
    LilyConfig {
        lifecycle: LifecycleConfig {
            shutdown_timeout_secs: 10,
        },
        postgresql: Some(PgConfig {
            connection_string: Some(postgresql_url.to_owned()),
            tls: PgTlsConfig {
                mode: PgTlsMode::Disable,
                additional_ca_bundle: None,
            },
            ..PgConfig::default()
        }),
        rabbitmq: RabbitMqConfig {
            consumer: Some(RabbitMqConsumerConfig {
                connection_string: Some(rabbitmq_url.to_owned()),
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

async fn pg_observer(url: &str) -> PgObserver {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("connect disposable PostgreSQL observer");
    tokio::spawn(async move {
        connection
            .await
            .expect("PostgreSQL observer connection failed");
    });
    client
}

async fn rabbit_observer(url: &str) -> (Connection, Channel) {
    let connection = Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("connect disposable RabbitMQ observer");
    let channel = connection
        .create_channel()
        .await
        .expect("create RabbitMQ observer channel");
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("enable publisher confirms");
    (connection, channel)
}

async fn declare_output_topology(channel: &Channel) {
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
        .expect("declare qualification output exchange");
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
        .expect("declare qualification output queue");
    channel
        .queue_bind(
            OUTPUT_QUEUE.into(),
            OUTPUT_EXCHANGE.into(),
            OUTPUT_ROUTING_KEY.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("bind qualification output queue");
}

async fn delete_output_topology(channel: &Channel) {
    let _ = channel
        .queue_delete(OUTPUT_QUEUE.into(), QueueDeleteOptions::default())
        .await;
    let _ = channel
        .exchange_delete(OUTPUT_EXCHANGE.into(), ExchangeDeleteOptions::default())
        .await;
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
        .with_content_type("application/json".into())
        .with_message_id(event_id.to_string().into())
        .with_delivery_mode(2)
        .with_headers(headers)
}

async fn publish_input(channel: &Channel, event_id: Uuid, input: &QualificationInput) {
    let body = serde_json::to_vec(input).expect("serialize qualification input");
    let confirmation = channel
        .basic_publish(
            INPUT_EXCHANGE.into(),
            INPUT_QUEUE.into(),
            BasicPublishOptions {
                mandatory: true,
                ..BasicPublishOptions::default()
            },
            &body,
            envelope(event_id),
        )
        .await
        .expect("publish transactional input")
        .await
        .expect("await transactional input confirmation");
    assert!(confirmation.is_ack());
}

async fn wait_for_output(channel: &Channel) -> lapin::message::BasicGetMessage {
    tokio::time::timeout(ASSERTION_DEADLINE, async {
        loop {
            if let Some(message) = channel
                .basic_get(OUTPUT_QUEUE.into(), BasicGetOptions::default())
                .await
                .expect("inspect output queue")
            {
                return message;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("transactional outbox output deadline")
}

async fn wait_for_pg_count(client: &PgObserver, query: &str, expected: i64, description: &str) {
    tokio::time::timeout(ASSERTION_DEADLINE, async {
        loop {
            let count: i64 = client
                .query_one(query, &[])
                .await
                .expect("query CAP-08 evidence")
                .get(0);
            if count == expected {
                return;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {description}"));
}

async fn wait_for_process_marker(path: &Path, boundary: &str) -> u32 {
    tokio::time::timeout(ASSERTION_DEADLINE, async {
        loop {
            if let Ok(value) = fs::read_to_string(path) {
                return value
                    .trim()
                    .parse::<u32>()
                    .unwrap_or_else(|_| panic!("{boundary} marker must contain a process ID"));
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{boundary} process marker deadline"))
}

fn handler_invocation_count(path: &Path) -> usize {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count()
}

fn long_string(headers: &FieldTable, name: &str) -> String {
    match headers.inner().get(name) {
        Some(AMQPValue::LongString(value)) => String::from_utf8(value.as_bytes().to_vec())
            .unwrap_or_else(|_| panic!("header {name} must be UTF-8")),
        other => panic!("missing canonical {name} header: {other:?}"),
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires LILY_CAP08_DISPOSABLE=1 plus dedicated LILY_CAP08_POSTGRES_URL and LILY_CAP08_RABBITMQ_URL"]
async fn postgresql_transactional_consumer_two_process_e2e() {
    if std::env::var_os(CHILD_ROLE).is_some() {
        run_child().await;
        return;
    }

    assert_eq!(
        std::env::var("LILY_CAP08_DISPOSABLE").as_deref(),
        Ok("1"),
        "refusing destructive qualification without an explicit disposable-fixture opt-in"
    );
    let postgresql_url = std::env::var("LILY_CAP08_POSTGRES_URL")
        .expect("LILY_CAP08_POSTGRES_URL must identify a dedicated database");
    let rabbitmq_url = std::env::var("LILY_CAP08_RABBITMQ_URL")
        .expect("LILY_CAP08_RABBITMQ_URL must identify a dedicated broker");
    let evidence = TempDir::new().expect("create CAP-08 qualification evidence directory");
    let config_path = evidence.path().join("lily.toml");
    let fixture = fixture_config(&postgresql_url, &rabbitmq_url);
    fs::write(
        &config_path,
        toml::to_string_pretty(&fixture).expect("serialize CAP-08 config"),
    )
    .expect("write CAP-08 config");

    let postgres = pg_observer(&postgresql_url).await;
    postgres
        .batch_execute("DROP SCHEMA IF EXISTS lily_queue CASCADE")
        .await
        .expect("reset dedicated CAP-08 schema");

    // Runtime startup is read-only: missing schema must fail before any DDL.
    let missing_container = build_container(&config_path).await;
    let missing_result = Consumer::builder()
        .container(Arc::clone(&missing_container))
        .tracing_disabled()
        .start_managed()
        .await;
    assert!(
        missing_result.is_err(),
        "transactional Consumer must reject a missing schema"
    );
    let schema_exists: bool = postgres
        .query_one("SELECT to_regnamespace('lily_queue') IS NOT NULL", &[])
        .await
        .expect("inspect missing schema")
        .get(0);
    assert!(
        !schema_exists,
        "runtime startup must never execute migration DDL"
    );
    missing_container
        .close()
        .await
        .expect("close failed-start container");

    // Explicit concurrent migration is serialized by the advisory lock. One
    // caller applies V1 and the other observes it; every later pass is a no-op.
    let migration_container = build_container(&config_path).await;
    let database = migration_container
        .resolve(None)
        .await
        .expect("resolve feature-selected PostgreSQL service");
    let migration_a = PostgresInboxOutboxMigrator::new(Arc::clone(&database));
    let migration_b = PostgresInboxOutboxMigrator::new(Arc::clone(&database));
    let (report_a, report_b) = tokio::join!(migration_a.migrate(), migration_b.migrate());
    let report_a = report_a.expect("first concurrent migration");
    let report_b = report_b.expect("second concurrent migration");
    assert_eq!(
        usize::from(report_a.applied) + usize::from(report_b.applied),
        1
    );
    assert_eq!(report_a.version, 1);
    assert_eq!(report_b.version, 1);
    let idempotent = PostgresInboxOutboxMigrator::new(Arc::clone(&database))
        .migrate()
        .await
        .expect("idempotent migration");
    assert!(!idempotent.applied);

    postgres
        .execute(
            "UPDATE lily_queue.schema_migrations SET version = 2 WHERE component = 'postgresql_inbox_outbox'",
            &[],
        )
        .await
        .expect("install future-schema evidence");
    assert!(matches!(
        PostgresInboxOutboxMigrator::new(Arc::clone(&database))
            .migrate()
            .await,
        Err(PostgresReliabilityError::SchemaTooNew {
            installed: 2,
            supported: 1
        })
    ));
    postgres
        .execute(
            "UPDATE lily_queue.schema_migrations SET version = 1 WHERE component = 'postgresql_inbox_outbox'",
            &[],
        )
        .await
        .expect("restore supported schema");
    postgres
        .batch_execute(
            r#"CREATE TABLE lily_queue.cap08_qualification_effects (
                   event_id UUID PRIMARY KEY,
                   marker TEXT NOT NULL,
                   committed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
               )"#,
        )
        .await
        .expect("create application-owned business evidence table");
    migration_container
        .close()
        .await
        .expect("close migration container");

    let (rabbit_connection, rabbit) = rabbit_observer(&rabbitmq_url).await;
    cleanup_rabbit(&rabbit).await;
    declare_output_topology(&rabbit).await;

    let mut child_a = ChildConsumer::spawn("a", &config_path, evidence.path());
    let mut child_b = ChildConsumer::spawn("b", &config_path, evidence.path());
    child_a.wait_ready().await;
    child_b.wait_ready().await;

    let incoming_event_id = Uuid::new_v4();
    let outgoing_event_id = Uuid::new_v4();
    let input = QualificationInput {
        marker: "concurrent-duplicate".to_owned(),
        outgoing_event_id: outgoing_event_id.to_string(),
    };
    tokio::join!(
        publish_input(&rabbit, incoming_event_id, &input),
        publish_input(&rabbit, incoming_event_id, &input),
    );

    let output = wait_for_output(&rabbit).await;
    assert_eq!(output.data, br#"{"kind":"order-applied"}"#);
    assert_eq!(
        output
            .properties
            .message_id()
            .as_ref()
            .map(|value| value.as_str()),
        Some(outgoing_event_id.to_string().as_str())
    );
    let headers = output
        .properties
        .headers()
        .as_ref()
        .expect("outbox headers");
    assert_eq!(
        long_string(headers, "x-lily-event-id"),
        outgoing_event_id.to_string()
    );
    assert_eq!(long_string(headers, "x-lily-schema-version"), "1");
    assert_eq!(long_string(headers, "x-lily-content-kind"), "json");
    assert!(
        output
            .ack(BasicAckOptions::default())
            .await
            .expect("ACK output")
    );

    wait_for_pg_count(
        &postgres,
        "SELECT COUNT(*)::BIGINT FROM lily_queue.cap08_qualification_effects",
        1,
        "one committed business effect",
    )
    .await;
    wait_for_pg_count(
        &postgres,
        "SELECT COUNT(*)::BIGINT FROM lily_queue.inbox WHERE state = 1",
        1,
        "one completed inbox identity",
    )
    .await;
    wait_for_pg_count(
        &postgres,
        "SELECT COUNT(*)::BIGINT FROM lily_queue.outbox WHERE delivered_at IS NOT NULL",
        1,
        "one delivered outbox record",
    )
    .await;

    // A serial duplicate after completion is ACKed without invoking the
    // handler or creating another durable output.
    publish_input(&rabbit, incoming_event_id, &input).await;
    sleep(Duration::from_millis(500)).await;
    assert!(
        rabbit
            .basic_get(OUTPUT_QUEUE.into(), BasicGetOptions::default())
            .await
            .expect("inspect duplicate output")
            .is_none()
    );
    let business_count: i64 = postgres
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM lily_queue.cap08_qualification_effects",
            &[],
        )
        .await
        .expect("count business effects after serial duplicate")
        .get(0);
    let outbox_count: i64 = postgres
        .query_one("SELECT COUNT(*)::BIGINT FROM lily_queue.outbox", &[])
        .await
        .expect("count outbox rows after serial duplicate")
        .get(0);
    assert_eq!(business_count, 1);
    assert_eq!(outbox_count, 1);

    // Kill the exact process after its business INSERT but before transaction
    // commit. PostgreSQL must roll the work back, RabbitMQ must redeliver, and
    // the surviving process must complete the same event exactly once.
    let precommit_marker = evidence.path().join("precommit-kill.marker");
    let crash_incoming = Uuid::new_v4();
    let crash_outgoing = Uuid::new_v4();
    publish_input(
        &rabbit,
        crash_incoming,
        &QualificationInput {
            marker: "precommit-kill".to_owned(),
            outgoing_event_id: crash_outgoing.to_string(),
        },
    )
    .await;
    let crashed_process = wait_for_process_marker(&precommit_marker, "pre-commit").await;
    if child_a.process_id() == crashed_process {
        child_a.kill_abruptly("pre-commit");
        child_a = ChildConsumer::spawn("replacement-a", &config_path, evidence.path());
        child_a.wait_ready().await;
    } else if child_b.process_id() == crashed_process {
        child_b.kill_abruptly("pre-commit");
        child_b = ChildConsumer::spawn("replacement-b", &config_path, evidence.path());
        child_b.wait_ready().await;
    } else {
        panic!("pre-commit marker named unknown child process {crashed_process}");
    }
    let recovered_output = wait_for_output(&rabbit).await;
    assert_eq!(
        recovered_output
            .properties
            .message_id()
            .as_ref()
            .map(|value| value.as_str()),
        Some(crash_outgoing.to_string().as_str())
    );
    assert!(
        recovered_output
            .ack(BasicAckOptions::default())
            .await
            .expect("ACK pre-commit crash recovery output")
    );
    wait_for_pg_count(
        &postgres,
        "SELECT COUNT(*)::BIGINT FROM lily_queue.cap08_qualification_effects",
        2,
        "one recovered business commit after pre-commit process death",
    )
    .await;
    wait_for_pg_count(
        &postgres,
        "SELECT COUNT(*)::BIGINT FROM lily_queue.outbox WHERE delivered_at IS NOT NULL",
        2,
        "one recovered outbox after pre-commit process death",
    )
    .await;

    // Pause at the exact opposite ambiguity boundary: PostgreSQL has committed
    // the business mutation, inbox completion and outbox insert, but RabbitMQ
    // settlement has not started. Killing that process must redeliver the input;
    // the replacement observes AlreadyCompleted and never invokes the handler.
    let post_handler_marker = evidence.path().join("post-handler-pre-settlement.marker");
    let handler_invocations = evidence.path().join("post-handler-invocations.log");
    let post_handler_incoming =
        Uuid::parse_str(POST_HANDLER_EVENT_ID).expect("post-handler input event ID");
    let post_handler_outgoing =
        Uuid::parse_str(POST_HANDLER_OUTPUT_EVENT_ID).expect("post-handler output event ID");
    publish_input(
        &rabbit,
        post_handler_incoming,
        &QualificationInput {
            marker: "post-handler-pre-settlement".to_owned(),
            outgoing_event_id: post_handler_outgoing.to_string(),
        },
    )
    .await;
    let post_handler_owner =
        wait_for_process_marker(&post_handler_marker, "post-handler/pre-settlement").await;
    wait_for_pg_count(
        &postgres,
        "SELECT COUNT(*)::BIGINT FROM lily_queue.cap08_qualification_effects",
        3,
        "committed business effect before input ACK",
    )
    .await;
    wait_for_pg_count(
        &postgres,
        "SELECT COUNT(*)::BIGINT FROM lily_queue.inbox WHERE state = 1",
        3,
        "completed inbox identity before input ACK",
    )
    .await;
    wait_for_pg_count(
        &postgres,
        "SELECT COUNT(*)::BIGINT FROM lily_queue.outbox",
        3,
        "single committed outbox before input ACK",
    )
    .await;
    assert_eq!(handler_invocation_count(&handler_invocations), 1);
    if child_a.process_id() == post_handler_owner {
        child_a.kill_abruptly("post-handler/pre-settlement");
        child_a = ChildConsumer::spawn("post-handler-replacement-a", &config_path, evidence.path());
        child_a.wait_ready().await;
    } else if child_b.process_id() == post_handler_owner {
        child_b.kill_abruptly("post-handler/pre-settlement");
        child_b = ChildConsumer::spawn("post-handler-replacement-b", &config_path, evidence.path());
        child_b.wait_ready().await;
    } else {
        panic!("post-handler marker named unknown child process {post_handler_owner}");
    }
    let post_handler_output = wait_for_output(&rabbit).await;
    assert_eq!(
        post_handler_output
            .properties
            .message_id()
            .as_ref()
            .map(|value| value.as_str()),
        Some(post_handler_outgoing.to_string().as_str())
    );
    assert!(
        post_handler_output
            .ack(BasicAckOptions::default())
            .await
            .expect("ACK post-handler crash output")
    );
    sleep(Duration::from_millis(500)).await;
    assert_eq!(
        handler_invocation_count(&handler_invocations),
        1,
        "AlreadyCompleted redelivery must bypass the application handler"
    );
    let post_handler_business_count: i64 = postgres
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM lily_queue.cap08_qualification_effects",
            &[],
        )
        .await
        .expect("count post-handler business effects")
        .get(0);
    let post_handler_outbox_count: i64 = postgres
        .query_one("SELECT COUNT(*)::BIGINT FROM lily_queue.outbox", &[])
        .await
        .expect("count post-handler outbox rows")
        .get(0);
    assert_eq!(post_handler_business_count, 3);
    assert_eq!(post_handler_outbox_count, 3);

    // Closing both channels turns any unresolved delivery back into a ready
    // message. A zero broker count after reconciled shutdown therefore proves
    // the AlreadyCompleted redelivery reached one ACK.
    let (post_handler_terminal_a, post_handler_terminal_b) =
        tokio::join!(child_a.stop(), child_b.stop());
    assert_eq!(
        post_handler_terminal_a.relay_in_flight + post_handler_terminal_b.relay_in_flight,
        0
    );
    let input_state = rabbit
        .queue_declare(
            INPUT_QUEUE.into(),
            QueueDeclareOptions {
                passive: true,
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("inspect settled input queue");
    assert_eq!(
        input_state.message_count(),
        0,
        "post-commit redelivery must not remain ready after reconciled shutdown"
    );

    child_a = ChildConsumer::spawn("post-confirm-a", &config_path, evidence.path());
    child_b = ChildConsumer::spawn("post-confirm-b", &config_path, evidence.path());
    child_a.wait_ready().await;
    child_b.wait_ready().await;

    // Pause after RabbitMQ publisher confirm and before PostgreSQL delivered
    // marking. The first output is externally visible, but the durable row is
    // intentionally still undelivered. Killing its exact owner and restarting
    // relays must publish a duplicate carrying the same stable event ID.
    let post_confirm_marker = evidence.path().join("post-confirm-pre-mark.marker");
    let post_confirm_incoming =
        Uuid::parse_str(POST_CONFIRM_EVENT_ID).expect("post-confirm input event ID");
    let post_confirm_outgoing =
        Uuid::parse_str(POST_CONFIRM_OUTPUT_EVENT_ID).expect("post-confirm output event ID");
    publish_input(
        &rabbit,
        post_confirm_incoming,
        &QualificationInput {
            marker: "post-confirm-pre-mark".to_owned(),
            outgoing_event_id: post_confirm_outgoing.to_string(),
        },
    )
    .await;
    let post_confirm_owner =
        wait_for_process_marker(&post_confirm_marker, "post-confirm/pre-mark").await;
    let first_confirmed_output = wait_for_output(&rabbit).await;
    let first_confirmed_id = first_confirmed_output
        .properties
        .message_id()
        .as_ref()
        .map(|value| value.as_str().to_owned())
        .expect("first post-confirm output message ID");
    assert_eq!(first_confirmed_id, post_confirm_outgoing.to_string());
    wait_for_pg_count(
        &postgres,
        &format!(
            "SELECT COUNT(*)::BIGINT FROM lily_queue.outbox WHERE event_id = '{post_confirm_outgoing}'::uuid AND delivered_at IS NULL AND claim_token IS NOT NULL"
        ),
        1,
        "confirmed but unmarked durable outbox row",
    )
    .await;
    assert!(
        first_confirmed_output
            .ack(BasicAckOptions::default())
            .await
            .expect("ACK first post-confirm output")
    );
    if child_a.process_id() == post_confirm_owner {
        child_a.kill_abruptly("post-confirm/pre-mark");
        child_b.stop().await;
    } else if child_b.process_id() == post_confirm_owner {
        child_b.kill_abruptly("post-confirm/pre-mark");
        child_a.stop().await;
    } else {
        panic!("post-confirm marker named unknown child process {post_confirm_owner}");
    }

    child_a = ChildConsumer::spawn("relay-restart-a", &config_path, evidence.path());
    child_b = ChildConsumer::spawn("relay-restart-b", &config_path, evidence.path());
    child_a.wait_ready().await;
    child_b.wait_ready().await;
    let duplicate_confirmed_output = wait_for_output(&rabbit).await;
    let duplicate_confirmed_id = duplicate_confirmed_output
        .properties
        .message_id()
        .as_ref()
        .map(|value| value.as_str().to_owned())
        .expect("duplicate post-confirm output message ID");
    assert_eq!(duplicate_confirmed_id, first_confirmed_id);
    let mut downstream_dedupe = HashSet::new();
    assert!(downstream_dedupe.insert(first_confirmed_id));
    assert!(
        !downstream_dedupe.insert(duplicate_confirmed_id),
        "downstream dedupe must collapse the stable duplicate identity"
    );
    assert!(
        duplicate_confirmed_output
            .ack(BasicAckOptions::default())
            .await
            .expect("ACK duplicate post-confirm output")
    );
    wait_for_pg_count(
        &postgres,
        &format!(
            "SELECT COUNT(*)::BIGINT FROM lily_queue.outbox WHERE event_id = '{post_confirm_outgoing}'::uuid AND delivered_at IS NOT NULL"
        ),
        1,
        "post-confirm row marked delivered after relay restart",
    )
    .await;
    let post_confirm_business_count: i64 = postgres
        .query_one(
            "SELECT COUNT(*)::BIGINT FROM lily_queue.cap08_qualification_effects",
            &[],
        )
        .await
        .expect("count post-confirm business effects")
        .get(0);
    let post_confirm_outbox_count: i64 = postgres
        .query_one("SELECT COUNT(*)::BIGINT FROM lily_queue.outbox", &[])
        .await
        .expect("count post-confirm outbox rows")
        .get(0);
    assert_eq!(post_confirm_business_count, 4);
    assert_eq!(post_confirm_outbox_count, 4);

    // Commit while the destination is absent. The business mutation, inbox
    // completion and outbox row must remain durable even though mandatory
    // broker publication fails. Graceful shutdown does not discard that row.
    delete_output_topology(&rabbit).await;
    let restart_incoming = Uuid::new_v4();
    let restart_outgoing = Uuid::new_v4();
    publish_input(
        &rabbit,
        restart_incoming,
        &QualificationInput {
            marker: "relay-restart".to_owned(),
            outgoing_event_id: restart_outgoing.to_string(),
        },
    )
    .await;
    wait_for_pg_count(
        &postgres,
        "SELECT COUNT(*)::BIGINT FROM lily_queue.cap08_qualification_effects",
        5,
        "business commit while the output route is absent",
    )
    .await;
    wait_for_pg_count(
        &postgres,
        "SELECT COUNT(*)::BIGINT FROM lily_queue.outbox WHERE delivered_at IS NULL AND publish_attempts >= 1 AND last_failure_code IS NOT NULL",
        1,
        "durable failed relay attempt",
    )
    .await;

    let (terminal_a, terminal_b) = tokio::join!(child_a.stop(), child_b.stop());
    assert_eq!(terminal_a.relay_in_flight + terminal_b.relay_in_flight, 0);
    assert_eq!(terminal_a.relay_uncertain + terminal_b.relay_uncertain, 0);
    assert_eq!(terminal_a.relay_exhausted + terminal_b.relay_exhausted, 0);
    assert!(
        terminal_a.relay_publish_failures + terminal_b.relay_publish_failures >= 1,
        "an absent mandatory route must be retained as relay failure evidence"
    );
    assert!(terminal_a.relay_delivered + terminal_b.relay_delivered >= 1);

    // Recreating only the application-owned route and restarting Consumer
    // processes must publish the retained row with the exact same event ID.
    declare_output_topology(&rabbit).await;
    let mut restart_a = ChildConsumer::spawn("restart-a", &config_path, evidence.path());
    let mut restart_b = ChildConsumer::spawn("restart-b", &config_path, evidence.path());
    restart_a.wait_ready().await;
    restart_b.wait_ready().await;
    let restarted_output = wait_for_output(&rabbit).await;
    assert_eq!(restarted_output.data, br#"{"kind":"order-applied"}"#);
    assert_eq!(
        restarted_output
            .properties
            .message_id()
            .as_ref()
            .map(|value| value.as_str()),
        Some(restart_outgoing.to_string().as_str())
    );
    let restarted_headers = restarted_output
        .properties
        .headers()
        .as_ref()
        .expect("restarted outbox headers");
    assert_eq!(
        long_string(restarted_headers, "x-lily-event-id"),
        restart_outgoing.to_string()
    );
    assert!(
        restarted_output
            .ack(BasicAckOptions::default())
            .await
            .expect("ACK restarted outbox output")
    );
    wait_for_pg_count(
        &postgres,
        "SELECT COUNT(*)::BIGINT FROM lily_queue.outbox WHERE delivered_at IS NOT NULL",
        5,
        "retained outbox delivery after process restart",
    )
    .await;
    let (restart_terminal_a, restart_terminal_b) = tokio::join!(restart_a.stop(), restart_b.stop());
    assert_eq!(
        restart_terminal_a.relay_delivered + restart_terminal_b.relay_delivered,
        1
    );
    assert_eq!(
        restart_terminal_a.relay_in_flight + restart_terminal_b.relay_in_flight,
        0
    );

    // Retention is application-scheduled but framework-executed. Age one
    // completed identity and one delivered row beyond the configured policy,
    // then prove one bounded cleanup pass deletes only those terminal records.
    let restart_incoming_text = restart_incoming.to_string();
    let restart_outgoing_text = restart_outgoing.to_string();
    postgres
        .execute(
            "UPDATE lily_queue.inbox SET completed_at = NOW() - INTERVAL '8 days' WHERE event_id = $1::text::uuid",
            &[&restart_incoming_text],
        )
        .await
        .expect("age completed inbox retention evidence");
    postgres
        .execute(
            "UPDATE lily_queue.outbox SET delivered_at = NOW() - INTERVAL '8 days' WHERE event_id = $1::text::uuid",
            &[&restart_outgoing_text],
        )
        .await
        .expect("age delivered outbox retention evidence");
    let cleanup_container = build_container(&config_path).await;
    let cleanup_runtime = prepare_postgresql_transactional_runtime(
        Arc::clone(&cleanup_container),
        &fixture.rabbitmq.topology.queues[0],
    )
    .await
    .expect("prepare retention cleanup runtime");
    let cleanup = cleanup_runtime
        .cleanup()
        .await
        .expect("execute bounded retention cleanup");
    assert_eq!(cleanup.inbox_rows, 1);
    assert_eq!(cleanup.outbox_rows, 1);
    drop(cleanup_runtime);
    cleanup_container
        .close()
        .await
        .expect("close retention cleanup DI container");
    wait_for_pg_count(
        &postgres,
        &format!(
            "SELECT COUNT(*)::BIGINT FROM lily_queue.inbox WHERE event_id = '{restart_incoming}'::uuid"
        ),
        0,
        "expired inbox cleanup",
    )
    .await;
    wait_for_pg_count(
        &postgres,
        &format!(
            "SELECT COUNT(*)::BIGINT FROM lily_queue.outbox WHERE event_id = '{restart_outgoing}'::uuid"
        ),
        0,
        "expired outbox cleanup",
    )
    .await;

    cleanup_rabbit(&rabbit).await;
    rabbit_connection
        .close(200, "CAP-08 qualification cleanup".into())
        .await
        .expect("close RabbitMQ observer");
    postgres
        .batch_execute("DROP SCHEMA lily_queue CASCADE")
        .await
        .expect("clean dedicated CAP-08 schema");
}
