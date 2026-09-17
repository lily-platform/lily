use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::{
    DeliveryTerminalOutcome,
    providers::rabbitmq::{
        RabbitMQConsumer, channel_manager::RabbitMQChannelManager,
        connection_manager::RabbitMQConnectionManager, queue_engine::RabbitMQQueueEngine,
        retry_engine::RabbitMQRetryEngine,
    },
    queue_engine_trait::{QueueEngine, QueueExecutionError},
    queue_service::{DeliveryScopeTracker, RegisteredQueueHandler},
    queue_trait::Queue,
    setting::MessageBrokerSetting,
};
use lapin::{
    BasicProperties,
    options::{
        BasicAckOptions, BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions,
        ExchangeDeleteOptions, QueueDeclareOptions, QueueDeleteOptions,
    },
    types::AMQPValue,
};
use lily_config::{
    QueueClientConfig, QueueDefinition, QueueRetentionConfig, RabbitMqConsumerConfig,
    RabbitMqTlsConfig,
};
use lily_error::application::QueueHandlerError;
use lily_queue_client::{
    PublishContentKind, PublishEnvelope, QueueClient, RabbitMQClient, RabbitMqOptions,
    await_publisher_confirm,
};
use tokio_util::sync::CancellationToken;

fn live_tls_config(uri: &str) -> RabbitMqTlsConfig {
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

#[tokio::test]
#[ignore = "environment-restricted: requires LILY_TEST_RABBITMQ_URL and a disposable RabbitMQ v2 fixture"]
async fn retry_handoff_malformed_envelope_recovery_and_shutdown_are_reconciled() {
    let uri = std::env::var("LILY_TEST_RABBITMQ_URL")
        .expect("LILY_TEST_RABBITMQ_URL must identify a disposable broker");
    let tls = live_tls_config(&uri);
    let suffix = uuid::Uuid::new_v4();
    let worker = format!("lily.worker.{suffix}");
    let queue_name = format!("lily.queue.{suffix}");
    let definition = QueueDefinition {
        name: queue_name.clone(),
        exchange_name: worker.clone(),
        routing_key: queue_name.clone(),
        concurrency: 2,
        prefetch_count: 4,
        retry_attempts: 2,
        retry_backoff_millis: Some(100),
        max_retry_backoff_millis: Some(500),
        retention: Some(QueueRetentionConfig {
            main_max_messages: 10_000,
            main_max_bytes: 256 * 1024 * 1024,
            retry_bucket_max_messages: 1_000,
            retry_bucket_max_bytes: 64 * 1024 * 1024,
            dead_letter_max_messages: 1_000,
            dead_letter_max_bytes: 64 * 1024 * 1024,
        }),
        durable: true,
        ..QueueDefinition::default()
    };
    let broker_config = RabbitMqConsumerConfig {
        connection_string: Some(uri.clone()),
        use_tls: Some(uri.starts_with("amqps://")),
        pool_size: 2,
        connection_timeout_secs: Some(10),
        confirm_timeout_secs: Some(5),
        heartbeat_secs: Some(30),
        max_reconnect_attempts: Some(2),
        reconnect_backoff_millis: Some(100),
        tls: tls.clone(),
        ..RabbitMqConsumerConfig::default()
    };
    let options = RabbitMqOptions::from_consumer(&broker_config).unwrap();
    let settings = MessageBrokerSetting::from_definitions(
        options.confirm_timeout(),
        std::slice::from_ref(&definition),
    )
    .unwrap();
    let topology = settings.topology(&queue_name).unwrap().clone();

    let connection_manager = Arc::new(RabbitMQConnectionManager::new(options.clone()));
    let channel_manager = Arc::new(RabbitMQChannelManager::new(connection_manager.clone()));
    let retry_engine = Arc::new(RabbitMQRetryEngine::new(
        channel_manager.clone(),
        settings.clone(),
    ));
    let engine = Arc::new(RabbitMQQueueEngine::new(
        settings,
        channel_manager,
        retry_engine,
    ));
    let engine_ledger = Arc::clone(&engine);
    let consumer = RabbitMQConsumer::new(engine, connection_manager);
    let consumer_cancellation = CancellationToken::new();
    consumer
        .start_async(consumer_cancellation.clone())
        .await
        .unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let handler_attempts = attempts.clone();
    let handler = Arc::new(move |_input: crate::delivery_context::DeliveryInput| {
        let attempt = handler_attempts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if attempt == 0 {
                Err(QueueHandlerError::retryable("LIVE_TEST_RETRY_ONCE").into())
            } else {
                Ok(())
            }
        }) as Pin<Box<dyn Future<Output = Result<(), QueueExecutionError>> + Send>>
    });
    consumer
        .create_queue(
            &worker,
            &queue_name,
            RegisteredQueueHandler {
                callback: handler,
                scope_tracker: Arc::new(DeliveryScopeTracker::default()),
            },
        )
        .await
        .unwrap();

    let client_options = RabbitMqOptions::from_client(&QueueClientConfig {
        connection_string: Some(uri),
        use_tls: Some(options.connection_uses_tls()),
        pool_size: Some(1),
        connection_timeout_secs: Some(10),
        confirm_timeout_secs: Some(5),
        heartbeat_secs: Some(30),
        max_reconnect_attempts: Some(2),
        reconnect_backoff_millis: Some(100),
        persistence_enabled: Some(true),
        tls,
        ..QueueClientConfig::default()
    })
    .unwrap();
    let fixture_connection = client_options
        .connect(&CancellationToken::new())
        .await
        .unwrap();
    let fixture_channel = fixture_connection.create_channel().await.unwrap();
    fixture_channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let queue = fixture_channel
                .queue_declare(
                    queue_name.clone().into(),
                    QueueDeclareOptions {
                        passive: true,
                        ..QueueDeclareOptions::default()
                    },
                    Default::default(),
                )
                .await
                .unwrap();
            if queue.consumer_count() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("exactly one dedicated broker consumer was not admitted");
    let client = RabbitMQClient::new(client_options);
    client.start(CancellationToken::new()).await.unwrap();

    // Bypass Lily's canonical publisher deliberately: `publish_raw` still
    // creates a valid binary envelope and therefore cannot qualify the poison
    // path. This frame has no Lily envelope headers at all.
    let malformed_publish_cancellation = CancellationToken::new();
    let malformed_confirm = fixture_channel
        .basic_publish(
            worker.clone().into(),
            queue_name.clone().into(),
            BasicPublishOptions {
                mandatory: true,
                ..BasicPublishOptions::default()
            },
            b"malformed-without-lily-envelope",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    await_publisher_confirm(
        malformed_confirm,
        Duration::from_secs(5),
        &malformed_publish_cancellation,
    )
    .await
    .unwrap();

    let dead_letter = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(delivery) = fixture_channel
                .basic_get(
                    topology.dead_letter_queue.clone().into(),
                    BasicGetOptions::default(),
                )
                .await
                .unwrap()
            {
                break delivery;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("permanent invalid envelope was not moved to DLQ");
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        0,
        "a malformed transport envelope must not invoke the application handler"
    );
    let dead_letter_headers = dead_letter
        .properties
        .headers()
        .as_ref()
        .expect("permanent handoff must attach bounded failure metadata");
    assert!(matches!(
        dead_letter_headers.inner().get("x-retry-count"),
        Some(AMQPValue::LongInt(0))
    ));
    assert!(matches!(
        dead_letter_headers
            .inner()
            .get("x-lily-failure-delivery-attempt"),
        Some(AMQPValue::LongInt(1))
    ));
    assert!(matches!(
        dead_letter_headers.inner().get("x-lily-rejection-code"),
        Some(AMQPValue::LongString(value))
            if value.as_bytes() == b"invalid_transport_envelope"
    ));
    assert!(
        !dead_letter_headers.inner().contains_key("x-lily-event-id"),
        "a missing remote event identity must not be fabricated during poison handoff"
    );
    assert!(
        dead_letter.ack(BasicAckOptions::default()).await.unwrap(),
        "qualification fixture must acknowledge the inspected DLQ delivery exactly once"
    );

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = engine_ledger.delivery_terminal_snapshot();
            if snapshot.deliveries == 1
                && snapshot.acked_confirmed_handoff == 1
                && snapshot.dead_letter_confirmed == 1
                && snapshot.is_reconciled()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("malformed envelope settlement did not reconcile");
    let poison_snapshot = engine_ledger.delivery_terminal_snapshot();
    assert!(poison_snapshot.is_ready());
    assert_eq!(poison_snapshot.acked_handler_success, 0);
    assert_eq!(poison_snapshot.retry_confirmed, 0);
    assert_eq!(poison_snapshot.unacked_or_in_flight(), 0);

    // Prove that the same receiver remains useful after poison settlement. The
    // first handler attempt is retryable and the redelivery then succeeds.
    let healthy_event_id = uuid::Uuid::new_v4();
    let healthy_event_id_text = healthy_event_id.hyphenated().to_string();
    client
        .publish_enveloped(
            &worker,
            &queue_name,
            br#"{"kind":"retry-after-poison"}"#.to_vec(),
            PublishEnvelope::new(healthy_event_id, PublishContentKind::Json),
        )
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let snapshot = engine_ledger.delivery_terminal_snapshot();
            if attempts.load(Ordering::SeqCst) == 2
                && snapshot.deliveries == 3
                && snapshot.acked_handler_success == 1
                && snapshot.acked_confirmed_handoff == 2
                && snapshot.retry_confirmed == 1
                && snapshot.dead_letter_confirmed == 1
                && snapshot.is_reconciled()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the receiver did not process a healthy delivery after poison settlement");
    assert!(engine_ledger.delivery_terminal_snapshot().is_ready());

    assert!(
        fixture_channel
            .basic_get(
                topology.dead_letter_queue.clone().into(),
                BasicGetOptions::default(),
            )
            .await
            .unwrap()
            .is_none(),
        "one malformed delivery must produce exactly one physical DLQ handoff"
    );

    client.stop().await.unwrap();
    consumer.stop_async().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let queue = fixture_channel
                .queue_declare(
                    queue_name.clone().into(),
                    QueueDeclareOptions {
                        passive: true,
                        ..QueueDeclareOptions::default()
                    },
                    Default::default(),
                )
                .await
                .unwrap();
            if queue.consumer_count() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("dedicated consumer channel shutdown left an orphan broker consumer");
    let terminal = engine_ledger.delivery_terminal_snapshot();
    assert!(terminal.is_reconciled());
    assert_eq!(terminal.deliveries, 3);
    assert_eq!(terminal.acked_handler_success, 1);
    assert_eq!(terminal.acked_confirmed_handoff, 2);
    assert_eq!(terminal.retry_confirmed, 1);
    assert_eq!(terminal.dead_letter_confirmed, 1);
    assert_eq!(terminal.unacked_or_in_flight(), 0);
    let observations = engine_ledger.delivery_terminal_observations();
    assert_eq!(observations.dropped, 0);
    assert_eq!(observations.observations.len(), 3);
    let retry = observations
        .observations
        .iter()
        .find(|observation| {
            observation.event_id.as_deref() == Some(healthy_event_id_text.as_str())
                && observation.outcome == DeliveryTerminalOutcome::RetryConfirmed
        })
        .expect("retry settlement must retain event identity");
    let success = observations
        .observations
        .iter()
        .find(|observation| {
            observation.event_id.as_deref() == Some(healthy_event_id_text.as_str())
                && observation.outcome == DeliveryTerminalOutcome::HandlerSuccess
        })
        .expect("redelivered success must retain event identity");
    let dead_letter = observations
        .observations
        .iter()
        .find(|observation| {
            observation.event_id.is_none()
                && observation.outcome == DeliveryTerminalOutcome::DeadLetterConfirmed
        })
        .expect("malformed envelope must have one DLQ settlement without fabricated identity");
    assert_eq!(retry.event_id, success.event_id);
    assert_ne!(retry.event_id, dead_letter.event_id);
    assert_eq!(retry.delivery_attempt, 1);
    assert_eq!(success.delivery_attempt, 2);
    assert_eq!(dead_letter.delivery_attempt, 1);
    assert!(dead_letter.event_id.is_none());
    let mut queues = vec![topology.main_queue, topology.dead_letter_queue];
    queues.extend(
        topology
            .retry_buckets
            .into_iter()
            .map(|bucket| bucket.queue),
    );
    for queue in queues {
        fixture_channel
            .queue_delete(queue.into(), QueueDeleteOptions::default())
            .await
            .unwrap();
    }
    for exchange in [
        topology.main_exchange,
        topology.retry_exchange,
        topology.dead_letter_exchange,
    ] {
        fixture_channel
            .exchange_delete(exchange.into(), ExchangeDeleteOptions::default())
            .await
            .unwrap();
    }
    fixture_connection
        .close(200, "qualification cleanup".into())
        .await
        .unwrap();
}
