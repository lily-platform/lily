use lapin::{
    options::{
        BasicAckOptions, BasicGetOptions, ExchangeDeclareOptions, ExchangeDeleteOptions,
        QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
    },
    types::FieldTable,
    ExchangeKind,
};
use lily_config::{QueueClientConfig, RabbitMqTlsConfig};
use lily_error::application::{
    message_broker::{RabbitMQError, RabbitMqTlsErrorKind},
    MessageBrokerError,
};
use lily_queue_client::{
    PublishContentKind, PublishEnvelope, QueueClient, RabbitMQClient, RabbitMqOptions,
};
use tokio_util::sync::CancellationToken;

fn live_config(uri: String) -> QueueClientConfig {
    let tls = live_tls_config(&uri);
    QueueClientConfig {
        connection_string: Some(uri.clone()),
        use_tls: Some(uri.starts_with("amqps://")),
        pool_size: Some(2),
        connection_timeout_secs: Some(10),
        confirm_timeout_secs: Some(5),
        heartbeat_secs: Some(30),
        max_reconnect_attempts: Some(2),
        reconnect_backoff_millis: Some(100),
        persistence_enabled: Some(true),
        tls,
        ..QueueClientConfig::default()
    }
}

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

fn assert_reachable_tls_rejection(error: &MessageBrokerError) {
    assert!(matches!(
        error,
        MessageBrokerError::RabbitMQError(RabbitMQError::Lapin(_))
    ));
    let rendered = error.to_string();
    assert!(!rendered.contains("Connection refused"));
    assert!(!rendered.contains("timed out"));
    assert!(!rendered.contains("lily_phase6_ephemeral"));
}

#[tokio::test]
#[ignore = "environment-restricted: requires LILY_TEST_RABBITMQ_URL and a disposable RabbitMQ v1 fixture"]
async fn confirmed_publish_return_headers_and_owned_shutdown() {
    let uri = std::env::var("LILY_TEST_RABBITMQ_URL")
        .expect("LILY_TEST_RABBITMQ_URL must identify a disposable broker");
    let suffix = uuid::Uuid::new_v4();
    let exchange = format!("lily.qualification.{suffix}");
    let queue = format!("lily.qualification.{suffix}");
    let routing_key = "accepted";
    let options = RabbitMqOptions::from_client(&live_config(uri)).unwrap();
    let cancellation = CancellationToken::new();

    let fixture_connection = options.connect(&cancellation).await.unwrap();
    let fixture_channel = fixture_connection.create_channel().await.unwrap();
    fixture_channel
        .exchange_declare(
            exchange.clone().into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    fixture_channel
        .queue_declare(
            queue.clone().into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    fixture_channel
        .queue_bind(
            queue.clone().into(),
            exchange.clone().into(),
            routing_key.into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let client = RabbitMQClient::new(options);
    client.start(cancellation.clone()).await.unwrap();
    assert!(client.is_connected());
    let event_id = uuid::Uuid::new_v4();
    client
        .publish_enveloped(
            &exchange,
            routing_key,
            br#"{"event":"accepted"}"#.to_vec(),
            PublishEnvelope::new(event_id, PublishContentKind::Json),
        )
        .await
        .unwrap();

    let delivery = fixture_channel
        .basic_get(queue.clone().into(), BasicGetOptions::default())
        .await
        .unwrap()
        .expect("confirmed publish must be present in the bound queue");
    let headers = delivery.properties.headers().as_ref().unwrap();
    for key in [
        "x-lily-event-id",
        "x-lily-schema-version",
        "x-lily-content-kind",
    ] {
        assert!(headers.inner().contains_key(key));
    }
    assert!(matches!(
        headers.inner().get("x-lily-event-id"),
        Some(lapin::types::AMQPValue::LongString(value))
            if std::str::from_utf8(value.as_bytes()).unwrap() == event_id.to_string()
    ));
    delivery.ack(BasicAckOptions::default()).await.unwrap();

    let error = client
        .publish(&exchange, "unroutable", b"{}".to_vec())
        .await
        .expect_err("mandatory publish without a binding must be returned");
    assert!(matches!(
        error,
        MessageBrokerError::RabbitMQError(RabbitMQError::Unroutable(_))
    ));
    let terminal = client.publish_terminal_snapshot();
    assert!(terminal.is_reconciled());
    assert_eq!(terminal.attempts, 2);
    assert_eq!(terminal.broker_ack_no_return, 1);
    assert_eq!(terminal.nack_or_return, 1);
    assert_eq!(terminal.in_flight, 0);

    client.stop().await.unwrap();
    assert!(!client.is_connected());
    fixture_channel
        .queue_delete(queue.into(), QueueDeleteOptions::default())
        .await
        .unwrap();
    fixture_channel
        .exchange_delete(exchange.into(), ExchangeDeleteOptions::default())
        .await
        .unwrap();
    fixture_connection
        .close(200, "qualification cleanup".into())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "environment-restricted: requires LILY_TEST_RABBITMQ_URL and a disposable RabbitMQ v1 fixture"]
async fn publisher_start_does_not_create_broker_topology() {
    let uri = std::env::var("LILY_TEST_RABBITMQ_URL")
        .expect("LILY_TEST_RABBITMQ_URL must identify a disposable broker");
    let missing_exchange = format!("lily.startup.must-not-create.{}", uuid::Uuid::new_v4());
    let options = RabbitMqOptions::from_client(&live_config(uri)).unwrap();
    let cancellation = CancellationToken::new();

    let inspector_connection = options.connect(&cancellation).await.unwrap();
    let client = RabbitMQClient::new(options);
    client.start(cancellation).await.unwrap();

    let inspector_channel = inspector_connection.create_channel().await.unwrap();
    inspector_channel
        .exchange_declare(
            missing_exchange.into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect_err("publisher startup must leave an unknown exchange absent");

    client.stop().await.unwrap();
    inspector_connection
        .close(200, "qualification cleanup".into())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "environment-restricted: requires LILY_TEST_RABBITMQ_URL and a disposable RabbitMQ v1 fixture"]
async fn closed_pool_connection_is_recovered_and_shutdown_is_reconciled() {
    use lily_queue_client::{ConnectionManager, RabbitMQConnectionManager};

    let uri = std::env::var("LILY_TEST_RABBITMQ_URL")
        .expect("LILY_TEST_RABBITMQ_URL must identify a disposable broker");
    let mut config = live_config(uri);
    config.pool_size = Some(1);
    let options = RabbitMqOptions::from_client(&config).unwrap();
    let manager = RabbitMQConnectionManager::new(options);
    let cancellation = CancellationToken::new();
    manager.start(cancellation.clone()).await.unwrap();

    let first = manager.get_connection(cancellation.clone()).await.unwrap();
    first
        .close(200, "qualification connection loss".into())
        .await
        .unwrap();
    let recovered = manager.get_connection(cancellation).await.unwrap();

    assert!(recovered.status().connected());
    assert!(!std::sync::Arc::ptr_eq(&first, &recovered));
    manager.close().await.unwrap();
    assert!(!manager.is_connected());
}

#[tokio::test]
#[ignore = "environment-restricted: requires the RabbitMQ mTLS qualification environment"]
async fn mtls_rejects_wrong_ca_hostname_missing_identity_and_mismatched_key() {
    let uri = std::env::var("LILY_TEST_RABBITMQ_URL")
        .expect("LILY_TEST_RABBITMQ_URL must identify a disposable mTLS broker");
    assert!(uri.starts_with("amqps://"));
    let valid_tls = live_tls_config(&uri);
    let wrong_ca = std::env::var_os("LILY_TEST_RABBITMQ_WRONG_CA_BUNDLE")
        .map(Into::into)
        .expect("LILY_TEST_RABBITMQ_WRONG_CA_BUNDLE is required");
    let wrong_key = std::env::var_os("LILY_TEST_RABBITMQ_WRONG_CLIENT_PRIVATE_KEY")
        .map(Into::into)
        .expect("LILY_TEST_RABBITMQ_WRONG_CLIENT_PRIVATE_KEY is required");
    let cancellation = CancellationToken::new();

    let valid_options = RabbitMqOptions::from_client(&live_config(uri.clone()))
        .expect("valid RabbitMQ mTLS options");
    let valid_connection = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        valid_options.connect(&cancellation),
    )
    .await
    .expect("valid RabbitMQ mTLS connection must complete within its deadline")
    .expect("the qualification fixture must accept the valid mTLS identity");
    assert!(valid_connection.status().connected());
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        valid_connection.close(200, "mTLS positive-control cleanup".into()),
    )
    .await
    .expect("valid RabbitMQ mTLS close must complete within its deadline")
    .expect("valid RabbitMQ mTLS connection must close normally");
    assert!(!valid_connection.status().connected());

    let mut wrong_ca_config = live_config(uri.clone());
    wrong_ca_config.tls.additional_ca_bundle = Some(wrong_ca);
    let error = RabbitMqOptions::from_client(&wrong_ca_config)
        .unwrap()
        .connect(&cancellation)
        .await
        .expect_err("an untrusted RabbitMQ server certificate must be rejected");
    assert_reachable_tls_rejection(&error);

    let mut wrong_hostname_uri = url::Url::parse(&uri).unwrap();
    wrong_hostname_uri.set_host(Some("127.0.0.1")).unwrap();
    let mut wrong_hostname_config = live_config(wrong_hostname_uri.into());
    wrong_hostname_config.tls = valid_tls.clone();
    let error = RabbitMqOptions::from_client(&wrong_hostname_config)
        .unwrap()
        .connect(&cancellation)
        .await
        .expect_err("a RabbitMQ TLS hostname mismatch must be rejected");
    assert_reachable_tls_rejection(&error);

    let mut missing_identity_config = live_config(uri.clone());
    missing_identity_config.tls = RabbitMqTlsConfig {
        additional_ca_bundle: valid_tls.additional_ca_bundle.clone(),
        ..RabbitMqTlsConfig::default()
    };
    let error = RabbitMqOptions::from_client(&missing_identity_config)
        .unwrap()
        .connect(&cancellation)
        .await
        .expect_err("a broker requiring a client certificate must reject anonymous TLS");
    assert_reachable_tls_rejection(&error);

    let mut mismatched_key_config = live_config(uri);
    mismatched_key_config.tls.client_private_key = Some(wrong_key);
    let error = RabbitMqOptions::from_client(&mismatched_key_config)
        .unwrap()
        .connect(&cancellation)
        .await
        .expect_err("a mismatched client key must fail before broker authentication");
    assert!(matches!(
        error,
        MessageBrokerError::RabbitMQError(RabbitMQError::Tls(
            RabbitMqTlsErrorKind::ClientIdentityRejected
        ))
    ));
}
