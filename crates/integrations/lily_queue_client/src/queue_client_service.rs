use std::sync::Arc;

#[cfg(feature = "single")]
use async_trait::async_trait;
use lily_error::application::{message_broker::RabbitMQError, MessageBrokerError};
#[cfg(feature = "single")]
use lily_error::injection::InjectionError;
use serde::Serialize;
#[cfg(feature = "single")]
use tokio_util::sync::CancellationToken;

use crate::{
    CustomPublishContent, PublishContentKind, PublishContractError, PublishEnvelope,
    PublishMetadata, PublishSchemaVersion, PublishTerminalSnapshot, QueueClient,
};
#[cfg(feature = "single")]
use crate::{RabbitMQClient, RabbitMqOptions};

// Single mode - with DI support
#[cfg(feature = "single")]
use lily_config::{ConfigService, QueueClientConfig};
#[cfg(feature = "single")]
use lily_injectable_derive::Injectable;
#[cfg(feature = "single")]
use lily_injection::ServiceTrait;

/// Injectable singleton RabbitMQ publisher used by default `single` mode.
///
/// The service is initialized from [`lily_config::QueueClientConfig`] by the
/// application container. Calling publish methods before successful
/// initialization returns the typed `BROKER_NOT_INITIALIZED` error.
#[cfg(feature = "single")]
#[derive(Injectable, Default)]
#[service(lifetime = "Singleton")]
pub struct QueueClientService {
    provider: Option<Arc<dyn QueueClient>>,

    #[inject]
    config_service: Arc<ConfigService>,
}

/// One initialized named RabbitMQ publisher owned by [`crate::QueueClientFactory`].
#[cfg(feature = "factory")]
pub struct QueueClientService {
    provider: Arc<dyn QueueClient>,
}

impl QueueClientService {
    #[cfg(feature = "factory")]
    pub(crate) fn new(provider: Arc<dyn QueueClient>) -> Self {
        Self { provider }
    }

    pub(crate) fn provider(&self) -> Result<Arc<dyn QueueClient>, MessageBrokerError> {
        #[cfg(feature = "single")]
        {
            self.provider
                .clone()
                .ok_or(MessageBrokerError::RabbitMQError(
                    RabbitMQError::NotInitialized,
                ))
        }
        #[cfg(feature = "factory")]
        {
            Ok(Arc::clone(&self.provider))
        }
    }

    /// Sampling-independent terminal accounting for this application-owned
    /// client. The snapshot is deliberately free of payload and identity
    /// fields so it is safe to persist as qualification evidence.
    pub fn publish_terminal_snapshot(&self) -> Result<PublishTerminalSnapshot, MessageBrokerError> {
        Ok(self.provider()?.publish_terminal_snapshot())
    }

    fn invalid_publish_contract(error: PublishContractError) -> MessageBrokerError {
        MessageBrokerError::RabbitMQError(RabbitMQError::InvalidMessage(format!(
            "{}: {error}",
            error.code()
        )))
    }

    fn json_body<T: Serialize>(message: &T) -> Result<Vec<u8>, MessageBrokerError> {
        serde_json::to_vec(message).map_err(|error| {
            MessageBrokerError::RabbitMQError(RabbitMQError::InvalidMessage(error.to_string()))
        })
    }

    async fn publish_bytes_with_metadata(
        &self,
        exchange: &str,
        routing_key: &str,
        body: Vec<u8>,
        metadata: PublishMetadata,
        content_kind: PublishContentKind,
    ) -> Result<(), MessageBrokerError> {
        let envelope = PublishEnvelope::with_metadata(metadata, content_kind);
        self.provider()?
            .publish_enveloped(exchange, routing_key, body, envelope)
            .await
    }

    /// Serializes `message` as JSON, assigns a fresh event ID, and waits for a
    /// successful mandatory RabbitMQ publisher confirmation.
    ///
    /// Use [`Self::publish_with_event_id`] when retrying an existing outbox
    /// record; this method intentionally creates a new identity per call.
    #[lily_trace::lily_trace(
        name = "queue_client.publish",
        skip(self, message),
        env = "development"
    )]
    pub async fn publish<T: Serialize + Send + Sync>(
        &self,
        exchange: &str,
        routing_key: &str,
        message: T,
    ) -> Result<(), MessageBrokerError> {
        lily_trace::prelude::debug!(
            "Publishing message to exchange: {}, routing_key: {}",
            exchange,
            routing_key
        );
        let body = serde_json::to_vec(&message).map_err(|y| {
            lily_trace::prelude::error!("Failed to serialize message: {}", y);
            MessageBrokerError::RabbitMQError(RabbitMQError::General(y.to_string()))
        })?;

        self.provider()?
            .publish(exchange, routing_key, body)
            .await
            .map(|_| {
                lily_trace::prelude::debug!(
                    "Message published successfully to exchange: {}",
                    exchange
                );
            })
    }

    /// Serializes `message` as JSON with an explicit positive schema version
    /// and a fresh event ID.
    ///
    /// The content-kind and AMQP content type are fixed to `json` and
    /// `application/json`; callers cannot override them independently.
    pub async fn publish_json<T: Serialize + Send + Sync>(
        &self,
        exchange: &str,
        routing_key: &str,
        schema_version: PublishSchemaVersion,
        message: T,
    ) -> Result<(), MessageBrokerError> {
        let metadata = PublishMetadata::fresh(schema_version);
        self.publish_json_with_metadata(exchange, routing_key, metadata, message)
            .await
    }

    /// Serializes JSON with caller-owned metadata that an outbox can reuse
    /// unchanged across handoff retries.
    pub async fn publish_json_with_metadata<T: Serialize + Send + Sync>(
        &self,
        exchange: &str,
        routing_key: &str,
        metadata: PublishMetadata,
        message: T,
    ) -> Result<(), MessageBrokerError> {
        let body = Self::json_body(&message)?;
        self.publish_bytes_with_metadata(
            exchange,
            routing_key,
            body,
            metadata,
            PublishContentKind::Json,
        )
        .await
    }

    /// Publishes UTF-8 text with an explicit positive schema version and a
    /// fresh event ID.
    ///
    /// The envelope is fixed to `text` and `text/plain; charset=utf-8`.
    pub async fn publish_text<T: AsRef<str> + Send + Sync>(
        &self,
        exchange: &str,
        routing_key: &str,
        schema_version: PublishSchemaVersion,
        message: T,
    ) -> Result<(), MessageBrokerError> {
        let metadata = PublishMetadata::fresh(schema_version);
        self.publish_text_with_metadata(exchange, routing_key, metadata, message)
            .await
    }

    /// Publishes UTF-8 text with stable caller-owned outbox metadata.
    pub async fn publish_text_with_metadata<T: AsRef<str> + Send + Sync>(
        &self,
        exchange: &str,
        routing_key: &str,
        metadata: PublishMetadata,
        message: T,
    ) -> Result<(), MessageBrokerError> {
        self.publish_bytes_with_metadata(
            exchange,
            routing_key,
            message.as_ref().as_bytes().to_vec(),
            metadata,
            PublishContentKind::Text,
        )
        .await
    }

    /// Publishes opaque bytes with an explicit positive schema version and a
    /// fresh event ID.
    ///
    /// The envelope is fixed to `binary` and `application/octet-stream`.
    pub async fn publish_binary<T: AsRef<[u8]> + Send + Sync>(
        &self,
        exchange: &str,
        routing_key: &str,
        schema_version: PublishSchemaVersion,
        body: T,
    ) -> Result<(), MessageBrokerError> {
        let metadata = PublishMetadata::fresh(schema_version);
        self.publish_binary_with_metadata(exchange, routing_key, metadata, body)
            .await
    }

    /// Publishes opaque bytes with stable caller-owned outbox metadata.
    pub async fn publish_binary_with_metadata<T: AsRef<[u8]> + Send + Sync>(
        &self,
        exchange: &str,
        routing_key: &str,
        metadata: PublishMetadata,
        body: T,
    ) -> Result<(), MessageBrokerError> {
        self.publish_bytes_with_metadata(
            exchange,
            routing_key,
            body.as_ref().to_vec(),
            metadata,
            PublishContentKind::Binary,
        )
        .await
    }

    /// Publishes application-encoded bytes with a validated custom
    /// content-kind/MIME pair and a fresh event ID.
    pub async fn publish_custom<T: AsRef<[u8]> + Send + Sync>(
        &self,
        exchange: &str,
        routing_key: &str,
        schema_version: PublishSchemaVersion,
        content: CustomPublishContent,
        body: T,
    ) -> Result<(), MessageBrokerError> {
        let metadata = PublishMetadata::fresh(schema_version);
        self.publish_custom_with_metadata(exchange, routing_key, metadata, content, body)
            .await
    }

    /// Publishes application-encoded bytes with stable caller-owned outbox
    /// metadata and a validated custom content-kind/MIME pair.
    pub async fn publish_custom_with_metadata<T: AsRef<[u8]> + Send + Sync>(
        &self,
        exchange: &str,
        routing_key: &str,
        metadata: PublishMetadata,
        content: CustomPublishContent,
        body: T,
    ) -> Result<(), MessageBrokerError> {
        self.publish_bytes_with_metadata(
            exchange,
            routing_key,
            body.as_ref().to_vec(),
            metadata,
            PublishContentKind::Custom(content),
        )
        .await
    }

    /// Publishes JSON with a stable caller-owned event ID.
    ///
    /// Transactional outbox relays should persist `event_id` alongside the
    /// payload and reuse it on every retry.
    pub async fn publish_with_event_id<T: Serialize + Send + Sync>(
        &self,
        exchange: &str,
        routing_key: &str,
        event_id: uuid::Uuid,
        message: T,
    ) -> Result<(), MessageBrokerError> {
        let metadata = PublishMetadata::try_new(event_id, PublishSchemaVersion::V1)
            .map_err(Self::invalid_publish_contract)?;
        self.publish_json_with_metadata(exchange, routing_key, metadata, message)
            .await
    }

    /// Publishes bytes with the binary content type and a fresh event ID.
    #[lily_trace::lily_trace(
        name = "queue_client.publish_raw",
        skip(self, message),
        env = "development"
    )]
    pub async fn publish_raw(
        &self,
        exchange: &str,
        routing_key: &str,
        message: Vec<u8>,
    ) -> Result<(), MessageBrokerError> {
        lily_trace::prelude::debug!(
            "Publishing raw message ({} bytes) to exchange: {}, routing_key: {}",
            message.len(),
            exchange,
            routing_key
        );

        self.provider()?
            .publish_raw(exchange, routing_key, message)
            .await
            .map(|_| {
                lily_trace::prelude::debug!(
                    "Raw message published successfully to exchange: {}",
                    exchange
                );
            })
    }
}

// ============================================================================
// Dependency Injection Support (Single mode only)
// ============================================================================

#[cfg(feature = "single")]
#[async_trait]
impl ServiceTrait for QueueClientService {
    /// Initialize the queue client service
    #[lily_trace::lily_trace(name = "queue_client.initialize", skip(self), env = "development")]
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        lily_trace::prelude::info!("Initializing queue client service");

        let lily_config = self.config_service.get_lily_config().await;

        let queue_client_config: QueueClientConfig = lily_config.queue_client.ok_or_else(|| {
            lily_trace::prelude::error!("Queue client configuration not found in lily.toml");
            InjectionError::General("Queue client configuration not found in lily.toml".to_string())
        })?;

        let options = RabbitMqOptions::from_client(&queue_client_config)
            .map_err(|error| InjectionError::InitError(error.to_string()))?;

        // Create and start client
        let client = RabbitMQClient::new(options);
        let ct = CancellationToken::new();

        lily_trace::prelude::info!("Connecting to RabbitMQ...");
        client.start(ct.clone()).await.map_err(|f| {
            lily_trace::prelude::error!("Failed to connect to RabbitMQ: {}", f);
            InjectionError::General(f.to_string())
        })?;
        lily_trace::prelude::info!("Connected to RabbitMQ successfully");

        self.provider = Some(Arc::new(client));
        lily_trace::prelude::info!("Queue client service initialized successfully");

        Ok(())
    }

    /// Dispose of the cache service and clean up resources
    async fn dispose(&self) -> Result<(), InjectionError> {
        if let Some(provider) = self.provider.as_ref() {
            provider
                .stop()
                .await
                .map_err(|f| InjectionError::General(f.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "single"))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RecordingClient {
        json_calls: AtomicUsize,
        raw_calls: AtomicUsize,
        enveloped_calls: std::sync::Mutex<Vec<(Vec<u8>, PublishEnvelope)>>,
    }

    #[async_trait]
    impl QueueClient for RecordingClient {
        async fn start(&self, _: CancellationToken) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn stop(&self) -> Result<(), MessageBrokerError> {
            Ok(())
        }

        async fn publish(&self, _: &str, _: &str, _: Vec<u8>) -> Result<(), MessageBrokerError> {
            self.json_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn publish_raw(
            &self,
            _: &str,
            _: &str,
            _: Vec<u8>,
        ) -> Result<(), MessageBrokerError> {
            self.raw_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn publish_enveloped(
            &self,
            _: &str,
            _: &str,
            body: Vec<u8>,
            envelope: PublishEnvelope,
        ) -> Result<(), MessageBrokerError> {
            self.enveloped_calls
                .lock()
                .expect("recording lock")
                .push((body, envelope));
            Ok(())
        }

        fn is_connected(&self) -> bool {
            true
        }

        fn publish_terminal_snapshot(&self) -> PublishTerminalSnapshot {
            PublishTerminalSnapshot::default()
        }
    }

    #[test]
    fn uninitialized_provider_is_a_typed_error() {
        let service = QueueClientService::default();

        let Err(error) = service.provider() else {
            panic!("provider must be absent");
        };
        assert_eq!(error.error_code(), "BROKER_NOT_INITIALIZED");
        let error = service
            .publish_terminal_snapshot()
            .expect_err("snapshot must fail before initialization");
        assert_eq!(error.error_code(), "BROKER_NOT_INITIALIZED");
    }

    #[tokio::test]
    async fn raw_publish_uses_the_binary_provider_path() {
        let provider = Arc::new(RecordingClient {
            json_calls: AtomicUsize::new(0),
            raw_calls: AtomicUsize::new(0),
            enveloped_calls: std::sync::Mutex::new(Vec::new()),
        });
        let service = QueueClientService {
            provider: Some(provider.clone()),
            ..QueueClientService::default()
        };

        service
            .publish_raw("events", "binary.created", vec![0, 1, 2])
            .await
            .unwrap();

        assert_eq!(provider.raw_calls.load(Ordering::Relaxed), 1);
        assert_eq!(provider.json_calls.load(Ordering::Relaxed), 0);
    }

    fn recording_service() -> (QueueClientService, Arc<RecordingClient>) {
        let provider = Arc::new(RecordingClient {
            json_calls: AtomicUsize::new(0),
            raw_calls: AtomicUsize::new(0),
            enveloped_calls: std::sync::Mutex::new(Vec::new()),
        });
        let service = QueueClientService {
            provider: Some(provider.clone()),
            ..QueueClientService::default()
        };
        (service, provider)
    }

    #[tokio::test]
    async fn typed_publishers_fix_content_kind_and_preserve_explicit_version() {
        let (service, provider) = recording_service();
        let version = PublishSchemaVersion::try_new(3).unwrap();

        service
            .publish_json("events", "json", version, serde_json::json!({"id": 7}))
            .await
            .unwrap();
        service
            .publish_text("events", "text", version, "hello")
            .await
            .unwrap();
        service
            .publish_binary("events", "binary", version, [0_u8, 1, 2])
            .await
            .unwrap();

        let calls = provider.enveloped_calls.lock().unwrap();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].1.schema_version(), version);
        assert_eq!(calls[0].1.content_kind(), &PublishContentKind::Json);
        assert_eq!(calls[1].0, b"hello");
        assert_eq!(calls[1].1.content_kind(), &PublishContentKind::Text);
        assert_eq!(calls[2].0, [0, 1, 2]);
        assert_eq!(calls[2].1.content_kind(), &PublishContentKind::Binary);
    }

    #[tokio::test]
    async fn outbox_metadata_is_replayed_without_identity_or_version_changes() {
        let (service, provider) = recording_service();
        let event_id = uuid::Uuid::new_v4();
        let version = PublishSchemaVersion::try_new(9).unwrap();
        let metadata = PublishMetadata::try_new(event_id, version).unwrap();

        for _ in 0..2 {
            service
                .publish_json_with_metadata(
                    "events",
                    "order.created",
                    metadata,
                    serde_json::json!({"id": 7}),
                )
                .await
                .unwrap();
        }

        let calls = provider.enveloped_calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        for (_, envelope) in calls.iter() {
            assert_eq!(envelope.event_id(), event_id);
            assert_eq!(envelope.schema_version(), version);
            assert_eq!(envelope.content_kind(), &PublishContentKind::Json);
        }
    }

    #[tokio::test]
    async fn custom_publish_keeps_validated_token_and_mime_pair_together() {
        let (service, provider) = recording_service();
        let content = CustomPublishContent::try_new("protobuf", "application/protobuf").unwrap();

        service
            .publish_custom(
                "events",
                "order.created",
                PublishSchemaVersion::V1,
                content.clone(),
                [8_u8, 1, 18, 2],
            )
            .await
            .unwrap();

        let calls = provider.enveloped_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].1.content_kind(),
            &PublishContentKind::Custom(content)
        );
    }

    #[tokio::test]
    async fn compatibility_outbox_method_rejects_nil_identity_before_transport() {
        let (service, provider) = recording_service();

        let error = service
            .publish_with_event_id("events", "order.created", uuid::Uuid::nil(), ())
            .await
            .unwrap_err();

        assert_eq!(error.error_code(), "BROKER_INVALID_MESSAGE");
        assert!(provider.enveloped_calls.lock().unwrap().is_empty());
    }
}
