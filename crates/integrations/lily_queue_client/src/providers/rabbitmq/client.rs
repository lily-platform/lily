// =============================================================================
// RabbitMQ Client - High-Level Client Facade
// =============================================================================

use async_trait::async_trait;
use lapin::{
    types::{AMQPValue, FieldTable},
    BasicProperties, Channel, Connection,
};
use lily_error::application::{message_broker::RabbitMQError, MessageBrokerError};
use lily_trace::tracing::{self, Instrument};
use opentelemetry::propagation::Injector;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    ChannelManager, ConnectionManager, PublishEnvelope, PublishTerminalLedger,
    PublishTerminalSnapshot, PublisherEngine, QueueClient, RabbitMqOptions,
};

use super::{RabbitMQChannelManager, RabbitMQConnectionManager, RabbitMQPublisherEngine};

/// RabbitMQ client facade
///
/// Provides high-level API for message publishing with automatic
/// connection and channel management.
pub struct RabbitMQClient {
    connection_manager: Arc<RabbitMQConnectionManager>,
    channel_manager: Arc<RabbitMQChannelManager>,
    publisher_engine: Arc<RabbitMQPublisherEngine>,
    cancellation_token: Mutex<Option<CancellationToken>>,
    persistence_enabled: bool,
    terminal_ledger: Arc<PublishTerminalLedger>,
}

struct AmqpHeaderInjector<'a>(&'a mut FieldTable);

fn add_lily_envelope_headers(headers: &mut FieldTable, envelope: &PublishEnvelope) {
    headers.insert(
        "x-lily-event-id".into(),
        AMQPValue::LongString(envelope.event_id().to_string().into()),
    );
    headers.insert(
        "x-lily-schema-version".into(),
        AMQPValue::LongString(envelope.schema_version().get().to_string().into()),
    );
    headers.insert(
        "x-lily-content-kind".into(),
        AMQPValue::LongString(envelope.content_kind().header_value().into()),
    );
}

fn publish_properties(
    headers: FieldTable,
    envelope: &PublishEnvelope,
    persistence_enabled: bool,
) -> BasicProperties {
    let properties = BasicProperties::default()
        .with_content_type(envelope.content_kind().content_type().into())
        .with_message_id(envelope.event_id().to_string().into())
        .with_headers(headers);
    if persistence_enabled {
        properties.with_delivery_mode(2)
    } else {
        properties.with_delivery_mode(1)
    }
}

impl Injector for AmqpHeaderInjector<'_> {
    #[inline]
    fn set(&mut self, key: &str, value: String) {
        self.0
            .insert(key.into(), AMQPValue::LongString(value.into()));
    }
}

impl RabbitMQClient {
    /// Create a new RabbitMQ client
    pub fn new(options: RabbitMqOptions) -> Self {
        let confirm_timeout = options.confirm_timeout();
        let persistence_enabled = options.persistence_enabled();
        let connection_manager = Arc::new(RabbitMQConnectionManager::new(options));

        let channel_manager = Arc::new(RabbitMQChannelManager::new(
            connection_manager.clone() as Arc<dyn ConnectionManager<Connection>>
        ));

        let terminal_ledger = Arc::new(PublishTerminalLedger::default());
        let publisher_engine = Arc::new(RabbitMQPublisherEngine::with_terminal_ledger(
            channel_manager.clone() as Arc<dyn ChannelManager<Channel>>,
            confirm_timeout,
            Arc::clone(&terminal_ledger),
        ));

        Self {
            connection_manager,
            channel_manager,
            publisher_engine,
            cancellation_token: Mutex::new(None),
            persistence_enabled,
            terminal_ledger,
        }
    }

    /// Sampling-independent reconciliation data for qualification assertions.
    pub(crate) fn publish_terminal_snapshot(&self) -> PublishTerminalSnapshot {
        self.terminal_ledger.snapshot()
    }

    async fn publish_inner(
        &self,
        exchange: &str,
        routing_key: &str,
        body: Vec<u8>,
        envelope: PublishEnvelope,
    ) -> Result<(), MessageBrokerError> {
        envelope.validate().map_err(|error| {
            MessageBrokerError::RabbitMQError(RabbitMQError::InvalidMessage(format!(
                "{}: {error}",
                error.code()
            )))
        })?;
        let ct = {
            let token = self.cancellation_token.lock().await;
            token.clone().ok_or(MessageBrokerError::RabbitMQError(
                RabbitMQError::NotInitialized,
            ))?
        };

        let span = tracing::info_span!(
            "messaging.publish",
            otel.kind = "producer",
            messaging.system = "rabbitmq",
            messaging.destination.name = %exchange,
            messaging.operation.type = "publish",
            messaging.message.body.size = body.len(),
            lily.event_id = %envelope.event_id(),
            lily.delivery_attempt = 0_u64,
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            lily.handoff_outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let mut headers = FieldTable::default();
        add_lily_envelope_headers(&mut headers, &envelope);
        let context = lily_trace::context_for_span(&span);
        lily_trace::inject_context(&context, &mut AmqpHeaderInjector(&mut headers));

        let properties = publish_properties(headers, &envelope, self.persistence_enabled);

        let result = self
            .publisher_engine
            .publish(exchange, routing_key, body, properties, ct)
            .instrument(span.clone())
            .await;
        match &result {
            Ok(_) => {
                span.record("lily.outcome", "success");
                span.record("lily.handoff_outcome", "broker_ack_no_return");
            }
            Err(error) => {
                let outcome = match error {
                    MessageBrokerError::RabbitMQError(RabbitMQError::PublisherNack) => "nack",
                    MessageBrokerError::RabbitMQError(RabbitMQError::Unroutable(_)) => "return",
                    MessageBrokerError::RabbitMQError(RabbitMQError::Timeout(_)) => "timeout",
                    MessageBrokerError::RabbitMQError(RabbitMQError::Cancelled) => "cancelled",
                    _ => "error",
                };
                span.record("lily.outcome", outcome);
                span.record("lily.error_code", error.error_code());
                span.record("otel.status_code", "ERROR");
            }
        }
        result.map(|_| ())
    }
}

#[async_trait]
impl QueueClient for RabbitMQClient {
    async fn start(&self, ct: CancellationToken) -> Result<(), MessageBrokerError> {
        self.connection_manager.start(ct.clone()).await?;
        if let Err(error) = self.channel_manager.get_channel(ct.clone(), true).await {
            if let Err(cleanup_error) = self.connection_manager.close().await {
                tracing::warn!(%cleanup_error, "RabbitMQ readiness rollback failed");
            }
            return Err(error);
        }
        let mut token = self.cancellation_token.lock().await;
        if let Some(previous) = token.replace(ct) {
            previous.cancel();
        }
        Ok(())
    }

    async fn stop(&self) -> Result<(), MessageBrokerError> {
        let mut token = self.cancellation_token.lock().await;
        if let Some(ct) = token.take() {
            ct.cancel();
        }
        drop(token);
        // Let Lapin close the connection (and its channels) before dropping
        // cached channel handles. Dropping the last cached channel first can
        // start an asynchronous channel close and race Connection::close,
        // producing `invalid channel state: Closing` during otherwise
        // graceful application shutdown.
        let result = self.connection_manager.close().await;
        self.channel_manager.clear();
        result
    }

    async fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        message: Vec<u8>,
    ) -> Result<(), MessageBrokerError> {
        self.publish_inner(
            exchange,
            routing_key,
            message,
            PublishEnvelope::fresh_json(),
        )
        .await
    }

    async fn publish_raw(
        &self,
        exchange: &str,
        routing_key: &str,
        body: Vec<u8>,
    ) -> Result<(), MessageBrokerError> {
        self.publish_inner(exchange, routing_key, body, PublishEnvelope::fresh_binary())
            .await
    }

    async fn publish_enveloped(
        &self,
        exchange: &str,
        routing_key: &str,
        body: Vec<u8>,
        envelope: PublishEnvelope,
    ) -> Result<(), MessageBrokerError> {
        self.publish_inner(exchange, routing_key, body, envelope)
            .await
    }

    fn is_connected(&self) -> bool {
        self.connection_manager.is_connected()
    }

    fn publish_terminal_snapshot(&self) -> PublishTerminalSnapshot {
        RabbitMQClient::publish_terminal_snapshot(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CustomPublishContent, PublishContentKind, PublishMetadata, PublishSchemaVersion};

    fn long_string<'a>(headers: &'a FieldTable, name: &str) -> &'a str {
        match headers.inner().get(name) {
            Some(AMQPValue::LongString(value)) => {
                std::str::from_utf8(value.as_bytes()).expect("UTF-8 header")
            }
            value => panic!("expected long-string header {name}, got {value:?}"),
        }
    }

    #[test]
    fn injects_w3c_context_without_overwriting_other_headers() {
        lily_trace::install_w3c_propagator();
        let mut incoming = std::collections::HashMap::new();
        incoming.insert(
            "traceparent".to_string(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
        );
        incoming.insert("tracestate".to_string(), "vendor=opaque".to_string());
        let context = lily_trace::extract_context(&incoming);

        let mut headers = FieldTable::default();
        headers.insert("application-header".into(), AMQPValue::LongInt(42));
        lily_trace::inject_context(&context, &mut AmqpHeaderInjector(&mut headers));

        assert!(headers.inner().contains_key("application-header"));
        assert!(matches!(
            headers.inner().get("traceparent"),
            Some(AMQPValue::LongString(value))
                if std::str::from_utf8(value.as_bytes()).unwrap()
                    == "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        ));
        assert!(matches!(
            headers.inner().get("tracestate"),
            Some(AMQPValue::LongString(value))
                if std::str::from_utf8(value.as_bytes()).unwrap() == "vendor=opaque"
        ));
    }

    #[test]
    fn lily_envelope_assigns_event_schema_and_content_metadata() {
        let mut headers = FieldTable::default();
        add_lily_envelope_headers(&mut headers, &PublishEnvelope::fresh_json());
        for key in [
            "x-lily-event-id",
            "x-lily-schema-version",
            "x-lily-content-kind",
        ] {
            assert!(headers.inner().contains_key(key));
        }
    }

    #[test]
    fn versioned_envelope_headers_and_amqp_properties_are_consistent() {
        let event_id = uuid::Uuid::new_v4();
        let version = PublishSchemaVersion::try_new(3).unwrap();
        let metadata = PublishMetadata::try_new(event_id, version).unwrap();
        let envelope = PublishEnvelope::with_metadata(metadata, PublishContentKind::Text);
        let mut headers = FieldTable::default();
        add_lily_envelope_headers(&mut headers, &envelope);
        let properties = publish_properties(headers, &envelope, true);

        let headers = properties.headers().as_ref().unwrap();
        assert_eq!(
            long_string(headers, "x-lily-event-id"),
            event_id.to_string()
        );
        assert_eq!(long_string(headers, "x-lily-schema-version"), "3");
        assert_eq!(long_string(headers, "x-lily-content-kind"), "text");
        assert_eq!(
            properties.content_type().as_ref().map(ToString::to_string),
            Some("text/plain; charset=utf-8".to_string())
        );
        assert_eq!(
            properties.message_id().as_ref().map(ToString::to_string),
            Some(event_id.to_string())
        );
        assert_eq!(properties.delivery_mode().as_ref().copied(), Some(2));
    }

    #[test]
    fn custom_token_and_mime_are_materialized_as_one_envelope_contract() {
        let custom = CustomPublishContent::try_new("protobuf", "application/protobuf").unwrap();
        let envelope = PublishEnvelope::with_metadata(
            PublishMetadata::fresh(PublishSchemaVersion::V1),
            PublishContentKind::Custom(custom),
        );
        let mut headers = FieldTable::default();
        add_lily_envelope_headers(&mut headers, &envelope);
        let properties = publish_properties(headers, &envelope, false);

        assert_eq!(
            long_string(
                properties.headers().as_ref().unwrap(),
                "x-lily-content-kind"
            ),
            "protobuf"
        );
        assert_eq!(
            properties.content_type().as_ref().map(ToString::to_string),
            Some("application/protobuf".to_string())
        );
        assert_eq!(properties.delivery_mode().as_ref().copied(), Some(1));
    }

    #[test]
    fn publish_span_does_not_declare_payload_credentials_or_event_id_metric_labels() {
        let source = include_str!("client.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production source");
        for forbidden in [
            "otel.status_message =",
            "messaging.message.payload =",
            "authorization =",
            "password =",
            "\"lily.event_id\" =>",
            "\"messaging.message.id\" =>",
        ] {
            assert!(
                !source.contains(forbidden),
                "forbidden telemetry declaration: {forbidden}"
            );
        }
        assert!(source.contains("\"messaging.publish\""));
    }

    #[test]
    fn publisher_start_has_no_topology_authority() {
        let source = include_str!("client.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production client source");
        let start = source
            .split("async fn start(")
            .nth(1)
            .and_then(|remaining| remaining.split("async fn stop(").next())
            .expect("QueueClient::start implementation");

        for forbidden in ["exchange_declare", "queue_declare", "queue_bind"] {
            assert!(
                !start.contains(forbidden),
                "publisher startup must not gain topology authority: {forbidden}"
            );
        }
    }
}
